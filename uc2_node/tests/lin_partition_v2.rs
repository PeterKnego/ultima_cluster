// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! L3 partition scenarios for the v2 SDK (M5 Task 13, spec §8) — the four v1
//! `uc_node/tests/lin_partition.rs` scenarios re-driven through the full v2
//! stack (real nodes + per-node services + cross-process clients), scripting
//! partitions through each node's `partition_handles`. Each scenario ends with
//! the UNCHANGED WGL checker reporting `Linearizable` plus a scenario-specific
//! live safety assert:
//!   1. minority partition + heal — the isolated follower serves NO stale
//!      linearizable `Ok`; the majority keeps committing; heal converges.
//!   2. leader isolation — the majority elects a NEW leader that serves; the OLD
//!      isolated leader must NOT serve a linearizable read (no split-brain).
//!   3. total quorum loss — `ok_count` is FLAT during the loss window (zero false
//!      acks); the cluster resumes after heal.
//!   4. lossy links — 10 % inbound drop on every node: the cluster progresses and
//!      stays linearizable (the NAK/retransmit path under the full SDK).
//!
//! Discipline (v1 parity): each scenario is wrapped in a 3-attempt
//! retry-on-TRANSIENT loop (a boot/convergence hiccup is retried); a SAFETY
//! assert — a stale/split-brain read, a false ack — panics IMMEDIATELY and is
//! NEVER retried. A `Violation` from the checker is a real bug → dump + panic.

#[path = "lincheck_v2/mod.rs"]
mod lincheck_v2;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lincheck_v2::{ClusterCfg, LinClusterV2, join_workers, serialize, spawn_workers};
use uc2_net::fault::FaultConfig;
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{Entry, History, Outcome};
use uc_lincheck::model::Op;

/// Hold a partition long enough to guarantee an election + a settled loss window
/// (election_timeout_max = 300 ms here, so a few hundred ms would do; 3.5 s is
/// comfortably clear of any boot jitter).
const HOLD: Duration = Duration::from_millis(3500);
/// Worker op pacing for the (short) partition runs.
const THROTTLE: Duration = Duration::from_millis(15);
/// Minimum `Ok` ops for a run to be non-vacuous.
const MIN_OK: usize = 30;

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-linpart-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// Shorten the client request timeout so workers pinned to an isolated/quorum-
/// less node cycle (Timeout → indeterminate) in ~1.5 s instead of the 10 s
/// default — keeps the partition runs snappy and workers responsive after heal.
///
/// SAFETY: called at the very top of each serialized scenario (the whole binary
/// runs under [`serialize`]), strictly BEFORE any `Client::connect` /
/// `spawn_workers`. The set therefore happens-before every environment read
/// (each in a later-spawned thread), and no other thread mutates the environment
/// — the sole precondition `set_var` requires.
fn set_fast_client_timeout() {
    unsafe { std::env::set_var("UC2_CLIENT_TIMEOUT_MS", "1500") };
}

/// Dump a history so a `Violation` reproduces offline.
fn dump_history(entries: &[Entry], seed: u64, label: &str) {
    let path = format!("/tmp/lin_partition_v2_{label}_{seed}.txt");
    let mut s = String::new();
    for e in entries {
        s.push_str(&format!("{e:?}\n"));
    }
    let _ = std::fs::write(&path, s);
    eprintln!("history ({} entries) dumped to {path}", entries.len());
}

/// Run the WGL checker. `Violation` is a real linearizability bug → dump + panic
/// NOW (never retried). `Inconclusive` (checker budget on an unlucky interleave)
/// is a TRANSIENT → `Err`, so the scenario is retried. `Linearizable` passes.
fn check_or_transient(entries: &[Entry], seed: u64, label: &str) -> Result<(), String> {
    let ok = History::ok_count(entries);
    eprintln!("[lin_partition_v2::{label}] seed={seed} ops={} ok={ok}", entries.len());
    match check_register(entries) {
        Verdict::Linearizable => Ok(()),
        Verdict::Inconclusive => Err(format!("checker Inconclusive (seed={seed}) — retrying")),
        Verdict::Violation => {
            dump_history(entries, seed, label);
            panic!("[{label}] LINEARIZABILITY VIOLATION (seed={seed}); history dumped");
        }
    }
}

/// Shared per-scenario runtime state.
struct Run {
    _dir: tempfile::TempDir,
    cluster: LinClusterV2,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Run {
    /// Boot a 3-node cluster (optionally with per-node `faults`), wait for a
    /// leader, and start the workers.
    fn start(seed: u64, faults: FaultConfig) -> Run {
        Self::start_cfg(seed, faults, ClusterCfg::default())
    }

    /// As [`start`](Self::start) but with an explicit `ClusterCfg` (M8 Task
    /// 15: the crypto-enabled scenario variants pass `crypto: true`). When
    /// crypto is on, asserts the elected leader genuinely MINTED a group
    /// epoch right after boot — proof the switch actually engaged, not just
    /// that the cluster (harmlessly) still formed with it silently doing
    /// nothing.
    fn start_cfg(seed: u64, faults: FaultConfig, ccfg: ClusterCfg) -> Run {
        set_fast_client_timeout();
        let dir = tempdir();
        let cluster = LinClusterV2::start_cfg(dir.path(), 3, faults, ccfg);
        let leader0 = cluster.await_single_serving(30);
        if ccfg.crypto {
            assert!(
                cluster.crypto_epoch_of(leader0).is_some(),
                "crypto was configured but the elected leader never minted a group epoch — \
                 wire crypto did not actually engage"
            );
        }
        let dirs = Arc::new(cluster.dirs());
        let history = Arc::new(History::default());
        let stop = Arc::new(AtomicBool::new(false));
        let last_seen = Arc::new(AtomicU64::new(0));
        let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, 3);
        Run { _dir: dir, cluster, history, stop, handles }
    }

    fn ok_now(&self) -> usize {
        History::ok_count(&self.history.snapshot())
    }

    /// Stop workers + cluster (node-first-then-service per slot) and return the
    /// recorded history. Re-raises a worker panic (a `Fatal` client error or a
    /// wrong CAS response — genuine bugs).
    fn finish(self) -> Vec<Entry> {
        self.stop.store(true, Ordering::Relaxed);
        join_workers(self.handles);
        self.cluster.stop();
        Arc::try_unwrap(self.history).ok().expect("sole history owner").into_entries()
    }
}

// ============================================================ 1. minority

fn run_minority(seed: u64, ccfg: ClusterCfg) -> Result<(), String> {
    let r = Run::start_cfg(seed, FaultConfig::default(), ccfg);

    std::thread::sleep(Duration::from_millis(1000)); // warm up
    let before = r.ok_now();

    let isolated = r.cluster.partition_minority();
    std::thread::sleep(HOLD);
    let after = r.ok_now();
    // TRANSIENT: the majority must keep committing while a follower is isolated.
    let majority_progressed = after > before;

    // Probe the isolated follower five times: it can never reach a quorum, so a
    // linearizable read there must come back error/Retry (→ Indeterminate),
    // NEVER a stale `Ok`. Record each for the checker AND assert live.
    for _ in 0..5 {
        let inv = r.history.invoke();
        let out = r.cluster.read_from(isolated);
        // SAFETY (never retried): a stale `Ok` from the isolated node is split-brain.
        assert!(
            !matches!(out, Outcome::Ok(_)),
            "isolated follower served a stale linearizable read — split-brain"
        );
        r.history.record(100, Op::Read, inv, out);
        std::thread::sleep(Duration::from_millis(50));
    }

    r.cluster.heal();
    r.cluster.await_reconverged(20);
    std::thread::sleep(Duration::from_millis(1000)); // let survivors catch up + commit

    let entries = r.finish();

    if !majority_progressed {
        return Err(format!("majority did not progress during minority partition ({before} -> {after})"));
    }
    if History::ok_count(&entries) < MIN_OK {
        return Err("too few Ok ops; run is vacuous".into());
    }
    check_or_transient(&entries, seed, "minority")
}

#[test]
fn minority_partition_and_heal() {
    let _g = serialize();
    for attempt in 1..=3 {
        match run_minority(7, ClusterCfg::default()) {
            Ok(()) => return,
            Err(e) => eprintln!("[lin_partition_v2::minority] attempt {attempt}/3 transient: {e}"),
        }
    }
    panic!("minority: failed after 3 transient attempts");
}

/// M8 Task 15: same scenario, wire crypto `Enabled` on every node.
#[test]
fn minority_partition_and_heal_with_crypto() {
    let _g = serialize();
    let ccfg = ClusterCfg { crypto: true, ..ClusterCfg::default() };
    for attempt in 1..=3 {
        match run_minority(7, ccfg) {
            Ok(()) => return,
            Err(e) => eprintln!("[lin_partition_v2::minority-crypto] attempt {attempt}/3 transient: {e}"),
        }
    }
    panic!("minority-with-crypto: failed after 3 transient attempts");
}

// ====================================================== 2. leader isolation

fn run_leader_isolation(seed: u64, ccfg: ClusterCfg) -> Result<(), String> {
    let r = Run::start_cfg(seed, FaultConfig::default(), ccfg);

    std::thread::sleep(Duration::from_millis(1000));
    let old_leader = r.cluster.leader().ok_or("no pre-partition leader")?;

    let isolated = r.cluster.partition_leader();
    assert_eq!(isolated, old_leader, "partition_leader isolated a non-leader");
    std::thread::sleep(HOLD);

    // The majority must elect a NEW leader that can COMMIT. Only the real leader
    // answers a linearizable read with `Ok` (a follower redirects → Indeterminate);
    // probe the two non-isolated nodes until one answers `Ok`. Record every probe.
    let majority: Vec<usize> = (0..3).filter(|&i| i != isolated).collect();
    let mut new_leader: Option<usize> = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && new_leader.is_none() {
        for &n in &majority {
            let inv = r.history.invoke();
            let out = r.cluster.read_from(n);
            let is_ok = matches!(out, Outcome::Ok(_));
            r.history.record(101, Op::Read, inv, out);
            if is_ok {
                new_leader = Some(n);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    // SAFETY (live, never retried): the isolated OLD leader must NOT serve a
    // linearizable read — it cannot pass its read-index barrier without a quorum,
    // so it answers Retry/NotLeader, never `Ok`. Probe while still partitioned.
    assert!(
        !matches!(r.cluster.read_from(isolated), Outcome::Ok(_)),
        "isolated old leader served a linearizable read — split-brain"
    );
    // SAFETY: whatever the majority elected, it is never the old leader.
    if let Some(n) = new_leader {
        assert_ne!(n, old_leader, "majority elected the OLD leader");
    }

    r.cluster.heal();
    r.cluster.await_reconverged(20);
    std::thread::sleep(Duration::from_millis(1000));

    let elected_new = new_leader.is_some();
    let entries = r.finish();

    if !elected_new {
        return Err("no committing new leader found within the probe window".into());
    }
    if History::ok_count(&entries) < MIN_OK {
        return Err("too few Ok ops; run is vacuous".into());
    }
    check_or_transient(&entries, seed, "leader-isolation")
}

#[test]
fn leader_isolation_elects_new_leader() {
    let _g = serialize();
    for attempt in 1..=3 {
        match run_leader_isolation(42, ClusterCfg::default()) {
            Ok(()) => return,
            Err(e) => eprintln!("[lin_partition_v2::leader-isolation] attempt {attempt}/3 transient: {e}"),
        }
    }
    panic!("leader-isolation: failed after 3 transient attempts");
}

/// M8 Task 15: same scenario, wire crypto `Enabled` on every node.
#[test]
fn leader_isolation_elects_new_leader_with_crypto() {
    let _g = serialize();
    let ccfg = ClusterCfg { crypto: true, ..ClusterCfg::default() };
    for attempt in 1..=3 {
        match run_leader_isolation(42, ccfg) {
            Ok(()) => return,
            Err(e) => {
                eprintln!("[lin_partition_v2::leader-isolation-crypto] attempt {attempt}/3 transient: {e}")
            }
        }
    }
    panic!("leader-isolation-with-crypto: failed after 3 transient attempts");
}

// ======================================================== 3. quorum loss

fn run_quorum_loss(seed: u64, ccfg: ClusterCfg) -> Result<(), String> {
    let r = Run::start_cfg(seed, FaultConfig::default(), ccfg);

    std::thread::sleep(Duration::from_millis(1000));

    r.cluster.partition_quorum_loss();
    std::thread::sleep(Duration::from_secs(3)); // settle into total loss
    let lo = r.ok_now();
    std::thread::sleep(HOLD);
    let hi = r.ok_now();
    // SAFETY (live, never retried): no op may commit `Ok` during total quorum
    // loss — an increase is a false ack / split-brain.
    assert_eq!(lo, hi, "ops committed Ok during total quorum loss ({lo} -> {hi}) — false ack");

    r.cluster.heal();
    r.cluster.await_reconverged(20);
    let resumed_from = r.ok_now();
    std::thread::sleep(Duration::from_secs(2));
    let resumed_to = r.ok_now();
    // TRANSIENT: the cluster must resume committing after heal.
    let resumed = resumed_to > resumed_from;

    let entries = r.finish();

    if !resumed {
        return Err(format!("cluster did not resume after heal ({resumed_from} -> {resumed_to})"));
    }
    if History::ok_count(&entries) < MIN_OK {
        return Err("too few Ok ops; run is vacuous".into());
    }
    check_or_transient(&entries, seed, "quorum-loss")
}

#[test]
fn total_quorum_loss_fails_clean_then_recovers() {
    let _g = serialize();
    for attempt in 1..=3 {
        match run_quorum_loss(88_888, ClusterCfg::default()) {
            Ok(()) => return,
            Err(e) => eprintln!("[lin_partition_v2::quorum-loss] attempt {attempt}/3 transient: {e}"),
        }
    }
    panic!("quorum-loss: failed after 3 transient attempts");
}

/// M8 Task 15: same scenario, wire crypto `Enabled` on every node.
#[test]
fn total_quorum_loss_fails_clean_then_recovers_with_crypto() {
    let _g = serialize();
    let ccfg = ClusterCfg { crypto: true, ..ClusterCfg::default() };
    for attempt in 1..=3 {
        match run_quorum_loss(88_888, ccfg) {
            Ok(()) => return,
            Err(e) => eprintln!("[lin_partition_v2::quorum-loss-crypto] attempt {attempt}/3 transient: {e}"),
        }
    }
    panic!("quorum-loss-with-crypto: failed after 3 transient attempts");
}

// ========================================================= 4. lossy links

fn run_lossy_links(seed: u64) -> Result<(), String> {
    // 10 % inbound drop on every ordered link (each node's sockets). Under UDP
    // this forces NAK/retransmit on ~1-in-10 datagrams; the cluster must elect,
    // progress, and stay linearizable through it.
    let faults = FaultConfig { seed, drop_per_million: 100_000, ..FaultConfig::default() };
    let r = Run::start(seed, faults);

    std::thread::sleep(Duration::from_millis(1000)); // warm up on the lossy link
    let before = r.ok_now();
    std::thread::sleep(Duration::from_secs(4)); // run long enough that retransmit engages
    let after = r.ok_now();
    // TRANSIENT: the cluster must keep committing under loss (not wedged).
    let progressed = after > before;

    let entries = r.finish();

    if !progressed {
        return Err(format!("cluster did not progress under 10% link loss ({before} -> {after})"));
    }
    if History::ok_count(&entries) < MIN_OK {
        return Err("too few Ok ops; run is vacuous".into());
    }
    check_or_transient(&entries, seed, "lossy")
}

#[test]
fn linearizable_under_lossy_links() {
    let _g = serialize();
    for attempt in 1..=3 {
        match run_lossy_links(31_337) {
            Ok(()) => return,
            Err(e) => eprintln!("[lin_partition_v2::lossy] attempt {attempt}/3 transient: {e}"),
        }
    }
    panic!("lossy: failed after 3 transient attempts");
}
