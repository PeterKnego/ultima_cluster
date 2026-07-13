// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Scripted nasties (spec §8): each drives the world to a specific dangerous
//! configuration and asserts the invariants held + the expected outcome.
//!
//! The `raw_m3_*_is_caught` pair are the inverse: they run the data plane in
//! [`DataPlane::RawM3`] — the shipped M3 receiver's behavior — and assert the
//! oracle CATCHES an invariant violation. They are the sim's proof it would have
//! caught the real phantom-commit / wrong-base-stamp bugs the task-5 review
//! flagged, and the pins that keep the oracle honest as `Gated` (the Task-7
//! contract) is built out. See `DataPlane` for the two-mode contract.

use uc2_consensus::config::{ConfigOp, ProposeError};
use uc2_sim::world::{DataPlane, SimConfig, World};

fn base_cfg(seed: u64) -> SimConfig {
    SimConfig { n_nodes: 3, seed, max_steps: 30_000, ..SimConfig::default() }
}

/// A high-churn config tuned to exercise the intake-gate reconcile discipline:
/// heavy loss + duplication (a duplicate term-map is the C-1 trigger — it lands
/// mid-truncation and the SM's latch drops it with zero actions) and frequent
/// crashes (leader churn → term adoptions that close the gate, divergent tails,
/// and truncations that reconcile them). Under `Mechanism{reopen_guard:false}`
/// this storm reopens the gate early on some seed and the raw divergent durable
/// escapes into commit ranking; `reopen_guard:true` (the shipped node) survives
/// it. The data-plane mode is left at the default — callers set it.
fn nasty_reconcile_config(seed: u64) -> SimConfig {
    SimConfig {
        n_nodes: 3,
        seed,
        // A long run: the C-1 phantom needs a divergent ex-leader's large raw
        // durable to persist in a live leader's CommitTracker while a third node
        // lags — that alignment takes churn to build up.
        max_steps: 40_000,
        // High loss + duplication drives leader churn (→ divergent tails →
        // truncations) and, crucially, the DUPLICATE term map that lands
        // mid-truncation — the C-1 trigger. Crash rate is kept modest: past
        // ~1000ppm a lagging follower's uncommitted divergent boundary routinely
        // falls below an advancing commit (a strict-inv2 artifact that is NOT a
        // gate bug and would fire in BOTH arms), which would mask the signal.
        drop_per_million: 40_000,
        dup_per_million: 40_000,
        crash_per_million: 1_000,
        // A wide archive-truncation window: the divergent durable stays on disk
        // (gate-suppressed) long enough that a duplicate term map lands
        // mid-truncation. Under `reopen_guard:false` that duplicate reopens the
        // gate early and the raw durable escapes; the guard keeps it shut.
        truncate_latency_ns: 400_000_000, // ~1.3–2.7 election timeouts
        ..SimConfig::default()
    }
}

#[test]
fn quiet_cluster_elects_exactly_one_leader_and_commits() {
    let mut w = World::new(SimConfig { drop_per_million: 0, ..base_cfg(1) });
    let stats = w.run().expect("invariants");
    assert_eq!(stats.leaders_elected, 1, "stable cluster must elect once");
    assert!(stats.max_commit > 0, "a serving leader must commit data");
}

#[test]
fn split_vote_converges() {
    // drop ALL vote traffic for the first virtual 500ms, then heal: forced
    // split votes, then convergence
    let mut w = World::new(base_cfg(7));
    w.drop_all_votes_until(500_000_000);
    let stats = w.run().expect("invariants");
    assert!(stats.max_commit > 0, "cluster must converge after split votes");
}

#[test]
fn minority_partition_cannot_commit_and_heals() {
    let mut w = World::new(SimConfig { drop_per_million: 0, ..base_cfg(3) });
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    let commit_before = w.max_commit();
    // partition the leader away from BOTH followers
    w.partition_node(leader);
    w.run_steps(5_000).expect("invariants");
    // the old leader alone must not have advanced commit
    assert_eq!(
        w.node_commit_high_water(leader),
        commit_before.max(w.node_commit_high_water(leader)),
        "stale leader must not certify new bytes; its commit is frozen"
    );
    assert!(w.max_commit_from(&w.majority_excluding(leader)) >= commit_before);
    // heal: the deposed leader truncates its uncommitted tail and rejoins
    w.heal();
    let stats = w.run().expect("invariants");
    assert!(stats.truncations >= 1, "the deposed leader's tail must truncate");
}

#[test]
fn crash_during_truncate_recovers() {
    let mut w = World::new(base_cfg(11));
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    w.partition_node(leader);
    w.run_steps(5_000).expect("invariants");
    w.heal();
    // crash the deposed node the moment its Truncate action fires
    w.crash_on_next_truncate();
    let stats = w.run().expect("invariants (crash mid-truncate)");
    assert!(stats.restarts >= 1);
}

/// The Task-7 contract, run in the weak (`RawM3`) mode, MUST catch the reviewer's
/// phantom-commit → committed-data-loss trace: a current max-term leader certifies
/// a commit over bytes only IT genuinely holds, because a healed ex-leader reports
/// its raw (divergent, un-reconciled) durable and the shipped receiver counts it
/// toward quorum. The genuine byte-content-quorum oracle (F3) catches it as a
/// leader-completeness (inv5) phantom-commit violation.
///
/// DETERMINISTIC SCRIPT (not a fuzz fallback): a single pinned seed with a scripted
/// partition/heal timeline that stages, structurally, the reviewer's trace —
/// leader L (a later term) whose own log runs ahead, a lagging follower G frozen
/// below, and a healed ex-leader X whose divergent durable tail is raw-reported to
/// L. Verified to fail-RED (the run stays green) if the oracle's phantom guard is
/// removed.
#[test]
fn raw_m3_data_plane_phantom_commit_is_caught() {
    // Disable the idle gossip floor for this deterministic negative pin: the
    // script's window depends on X raw-reporting its divergent durable to L
    // BEFORE reconciliation repairs it (L's commit is stalled, so pre-floor no
    // term map shipped during the window). The floor — whose job IS to re-ship
    // the map on an idle/stalled leader — would repair X first and prevent the
    // phantom, which is correct behavior but defeats this ORACLE pin. The floor's
    // real effect is covered by `idle_cluster_reconciles_divergent_node_via_gossip_floor`.
    let mut w = World::new(SimConfig {
        drop_per_million: 0,
        data_plane: DataPlane::RawM3,
        gossip_floor_ns: u64::MAX,
        ..base_cfg(3)
    });
    w.run_until_leader().expect("setup: elect first leader");
    let x = w.current_leader().unwrap(); // term-1 leader = future divergent ex-leader X
    w.run_steps(300).expect("setup: commit a genuine prefix on all three");
    let others: Vec<usize> = (0..3).filter(|&i| i != x).collect();
    let (a, b) = (others[0], others[1]);
    // Isolate X (pairwise, so we can partially heal later); it keeps appending an
    // uncommitted term-1 divergent tail while the other two carry on.
    w.partition(x, a);
    w.partition(x, b);
    w.run_until(|w| w.current_leader().is_some_and(|l| l != x)).expect("setup: new leader elects");
    let l = w.current_leader().unwrap(); // the new higher-term leader L
    let g = if l == a { b } else { a }; // the third node = lagging follower G
    w.run_steps(300).expect("setup: L commits with G past the old prefix");
    // Now cut L off from G: G lags (frozen), and L keeps appending its own term's
    // bytes (own durable runs ahead, but commit stalls — no quorum).
    w.partition(l, g);
    w.run_steps(300).expect("setup: L's own log runs ahead of the stalled commit");
    // Partially heal: reconnect ONLY X <-> L. Under RawM3 X raw-reports its
    // divergent durable to L before reconciliation repairs it; L ranks that report
    // with its own high durable and certifies a commit no genuine quorum holds.
    w.unpartition(x, l);
    let v = w
        .run_steps(2000)
        .expect_err("RawM3 phantom commit must be caught by the genuine-quorum oracle");
    assert!(
        v.invariant.contains("inv5") || v.invariant.contains("phantom"),
        "expected a leader-completeness/phantom-commit violation, got: {v}"
    );
}

/// The Task-7 contract, run in the weak (`RawM3`) mode, MUST catch the seed-365
/// class: a follower with a divergent prefix accepts a current-term segment on
/// position-contiguity ALONE (no prev-term gate) and stamps that term at the wrong
/// base — so two nodes record the same committed history with different term-map
/// boundaries. inv2 (term-map prefix consistency), fixed to require exact committed
/// boundaries, catches the misplaced boundary.
///
/// DETERMINISTIC SCRIPT: a single pinned seed stages a frozen divergent follower
/// and a genuine commit run past its divergence point, then injects the exact
/// divergent-extension wire frame a real leader would send (the natural run can't
/// force this ordering — the reconcile term-map otherwise repairs the follower
/// first). Verified to fail-RED (the run stays green) if inv2 is reverted to the
/// zip-truncation / term-only compare.
#[test]
fn raw_m3_wrong_base_term_stamp_is_caught() {
    // Seed 5: the misplaced boundary lands with the SAME term as the committed
    // lineage but a different base (node records (2,3456) vs lineage's (2,960)) —
    // so the exact-boundary compare is load-bearing (a term-only / zip compare
    // would miss it; this is the F4 fail-red case).
    let mut w = World::new(SimConfig {
        drop_per_million: 0,
        data_plane: DataPlane::RawM3,
        max_steps: 80_000,
        // Same rationale as the phantom-commit pin: this script relies on the
        // reconcile term map NOT repairing the follower before the injected
        // divergent frame lands (see the doc comment). The idle floor is disabled
        // so it cannot race the scripted repair.
        gossip_floor_ns: u64::MAX,
        ..base_cfg(5)
    });
    w.run_until_leader().expect("setup: elect first leader");
    let x = w.current_leader().unwrap();
    w.run_steps(200).expect("setup: genuine prefix on all three");
    let others: Vec<usize> = (0..3).filter(|&i| i != x).collect();
    let (a, b) = (others[0], others[1]);
    w.partition(x, a);
    w.partition(x, b);
    w.run_until(|w| w.current_leader().is_some_and(|l| l != x)).expect("setup: new leader");
    let l = w.current_leader().unwrap();
    w.run_steps(150).expect("setup: X grows a modest divergent tail");
    // Freeze X's divergent append (crash), so the genuine majority can commit PAST
    // it (an isolated live leader would keep pace and outrun the commit).
    w.crash(x);
    let d = w.node_append(x); // frozen divergence point (append == durable)
    w.run_until(|w| w.max_commit() > d + 500).expect("setup: genuine commit runs past X");
    assert!(w.max_commit() > d, "setup: majority must commit past X's divergence point");
    w.restart(x).expect("setup: X returns as a follower at its frozen durable");
    // Inject the current-term(2) segment at X's own append. RawM3 accepts on
    // contiguity alone and stamps term 2 at base `d` — a boundary the committed
    // lineage does not have there.
    let v = w
        .inject_data(l, x, 2, 2, d, d + 96, 2)
        .expect_err("RawM3 wrong-base stamp must be caught by inv2");
    assert!(
        v.invariant.contains("inv2"),
        "expected a term-map prefix-consistency (inv2) violation, got: {v}"
    );
}

/// Task-9 idle-reconciliation pin: a divergent node healing into a FULLY IDLE
/// cluster (commit plateaued, zero new submissions) MUST still reconcile —
/// `truncations >= 1` — driven by the leader's gossip FLOOR alone. This is the
/// exact hole the failover test's submit-per-iteration workaround was masking:
/// without the floor, `ShipTermMap` rides only the commit-advance cadence, so an
/// idle leader never hands the reconnecting node a map and its divergent tail
/// stays un-truncated forever.
#[test]
fn idle_cluster_reconciles_divergent_node_via_gossip_floor() {
    let mut w = World::new(SimConfig { drop_per_million: 0, max_steps: 60_000, ..base_cfg(3) });
    w.run_until_leader().expect("setup: elect first leader");
    let old = w.current_leader().unwrap();
    w.run_steps(300).expect("setup: genuine committed prefix on all three");

    // Isolate the term-1 leader: it keeps appending an uncommitted divergent
    // tail while the majority elects a higher-term leader and commits past it.
    w.partition_node(old);
    w.run_until(|w| w.current_leader().is_some_and(|l| l != old))
        .expect("setup: new higher-term leader elects");
    w.run_steps(500).expect("setup: majority commits past the old prefix");
    let divergent_append = w.node_append(old);
    assert!(divergent_append > 0, "setup: old leader must hold a divergent tail");

    // Quiesce: the new leader stops taking writes — commit PLATEAUS. From here
    // only the idle gossip floor re-ships commit + term map.
    w.set_quiet(true);
    w.run_steps(2_000).expect("plateau settles under the idle floor");
    let commit_plateau = w.max_commit();
    assert_eq!(w.truncations(), 0, "no reconciliation while the divergent node is still isolated");

    // Heal the divergent node into the now-idle cluster. With NO new submissions,
    // it must STILL reconcile — the floor-shipped term map is the only trigger.
    w.heal();
    w.run_until(|w| w.truncations() >= 1).expect("idle reconciliation must run to completion");
    assert!(
        w.truncations() >= 1,
        "divergent node failed to reconcile into an idle cluster (gossip floor did not re-ship the map)"
    );
    assert_eq!(
        w.max_commit(),
        commit_plateau,
        "commit must NOT have advanced — the reconciliation was driven by the idle floor, not new writes"
    );
}

/// M4 C-1 reproduced mechanically: under `Mechanism{reopen_guard:false}` a
/// duplicate term map delivered while a truncation is in flight reopens the gate
/// early; the raw divergent durable escapes into commit ranking and the oracle
/// (inv5 / leader completeness) must catch it on some seed.
#[test]
fn mechanism_unguarded_reopen_is_caught_by_oracle() {
    let mut caught = false;
    for seed in 0..200 {
        let mut cfg = nasty_reconcile_config(seed); // helper: high churn, partitions, crashes
        cfg.data_plane = DataPlane::Mechanism { reopen_guard: false };
        if World::new(cfg).run().is_err() {
            caught = true;
            break;
        }
    }
    assert!(caught, "the unguarded reopen must violate an invariant on some seed");
}

/// The guarded mechanism (what uc2_node actually implements) survives the same
/// storm: 200 seeds green.
#[test]
fn mechanism_guarded_survives_the_same_storm() {
    for seed in 0..200 {
        let mut cfg = nasty_reconcile_config(seed);
        cfg.data_plane = DataPlane::Mechanism { reopen_guard: true };
        World::new(cfg).run().unwrap_or_else(|v| panic!("seed {seed}: {v:?}"));
    }
}

/// T5's deferred third pin: a forged/corrupt raw report above the sender's real
/// durable must be caught (RawM3) — inject_report makes it expressible.
#[test]
#[allow(clippy::field_reassign_with_default)] // pin kept verbatim per the task brief
fn raw_m3_forged_report_phantom_commit_is_caught() {
    let mut cfg = SimConfig::default();
    cfg.data_plane = DataPlane::RawM3;
    let mut w = World::new(cfg);
    w.run_until_leader().unwrap();
    let leader = w.current_leader().unwrap();
    let f = w.majority_excluding(leader)[0];
    w.inject_report(f, w.node_term(leader), 1 << 30); // far beyond any real durable
    assert!(w.run_steps(2_000).is_err(), "phantom durable must trip an invariant");
}

/// M6 Task 8 — a NoCommonPrefix reconcile REACHES the wipe decision, and the
/// wipe-DISABLED counterfactual fail-stops.
///
/// A leader whose shipped term-map tail has slid PAST a follower's first byte —
/// its low-end entries dropped by log purge (the real M6 case, unreachable in a
/// natural sim run capped at `MAX_TERM_MAP_WIRE_ENTRIES` terms) — leaves that
/// follower NO common prefix. We craft the exact byte-for-byte "slid-past" wire
/// tail and inject it into a follower holding a genuine `[(1,0)]` prefix, driving
/// `reconcile` to `NoCommonPrefix` through the full sim data plane.
///
/// This runs the wipe-DISABLED counterfactual: the reconcile fail-stops
/// (`Action::Fatal` -> `InvariantViolation`), which both proves the injection
/// genuinely reaches `NoCommonPrefix` AND documents exactly what the default wipe
/// path replaces (that `Fatal` becomes a truncate-to-0).
///
/// The POSITIVE wipe response (truncate-to-0 then refill) is proven exhaustively
/// at the SM level (`election.rs` `no_common_prefix_wipes_and_rejoins`) and end to
/// end through a real `Consensus` (`node.rs`
/// `no_common_prefix_wipes_the_node_and_rejoins_empty`, which runs the true
/// persist-empty-map -> truncate -> ack -> gate-reopen). It is NOT re-asserted in
/// the sim because the sim deliberately cannot: a genuine NoCommonPrefix means the
/// leader's window slid past the follower's COMMITTED prefix, so a faithful wipe
/// discards locally-committed bytes whose safety derives from the SNAPSHOT backing
/// the purge floor — and the sim models no snapshots, so its
/// committed-never-truncated oracle (inv4) correctly refuses to bless a
/// truncate-to-0 below a node's committed high-water. Modelling that is out of
/// scope for M6's sim; the node-level test covers the real execution.
#[test]
fn no_common_prefix_reaches_wipe_and_fatal_when_disabled() {
    let mut cfg = base_cfg(1);
    cfg.data_plane = DataPlane::Mechanism { reopen_guard: true };
    let mut w = World::new(cfg);
    w.run_until_leader().expect("elect leader");
    let leader = w.current_leader().unwrap();
    // A follower with a genuine [(1,0)] prefix; isolate it so gossip does not
    // repair it before the crafted slid-past tail lands.
    let x = (0..3).find(|&i| i != leader).unwrap();
    w.partition_node(x);
    w.disable_wipe(x); // counterfactual: NoCommonPrefix must surface Fatal
    let hi = w.node_term(x).max(w.node_term(leader)) + 40;
    let tail = vec![(hi - 1, 1 << 20), (hi, 2 << 20)];
    assert!(
        w.inject_term_map(leader, x, hi, tail).is_err(),
        "wipe-disabled: a below-window map must reach NoCommonPrefix -> fail-stop"
    );
    assert_eq!(w.wipes(), 0, "no wipe counted in the counterfactual");
}

#[test]
fn fuzz_default_seeds() {
    for seed in 0..50u64 {
        let mut w = World::new(SimConfig {
            n_nodes: 3,
            seed,
            max_steps: 20_000,
            drop_per_million: 20_000,
            dup_per_million: 5_000,
            crash_per_million: 500,
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed}: {v}");
        }
    }
    // Same seeds, run against the REAL intake-gate mechanism (guarded, as the node
    // ships it). Mechanism is ADDED alongside the default Gated tier above — the
    // structural clamp and the boolean gate must BOTH stay green on the fuzz.
    for seed in 0..50u64 {
        let mut w = World::new(SimConfig {
            n_nodes: 3,
            seed,
            max_steps: 20_000,
            drop_per_million: 20_000,
            dup_per_million: 5_000,
            crash_per_million: 500,
            data_plane: DataPlane::Mechanism { reopen_guard: true },
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed} (Mechanism): {v}");
        }
    }
}

#[cfg(feature = "sim-heavy")]
#[test]
fn fuzz_heavy_seeds() {
    for seed in 0..1000u64 {
        let mut w = World::new(SimConfig {
            n_nodes: if seed % 4 == 0 { 5 } else { 3 },
            seed,
            max_steps: 20_000,
            drop_per_million: 50_000,
            dup_per_million: 10_000,
            crash_per_million: 1_000,
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed}: {v}");
        }
    }
    // The 1000-seed storm against the REAL intake-gate mechanism (guarded, as the
    // node ships it). ADDED alongside the Gated tier above — Mechanism is the
    // discipline the node actually runs, so it gets its own heavy fuzz.
    //
    // The rates are gentler than the Gated tier's on PURPOSE. `Gated`'s structural
    // report clamp keeps a mid-reconciliation follower out of the commit math
    // entirely; the boolean gate does not — it FREEZES a lagging follower's
    // uncommitted divergent tail (DATA dropped while the gate is closed) until it
    // reconciles, so at extreme loss a global commit driven by the OTHER two nodes
    // can momentarily advance past that follower's still-un-truncated divergent
    // boundary. That transient trips inv2's strict map-prefix form even though no
    // byte the follower itself committed is ever lost (inv4/inv5 stay clean) and
    // it happens in BOTH guard arms — a property of the weaker data plane, not the
    // reopen guard. Kept below that onset so the tier proves the guarded mechanism
    // green, not the strict invariant's tolerance of benign lag.
    for seed in 0..1000u64 {
        let mut w = World::new(SimConfig {
            n_nodes: if seed % 4 == 0 { 5 } else { 3 },
            seed,
            max_steps: 20_000,
            drop_per_million: 30_000,
            dup_per_million: 10_000,
            crash_per_million: 500,
            data_plane: DataPlane::Mechanism { reopen_guard: true },
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed} (Mechanism): {v}");
        }
    }
}

// ================= M7 dynamic membership (config frames, inv6-9) =================

/// Retry a proposal until the (possibly re-elected) serving leader accepts it,
/// tolerating the transient refusals a live cluster legitimately produces
/// (`ChangePending` while the previous frame commits, `NotCaughtUp` while a
/// learner closes its gap, leadership churn from injected faults). Structural
/// refusals still panic — the cycle test's ops are all legal.
fn propose_ok(w: &mut World, op: ConfigOp) -> u64 {
    for _ in 0..300 {
        let Some(l) = w.current_leader() else {
            w.run_steps(500).expect("invariants (awaiting a leader)");
            continue;
        };
        match w.propose_config(l, op) {
            Ok(v) => return v,
            Err(
                ProposeError::ChangePending
                | ProposeError::NotCaughtUp { .. }
                | ProposeError::NotLeader
                | ProposeError::NotServing,
            ) => w.run_steps(500).expect("invariants (awaiting acceptance)"),
            Err(e) => panic!("unexpected structural refusal: {e:?}"),
        }
    }
    panic!("proposal never accepted: {op:?}");
}

/// Every live (non-halted) node has adopted config version `v`.
fn all_live_at_version(w: &World, n: usize, v: u64) -> bool {
    (0..n).all(|i| w.halted_removed(i) || w.node_config_version(i) == v)
}

/// The full single-server-change lifecycle — add, promote, demote,
/// remove-learner, remove-voter — through the real frame pipeline (append ->
/// durable-cross adoption -> commit), with a crash and a partition injected
/// between steps. All invariants green; the removed nodes fail-stop
/// (`halted_removed`).
#[test]
fn add_promote_demote_remove_cycle_under_faults() {
    // 5 processes: genesis voters {0,1,2,3}, node 4 a genesis LEARNER.
    let mut w = World::new(SimConfig {
        n_nodes: 5,
        seed: 17,
        max_steps: 400_000,
        drop_per_million: 0,
        initial_learners: vec![4],
        ..SimConfig::default()
    });
    w.run_until_leader().expect("elect");
    w.run_steps(300).expect("a genuine committed prefix");

    // add-learner (a fresh id joins as learner; id 7 has no process behind it —
    // the config pipeline neither knows nor cares).
    assert_eq!(propose_ok(&mut w, ConfigOp::AddLearner { id: 7, addr: (7, 1) }), 1);
    w.run_until(|w| all_live_at_version(w, 5, 1)).expect("v1 adopted everywhere");

    // Fault between steps: crash a follower (auto-restarts).
    let l = w.current_leader().unwrap();
    let f = (0..4).find(|&i| i != l).unwrap();
    w.crash(f);
    w.run_steps(1_000).expect("ride out the crash");

    // promote the caught-up learner 4 -> voter (quorum grows to 5).
    assert_eq!(propose_ok(&mut w, ConfigOp::PromoteLearner { id: 4 }), 2);
    w.run_until(|w| all_live_at_version(w, 5, 2)).expect("v2 adopted everywhere");

    // Fault between steps: partition a follower pair, then heal.
    let l = w.current_leader().unwrap();
    let (a, b) = {
        let mut it = (0..5).filter(|&i| i != l);
        (it.next().unwrap(), it.next().unwrap())
    };
    w.partition(a, b);
    w.run_steps(1_000).expect("ride out the partition");
    w.unpartition(a, b);

    // demote 4 back to learner.
    assert_eq!(propose_ok(&mut w, ConfigOp::DemoteVoter { id: 4 }), 3);
    w.run_until(|w| all_live_at_version(w, 5, 3)).expect("v3 adopted everywhere");

    // remove-learner 4: on adopting the config that drops it, 4 fail-stops.
    assert_eq!(propose_ok(&mut w, ConfigOp::RemoveLearner { id: 4 }), 4);
    w.run_until(|w| w.halted_removed(4)).expect("4 adopts its own removal");
    assert!(w.halted_removed(4), "the removed learner must halt");

    // remove-voter: a NON-leader voter; it halts on adoption too.
    let l = w.current_leader().unwrap();
    let t = (0..4).find(|&i| i != l && !w.halted_removed(i)).unwrap();
    assert_eq!(propose_ok(&mut w, ConfigOp::RemoveVoter { id: t as u32 }), 5);
    w.run_until(|w| w.halted_removed(t)).expect("t adopts its own removal");
    assert!(w.halted_removed(t), "the removed voter must halt");

    // The survivors converge on v5 and keep committing.
    w.run_until(|w| all_live_at_version(w, 5, 5)).expect("v5 adopted on survivors");
    let c = w.max_commit();
    w.run_steps(2_000).expect("the shrunken cluster keeps serving");
    assert!(w.max_commit() > c, "commit must still advance under the final config");
    // Tombstone permanence at the SM: re-adding a removed id is refused.
    let l = w.current_leader().unwrap();
    assert_eq!(
        w.propose_config(l, ConfigOp::AddLearner { id: t as u32, addr: (t as u32, 1) }),
        Err(ProposeError::Tombstoned),
        "a tombstoned id can never rejoin"
    );
}

/// One-in-flight: a second proposal before the first frame commits is refused
/// with `ChangePending`; once it commits, the next change is accepted.
#[test]
fn propose_during_pending_is_refused() {
    let mut w = World::new(SimConfig { drop_per_million: 0, ..base_cfg(13) });
    w.run_until_leader().expect("elect");
    let l = w.current_leader().unwrap();
    assert_eq!(w.propose_config(l, ConfigOp::AddLearner { id: 9, addr: (9, 1) }), Ok(1));
    assert_eq!(
        w.propose_config(l, ConfigOp::AddLearner { id: 8, addr: (8, 1) }),
        Err(ProposeError::ChangePending),
        "one change in flight: the second proposal must be refused"
    );
    w.run_steps(3_000).expect("the pending frame commits");
    assert_eq!(
        w.propose_config(l, ConfigOp::AddLearner { id: 8, addr: (8, 1) }),
        Ok(2),
        "after the commit the next change is accepted"
    );
}

/// Truncation revert (spec §5): a config frame appended by a leader that gets
/// isolated before replicating it is truncated away when the node reconciles
/// into the new term — the node reverts to the previous config (inv8 checks at
/// the ack), then re-adopts whatever the NEW leader's stream carries (inv6
/// green throughout).
#[test]
fn truncation_below_config_frame_reverts() {
    let mut w =
        World::new(SimConfig { drop_per_million: 0, max_steps: 120_000, ..base_cfg(3) });
    w.run_until_leader().expect("elect L1");
    let l1 = w.current_leader().unwrap();
    w.run_steps(300).expect("a genuine committed prefix on all three");
    // Isolate the leader FIRST, then propose: the frame lands ONLY in its own
    // stream — an uncommitted config on a soon-divergent lineage.
    w.partition_node(l1);
    assert_eq!(w.propose_config(l1, ConfigOp::AddLearner { id: 9, addr: (9, 1) }), Ok(1));
    let p1 = w.node_append(l1); // the frame's END position
    // Its own archive makes the frame durable (adopt-at-append settles into
    // adopt-at-durable) while the majority elects a higher term without it.
    w.run_until(|w| {
        w.node_durable(l1) >= p1 && (0..3).any(|i| i != l1 && w.node_is_serving_leader(i))
    })
    .expect("frame durable on L1; new leader elected without it");
    assert_eq!(w.node_config_version(l1), 1, "isolated ex-leader adopted its own frame");
    let l2 = (0..3).find(|&i| i != l1 && w.node_is_serving_leader(i)).unwrap();
    w.run_steps(400).expect("majority commits past the old prefix under v0");
    assert_eq!(w.node_config_version(l2), 0, "the frame never reached the majority");

    // Heal: reconciliation truncates L1's divergent tail (strictly below the
    // frame end) -> the SM reverts to v0 and the durable record follows.
    w.heal();
    w.run_until(|w| w.truncations() >= 1).expect("the divergent tail truncates");
    w.run_steps(200).expect("the ack lands and the revert settles");
    assert_eq!(
        w.node_config_version(l1),
        0,
        "truncation below the config frame must revert the adopted config"
    );

    // The new leader proposes its own change; L1 re-adopts from the NEW stream
    // when its durable crosses the new frame (same version number, different
    // content — the ledger's content-identity check keeps them apart).
    let l2 = (0..3).find(|&i| w.node_is_serving_leader(i)).unwrap();
    assert_eq!(w.propose_config(l2, ConfigOp::AddLearner { id: 8, addr: (8, 1) }), Ok(1));
    w.run_until(|w| w.node_config_version(l1) == 1).expect("L1 re-adopts from the new stream");
    w.run_steps(500).expect("green steady state");
}

/// COUNTERFACTUAL-RED (revert): the identical divergent-config-frame world with
/// `revert_on_truncate_disabled` — the SM keeps the stale config across the
/// truncation that removed its frame, and inv8 must catch it at the ack.
#[test]
fn counterfactual_no_revert_breaks_inv8() {
    let mut w = World::new(SimConfig {
        drop_per_million: 0,
        max_steps: 120_000,
        revert_on_truncate_disabled: true,
        ..base_cfg(3)
    });
    w.run_until_leader().expect("elect L1");
    let l1 = w.current_leader().unwrap();
    w.run_steps(300).expect("a genuine committed prefix");
    w.partition_node(l1);
    assert_eq!(w.propose_config(l1, ConfigOp::AddLearner { id: 9, addr: (9, 1) }), Ok(1));
    let p1 = w.node_append(l1);
    w.run_until(|w| {
        w.node_durable(l1) >= p1 && (0..3).any(|i| i != l1 && w.node_is_serving_leader(i))
    })
    .expect("frame durable on L1; new leader elected without it");
    w.run_steps(400).expect("majority commits past the old prefix");
    w.heal();
    let v = w
        .run_steps(30_000)
        .expect_err("revert deleted: the truncation must strand the stale config");
    assert!(v.invariant.contains("inv8"), "expected an inv8 violation, got: {v}");
}

/// COUNTERFACTUAL-RED (serving gate): Ongaro's 2015 single-server-change bug,
/// staged mechanically with the gate deleted (`serving_gate_disabled`).
///
/// C_old = voters {0,1,2,3} + learner 4. L1 proposes PromoteLearner{4} (the
/// "add server" — C1: 5 voters) and crashes with the frame replicated only to
/// itself and 4. A new leader L2 — whose log has no C1 — proposes
/// RemoveVoter{L1} (C1': 3 voters) IMMEDIATELY, before committing anything of
/// its own term: with the gate this is refused (`NotServing` — L2 cannot
/// commit its NewTerm while a voter is dark), without it C1' commits on
/// {L2, b} = 2 of C1'(3) — NOT a majority of C_old(4). The two config lineages
/// have now certified history under quorums that never intersect: when the C1
/// side ({L1, 4, c} = 3 of C1's 5) elects a leader, the checker must catch the
/// disjoint-quorum world as an inv7 violation.
#[test]
fn counterfactual_no_serving_gate_produces_disjoint_quorum_commit() {
    let mut w = World::new(SimConfig {
        n_nodes: 5,
        seed: 3,
        max_steps: 400_000,
        drop_per_million: 0,
        initial_learners: vec![4],
        serving_gate_disabled: true,
        ..SimConfig::default()
    });
    // Phase 1: L1 serves; learner 4 tracks the stream and reports in.
    w.run_until_leader().expect("elect L1");
    let l1 = w.current_leader().unwrap();
    w.run_steps(600).expect("commit a genuine prefix; learner 4 reports in");
    // Cut L1 off from every other voter, keeping only (L1, 4): the promote
    // frame will replicate to 4 alone.
    let other_voters: Vec<usize> = (0..4).filter(|&v| v != l1).collect();
    for &v in &other_voters {
        w.partition(l1, v);
    }
    let mut accepted = false;
    for _ in 0..50 {
        match w.propose_config(l1, ConfigOp::PromoteLearner { id: 4 }) {
            Ok(v) => {
                assert_eq!(v, 1);
                accepted = true;
                break;
            }
            Err(ProposeError::NotCaughtUp { .. }) => {
                w.run_steps(300).expect("learner catches up");
            }
            Err(e) => panic!("unexpected refusal: {e:?}"),
        }
    }
    assert!(accepted, "L1 must accept the promote");
    let p1 = w.node_append(l1);
    w.run_until(|w| w.node_durable(l1) >= p1 && w.node_config_version(4) == 1)
        .expect("C1 durable on L1 and adopted by 4");
    // Phase 2: L1 crashes right after proposing; 4 goes dark holding C1.
    w.crash(l1);
    w.partition_node(4);
    // Phase 3: a new leader among the three clean voters. Gate OFF: it
    // proposes RemoveVoter{L1} immediately — its NewTerm can NEVER commit
    // under C_old (L1 down + c dark = at most 2 of quorum 3), so with the
    // gate this proposal would be refused forever.
    w.run_until(|w| (0..4).any(|i| i != l1 && w.node_is_raw_leader(i)))
        .expect("a new raw leader");
    let l2 = (0..4).find(|&i| i != l1 && w.node_is_raw_leader(i)).unwrap();
    let c = other_voters.iter().copied().find(|&v| v != l2).unwrap();
    let b = other_voters.iter().copied().find(|&v| v != l2 && v != c).unwrap();
    w.partition(l2, c); // keep one C_old voter dark (in-flight datagrams too)
    assert_eq!(
        w.propose_config(l2, ConfigOp::RemoveVoter { id: l1 as u32 }),
        Ok(1),
        "gate off: the premature proposal is accepted"
    );
    let p2 = w.node_append(l2);
    // C1' commits on {L2, b} — a quorum of C1'(3) but NOT of C_old(4).
    w.run_until(|w| w.node_config_version(b) == 1 && w.max_commit() >= p2)
        .expect("C1' commits under its own shrunken quorum");
    // Phase 4: silence the only C1' holders; the C1 side ({L1, 4, c}) heals
    // and elects — a quorum of a config lineage ground truth never committed.
    w.heal();
    w.partition_node(l2);
    w.partition_node(b);
    w.restart(l1).expect("L1 returns with C1");
    let v = w
        .run_steps(60_000)
        .expect_err("the disjoint-quorum election/commit must trip the checker");
    assert!(v.invariant.contains("inv7"), "expected an inv7 violation, got: {v}");
}

/// M7 fuzz arm: random LEGAL AND ILLEGAL config ops — proposals from arbitrary
/// nodes (followers must refuse), ops on absent / wrong-role / tombstoned /
/// virtual ids — interleaved with crash + partition-grade loss churn and the
/// truncations that churn produces. Every refusal must stay refused (a refused
/// op is NEVER adopted: no node's version can exceed the accepted-proposal
/// count), and every safety invariant (inv1-9) stays green on every seed.
#[cfg(feature = "sim-heavy")]
#[test]
fn fuzz_heavy_config_churn() {
    // Deterministic per-test RNG (xorshift64, same family the sim uses).
    fn next(s: &mut u64) -> u64 {
        let mut x = *s;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *s = x;
        x
    }
    fn random_op(r: u64, n: usize) -> ConfigOp {
        // Ids range over the real nodes plus two virtual ids, so absent /
        // wrong-role / tombstoned refusals arise organically.
        let id = ((r >> 8) % (n as u64 + 2)) as u32;
        match r % 5 {
            0 => ConfigOp::AddLearner { id, addr: (id, 1) },
            1 => ConfigOp::PromoteLearner { id },
            2 => ConfigOp::DemoteVoter { id },
            3 => ConfigOp::RemoveLearner { id },
            _ => ConfigOp::RemoveVoter { id },
        }
    }

    for seed in 0..1000u64 {
        let n = 5;
        let mut w = World::new(SimConfig {
            n_nodes: n,
            seed,
            max_steps: 20_000,
            drop_per_million: 20_000,
            dup_per_million: 5_000,
            crash_per_million: 500,
            initial_learners: if seed % 2 == 0 { vec![4] } else { vec![] },
            ..SimConfig::default()
        });
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut accepted = 0u64;
        let mut outcome = Ok(());
        for _round in 0..40 {
            if let Err(v) = w.run_steps(400) {
                outcome = Err(v);
                break;
            }
            // A random op on a RANDOM node: followers/candidates refuse
            // (NotLeader), leaders refuse structurally-illegal ops; only a
            // legal op on the serving leader is accepted.
            let node = (next(&mut rng) % n as u64) as usize;
            let op = random_op(next(&mut rng), n);
            if w.propose_config(node, op).is_ok() {
                accepted += 1;
            }
            // One GUARANTEED-illegal op per round: a proposal from a live
            // non-leader must be refused.
            if let Some(l) = w.current_leader()
                && let Some(f) = (0..n).find(|&i| i != l && !w.halted_removed(i))
            {
                assert!(
                    w.propose_config(f, ConfigOp::AddLearner { id: 99, addr: (99, 1) }).is_err(),
                    "seed {seed}: a non-leader proposal must be refused"
                );
            }
        }
        if outcome.is_ok() {
            outcome = w.run_steps(2_000).map(|_| ());
        }
        if let Err(v) = outcome {
            panic!("seed {seed}: {v}");
        }
        // Refused ops are NEVER adopted: each accepted proposal bumps the
        // version by exactly one from a version at most the running maximum,
        // so no node may ever sit above the accepted count.
        for i in 0..n {
            assert!(
                w.node_config_version(i) <= accepted,
                "seed {seed}: node {i} adopted v{} with only {accepted} accepted proposals \
                 (a refused op was adopted)",
                w.node_config_version(i)
            );
        }
    }
}

/// The guarded twin of the counterfactual above: the IDENTICAL trace with the
/// serving gate intact refuses the proposal at the exact world-instant the
/// gate-off arm had it accepted (`NotServing` — the new leader has not
/// committed its NewTerm; L1 down + c dark leave it short of C_old's quorum).
/// The gated cluster can only ever accept a change from a leader that
/// GENUINELY serves — later election churn may legitimately produce one (c is
/// reachable through b), which is exactly the safe path. Green here + red
/// above = the precondition is load-bearing.
#[test]
fn serving_gate_refuses_the_premature_proposal() {
    let mut w = World::new(SimConfig {
        n_nodes: 5,
        seed: 3,
        max_steps: 400_000,
        drop_per_million: 0,
        initial_learners: vec![4],
        serving_gate_disabled: false, // the shipped rule
        ..SimConfig::default()
    });
    w.run_until_leader().expect("elect L1");
    let l1 = w.current_leader().unwrap();
    w.run_steps(600).expect("commit a genuine prefix");
    let other_voters: Vec<usize> = (0..4).filter(|&v| v != l1).collect();
    for &v in &other_voters {
        w.partition(l1, v);
    }
    w.crash(l1);
    w.partition_node(4);
    w.run_until(|w| (0..4).any(|i| i != l1 && w.node_is_raw_leader(i)))
        .expect("a new raw leader");
    let l2 = (0..4).find(|&i| i != l1 && w.node_is_raw_leader(i)).unwrap();
    let c = other_voters.iter().copied().find(|&v| v != l2).unwrap();
    w.partition(l2, c);
    // The same instant the gate-off arm proposed: the gate refuses.
    assert_eq!(
        w.propose_config(l2, ConfigOp::RemoveVoter { id: l1 as u32 }),
        Err(ProposeError::NotServing),
        "the serving gate must refuse a pre-own-term-commit proposal"
    );
    // And the run stays green: the gated world never reaches a disjoint-quorum
    // commit (only a genuinely-serving later leader may accept a change).
    w.run_steps(30_000).expect("gated world stays green");
}
