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

use uc2_sim::world::{DataPlane, SimConfig, World};

fn base_cfg(seed: u64) -> SimConfig {
    SimConfig { n_nodes: 3, seed, max_steps: 30_000, ..SimConfig::default() }
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
}
