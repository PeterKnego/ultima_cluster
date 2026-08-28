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
use uc2_sim::invariants::InvariantViolation;
use uc2_sim::world::{DataPlane, SimConfig, World};

// ============================================================================
// T13: crypto-plane sim coverage.
//
// `Peers` (handshake.rs) and `GroupPlane` (group.rs) are pure `(input,
// now_ns) -> Vec<HandshakeAction>` transition functions with no sockets and
// no clock reads, so `World::enable_crypto_plane` can drive them exactly
// like `ElectionSm` — deterministic ordering/loss/reorder/partition/timer
// domain, per the task brief. Known limitation, stated plainly rather than
// silently relied on: `snow`'s Noise ephemerals and `GroupPlane::mint`'s
// fresh key material both draw from the OS RNG, so handshake/rotation BYTES
// are not reproducible run-to-run even at a fixed seed — only the SCHEDULE
// is. `snow::Builder::fixed_ephemeral_key_for_testing_only` exists for
// bit-exact byte replay; it is deliberately NOT used here because nothing
// below needs byte-for-byte handshake transcripts, only that the state
// machine trajectory (who is established when, who has which epoch, which
// gaps get NAK-repaired) reproduces — which it does, since every fault/
// timing decision in the sim is schedule-driven, not payload-driven.


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
        // ~800ppm a lagging follower's uncommitted divergent boundary routinely
        // falls below an advancing commit (a strict-inv2 artifact that is NOT a
        // gate bug and would fire in BOTH arms), which would mask the signal.
        // The onset moved down from ~1000ppm when the Mechanism report FLOOR
        // (the receiver.rs 20 ms AppendPosition re-send, added with the
        // Finding #5 pin) started closing report-loss gaps — commits certify
        // sooner under loss, overtaking a laggard's un-truncated boundary
        // earlier. Probed at 500..=1000ppm over 200 seeds: guarded arm clean
        // through 700, first benign inv2 transient (seed 22) at 800.
        // Finding #6b re-probe (2026-07-17, post §5.4.2 commit clamp): the
        // clamp masks sub-NewTerm-quorum phantoms, so at 700ppm the UNGUARDED
        // arm no longer catches (200 seeds; nor 200..800, truncate-latency
        // 500-1000ms, drop/dup 60-80k, max_steps 80k); its genuine inv7
        // phantom first appears at crash 1000ppm (seed 21). The red arm
        // therefore overrides crash to 1000ppm with a phantom-class-only
        // catch predicate; this shared config keeps the green arm's 700.
        drop_per_million: 40_000,
        dup_per_million: 40_000,
        crash_per_million: 700,
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

/// M14b (spec §12): a per-node apply ceiling models "the slowest FSM's
/// applied position + fsm_lag" — what M14a's report ceiling clamps a real
/// node's AppendPosition to. With BOTH followers capped, the leader alone is
/// not a quorum, so commit freezes at the cap while the leader's durable runs
/// past it; releasing ONE follower restores a quorum and commit resumes.
/// Every existing invariant runs throughout (`run*` returning `Ok`).
#[test]
fn capped_quorum_stalls_commit_and_releasing_one_follower_resumes_it() {
    const FRAME: u64 = 96;
    let mut w = World::new(base_cfg(11));
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    let followers = w.majority_excluding(leader);
    let cap = w.max_commit() + 2 * FRAME;
    for &f in &followers {
        w.set_apply_ceiling(f, Some(cap));
    }
    w.append_and_replicate(40 * FRAME);
    w.run_steps(8_000).expect("invariants");
    assert!(
        w.leader_durable() > cap + 10 * FRAME,
        "vacuity: the leader must hold durable bytes well past the cap (durable {})",
        w.leader_durable()
    );
    assert!(
        w.max_commit() <= cap,
        "commit {} ran past the capped quorum's ceiling {cap}",
        w.max_commit()
    );
    for &f in &followers {
        assert!(w.last_report(f) <= cap, "follower {f} reported {} > cap {cap}", w.last_report(f));
    }
    w.set_apply_ceiling(followers[0], None);
    assert!(
        w.run_until(|w| w.max_commit() > cap).expect("invariants"),
        "commit must resume once a quorum is uncapped (timed out)"
    );
    assert!(w.last_report(followers[1]) <= cap, "the still-capped follower stays capped");
}

/// M14b: a capped MINORITY never stalls the cluster — the leader plus the
/// uncapped follower are a quorum (spec §5.3: a lagging minority falls to
/// journal replay, the cluster does not wait for it).
#[test]
fn a_capped_minority_does_not_stall_commit() {
    const FRAME: u64 = 96;
    let mut w = World::new(base_cfg(12));
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    let f = w.majority_excluding(leader)[0];
    let cap = w.max_commit() + FRAME;
    w.set_apply_ceiling(f, Some(cap));
    w.append_and_replicate(20 * FRAME);
    assert!(
        w.run_until(|w| w.max_commit() > cap + 10 * FRAME).expect("invariants"),
        "one capped follower must not stall a 3-node cluster (timed out)"
    );
    assert!(w.last_report(f) <= cap, "the capped follower never reports past its ceiling");
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
    assert!(
        w.run_until(|w| w.current_leader().is_some_and(|l| l != x)).unwrap(),
        "setup: new leader elects timed out"
    );
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
    assert!(
        w.run_until(|w| w.current_leader().is_some_and(|l| l != x)).unwrap(),
        "setup: new leader timed out"
    );
    let l = w.current_leader().unwrap();
    w.run_steps(150).expect("setup: X grows a modest divergent tail");
    // Freeze X's divergent append (crash), so the genuine majority can commit PAST
    // it (an isolated live leader would keep pace and outrun the commit).
    w.crash(x);
    let d = w.node_append(x); // frozen divergence point (append == durable)
    assert!(
        w.run_until(|w| w.max_commit() > d + 500).unwrap(),
        "setup: genuine commit runs past X timed out"
    );
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
    assert!(
        w.run_until(|w| w.current_leader().is_some_and(|l| l != old)).unwrap(),
        "setup: new higher-term leader elects timed out"
    );
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
    assert!(
        w.run_until(|w| w.truncations() >= 1).unwrap(),
        "idle reconciliation must run to completion (timed out)"
    );
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
/// early; the raw divergent durable escapes into commit ranking and the
/// genuine-quorum oracle (inv7 phantom) must catch it on some seed.
///
/// Finding #6b re-tune (2026-07-17): the §5.4.2 commit clamp
/// (`rank_leader` advances only once `ranked >= new_term_pos`) is genuine
/// defense-in-depth for THIS class too — an escaped divergent report can no
/// longer certify anything below the new leader's NewTerm frame — so the
/// C-1 phantom needs heavier churn than before: at the guarded arm's
/// 700 ppm nothing catches in 200 seeds (nor in a 200..800 sweep, nor with
/// truncate-latency 500-1000 ms, nor drop/dup 60-80k, nor max_steps 80k);
/// the genuine inv7 phantom first appears at 1000 ppm (seed 21: "term-4
/// leader certified 17472, genuine quorum-frontier 17376"). The guarded
/// arm cannot follow to 1000 ppm because the DOCUMENTED benign strict-inv2
/// laggard transient (fires in BOTH arms, seed 22, onset 800 ppm — see
/// `nasty_reconcile_config`) caps its rate, so the twin runs asymmetric
/// rates post-clamp: red arm 1000 ppm, green arm 700 ppm. To keep the red
/// pin honest under the heavier rate, the catch predicate is SHARPENED to
/// the inv7 phantom class — the benign both-arms inv2 transient can never
/// satisfy it (a strictly stronger assertion; oracles untouched).
#[test]
fn mechanism_unguarded_reopen_is_caught_by_oracle() {
    // Protocol 0.5.0 note: with report CONTENT ATTESTATION on, this injected
    // bug no longer MANIFESTS — the leader declines the reopened follower's
    // report because its term attribution disagrees, so no phantom commit
    // forms. That is defence in depth, not a reason to drop the pin: ablate
    // attestation so this twin keeps proving the reopen guard is load-bearing
    // on its own. (Ablation lives in `SimConfig::attest_reports`.)
    let mut caught = false;
    for seed in 0..200 {
        let mut cfg = nasty_reconcile_config(seed); // helper: high churn, partitions, crashes
        cfg.crash_per_million = 1_000; // red-arm rate, see the doc comment above
        cfg.attest_reports = false; // isolate the reopen guard (see above)
        cfg.data_plane = DataPlane::Mechanism { reopen_guard: false, handle_keyed: true };
        if let Err(v) = World::new(cfg).run()
            && v.invariant.contains("phantom")
        {
            caught = true;
            break;
        }
    }
    assert!(caught, "the unguarded reopen must produce an inv7 phantom commit on some seed");
}

/// The guarded mechanism (what uc2_node actually implements) survives the
/// storm: 200 seeds green at the config's 700 ppm — the heaviest rate below
/// the documented benign strict-inv2 laggard onset (800 ppm, fires in BOTH
/// arms). Post-Finding-#6b the red twin above runs at 1000 ppm (the §5.4.2
/// clamp pushed the genuine C-1 phantom past this arm's ceiling — see its
/// doc comment); this arm deliberately stays at the shared-config rate.
#[test]
fn mechanism_guarded_survives_the_same_storm() {
    for seed in 0..200 {
        let mut cfg = nasty_reconcile_config(seed);
        cfg.data_plane = DataPlane::Mechanism { reopen_guard: true, handle_keyed: true };
        World::new(cfg).run().unwrap_or_else(|v| panic!("seed {seed}: {v:?}"));
    }
}

/// T5's deferred third pin, STRENGTHENED 2026-08-16: a forged/corrupt raw
/// report above the sender's real durable must never silently certify bytes no
/// quorum holds (RawM3; `inject_report` makes it expressible).
///
/// The contract changed with the report-slot fix. Slots used to be high-water
/// marks, so ONE forged report poisoned the leader's ranking permanently and
/// the phantom commit was inevitable — this pin asserted exactly that. Slots
/// now hold the follower's LATEST report (a durable genuinely regresses when a
/// follower truncates), which also makes a one-shot forgery SELF-CORRECTING:
/// the sender's next honest report overwrites it. Both halves are pinned here.
#[test]
#[allow(clippy::field_reassign_with_default)] // pin kept verbatim per the task brief
fn raw_m3_forged_report_phantom_commit_is_caught() {
    // (a) One-shot forgery: corrected by the next honest report, no violation.
    let mut cfg = SimConfig::default();
    cfg.data_plane = DataPlane::RawM3;
    let mut w = World::new(cfg);
    w.run_until_leader().unwrap();
    let leader = w.current_leader().unwrap();
    let f = w.majority_excluding(leader)[0];
    w.inject_report(f, w.node_term(leader), 1 << 30); // far beyond any real durable
    assert!(
        w.run_steps(2_000).is_ok(),
        "a single forged report must be overwritten by the sender's next honest \
         report, not latched into the ranking forever"
    );

    // (b) SUSTAINED forgery still reaches — and is caught as — a phantom
    // commit: the safety detector is intact, only the one-shot case healed.
    let mut cfg2 = SimConfig::default();
    cfg2.data_plane = DataPlane::RawM3;
    let mut w2 = World::new(cfg2);
    w2.run_until_leader().unwrap();
    let leader2 = w2.current_leader().unwrap();
    let f2 = w2.majority_excluding(leader2)[0];
    let mut caught = None;
    for _ in 0..40 {
        let term = w2.node_term(leader2);
        w2.inject_report(f2, term, 1 << 30);
        if let Err(v) = w2.run_steps(50) {
            caught = Some(v);
            break;
        }
    }
    assert!(caught.is_some(), "a persistently forged durable must trip an invariant");
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
    cfg.data_plane = DataPlane::Mechanism { reopen_guard: true, handle_keyed: true };
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
            data_plane: DataPlane::Mechanism { reopen_guard: true, handle_keyed: true },
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed} (Mechanism): {v}");
        }
    }
    // M14b: the same seeds with one node's report capped from the first
    // leader on — a capped MINORITY under drops/dups/crashes. Every invariant
    // (inv10 included) must hold; liveness is not asserted here (the capped
    // node may be the leader, which never reports).
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
        if let Err(v) = w.run_until_leader() {
            panic!("seed {seed} (capped): {v}");
        }
        let capped = (seed % 3) as usize;
        w.set_apply_ceiling(capped, Some(w.max_commit() + 96));
        if let Err(v) = w.run() {
            panic!("seed {seed} (capped node {capped}): {v}");
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
    //
    // Onset re-tune (Finding #5/#6 fixes): mirroring the receiver's 20ms
    // AppendPosition report floor into the Mechanism plane made follower reports
    // DENSER, which lowered the benign-transient onset — the old 30_000ppm drop
    // began tripping the strict-inv2 laggard transient at seed 629 (verified
    // bit-identical in BOTH guard arms, step 7360, inv4/inv5/inv7 all clean).
    // Onset now sits in (20_000, 30_000]; 20_000ppm clears all 1000 seeds with a
    // ~33% margin. inv2's strictness is NOT weakened (it is exactly what caught
    // Finding #6b) — only the loss rate is kept below the (now denser) onset.
    for seed in 0..1000u64 {
        let mut w = World::new(SimConfig {
            n_nodes: if seed % 4 == 0 { 5 } else { 3 },
            seed,
            max_steps: 20_000,
            drop_per_million: 20_000,
            dup_per_million: 10_000,
            crash_per_million: 500,
            data_plane: DataPlane::Mechanism { reopen_guard: true, handle_keyed: true },
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
    assert!(w.run_until(|w| all_live_at_version(w, 5, 1)).unwrap(), "v1 adopted everywhere timed out");

    // Fault between steps: crash a follower (auto-restarts).
    let l = w.current_leader().unwrap();
    let f = (0..4).find(|&i| i != l).unwrap();
    w.crash(f);
    w.run_steps(1_000).expect("ride out the crash");

    // promote the caught-up learner 4 -> voter (quorum grows to 5).
    assert_eq!(propose_ok(&mut w, ConfigOp::PromoteLearner { id: 4 }), 2);
    assert!(w.run_until(|w| all_live_at_version(w, 5, 2)).unwrap(), "v2 adopted everywhere timed out");

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
    assert!(w.run_until(|w| all_live_at_version(w, 5, 3)).unwrap(), "v3 adopted everywhere timed out");

    // remove-learner 4: on adopting the config that drops it, 4 fail-stops.
    assert_eq!(propose_ok(&mut w, ConfigOp::RemoveLearner { id: 4 }), 4);
    assert!(w.run_until(|w| w.halted_removed(4)).unwrap(), "4 adopts its own removal (timed out)");
    assert!(w.halted_removed(4), "the removed learner must halt");

    // remove-voter: a NON-leader voter; it halts on adoption too.
    let l = w.current_leader().unwrap();
    let t = (0..4).find(|&i| i != l && !w.halted_removed(i)).unwrap();
    assert_eq!(propose_ok(&mut w, ConfigOp::RemoveVoter { id: t as u32 }), 5);
    assert!(w.run_until(|w| w.halted_removed(t)).unwrap(), "t adopts its own removal (timed out)");
    assert!(w.halted_removed(t), "the removed voter must halt");

    // The survivors converge on v5 and keep committing.
    assert!(
        w.run_until(|w| all_live_at_version(w, 5, 5)).unwrap(),
        "v5 adopted on survivors timed out"
    );
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

/// T9 integration-catch coverage (M7 Task 9 fix): a SECOND process admitted
/// entirely after prior config history already exists — the exact
/// `resize_3_to_5_to_3` shape (`uc2_node/tests/reconfig.rs`) that caught the
/// production `adopt_config` self-removal-latch bug, reproduced at the sim
/// level. Nodes 3 and 4 are REAL, ticking sim processes that start entirely
/// OUTSIDE the genesis `ClusterConfig` (`SimConfig::genesis_absent`) — not
/// even learners — mirroring a real process that hasn't joined yet. Node 3
/// is added and promoted first (v1, v2); node 4 is added ONLY afterward
/// (v3), so its catch-up must genuinely replay v1 and v2 — both of which
/// exclude ITS OWN id — before it ever reaches v3, the version that finally
/// admits it. The old absence-based predicate would have wrongly latched
/// `self_removed` on v1/v2 and permanently fail-stopped node 4 before it
/// ever got a chance to adopt its own admission or promotion; asserted
/// directly via `halted_removed`, not just convergence timing — a
/// convergence-only check would falsely "pass" (`all_live_at_version`
/// exempts halted nodes from the version check).
#[test]
fn second_learner_admitted_after_prior_config_history_converges() {
    let mut w = World::new(SimConfig {
        n_nodes: 5,
        seed: 61,
        max_steps: 400_000,
        drop_per_million: 0,
        genesis_absent: vec![3, 4],
        ..SimConfig::default()
    });
    w.run_until_leader().expect("elect among the 3 genesis voters");
    w.run_steps(300).expect("a genuine committed prefix");

    // v1: admit node 3 as a learner. It must not halt (it is being ADDED,
    // not removed) — and node 4 (still entirely genesis-absent) must be
    // untouched by a version that doesn't concern it. Convergence is checked
    // over nodes {0,1,2,3} only (`all_live_at_version(w, 4, ..)`): node 4
    // legitimately stays at config_version 0 until v3 ever reaches it, so
    // including it in the "all nodes at v1" predicate would make it
    // unsatisfiable. The `assert!` on `run_until`'s `Ok(bool)` (task-12
    // ledger x) is now what catches that; the follow-up `all_live_at_version`
    // assert stays as a stronger, more specific message on failure.
    assert_eq!(propose_ok(&mut w, ConfigOp::AddLearner { id: 3, addr: (3, 1) }), 1);
    assert!(
        w.run_until(|w| all_live_at_version(w, 4, 1)).unwrap(),
        "invariants while converging on v1 timed out"
    );
    assert!(all_live_at_version(&w, 4, 1), "v1 must actually converge on nodes 0-3");
    assert!(!w.halted_removed(3), "node 3's own admission must not halt it");
    assert!(!w.halted_removed(4), "node 4 (still genesis-absent) must not be affected by v1");

    // v2: promote node 3 to voter.
    assert_eq!(propose_ok(&mut w, ConfigOp::PromoteLearner { id: 3 }), 2);
    assert!(
        w.run_until(|w| all_live_at_version(w, 4, 2)).unwrap(),
        "invariants while converging on v2 timed out"
    );
    assert!(all_live_at_version(&w, 4, 2), "v2 must actually converge on nodes 0-3");
    assert!(!w.halted_removed(3));
    assert!(!w.halted_removed(4), "node 4 must still be unaffected before its own admission");

    // v3: admit node 4 — the SECOND joiner, AFTER v1/v2 already exist. Its
    // catch-up must replay v1 (voters {0,1,2}, learner [3] — 4 legitimately
    // absent) and v2 (3 promoted — 4 still legitimately absent) before it
    // ever reaches v3. This is the exact bug shape: the old code would
    // wrongly halt node 4 while it replays v1/v2, and it would never reach
    // v3 at all. Node 4 is now part of the relevant set.
    assert_eq!(propose_ok(&mut w, ConfigOp::AddLearner { id: 4, addr: (4, 1) }), 3);
    assert!(
        w.run_until(|w| all_live_at_version(w, 5, 3)).unwrap(),
        "invariants while converging on v3 timed out"
    );
    assert!(
        !w.halted_removed(4),
        "node 4 must not halt while replaying pre-admission config history (v1/v2)"
    );
    assert!(all_live_at_version(&w, 5, 3), "v3 must actually converge on all 5 nodes");
    assert_eq!(w.node_config_version(4), 3, "node 4 must genuinely reach v3, not stall on replay");

    // v4: promote node 4 too — proves it kept participating normally after
    // admission (not just limping along at v3 with a dead latch waiting to
    // fire on the next higher-term event).
    assert_eq!(propose_ok(&mut w, ConfigOp::PromoteLearner { id: 4 }), 4);
    assert!(
        w.run_until(|w| all_live_at_version(w, 5, 4)).unwrap(),
        "invariants while converging on v4 timed out"
    );
    assert!(all_live_at_version(&w, 5, 4), "v4 must actually converge on all 5 nodes");
    assert!(!w.halted_removed(4), "node 4 must not halt on its own promotion either");

    // The 5-voter cluster (now {0,1,2,3,4}) keeps committing.
    let c = w.max_commit();
    w.run_steps(2_000).expect("the grown cluster keeps serving");
    assert!(w.max_commit() > c, "commit must keep advancing under the grown 5-voter config");
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

/// M7 Task 8 (controller amendment carry #4): a LEADER proposes its own
/// removal — it keeps serving through the adoption window (`halted_removed`
/// stays false, it is still the raw/serving leader), then steps down
/// (`Action::StepDownRemoved` -> `halted_removed`) the instant the removal
/// entry itself commits — never before. The surviving C_new voters (the
/// other 2 of the original 3) elect a new leader and keep committing. All
/// safety invariants (inv1-9) stay green throughout (the oracle sweep runs
/// after every scripted step and every natural step via `step_once`).
#[test]
fn leader_self_removal_steps_down_after_commit_and_c_new_elects() {
    let mut w = World::new(SimConfig { drop_per_million: 0, ..base_cfg(21) });
    w.run_until_leader().expect("elect L1");
    let l1 = w.current_leader().unwrap();
    w.run_steps(300).expect("a genuine committed prefix");

    assert_eq!(propose_ok(&mut w, ConfigOp::RemoveVoter { id: l1 as u32 }), 1);
    // Removing itself: the leader keeps serving through the adoption window —
    // it must NOT halt yet (Task 8's whole point: C_new must be replicated by
    // a leader that still exists).
    assert!(!w.halted_removed(l1), "self-removing leader must not halt at adoption");
    assert!(w.node_is_serving_leader(l1), "leader keeps serving pre-commit");

    // Once the removal entry itself commits, the leader steps down.
    assert!(
        w.run_until(|w| w.halted_removed(l1)).unwrap(),
        "leader steps down once its own removal commits (timed out)"
    );
    assert!(
        !w.node_is_raw_leader(l1),
        "the stepped-down leader must no longer be a live raw leader"
    );

    // C_new (the surviving 2 voters) elects a new leader and keeps committing.
    w.run_until_leader().expect("C_new elects a new leader");
    let l2 = w.current_leader().unwrap();
    assert_ne!(l2, l1, "the new leader must be one of the survivors");
    let c = w.max_commit();
    w.run_steps(2_000).expect("the shrunken cluster keeps serving");
    assert!(w.max_commit() > c, "commit must keep advancing under the new (2-voter) config");
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
    assert!(
        w.run_until(|w| {
            w.node_durable(l1) >= p1 && (0..3).any(|i| i != l1 && w.node_is_serving_leader(i))
        })
        .unwrap(),
        "frame durable on L1; new leader elected without it (timed out)"
    );
    assert_eq!(w.node_config_version(l1), 1, "isolated ex-leader adopted its own frame");
    let l2 = (0..3).find(|&i| i != l1 && w.node_is_serving_leader(i)).unwrap();
    w.run_steps(400).expect("majority commits past the old prefix under v0");
    assert_eq!(w.node_config_version(l2), 0, "the frame never reached the majority");

    // Heal: reconciliation truncates L1's divergent tail (strictly below the
    // frame end) -> the SM reverts to v0 and the durable record follows.
    w.heal();
    assert!(
        w.run_until(|w| w.truncations() >= 1).unwrap(),
        "the divergent tail truncates (timed out)"
    );
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
    assert!(
        w.run_until(|w| w.node_config_version(l1) == 1).unwrap(),
        "L1 re-adopts from the new stream (timed out)"
    );
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
    assert!(
        w.run_until(|w| {
            w.node_durable(l1) >= p1 && (0..3).any(|i| i != l1 && w.node_is_serving_leader(i))
        })
        .unwrap(),
        "frame durable on L1; new leader elected without it (timed out)"
    );
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
    assert!(
        w.run_until(|w| w.node_durable(l1) >= p1 && w.node_config_version(4) == 1).unwrap(),
        "C1 durable on L1 and adopted by 4 (timed out)"
    );
    // Phase 2: L1 crashes right after proposing; 4 goes dark holding C1.
    w.crash(l1);
    w.partition_node(4);
    // Phase 3: a new leader among the three clean voters. Gate OFF: it
    // proposes RemoveVoter{L1} immediately — its NewTerm can NEVER commit
    // under C_old (L1 down + c dark = at most 2 of quorum 3), so with the
    // gate this proposal would be refused forever.
    assert!(
        w.run_until(|w| (0..4).any(|i| i != l1 && w.node_is_raw_leader(i))).unwrap(),
        "a new raw leader (timed out)"
    );
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
    assert!(
        w.run_until(|w| w.node_config_version(b) == 1 && w.max_commit() >= p2).unwrap(),
        "C1' commits under its own shrunken quorum (timed out)"
    );
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
    assert!(
        w.run_until(|w| (0..4).any(|i| i != l1 && w.node_is_raw_leader(i))).unwrap(),
        "a new raw leader (timed out)"
    );
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

/// Finding #5 (lean leader-completeness effort, gate doc 2026-07-16): a voter
/// that GRANTED a term-T vote (persisted), holds a divergent tail, and crashes
/// BEFORE reconciling into term T must NOT certify a phantom commit after it
/// reboots. Pre-fix, `uc2_node` booted the intake gate OPEN (`node.rs` 516,
/// `awaiting_reconcile: false` at 801) while term recovery is
/// `max(vote_term, map_term)` (`election.rs` 400-402) — so the rebooted voter
/// reports `(term T, raw divergent durable)` on the receiver's 20 ms
/// AppendPosition floor (`receiver.rs` 1052-1078) before the leader's 100 ms
/// idle map re-ship can reconcile it, and the T-leader's tracker certifies a
/// commit over content the reporter does not hold.
///
/// DETERMINISTIC SCRIPT (Mechanism = the shipped node's gate discipline):
/// - V leads term 1, all three commit a genuine prefix; V is then isolated
///   pairwise and keeps appending an uncommitted term-1 divergent tail.
/// - The majority elects a term-2 leader L2 and commits past the old prefix.
/// - L2 is cut from the third voter F; V<->F is opened. F (last_term 2)
///   out-ranks V (last_term 1) lexicographically, so the ensuing election
///   churn settles on F winning a term T > 2 **with V's persisted grant**.
/// - V crashes at the grant — before any term-T map reaches it — and reboots:
///   recovered term T (the vote), term map still ending at term 1.
/// - The race: V's report floor (archive cadence) vs F's 100 ms map floor.
///   Pre-fix the boot-open gate ships `(T, divergent durable)` and F rank-
///   commits its term-T NewTerm frame with quorum {F, V} — but V's content
///   diverges from F's lineage right above the term-2 base, and the third
///   voter (L2) holds F's lineage only up to F's election base: the inv7
///   phantom oracle flags the commit. Post-fix (gate boots CLOSED iff
///   vote_term > map_term) the report is suppressed, F's map reconciles V
///   (one truncation), and the cluster resumes committing genuinely.
///
/// This is the permanent regression pin for the fix: the run must stay GREEN,
/// V must reconcile, and commit must advance genuinely afterwards.
#[test]
fn rebooted_unreconciled_voter_must_not_certify_phantom_commit() {
    let mut w = World::new(SimConfig {
        drop_per_million: 0,
        data_plane: DataPlane::Mechanism { reopen_guard: true, handle_keyed: true },
        max_steps: 200_000,
        ..base_cfg(3)
    });
    // Phase 1: V leads term 1; a genuine committed prefix lands on all three.
    w.run_until_leader().expect("setup: elect the term-1 leader");
    let v = w.current_leader().unwrap();
    w.run_steps(300).expect("setup: genuine committed prefix on all three");
    let others: Vec<usize> = (0..3).filter(|&i| i != v).collect();
    let (a, b) = (others[0], others[1]);

    // Phase 2: isolate V pairwise (so a precise partial heal is possible).
    // V keeps appending + archiving an uncommitted term-1 divergent tail while
    // the majority elects term 2 and commits past the old prefix.
    w.partition(v, a);
    w.partition(v, b);
    assert!(
        w.run_until(|w| w.current_leader().is_some_and(|l| l != v)).unwrap(),
        "setup: term-2 leader election timed out"
    );
    let l2 = w.current_leader().unwrap();
    let f = if l2 == a { b } else { a };
    w.run_steps(300).expect("setup: term-2 commits past the old prefix; V's tail grows");

    // Phase 3: cut L2 from F and open V<->F. F times out and campaigns; V
    // grants (F's last_term 2 > V's 1, lexicographic vote order) and persists
    // the vote. The churn may take an extra round (V's own doomed candidacy
    // bumps terms), but only F can assemble a quorum ({F, V}), so the run
    // settles on F as a raw leader of some term T > 2.
    w.partition(l2, f);
    w.unpartition(v, f);
    assert!(
        w.run_until(|w| w.node_is_raw_leader(f) && w.node_term(f) > 2).unwrap(),
        "setup: F wins a term with V's grant (timed out)"
    );
    let t = w.node_term(f);
    assert_eq!(w.node_term(v), t, "V granted (and adopted) F's winning term");
    // 96 = one sim frame: V's frontier must sit beyond F's whole post-win
    // append (base + NewTerm frame), so F has nothing to ship at V's frontier
    // and the ONLY datagram V can contribute is its AppendPosition report.
    assert!(w.node_durable(v) > w.node_durable(f) + 96, "V's divergent durable outruns F's base");

    // Phase 4: crash V at the grant — its vote at term T is persisted, its
    // term map still ends at term 1, and NO term-T map has reached it. Let
    // F's first idle-floor map ship fire into the void (V is down), so the
    // post-reboot race window is maximal and deterministic.
    w.crash(v);
    w.run_steps(60).expect("setup: F's initial gossip is dropped at the dark V");

    // Phase 5: reboot V. Recovery: term = max(vote T, map last 1) = T.
    // Pre-fix the intake gate boots OPEN and V's report floor ships
    // (T, divergent durable) to F before F's next 100 ms map re-ship — the
    // phantom-commit trace this pin exists for. Post-fix the gate boots
    // CLOSED (vote_term > map_term), the report is suppressed, and the map
    // reconciles V first.
    let truncations_before = w.truncations();
    let commit_before = w.max_commit();
    w.restart(v).expect("reboot the unreconciled voter");
    w.run_steps(4_000)
        .expect("Finding #5: rebooted unreconciled voter's report must not certify a phantom commit");

    // Liveness of the closed-gate boot: the leader's idle-floor map reconciles
    // V (one extra reconcile round — the divergent tail truncates), and the
    // cluster then commits GENUINELY past the pre-reboot high-water.
    assert!(
        w.truncations() > truncations_before,
        "the rebooted voter must reconcile (truncate its divergent tail)"
    );
    assert!(
        w.run_until(|w| w.max_commit() > commit_before).unwrap(),
        "commit must resume genuinely after reconciliation (timed out)"
    );
}

/// Finding #6b (lean leader-completeness effort, gate doc 2026-07-16): Raft
/// §5.4.2 / Figure 8 — a leader must NEVER commit a prior-term range by
/// counting replicas; the commit may only advance once the current term's
/// NewTerm frame (`new_term_pos`) is quorum-durable. Pre-fix,
/// `election.rs::rank_leader` pushed `Action::AdvanceCommit` UNCONDITIONALLY
/// off the positions-only `CommitTracker`: `new_term_pos` gated only
/// reads/ingress/M7 (`serving`), never the commit store. At every failover
/// inheriting an uncommitted tail, followers reconcile clean and their
/// gate-open AppendPosition floor reports the election base BEFORE the
/// NewTerm frame is quorum-durable — so the OLD-TERM-ONLY range commits
/// (acks/apply/outputs fire below the §5.4.2 barrier), and a divergent
/// higher-lastTerm rival can then win the next term with a commit-quorum
/// member's grant and truncate the committed bytes cluster-wide (the loss
/// continuation, machine-checked as the 46-step Lean countermodel
/// `finding_fig8_old_term_commit_data_loss`, deleted with the fix).
///
/// DETERMINISTIC SCRIPT (5 nodes, Mechanism = the shipped gate discipline):
/// - t1: leader L commits a genuine prefix on all five; then {L, A} are
///   partitioned from {B, C, R'} and L keeps serving — an uncommitted
///   term-1 tail W grows on {L, A} only.
/// - t2: the majority-side trio elects a rival R (equal-credential grants);
///   R is isolated the instant it wins, so its term-t2 NewTerm frame stays
///   local: R holds the divergent higher-stamped map entry `(t2, r_base)`
///   with `last_term = t2` while everyone else is still at `last_term 1`.
/// - t3: W is grown far past R's frame, the cluster quiesced, C isolated,
///   and B reconnected to {L, A}; the ensuing churn re-elects L (or A —
///   both hold the full W tail) at a term T > t2, base = the tail end P_W.
/// - The window: B reconciles CLEAN against the T-leader's map (its log is
///   a pristine prefix), its intake gate reopens, and its 20 ms-analog
///   report floor ships its rising durable at term T while it re-replicates
///   the term-1 tail — long before the T NewTerm frame at [P_W, P_W+96) is
///   quorum-durable. Pre-fix `rank_leader` certifies the OLD-TERM range as
///   those reports land; the moment the commit crosses R's divergence base,
///   the inv2 sweep flags the live rival's higher-stamped boundary sitting
///   below the committed high-water — the §5.4.2 violation, caught by an
///   existing oracle at the exact violating `AdvanceCommit`.
///
/// ORACLE NOTE (why inv2, not inv5/inv4): the loss continuation — R winning
/// t4 off a commit-quorum member's grant (inv5: `base < global_max_commit`)
/// and truncating the committed byte (inv4) — is structurally UNREACHABLE
/// behind inv2 in this sim: `check_prefix_consistency` sweeps every node's
/// persisted map after EVERY event, so the very `AdvanceCommit` that first
/// carries gmc past the rival's divergent `(t2, r_base)` boundary reds the
/// run before any later election or truncation event can occur. inv2 is
/// the earliest existing oracle for this class; the t4 continuation is
/// pinned by the Lean countermodel instead (kernel-`decide`d, n = 5).
///
/// This is the permanent regression pin for the rank_leader clamp: the run
/// must stay GREEN, the commit must stay FROZEN until the NewTerm frame is
/// quorum-durable (asserted directly), the rival must reconcile, and the
/// commit must then advance past the whole tail + NewTerm frame.
#[test]
fn old_term_range_must_not_commit_before_new_term_quorum() {
    let mut w = World::new(SimConfig {
        n_nodes: 5,
        seed: 3,
        max_steps: 400_000,
        drop_per_million: 0,
        data_plane: DataPlane::Mechanism { reopen_guard: true, handle_keyed: true },
        ..SimConfig::default()
    });
    // Phase 1: elect the term-1 leader; a genuine committed prefix lands on
    // all five voters.
    w.run_until_leader().expect("setup: elect the term-1 leader");
    let l = w.current_leader().unwrap();
    w.run_steps(400).expect("setup: genuine committed prefix on all five");
    assert!(w.max_commit() > 0, "setup: the prefix must genuinely commit");
    let followers: Vec<usize> = (0..5).filter(|&i| i != l).collect();
    let a = followers[0];
    let trio = [followers[1], followers[2], followers[3]];

    // Phase 2: partition {L, A} | {trio}. L keeps serving into A alone —
    // the uncommitted term-1 tail W. The trio times out and elects the
    // rival on equal credentials.
    for &g in &trio {
        w.partition(l, g);
        w.partition(a, g);
    }
    assert!(
        w.run_until(|w| trio.iter().any(|&g| w.node_is_raw_leader(g))).unwrap(),
        "setup: the trio must elect the rival (timed out)"
    );
    let r = trio.iter().copied().find(|&g| w.node_is_raw_leader(g)).unwrap();
    let t2 = w.node_term(r);
    // The rival's divergent map entry base: its durable at the win (its
    // NewTerm frame is appended above it and archives only later).
    let r_base = w.node_durable(r);
    // Isolate R the instant it wins: its term-t2 frame/map never reach the
    // other two (partitions are consulted at delivery).
    let (b, c) = {
        let rest: Vec<usize> = trio.iter().copied().filter(|&g| g != r).collect();
        (rest[0], rest[1])
    };
    w.partition(r, b);
    w.partition(r, c);
    // Grant-order guarantee: the granters' durables (and hence their last
    // reports, and hence the stale leader's rank) never exceed the rival's
    // election base — nothing above r_base is committed while W grows.
    assert!(w.max_commit() <= r_base, "setup: stale commit must not pass the rival's base");

    // Phase 3 prep: grow W far past the rival's frame (the drain of this
    // tail at t3 is the deterministic §5.4.2 window), then quiesce and let
    // {L, A} drain fully so the t3 election base is the frozen tail end.
    assert!(
        w.run_until(|w| w.node_append(l) >= r_base + 96 * 600).unwrap(),
        "setup: the term-1 tail must grow past the rival's frame (timed out)"
    );
    w.set_quiet(true);
    assert!(
        w.run_until(|w| {
            let pw = w.node_append(l);
            w.node_durable(l) == pw && w.node_durable(a) == pw
        })
        .unwrap(),
        "setup: L and A must drain the tail fully (timed out)"
    );
    let pw = w.node_append(l);
    assert!(pw > r_base + 96, "setup: W must extend past the rival's divergent frame");

    // Phase 3: silence C entirely (its lonely candidacies must not perturb
    // the t3 term), reconnect B to {L, A}. B's floor reports depose the
    // stale term-1 leader; the churn settles on L or A (the only logs that
    // out-rank everyone) winning a term T > t2 at base P_W.
    w.partition(b, c);
    w.unpartition(l, b);
    w.unpartition(a, b);
    assert!(
        w.run_until(|w| {
            (w.node_is_raw_leader(l) && w.node_term(l) > t2)
                || (w.node_is_raw_leader(a) && w.node_term(a) > t2)
        })
        .unwrap(),
        "setup: L or A must re-win above the rival's term (timed out)"
    );
    let t3l = if w.node_is_raw_leader(l) { l } else { a };
    assert_eq!(w.node_durable(t3l), pw, "the t3 election base is the frozen tail end");
    let commit_frozen = w.max_commit();
    assert!(commit_frozen <= r_base, "nothing above the rival's base is committed yet");

    // THE PIN. B reconciles clean, reopens its gate, re-replicates the
    // term-1 tail, and floor-reports its rising durable at term T. Pre-fix
    // rank_leader commits the old-term range as those reports land — RED
    // (inv2, at the exact AdvanceCommit that crosses r_base). Post-fix the
    // clamp suppresses every advance until ranked >= new_term_pos, so the
    // commit stays FROZEN through this whole window.
    w.run_steps(1_500).expect(
        "Finding #6b: an old-term-only range must not commit before the \
         NewTerm frame is quorum-durable",
    );
    assert_eq!(
        w.max_commit(),
        commit_frozen,
        "Raft 5.4.2 clamp: no commit may advance before the current term's \
         NewTerm frame is quorum-durable"
    );

    // Liveness of the clamp: heal the rival inside the clamp window — the
    // T-leader's idle-floor map reconciles it (one truncation of the
    // divergent t2 frame, at exactly r_base >= its committed high-water).
    // ORDER MATTERS: the rival must reconcile (heal) BEFORE the clamp
    // releases and commit resumes below — inv2 is deliberately strict and
    // would fire benignly on a commit that advances while the rival's
    // divergent boundary is still live. Do not reorder heal after release.
    let truncs_before = w.truncations();
    w.unpartition(l, r);
    w.unpartition(a, r);
    assert!(
        w.run_until(|w| w.truncations() > truncs_before).unwrap(),
        "the healed rival must reconcile (truncate its divergent frame)"
    );
    // Once the NewTerm frame is quorum-durable the clamp releases: the
    // commit advances past the WHOLE term-1 tail + the NewTerm frame in one
    // certification — W is committed under the 5.4.2 barrier and survives.
    assert!(
        w.run_until(|w| w.max_commit() >= pw + 96).unwrap(),
        "commit must resume past the tail + NewTerm frame once quorum-durable (timed out)"
    );
}

// ================= Task 12 ledger: run_until timeout signal + parked violation =================

/// Ledger minor (x): `run_until` must distinguish "predicate held" from
/// "budget exhausted first" — the old `Ok(())` collapsed both into a single
/// success value, so a scenario whose phase silently timed out could still
/// read as green.
#[test]
fn run_until_reports_timeout_distinctly() {
    let mut w = World::new(SimConfig { n_nodes: 3, seed: 99, max_steps: 50, ..SimConfig::default() });
    let held = w.run_until(|_| false).unwrap();
    assert!(!held, "an unsatisfiable predicate must report Ok(false), not silent success");
    let held = w.run_until(|_| true).unwrap();
    assert!(held, "an already-true predicate must report Ok(true)");
}

/// Ledger minor (g): a violation parked by a scripted self-feed whose
/// signature cannot return it (`propose_config` at world.rs:1803, taken by
/// `step_once` at world.rs:662) must not be dropped if the caller's very next
/// `run`/`run_until*` call never steps because its predicate/budget is
/// already satisfied on entry. Reached via `test_only_park_violation`
/// (world.rs) rather than a real inv9 trace: on the shipped SM, no legal
/// `ConfigOp` sequence produces a self-inconsistent adopted config, so the
/// only way to exercise the parked-violation state deterministically and
/// cheaply is to park one directly — the real self-feed call site
/// (`propose_config`) is otherwise unreachable-by-design in a healthy run.
#[test]
fn parked_violation_surfaces_without_a_step() {
    let mut w = World::new(SimConfig { n_nodes: 3, seed: 7, max_steps: 50, ..SimConfig::default() });
    w.test_only_park_violation(InvariantViolation {
        invariant: "test-parked (task-12 ledger g)",
        step: 0,
        seed: 7,
        detail: "synthetic parked violation".to_string(),
    });
    assert!(
        w.run_until(|_| true).is_err(),
        "ledger (g): a parked violation must not be dropped when pred is already true"
    );
}



fn drive_to_candidate_lagged(seed: u64, handle_keyed: bool) -> Option<(World, usize, u32)> {
    let mut w = World::new(SimConfig {
        drop_per_million: 0,
        data_plane: DataPlane::Mechanism { reopen_guard: true, handle_keyed },
        max_steps: 2_000_000,
        ..base_cfg(seed)
    });
    w.run_until_leader().ok()?;
    let l1 = w.current_leader()?;
    w.run_steps(300).ok()?;
    let others: Vec<usize> = (0..3).filter(|&i| i != l1).collect();
    let (a, b) = (others[0], others[1]);
    w.partition(l1, a);
    w.partition(l1, b);
    w.run_until(|w| w.current_leader().is_some_and(|l| l != l1)).ok()?;
    let l2 = w.current_leader()?;
    let f = if l2 == a { b } else { a };
    w.run_steps(300).ok()?;
    w.partition(l2, f);
    w.unpartition(l1, f);
    // Step finely; the instant f is a closed-gate follower at term >= 3, isolate
    // it so it times out into candidacy with the handle frozen at that term.
    let mut adopted = 0u32;
    for _ in 0..2000 {
        w.run_steps(10).ok()?;
        if !w.node_is_candidate(f) && w.node_adopted_term(f) >= 3 && !w.node_intake_gate(f) {
            adopted = w.node_adopted_term(f);
            break;
        }
    }
    if adopted == 0 { return None; }
    w.heal();
    w.partition_node(f);
    for _ in 0..2000 {
        w.run_steps(10).ok()?;
        if w.node_is_candidate(f) && w.node_adopted_term(f) == adopted
            && w.node_term(f) > adopted && !w.node_intake_gate(f) {
            let ct = w.node_term(f);
            return Some((w, f, ct));
        }
    }
    None
}

/// Finding #9 (lean LC2 — candidate cross-stream accept; adjudicated a REAL
/// reachable acked-write-loss gap, §5.4.2 / #6b family): the intake-gate REOPEN
/// was keyed to `current_term`, not the data-plane term handle the receiver
/// filters DATA at. A CANDIDATE's handle lags its `StartElection`-bumped
/// `current_term` (StartElection stores no handle), so a candidate that cleanly
/// reconciles a co/higher-term map REOPENED intake for its stale handle-term
/// stream — the door through which a cross-stream old-term byte is then accepted
/// (the acked-write-loss source). The fix keys BOTH reopen arms (clean reconcile
/// in `feed`, truncation ack in `on_truncated`) to `current_term == adopted_term`
/// (== the handle); a candidate never reopens.
///
/// Directed twin over the SAME reachable state (`drive_to_candidate_lagged`: a
/// lagged-handle candidate — handle < current_term, gate CLOSED — reached by
/// natural elections + partitions), asserting the ROOT MECHANISM the fix changes:
///   - RED (`handle_keyed:false`, the counterfactual): the candidate cleanly
///     reconciles the candidate-term leader's map (identity, no truncation) and
///     the gate WRONGLY REOPENS — the exact violating event that lets a
///     cross-stream byte in.
///   - GREEN (`handle_keyed:true`, shipped): the gate STAYS CLOSED on the same
///     reconcile, and the candidate still CONVERGES once unpartitioned (liveness:
///     no stranded candidate — it adopts a real leader's term / steps down).
///
/// Scope note (two deeper couplings the sim cannot cheaply cross — the reopen is
/// the fix's exact lever, so it is what this pins; the end-to-end acked-write-loss
/// is proved authoritatively by the machine-checked Lean countermodel
/// `finding_candidate_gate_reopen_fca_violation`, n=5, 56 steps):
///   (1) the sim's DATA-accept path keys on `current_term`, not the term handle
///       (`deliver` `Msg::Data`: `if term == cur`), so a current-term Data frame
///       carries a `LeaderSeen` that RESOLVES the candidacy (adopts the term as a
///       clean follower) before the accept — the sim cannot express a
///       handle-filtered DATA accept while `current_term > handle` (the LC1b
///       header/handle split the Lean model added lives only there);
///   (2) the report-path phantom the reopened gate feeds needs a co-term leader
///       to rank the divergent report — a multi-actor setup the storm does not
///       isolate (the discriminating-seed probe found none: at storm rates every
///       counterfactual catch is the documented benign both-arms inv2 laggard).
/// Working seed: 3. (Was 7 before issue #7 split `SimEvent::ConsensusStep` out
/// of `ArchiveStep`; that added an event per node per ms, which reshuffles every
/// schedule and so re-rolls which seed reaches this state. The state itself — a
/// lagged-handle candidate with the gate closed — is asserted as a precondition
/// below, so a stale seed fails loudly rather than silently testing nothing.)
#[test]
fn finding9_lagged_handle_candidate_reopen_needs_handle_keyed() {
    // ---- RED: the counterfactual reopens the lagged-handle candidate's gate. ----
    let (mut w, f, ct) =
        drive_to_candidate_lagged(3, false).expect("reach the lagged-handle candidate (red)");
    assert!(!w.node_intake_gate(f), "precondition: candidate's gate is closed");
    assert!(
        w.node_adopted_term(f) < w.node_term(f),
        "precondition: the handle ({}) lags current_term ({})",
        w.node_adopted_term(f),
        w.node_term(f)
    );
    let map = w.node_map(f);
    // The candidate-term leader's map (identical to f's -> a clean, no-truncation
    // reconcile). `from` is any peer id; delivery bypasses the partition table.
    let from = (0..3).find(|&i| i != f).unwrap();
    w.inject_term_map(from, f, ct, map.clone()).expect("clean reconcile (red)");
    assert!(
        w.node_intake_gate(f),
        "RED (violating event): the lagged-handle candidate's clean reconcile of a \
         co-term map WRONGLY reopened the intake gate for its stale handle-term stream"
    );

    // ---- GREEN: the shipped fix keeps the gate closed; the candidate converges. ----
    let (mut w, f, ct) =
        drive_to_candidate_lagged(3, true).expect("reach the lagged-handle candidate (green)");
    assert!(!w.node_intake_gate(f), "precondition: candidate's gate is closed");
    assert!(
        w.node_adopted_term(f) < w.node_term(f),
        "precondition: the handle ({}) lags current_term ({})",
        w.node_adopted_term(f),
        w.node_term(f)
    );
    let map = w.node_map(f);
    let from = (0..3).find(|&i| i != f).unwrap();
    w.inject_term_map(from, f, ct, map.clone()).expect("clean reconcile (green)");
    assert!(
        !w.node_intake_gate(f),
        "GREEN: the fix must keep a lagged-handle candidate's gate CLOSED on a clean reconcile"
    );
    // Liveness: unpartition; the candidate must converge (adopt a real leader's
    // term / step down and reconcile), not strand with a permanently shut gate.
    w.heal();
    let converged = w
        .run_until(|w| w.current_leader().is_some() && !w.node_is_candidate(f) && w.node_intake_gate(f))
        .expect("run");
    assert!(converged, "GREEN liveness: the candidate must converge once unpartitioned");
}

/// Finding #9 — F2 twin for the SEPARATE truncating-ack reopen arm
/// (`on_truncated`/`on_truncated_feedback`), a distinct expression at a distinct
/// site from the clean-reconcile arm. A lagged-handle candidate whose reconcile
/// TRUNCATES (a divergent map) must not reopen its stale handle-stream intake
/// when the archive ack lands. Same reachable candidate as the clean twin;
/// inject a DIVERGENT co-term map (`[(1,0),(2,1344),(4,2800)]` vs f's
/// `[(1,0),(2,1344)]` — shared prefix intact, term 4 opening at 2800 truncates
/// f's UNCOMMITTED tail at 2800; a below-committed cut would trip inv4), then
/// let the truncation ack process.
///   - RED (`handle_keyed:false`): the ack REOPENS the candidate's gate (bug).
///   - GREEN (`handle_keyed:true`): the ack leaves it CLOSED.
#[test]
fn finding9_truncating_arm_reopen_needs_handle_keyed() {
    for (handle_keyed, expect_open) in [(false, true), (true, false)] {
        let (mut w, f, ct) = drive_to_candidate_lagged(3, handle_keyed)
            .expect("reach the lagged-handle candidate");
        assert!(!w.node_intake_gate(f), "precondition: candidate's gate is closed");
        assert!(
            w.node_adopted_term(f) < w.node_term(f),
            "precondition: the handle ({}) lags current_term ({})",
            w.node_adopted_term(f),
            w.node_term(f)
        );
        let from = (0..3).find(|&i| i != f).unwrap();
        // A divergent co-term map: f's OWN map as the shared prefix, plus the
        // candidate term opening inside f's uncommitted tail, so reconcile
        // truncates there (produces `Action::Truncate`).
        //
        // DERIVED from f's live state, not hardcoded. The cut must land strictly
        // above f's committed high-water — a below-committed cut is an inv4
        // violation, i.e. a broken test rather than a test of the reopen — and
        // at or below its durable frontier. The original positions were tuned to
        // one seed's trace and silently became wrong the moment the schedule
        // changed (issue #7 added a per-node event, re-rolling every seed).
        let mut map = w.node_map(f);
        let prev_base = map.last().map(|&(_, b)| b).unwrap_or(0);
        let cut = prev_base.max(w.node_commit_high_water(f)) + 1;
        assert!(
            cut <= w.node_durable(f),
            "precondition: f needs an uncommitted tail to cut (cut {cut} > durable {})",
            w.node_durable(f)
        );
        map.push((ct, cut));
        let truncs_before = w.truncations();
        w.inject_term_map(from, f, ct, map).expect("divergent reconcile");
        // Let the archive truncation ack land (on_truncated_feedback runs the
        // truncating-arm reopen check).
        w.run_steps(50).expect("process the truncation ack");
        assert!(
            w.truncations() > truncs_before,
            "the divergent map must actually truncate (arm precondition)"
        );
        assert_eq!(
            w.node_intake_gate(f),
            expect_open,
            "truncating-arm reopen: handle_keyed={handle_keyed} expected gate open={expect_open}"
        );
    }
}

// ---- T13: crypto-plane scenarios ------------------------------------------

/// A 3-node config with a step budget generous enough for the crypto
/// scenarios' tens-of-seconds virtual-time windows: periodic tick (10ms) +
/// archive (5ms) events alone cost ~1800 steps/node/second, before any
/// message traffic.
fn crypto_cfg(seed: u64) -> SimConfig {
    SimConfig { n_nodes: 3, seed, max_steps: 400_000, ..SimConfig::default() }
}

/// Scenario 1 (brief): the Noise `IK` exchange plus its retry/backoff must
/// converge, not livelock, under loss and reorder. `drop_per_million` also
/// drops `Msg::Handshake` datagrams — kinds 18/19/20 ride the SAME lossy
/// `send`/`deliver` path as every consensus message (`World::
/// enable_crypto_plane`'s doc) — and the sim's existing latency jitter
/// (`latency_min_ns..latency_max_ns`, applied per-send) reorders concurrent
/// sends the same way it already does for `Data`/`Ack`/etc., so no separate
/// "reorder" knob is needed.
///
/// `drop_per_million: 50_000` (5%), not the brief's illustrative 20%: at
/// 20% (and even at 10%), this specific test — a full election+replication
/// world, not just the handshake — hits a `check_prefix_consistency`
/// (inv2) firing a few percent of the time. **Confirmed, by the T13
/// review, to be a checker OVER-APPROXIMATION, not a State-Machine-Safety
/// violation**: 13 firings reproduced across three configurations were
/// each checked by hand — SM-Safety held in 13/13, every one healed within
/// 0-10 further steps, and in every case the "divergent" node had
/// committed NOTHING at or above the divergence point (a textbook
/// uncommitted divergent tail correctly awaiting truncation; inv2 is
/// strictly stronger than SM-Safety and does not yet tolerate this
/// transient shape). **Confirmed pre-existing** with crypto entirely OFF —
/// not merely "reachable in principle": reproduced at 80% loss, seed 35,
/// identical shape. **The mechanism is a real, production-faithful design
/// consequence of M8, not a sim artifact**: `scope_of` classifies both
/// `DATA` and `HEARTBEAT` as `Scope::Group`, and `seal_scratch` drops both
/// on `NoGroupKey` — so a freshly-elected leader is silent (no
/// heartbeats either) until its epoch activates, in production as much as
/// here. Measured directly: max term reached at 20% loss is 1.2 without
/// crypto vs. 3.7 with it — roughly 3x the election churn, invisible
/// below ~5%. One mitigating fact: the T17 inherited-clock fix (`mint`'s
/// doc) BOUNDS this — once a process's `active_epoch` latches, that
/// process pays the dark window at most once, not once per election. See
/// `known_red_inv2_over_approximation_at_20pct_loss` below for a
/// pinned standing repro, and the task report ("Concerns") for the full
/// account. **Empirical accuracy note (I-4):** even at the 5% rate used
/// here, this is measurably rare rather than zero — 600 seeds at exactly
/// 5% turned up 2 firings (seeds 82 and 318, both the same shape, both
/// during the very first commit race), i.e. ~0.33%, not clean. The five
/// seeds this file actually pins (1, 11, 13, 17, 21) are individually
/// stable (3/3 repeat runs each came back green, re-verified against this
/// commit) so the suite will not flake in CI, but "narrower" is the honest
/// claim — not "clean."
#[test]
fn handshakes_complete_under_loss_and_reorder() {
    // seed 1, not 7: T13 review (I-2) applied the retry-disabled mutant
    // across 40 seeds at this exact rate and found seed 7 is the one that
    // does NOT discriminate (survives on luck — a pair only needs its one
    // shot at message 1 + message 2 to land). Seed 1 fails under the same
    // mutant (confirmed below and in the task report's re-verification).
    let mut w = World::new(SimConfig { drop_per_million: 50_000, ..crypto_cfg(1) });
    w.enable_crypto_plane(3);
    assert!(
        w.run_until_within(|w| w.all_peer_sessions_established(), 60_000_000_000).unwrap(),
        "every pairwise session must establish within 60s of virtual time despite 5% loss"
    );
}

/// T13 review (I-4): a pinned, standing repro of the `check_prefix_consistency`
/// (inv2) over-approximation documented on
/// `handshakes_complete_under_loss_and_reorder` above — NOT a regression
/// gate for this crate (nothing here is expected to ever be "fixed" by a
/// change in `uc2_sim` or `uc2_crypto`; the candidate fix, if one is ever
/// made, lives in `check_prefix_consistency` itself). `#[ignore]`d because
/// it is EXPECTED to return `Err` — an ordinary `.unwrap()` would abort the
/// whole test binary rather than just fail one test. Run explicitly with
/// `cargo test -p uc2_sim --test scenarios -- --ignored
/// known_red_inv2_over_approximation_at_20pct_loss`.
///
/// Seed 78, not seed 1 or seed 71: this exact repro is sensitive to
/// precisely how many `self.draw()` calls a run consumes (it is a
/// coincidental-timing transient, not a structural one), so a seed pin is
/// only valid against the EXACT code it was swept against. This has now
/// bitten twice — a lesson worth stating plainly rather than repeating
/// silently a third time:
///
/// - The T13 review's own suggested seed (1) stopped reproducing once
///   `CommitGossip` was gated the same as `Data` (a Minor from that same
///   review round — more `send()` calls consumed on a withheld tick shifts
///   every subsequent RNG draw).
/// - The FIRST replacement (71) was swept and verified AGAINST THAT
///   intermediate build, but `CRYPTO_SWEEP_INTERVAL_NS` and
///   `block_key_delivery_to` were reworked (2.5s one-shot-count ->
///   50ms time-window) in the SAME fix round, AFTER seed 71 was picked and
///   "verified" — invalidating it again before it was ever re-checked
///   against the code that actually got committed. The stale "4/4 stable"
///   claim in an earlier draft of this report measured a build that was
///   never the one shipped; it should have been re-run one more time,
///   after the LAST change in the round, not the second-to-last.
///
/// Re-swept 0..150 against the exact code in the commit this test ships
/// with, confirmed multiple independent hits (78, 111, 141), and pinned
/// 78 (5/5 separate `cargo test` process invocations green — i.e. the
/// `Err` fired in all 5 — re-verified again immediately before this
/// commit). If this ever stops firing again: re-sweep AND re-verify
/// against the exact final diff being committed, not an intermediate
/// state, and paste the actual multi-run output into the task report
/// rather than asserting stability from memory.
///
/// If this ever stops firing (the checker's tolerance widened, or the
/// activation-grace design changed), DELETE this test rather than leaving
/// it silently green — a green `#[ignore]`d "known red" is worse than no
/// test at all, since nobody re-checks an ignored test's premise.
#[test]
#[ignore = "expected-red diagnostic (checker over-approximation, not a regression gate) — see doc comment"]
fn known_red_inv2_over_approximation_at_20pct_loss() {
    let mut w = World::new(SimConfig { drop_per_million: 200_000, ..crypto_cfg(78) });
    w.enable_crypto_plane(3);
    let err = w.run_until_within(|_w| false, 60_000_000_000).expect_err(
        "seed 78 @ 20% loss is EXPECTED to trip the inv2 over-approximation; if it no longer \
         does, don't just delete this assert — re-sweep for a fresh repro seed against the \
         EXACT commit being shipped, re-verify with several separate process runs, and update \
         the doc comment on handshakes_complete_under_loss_and_reorder",
    );
    assert!(
        err.invariant.contains("inv2") || err.invariant.contains("prefix"),
        "expected the term-map prefix consistency (inv2) over-approximation, got: {err}"
    );
}

/// Scenario 2 (brief): a node isolated at rotation time misses the epoch;
/// once the partition heals it must converge, not stay permanently unable
/// to open group traffic. `victim` is picked dynamically (whichever node
/// ISN'T the elected leader) rather than hardcoded, since which node an
/// election settles on depends on the SM's own timeout jitter.
#[test]
fn rotation_during_a_partition_converges_once_healed() {
    let mut w = World::new(crypto_cfg(11));
    w.enable_crypto_plane(3);
    assert!(
        w.run_until_within(
            |w| w.all_peer_sessions_established() && w.current_leader().is_some(),
            10_000_000_000
        )
        .unwrap(),
        "pairwise handshakes must complete AND a leader must be elected on a quiet network"
    );

    let leader = w.current_leader().expect("a leader must be elected before scripting the rotation");
    let victim = (0..3).find(|&i| i != leader).expect("a 3-node cluster has a non-leader");

    w.partition_node(victim);
    w.rotate_group_key();
    // Captured for the "must not have it while still partitioned" pin below
    // — see the `heal()` comment for why the CONVERGENCE assertion does not
    // also pin this same value.
    let rotated_epoch = w.current_epoch();
    // Past uc2_crypto::group::ACTIVATION_TIMEOUT_NS (2s) while still
    // partitioned: the leader activates the new epoch via the timeout half
    // of GroupPlane's rule (every OTHER peer already acked instantly; the
    // isolated victim never can).
    w.run_for(3_000_000_000).unwrap();
    assert_ne!(rotated_epoch, 0, "the leader must have actually minted an epoch");
    assert!(
        !w.node_has_group_epoch(victim, rotated_epoch),
        "the isolated node must not have the rotated epoch while still partitioned"
    );

    w.heal();
    // Deliberately checked against `w.current_epoch()` LIVE, not the
    // `rotated_epoch` captured above — found the hard way. A partition long
    // enough to cross the 2s activation grace also leaves the ISOLATED node
    // repeatedly calling elections nobody can answer, inflating its own term
    // by roughly one per election timeout; reconnecting then legitimately
    // triggers a brief, textbook Raft "disruptive server" flurry (several
    // elections in quick succession — nothing this SM does wrong; it is
    // what PreVote exists to soften, and there is no PreVote here) during
    // which the leader can mint AGAIN before the redelivery sweep
    // (`GroupPlane::unacked_peers`/`redeliver_to`, T12) reaches `victim`
    // with `rotated_epoch` specifically. By design `redeliver_to` only ever
    // targets the CURRENT `pending` epoch — once superseded, `rotated_epoch`
    // is folded into `active_epoch` and is never re-offered — so pinning
    // that exact epoch here is a race against an unrelated, legitimate
    // election storm, not a property this scenario is actually about. What
    // the brief asks for — "converges, does not stay permanently unable to
    // open group traffic" — is captured by "eventually holds WHATEVER the
    // leader's current epoch is," which self-heals either via
    // `rotated_epoch`'s own redelivery (the common case) or via a
    // subsequent mint's ordinary one-shot delivery once the storm settles.
    // See the task report ("Concerns") for the full reproduction, including
    // that a captured-epoch version of this assertion is what surfaced the
    // storm in the first place.
    assert!(
        w.run_until_within(|w| w.node_has_group_epoch(victim, w.current_epoch()), 20_000_000_000)
            .unwrap(),
        "the isolated node must converge on group traffic once healed"
    );
}

/// Scenario 3 (brief): a peer that missed an epoch recovers via the
/// EXISTING NAK path — no new recovery mechanism (`group.rs`'s own module
/// doc). Asserts `nak_count(victim) > 0`, not merely that convergence
/// eventually happened, per the brief's explicit instruction.
///
/// Timing is scripted rather than left to a network-speed race:
/// `block_key_delivery_to` holds a real gap open for a SCRIPTED WINDOW, and
/// the group-key activation rule (`GroupPlane::sealing_epoch`) keeps
/// sealing under the OLD epoch until either every named peer acks the new
/// one OR the 2s timeout elapses — so if the redelivery sweep (a real,
/// bounded retry, `World`'s `CRYPTO_SWEEP_INTERVAL_NS`) reaches `victim`
/// before the timeout, `victim` acks and full-consensus activation and
/// "victim already has the key" happen in the same instant: no gap, no
/// NAK, by design (a nice liveness property, but the wrong shape for THIS
/// test). Holding the block open past the activation grace forces the
/// leader down the timeout arm while `victim` still lacks the key, which
/// is `group.rs`'s documented liveness trap, deliberately triggered; the
/// block is released with only a small margin so the (fast) redelivery
/// sweep reaches `victim` comfortably inside its own election timeout —
/// see `block_key_delivery_to`'s call site below for why that margin
/// matters now that `Msg::CommitGossip` is correctly gated (T13 review).
///
/// **Discrimination, two independent checks, one of which the T13 review
/// found does NOT hold end-to-end anymore — disclosed, not hidden:**
///
/// 1. Sim-side: bypassing the crypto gate (receiver treats every epoch as
///    openable) makes `nak_count(victim)` stay 0 while convergence still
///    happens — proving that assertion is load-bearing, not vacuous. Still
///    holds; re-verified against this commit.
/// 2. Production-side: `GroupPlane::unacked_peers -> []` (bug #1, T12's
///    fix reverted) — this does **NOT** turn this test red, for the SAME
///    structural reason documented on
///    `rotation_during_a_partition_converges_once_healed` above: once
///    `victim` cannot open ANY current group traffic (heartbeats
///    included, now that `CommitGossip` is properly gated), it calls its
///    own election on the SM's ordinary ~150-300ms timeout, and that
///    ALWAYS triggers a fresh `BecomeLeader` auto-mint whose one-shot
///    delivery is untouched by the mutant. This is unavoidable in an
///    end-to-end scenario with live elections, not a scripting mistake —
///    a full election-capable world cannot isolate "redelivery sweep
///    specifically" from "the SM's own liveness recovery" once a peer
///    goes fully deaf for longer than one election timeout, and a peer
///    that has lost E2 is fully deaf by construction. The mutation-verified
///    regression coverage for `unacked_peers`/`redeliver_to` themselves
///    remains `uc2_crypto::group`'s own unit tests
///    (`a_lost_key_delivery_can_be_re_sent_byte_identically` et al.),
///    unaffected by this scenario's re-election confound. See the task
///    report for the reproduction (with `DBG`-level tracing) that pinned
///    this down.
#[test]
fn a_node_that_missed_an_epoch_recovers_via_the_existing_nak_path() {
    let mut w = World::new(crypto_cfg(13));
    w.enable_crypto_plane(3);
    assert!(
        w.run_until_within(
            |w| w.all_peer_sessions_established() && w.current_leader().is_some(),
            10_000_000_000
        )
        .unwrap(),
        "pairwise handshakes must complete AND a leader must be elected on a quiet network"
    );

    let leader = w.current_leader().expect("a leader must be elected");
    let victim = (0..3).find(|&i| i != leader).expect("a 3-node cluster has a non-leader");

    // Blocked for a SCRIPTED WINDOW (T13 review), not a one-shot drop: hold
    // the gap open past uc2_crypto::group::ACTIVATION_TIMEOUT_NS (2s) so the
    // leader is forced down the timeout arm of `sealing_epoch` while victim
    // still lacks the key, then release with a small margin (50ms) so the
    // fast redelivery sweep (`CRYPTO_SWEEP_INTERVAL_NS`, 50ms) reaches
    // victim comfortably inside its own ~150-300ms election timeout —
    // otherwise victim, unable to open ANY current group traffic
    // (`Msg::CommitGossip` is gated the same as `Msg::Data` — see its doc),
    // calls its own election, and recovers via an unrelated fresh mint
    // instead of via the redelivery path this scenario is about (found the
    // hard way: an earlier version used a one-shot drop, and once
    // `CommitGossip` gating landed, a several-second-wide gap reliably
    // triggered a full re-election storm that masked the T12 fix's own
    // role — see the task report).
    let deadline = w.now() + 2_050_000_000;
    w.block_key_delivery_to(victim, deadline);
    w.rotate_group_key();
    w.run_for(2_050_000_000).unwrap(); // just past ACTIVATION_TIMEOUT_NS
    assert_ne!(w.current_epoch(), 0);
    assert!(
        !w.node_has_group_epoch(victim, w.current_epoch()),
        "the victim must still be missing the rotated epoch past the activation grace \
         (if this fails, `CRYPTO_SWEEP_INTERVAL_NS` raced the activation timeout)"
    );

    // New writes now arrive sealed under an epoch `victim` cannot open.
    w.append_and_replicate(64 * 1024);
    let target = w.node_append(leader);

    assert!(
        w.run_until_within(|w| w.node_durable(victim) >= target, 30_000_000_000).unwrap(),
        "the victim must still converge once it recovers the key"
    );
    assert!(
        w.nak_count(victim) > 0,
        "recovery must have gone through NAK repair, not merely happened eventually"
    );
}

/// Scenario 4 (brief): every existing safety invariant still holds with the
/// crypto plane on. `World::run_for` runs the FULL post-event invariant
/// sweep (`step_once`'s inv2/inv6 checks plus the point-in-time inv1/3/4/5/7/
/// 8/9 checks inside `apply_action`) exactly as every other scenario in this
/// file does — an `Err` here is a genuine safety violation, crypto-gated
/// DATA sends included (the crypto gate only ever WITHHOLDS or DROPS a send;
/// it never mutates state the invariants check).
#[test]
fn every_existing_safety_invariant_still_holds_with_the_crypto_plane_on() {
    let mut w = World::new(SimConfig {
        n_nodes: 5,
        max_steps: 700_000,
        drop_per_million: 50_000,
        ..crypto_cfg(21)
    });
    w.enable_crypto_plane(5);
    w.run_for(120_000_000_000).unwrap();
    assert!(w.max_commit() > 0, "a crypto-gated cluster must still make genuine commit progress");
}

/// Discrimination proof for scenario 4: the crypto plane's additions to
/// `World` (the seal gate, `Msg::Nak`, the maintenance tick) must not mask
/// or weaken the sim's EXISTING bug-catching machinery. Crypto-enabled
/// twin of `mechanism_unguarded_reopen_is_caught_by_oracle` — same fuzz over
/// `nasty_reconcile_config`'s high-churn seeds, same
/// `Mechanism{reopen_guard:false}` counterfactual, `w.enable_crypto_plane(3)`
/// the only addition. A budget-bounded fuzz over many seeds (rather than one
/// step-count-precise partition script) is deliberately the shape here: the
/// crypto bootstrap (handshake + first mint/activation) consumes a real, if
/// small, slice of virtual time and event budget up front, so a script tuned
/// to land a partition at an EXACT step count is the wrong instrument for
/// checking "does the oracle still fire" — this asks the more robust
/// question, "somewhere in a long high-churn run, with plenty of margin,
/// does the genuine-quorum oracle still catch the unguarded reopen's phantom
/// commit." If the crypto gate's `Withhold`/drop paths ever swallowed an
/// `Err` or silently altered leader/commit/durable state the oracle reads,
/// this would go green when it must not.
#[test]
fn crypto_plane_does_not_mask_the_mechanism_phantom_commit_oracle() {
    // See the sibling twin: attestation (0.5.0) independently defends this
    // injected bug, so the pin ablates it to keep testing what it names.
    let mut caught = false;
    for seed in 0..60 {
        let mut cfg = nasty_reconcile_config(seed);
        cfg.crash_per_million = 1_000;
        cfg.attest_reports = false; // isolate the reopen guard (see above)
        cfg.data_plane = DataPlane::Mechanism { reopen_guard: false, handle_keyed: true };
        let mut w = World::new(cfg);
        w.enable_crypto_plane(3);
        if let Err(v) = w.run()
            && v.invariant.contains("phantom")
        {
            caught = true;
            break;
        }
    }
    assert!(
        caught,
        "the unguarded reopen must still produce a phantom-commit violation on some seed \
         with the crypto plane on"
    );
}

/// Bonus (brief: "a cold-start-with-a-member-down scenario is worth
/// having"): reproduces the SHAPE of the SECOND real bug this task's brief
/// calls out — `GroupPlane::mint` used to restart the activation clock on
/// EVERY mint, and the node layer mints on every `BecomeLeader` while
/// elections retry (150-300ms) far faster than the 2s activation grace, so
/// a cluster cold-starting with one member permanently unreachable — and
/// therefore churning leadership among the survivors — never activated an
/// epoch and never sealed a single `DATA`: a livelock. Fixed by inheriting
/// the un-activated epoch's clock (`mint`'s doc,
/// `a_superseding_mint_inherits_an_unactivated_epochs_activation_clock`).
///
/// `partition_node(2)` stands in for "never comes up" (permanently
/// unreachable, for the group-key plane's purposes); a moderate crash rate
/// on the two survivors was meant to force the repeated-mint-faster-than-
/// grace condition the fix targets.
///
/// **This does NOT carry a mutation proof — do not cite it as T17
/// regression protection.** T13 review (I-3/the "one thing you do not need
/// to fix, but should record" item): reverting `mint`'s inherited-clock fix
/// does not turn this test red at any `(seed, crash_rate)` tried, and the
/// reviewer found the structural reason: `World::on_restart` never touches
/// `Node::crypto`, so a simulated crash in this sim preserves the crashed
/// node's Noise sessions, group-key schedule, and latched `active_epoch`
/// intact — a REAL process restart loses all three. The T17 mutant is
/// therefore unreachable through this test's wiring at ANY crash rate, not
/// merely hard to hit; `crash_per_million` churns leadership without ever
/// making a node's crypto state actually disappear. The authoritative,
/// mutation-verified regression test for T17 remains
/// `uc2_crypto::group::a_superseding_mint_inherits_an_unactivated_epochs_activation_clock`.
/// This test still earns its keep as a plain liveness check: a 3-node
/// cluster with one member permanently unreachable, under real crash
/// churn on the survivors, must still elect and commit.
#[test]
fn cold_start_with_a_member_down_still_forms_and_seals() {
    let mut w = World::new(SimConfig {
        crash_per_million: 3_000,
        max_steps: 500_000,
        ..crypto_cfg(17)
    });
    w.enable_crypto_plane(3);
    w.partition_node(2);
    assert!(
        w.run_until_within(|w| w.max_commit() > 0, 60_000_000_000).unwrap(),
        "the surviving majority must still form and commit despite one member \
         permanently unreachable and frequent re-election"
    );
}

/// Issue #7 — a leader that opens its term below a committed position, reached
/// through the durable dual-reader skew. THE TEETH: this is what the
/// `SimEvent::ConsensusStep` split exists to make reachable.
///
/// Construction (a live leader must still be committing while someone else
/// campaigns — a crash storm cannot produce this, which is why 200 seeds of
/// `nasty_reconcile_config` find nothing):
///   1. run to a leader `l`;
///   2. cut `c` off from `l` — `c`'s durable freezes and it will time out;
///   3. cut `b` off from `l` a little later — `b`'s durable freezes at a
///      position it has ALREADY REPORTED, and which `l` has therefore already
///      ranked into `commit`, while `b`'s consensus agent has not yet absorbed
///      it;
///   4. `c` campaigns. `b` judges `c`'s credential against its stale absorbed
///      copy, grants, and `c` opens a term BELOW the commit.
///
/// `consensus_step_ns` is deliberately coarse (20 ms vs the 5 ms archive
/// cadence). On real hardware the consensus agent absorbs within microseconds,
/// so this window is genuinely narrow — the bug needs a vote to arrive inside
/// one duty cycle of an archive advance. The sim's job is REACHABILITY, not
/// probability: a schedule that is rare on hardware must be routinely
/// explorable here, or the invariants never get to judge it at all.
#[test]
fn stale_vote_credential_opens_a_term_below_a_committed_position() {
    for seed in 0..40u64 {
        let mut w = issue7_world(seed, false);
        if let Err(v) = issue7_drive(&mut w) {
            assert!(
                w.stale_vote_windows() > 0,
                "seed {seed} caught {} but never answered a vote across the skew — \
                 that is some OTHER bug, not issue #7",
                v.invariant
            );
            // inv2 convicts first: the bad grant produces a term boundary that is
            // not a leading slice of the committed lineage — i.e. a term opened
            // below a committed position. inv4/inv5 are the deeper statements of
            // the same loss; whichever fires, a safety invariant must.
            println!("issue #7 RED, seed {seed}: [{}] {}", v.invariant, v.detail);
            return;
        }
    }
    panic!(
        "no seed reached the loss — if ArchiveStep and ConsensusStep have been re-fused \
         (issue #7), or the pre-vote refresh has been moved back into the SM-fed path, \
         this trace becomes UNREACHABLE rather than absent, and the invariants below \
         silently stop guarding the vote-credential plane"
    );
}

/// The GREEN twin: the shipped behaviour (`vote_refresh_durable: true`, the
/// default) survives the IDENTICAL construction across the same seeds. The only
/// difference between the arms is whether a voter re-reads its own durable
/// counter before answering a `RequestVote`, so this is a genuine discrimination
/// rather than a one-sided pin.
#[test]
fn fresh_vote_credential_survives_the_same_construction() {
    for seed in 0..40u64 {
        let mut w = issue7_world(seed, true);
        if let Err(v) = issue7_drive(&mut w) {
            panic!("seed {seed}: shipped behaviour must survive: {v:?}");
        }
    }
}

fn issue7_world(seed: u64, vote_refresh_durable: bool) -> World {
    let mut cfg =
        SimConfig { n_nodes: 3, drop_per_million: 0, max_steps: 200_000, ..base_cfg(seed) };
    cfg.vote_refresh_durable = vote_refresh_durable;
    cfg.consensus_step_ns = 4 * cfg.archive_step_ns; // see the RED doc comment
    World::new(cfg)
}

/// Shared construction for both arms — identical modulo `vote_refresh_durable`.
fn issue7_drive(w: &mut World) -> Result<(), InvariantViolation> {
    w.run_until_leader()?;
    let l = w.current_leader().expect("a leader");
    let c = (0..3).find(|&i| i != l).expect("follower c");
    let b = (0..3).find(|&i| i != l && i != c).expect("follower b");
    w.run_steps(200)?;
    w.partition(l, c); // c freezes and will campaign; b keeps streaming + reporting
    w.run_steps(400)?;
    w.partition(l, b); // b freezes at an already-reported, already-committed position
    w.run()?;
    Ok(())
}
