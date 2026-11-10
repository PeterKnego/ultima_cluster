# Deterministic simulation

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

## The short version

`uc_sim` runs a whole cluster inside one test process, in virtual time, with the real consensus code at its core. It throws faults at that cluster, chosen by a random seed, and after every single event it checks a list of safety rules that must never be broken. Give it the same seed and it replays the same run exactly, which is what makes a failure reproducible. The default tier is 150 runs and the heavy tier is a thousand seeds.

The simulator cannot prove anything. What it does is find bugs the proofs cannot reach, because it exercises the parts of the node the Lean model does not describe. Three times, a real bug hid in something the simulator could not express, and each time the simulator was extended so it could. Those three stories are the most useful part of this page, because they show what "the sim is green" does and does not mean.

The simulator lives in [`uc_sim/`](/uc_sim).

## What the simulator is

A few words of UC vocabulary are needed to read the rest.

- A node's **durable** position is how far its log is safely on disk. Its **commit** position is how far the cluster as a whole has agreed the log is final.
- The **term map** is a node's record of which leader term wrote which stretch of the log. When a follower's term map disagrees with the leader's, the follower's log tail is **truncated**, meaning cut off and rewritten.
- The **config** is the cluster's membership: which nodes exist and which may vote. A config is **adopted** by a node when the node starts acting on it. A removed node leaves a **tombstone**, a record that its id must never come back.

The simulator drives the *real* `ElectionSm`, the state machine from `uc_consensus` that runs elections and decides on truncation. `world.rs` links that crate directly. So when a bug is fixed in the consensus crate, the simulator picks up the fix on its own. Nobody mirrors the logic by hand, and the sim cannot drift from the code it tests.

Around that real core, the simulator fakes everything else: the network, the disks, the clock and the crashes. Messages can be lost, duplicated, reordered, corrupted or replayed. Nodes can crash and restart. Every one of these choices is drawn from the seed.

## The invariants

An invariant is a rule that must hold in every state the cluster ever reaches. The simulator sweeps every invariant after every event. One violation fails the run and prints the seed.

The twelve invariants this record covers:

| | name | what it says in plain terms |
|---|---|---|
| inv1 | Election safety | Two nodes never both believe they lead the same term. No split brain. |
| inv2 | Term-map prefix consistency | Where two nodes' term maps overlap, they agree. |
| inv3 | Commit monotonicity | A node's commit position never moves backwards. |
| inv4 | Committed bytes are never truncated | Once the cluster has committed a byte, no node ever cuts it. |
| inv5 | Leader completeness | A new leader already holds everything that was committed before it won. |
| inv6 | Config determinism | The config a node has adopted is exactly the one its own log implies. The world recomputes that implication from its ledger of config frames. |
| inv7 | Quorum legality | A leader's commit is certified by a genuine quorum of the voters in the adopting node's config, and that config chains directly off the committed one. There is no phantom commit. |
| inv8 | Revert correctness | Once a truncation has settled, the node's adopted config again equals what its log implies. |
| inv9 | Tombstone permanence | No adopted config re-lists a tombstoned id, and the set of tombstones never shrinks. |
| inv10 | Report ceiling | A node's outgoing durable report never exceeds what it would have reported unclamped, never exceeds its apply ceiling, and never decreases except across a truncation, a restart, a role change or a changed ceiling. Added in M14b. |
| inv11 | Set alignment | A node never lists a complete snapshot set above its own commit. Every listed instant is a real `SNAPSHOT` frame inside the cluster-wide committed prefix. When two nodes both list an instant, they mean the same frame. Added in `2.11.0`, from the coordinated-snapshot spec §5.3 and §5.4. Explained below. |
| inv12 | Two readers | The cluster FSM's membership is always a committed prefix of the consensus kernel's durable-time config. Added in `2.11.0`, from the cluster-FSM spec §4.6. Explained below. |

The simulator has since gained two more that this record does not describe: inv13, the snapshot catalog, and inv14, the seeded lineage of a node restored from the backup tier.

## Historical bugs, pinned forever

Besides the random runs, the suite holds directed scenarios that stage a specific bug from the project's history. Each stays in the suite for good, so the bug cannot quietly return. A scenario is only trusted once it has been watched fail against the buggy code and pass against the fix, which the record writes as "RED before, GREEN after".

- `rebooted_unreconciled_voter_must_not_certify_phantom_commit` pins Finding #5, the voter that rebooted with its reconciliation gate open and certified a commit over content it did not hold. RED before its fix, GREEN after.
- `old_term_range_must_not_commit_before_new_term_quorum` pins Finding #6b, the leader that committed an old-term range below the safety barrier. RED before its fix, GREEN after.
- `capped_quorum_stalls_commit_and_releasing_one_follower_resumes_it` models M14a's report ceiling as a per-node cap on how far a node may apply. It pins that commit stalls exactly when a commit quorum is capped, and resumes when one follower is released.

## Three times the simulator was blind

A simulator only finds the bugs it can describe. If its model of the node leaves something out, no number of seeds will find a bug that lives in the missing part. That has happened three times. Each time, the sim's model was corrected and a test was added that pins the correction. Two of the three also have a **red twin**: a companion test that deliberately puts the bug back and checks that the sim now catches it. Without a red twin, a green invariant might be green only because it never looked.

### Blind spot 1: one counter, two readers

**The real node.** Two threads read the durable counter, at different moments. The receiver thread reads it to report it to the leader. The consensus thread reads it a little later to use as the node's credential when it votes. For a short window the two can hold different values. The [Lean proofs page](lean-proofs.md) describes the same hazard from the model's side.

**What the sim did.** `world.rs` advanced the counter and fed the `DurableAdvanced` event to the consensus logic in the same handler, in one step. So in the sim, the report path and the vote-credential path always saw the same number. The window in which they can disagree did not exist.

**The fix.** The consensus step, `SimEvent::ConsensusStep`, now absorbs the counter on its own cadence, exactly as the second thread does in the node.

**The pin.** `stale_vote_credential_opens_a_term_below_a_committed_position`.

### Blind spot 2: a run that stopped before the window slid

**The real node.** A leader sends its followers only a 64-entry window of its term map, not the whole thing. Once a cluster has lived through more than 64 terms, that window no longer starts at the beginning of a follower's map. A fix on 2026-08-16 for an acked-write-loss bug taught `reconcile` to line the window up inside the follower's map before comparing.

**What the sim did.** Every `run_*` loop stopped a run as soon as any node's term map reached `MAX_TERM_MAP_WIRE_ENTRIES - 2` entries. The comment called this "a wire limitation, not a safety bug" and chose to "cap the run rather than provoke it". So the leader's window never slid inside a follower's map, and the window-aligned branch the fix added ran exactly never. That includes all thousand seeds of `sim-heavy`. The sim was blind to this until 2026-08-29.

**The fix.** The cap is gone.

**The pin.** `window_slide_past_64_lifetime_terms_reconciles_healthy_followers_clean` drives a cluster through more than 70 leader changes. It pins that no healthy follower is wiped or truncated and that every term map converges. Watched RED against the cap, where it failed with "round 61: the cluster did not reconverge".

**The red twin.** `window_slide_with_index_aligned_reconcile_wipes_healthy_followers` puts the old matching rule back and checks that the sim sees the wipes. It does that through a **tooth**: a bug compiled in on purpose, available only behind the `mutation-testing` feature. The tooth here is `reconcile::reconcile_index_aligned`, the pre-fix body kept verbatim. Nightly runs both tests.

### Blind spot 3: a config observation the sim replayed and the node lost

**The real node.** The archive thread notices each config frame in the log once, and tells the election logic about it once. If a truncation is in progress at that moment, the election logic's latch drops the observation. So an observation can be lost across a truncation.

**What the sim did.** `observe_config_frames` re-fed the `ConfigObserved` event for *every* config frame at or below durable, on *every* archive step. The justification was "idempotent by version monotonicity": re-feeding cannot hurt. But it meant a lost observation was always re-delivered a step later, so "a config observation lost across a truncation" was a class of bug the sim could not express. It was blind to it until 2026-09-03.

**How the bug was found instead.** Nightly run 33605909828 on 2026-09-02 caught it on a 4-vCPU runner, in `sigkill_mid_config_window`. A restarted ex-leader re-archived its own CONFIG v2 frame. The reconcile cut landed at exactly that frame's end. The one observation died under the latch, and the node replicated at config v1 for 90 seconds.

**The fix.** The sim now emits each observation once per node. It delivers it `archive_obs_latency_ns` after the counter moves, as `SimEvent::CfgObsDelivered`, which models the archive thread's store-then-send gap. It consumes the observation on the consensus cadence and holds it across the latch, exactly as `uc_node` has done since commit `cc4e321`.

**The pin.** `config_observation_under_the_latch_is_adopted_at_the_ack` stages the interleaving with `set_archive_frozen` and an injected term map, and pins that the observation is adopted.

**The red twin.** `counterfactual_unheld_config_observation_breaks_inv8` deletes the hold by setting `config_obs_latch_buffer = false`, which is the pre-fix node, and pins that inv8 fires at the acknowledgement.

## inv11: snapshot sets line up

Since `2.11.0`, a snapshot is taken at a coordinated instant: a `SNAPSHOT` frame in the log whose end position, P, is where every row freezes its state. A node that holds every row's artifact at P has a complete set, and may move its purge floor there, discarding the log below.

inv11 is the oracle for those sets. An oracle is the rule that decides pass or fail. It indexes sets by frame end and checks three things:

1. **Nothing is listed above the node's own commit.** A row can only freeze at a position it has applied, and apply waits for `min(commit, durable)`. A set above commit would let a node purge log under bytes that are not yet final.
2. **Every listed instant is a real `SNAPSHOT` frame inside the genuine committed frontier of the whole cluster.** A truncated instant is never a set.
3. **Two nodes listing the same P mean the same frame.**

### Why it is not a prefix rule

The coordinated-snapshot spec's §11 first sketched inv11 as a prefix rule: one node's list of sets should be a prefix of another's. That would have failed on correct behaviour. An instant can be abandoned when a later one supersedes it, and a row can decline an instant. Both make `[P1, P2]` on one node and `[P2]` on another perfectly legitimate. The full argument is in `check_set_alignment`.

### It is exercised, not just green

An invariant that nothing exercises is green for the wrong reason. So the fuzz runs command instants on purpose. All three fuzz arms of the default tier do, and so does the heavy tier's five-node arm.

| tier | instants commanded | complete sets judged | inv11 fired |
|---|---|---|---|
| default tier, 150 runs | 5 646 | 16 549 | zero times |
| heavy tier | about 38 000 more | not recorded | zero times |

### The truncated-instant scenario

A directed scenario stages a `SNAPSHOT` frame that a leader change truncates. Its red twin is a world model that wrongly lists that truncated instant as a set. The scenario's own per-step assertion catches the twin, not inv11. That is expected: a listed P is by construction at or below `global_max_commit`, the genuine cluster-wide commit, and inv4 already forbids truncating there.

## inv12: two readers of the same membership

Membership travels in one frame, but two consumers read it at two different moments:

- **The consensus kernel reads it when the frame is durable.** Raft §4.1 requires a node to act on the newest config in its log, committed or not.
- **The cluster FSM reads it when the frame is committed.** A state machine applies at commit, and the cluster FSM is what a snapshot records.

inv12 pins how those two views relate. At every step, the FSM's membership equals the kernel's durable-time config at some position at or below commit. At every committed position, the kernel's config equals what the FSM applied there.

**How the sim stands in for the FSM.** The sim has no cluster FSM, and no frames at all. So it computes what the FSM's membership would be, using the cluster-FSM spec's definition: the highest-version config frame the node genuinely holds at or below `min(commit, durable)`.

**Proving the sweep did something.** Each sweep returns how many `(node, frame)` pairs it related. `inv12_the_cluster_fsms_membership_is_a_committed_prefix_of_the_kernels` asserts that count is not zero.

### The red twin, and an honest note about what it pins

`counterfactual_kernel_on_the_committed_view_is_caught_by_inv6_the_durable_time_oracle` runs behind `mutation-testing`. It feeds the consensus kernel from the **committed** view instead of the durable one. That is the reader a design that puts the FSM first would naturally reach for, and it is the one Raft forbids. The test checks that the invariant set notices.

What fires is **inv6**, config determinism, because the wrong reader breaks inv6's durable-time adoption rule head-on. It fires at the first config frame a follower makes durable, on 64 of 64 seeds.

inv12 stays green under that tooth, and that is correct. inv12 says the FSM's view is a prefix of the kernel's. A kernel reading the committed view is not ahead of the FSM, it is level with it, and a prefix rule allows that.

**So in the sim, inv12 is largely a corollary of inv6.** With inv6 suppressed, inv12 does fire, on 64 of 64 seeds, but it fires because of the archive-scan lag rather than because of the prefix relation. inv6 and inv12 together pin the reader. Neither does it alone.

inv7 and inv5 are reachable only with inv6 and inv12 both suppressed and the fault rate raised to the storm settings, and even then at 1 seed in 64. The cluster-FSM spec's §11 sentence that names "inv7 or inv4" is an erratum, recorded as such.

## The timers oracle

### A reference model of the leader pass

Since `2.11.0` the leader fires due timers at the start of each pass, before any client frame of that pass. [`uc_sim::timers`](/uc_sim/src/timers.rs) is a pure, dependency-free reference model of that algorithm, which the time-and-timers spec describes in §4.3. 64 seeds sweep five rules, numbered 1 to 5.

Rule 5 says lateness must pre-date the pass. A `TIMER` frame's `time_ns` may exceed its `deadline_ns` only when the pass that emitted it was itself running late. Lateness must never be an artefact of the order frames were decoded in.

### The same oracle over the real code

A reference model with no link to the code it mirrors can drift from it without any test noticing. `uc_sim::timers` has no `use` statements at all, so nothing tied it to `uc_node`.

The fix was to split the model's check out as a free function over a slice of frames, `uc_sim::timers::check_frames`, and run the real code through it. `uc_node::node::tests::the_real_leader_pass_satisfies_the_sim_oracle_across_seeds` drives the **real** `Consensus::do_work` one pass at a time. The real `fire_due_timers`, the real `RowTimers` heap, the real `Appender` clamp and the real ordering of step 3, firing timers, before step 3b, draining client input, all run. The test reads back the frames each pass appended and hands them to `check_frames`.

**The workload** is 24 seeds, 10 passes each, over 2 timer rows. Client payloads are queued behind the timers, and one pass in eight arms a burst larger than `TIMERS_PER_PASS`.

**The clock.** The pass clock is pinned through a `#[cfg(test)]` override on the single `wall_now_ns()` read, so the release build carries neither the field nor the branch. Rule 5 needs one input that is not on the wire, `pass_start_stamp`. That needs no instrumentation, because a test that calls `do_work()` itself owns the pass boundaries.

### The mutation that only this test catches

A mutation is a bug injected on purpose to check that a test can see it. The mutation here reorders `do_work` so that `drain_ingress` runs before `fire_due_timers`, the clients-before-timers bug that rule 5 exists to name.

That mutation survives everything else. All seven integration tests in `uc_node/tests/timers.rs` stay GREEN, including `a_scheduled_timer_fires_at_its_deadline_in_order_and_once` and its `time_ns == deadline_ns` assertion. That assertion cannot see the reorder, because its scenario has no client frame in flight during the timer's pass.

`the_real_leader_pass_satisfies_the_sim_oracle_across_seeds` kills the mutation on seed 1. Its failure names the late timer, its deadline, the pass start stamp and the culprit frame. The mutation was applied by hand and reverted. It is not a shipped feature flag.

## How to run it

```bash
cargo test -p uc_sim                          # standard tier
cargo test -p uc_sim --features sim-heavy     # 1000-seed fuzz
cargo test -p uc_sim --features mutation-testing --test scenarios window_slide  # red twin
```
