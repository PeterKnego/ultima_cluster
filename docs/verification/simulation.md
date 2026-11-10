# Deterministic simulation

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

The deterministic simulator runs a whole cluster in virtual time inside one test process. It injects faults chosen by a seed and checks safety rules. A seed is the number that drives the pseudo-random choices, so one seed always replays the same run. This tier is seeded bug-hunting over many runs, plus directed scenarios that pin specific historical bugs. It lives in [`uc_sim/`](/uc_sim).

## What the simulator runs

The simulator drives the *real* `ElectionSm`. `world.rs` wires `uc_consensus` directly. So a fix in the consensus crate is reflected in the sim automatically. Nobody has to mirror it by hand.

An invariant is a rule that must hold in every state the cluster reaches. The sim runs seeded fault fuzz and checks **twelve** whole-cluster safety invariants. It sweeps all of them after **every** event.

## The twelve invariants

| | Invariant |
|---|---|
| inv1 | Election safety: no split brain. |
| inv2 | Term-map prefix consistency. |
| inv3 | Commit monotonicity. |
| inv4 | Committed bytes are never truncated. |
| inv5 | Leader completeness. |
| inv6 | Config determinism. |
| inv7 | Quorum legality: no phantom commit, and the commit rides a chaining config. |
| inv8 | Revert correctness after truncation settles. |
| inv9 | Tombstone permanence. |
| inv10 | Report ceiling: a clamped report never exceeds its unclamped value or its apply ceiling. It never decreases except across a truncation, restart, role change or ceiling change. Milestone: M14b. |
| inv11 | Set alignment: no node lists a complete snapshot set above its own `commit`. Every listed instant is a `SNAPSHOT` frame inside the cluster-wide committed prefix. An instant two nodes both list names the **same** frame. Release: `2.11.0`. Spec: coordinated-snapshot spec §5.3/§5.4. |
| inv12 | Two readers: the cluster FSM's membership is always a **committed prefix** of the consensus kernel's durable-time config. Release: `2.11.0`. Spec: cluster-FSM spec §4.6. |

## Directed regression scenarios

Besides the random fuzz, directed scenarios stage specific historical bugs. Each one stays in the suite as a permanent regression pin. They include the following.

**Pins for two findings.** Each was verified RED before its fix and GREEN after.

- `rebooted_unreconciled_voter_must_not_certify_phantom_commit` pins Finding #5.
- `old_term_range_must_not_commit_before_new_term_quorum` pins Finding #6b.

**The report ceiling.** `capped_quorum_stalls_commit_and_releasing_one_follower_resumes_it` models M14a's report ceiling as a per-node apply cap. It pins that commit stalls if and only if a commit quorum is capped.

## Three places the simulator was blind

A simulator only finds bugs it can express. Three times, the sim's own model could not express a real class of bug. Each gap was closed in the sim and pinned by a test. A red twin is a companion test that deliberately puts the bug back and asserts that the sim now catches it. It proves the check can actually see the bug. Two of the fixes come with a red twin.

### Blind spot 1: the durable dual reader

**What the sim could not see.** The simulator was blind to the durable dual-reader hazard until recently. In the node, two paths on two threads read the durable counter: the report path and the vote-credential path. `world.rs` advanced the counter and fed `DurableAdvanced` in the same handler. So in the sim, those two paths could never disagree.

**The fix.** `SimEvent::ConsensusStep` now absorbs the counter on its own cadence. That is exactly how those two threads behave in the node.

**The pin.** `stale_vote_credential_opens_a_term_below_a_committed_position`.

### Blind spot 2: the term-map cap

**What the sim could not see.** The sim was blind to term-map window slides until 2026-08-29. Every `run_*` loop stopped a run the moment any node's term map reached `MAX_TERM_MAP_WIRE_ENTRIES - 2` entries. The code called this "a wire limitation, not a safety bug" and chose to "cap the run rather than provoke it". As a result, the leader's 64-entry wire window never slid inside a follower's map.

The 2026-08-16 acked-write-loss fix had added a window-aligned `reconcile` branch. The simulator executed that branch exactly never. That includes `sim-heavy`'s thousand seeds.

**The fix.** The cap is gone.

**The pin.** `window_slide_past_64_lifetime_terms_reconciles_healthy_followers_clean` drives a cluster through 70+ leader changes. It pins zero wipes and zero truncations of healthy followers. It also pins that every map converges. It was verified RED against the cap, with the failure "round 61: the cluster did not reconverge".

**The red twin.** A tooth is a bug injected on purpose, compiled in only behind the `mutation-testing` feature. `window_slide_with_index_aligned_reconcile_wipes_healthy_followers` restores the pre-fix index-aligned match. It does so through a `mutation-testing` tooth. This tooth is `reconcile::reconcile_index_aligned`, the old body verbatim. The twin pins that the sim then sees the wipes. Nightly runs both tests.

### Blind spot 3: the config-observation re-feed

**What the sim could not see.** The sim was blind to a lost config observation until 2026-09-03. `observe_config_frames` re-fed `ConfigObserved` for *every* ledger frame at or below durable on *every* archive step. Its justification was "idempotent by version monotonicity". But the real archive emits each observation exactly once, on its own thread. And `ElectionSm`'s truncating latch drops whatever is fed under it. So the class "a config observation lost across a truncation" was unexpressible in the sim.

**How the bug was found instead.** Nightly 33605909828 found the lost observation on a 4-vCPU runner. The failing test was `sigkill_mid_config_window`, on 2026-09-02. A restarted ex-leader re-archived its own CONFIG v2 frame. The reconcile cut landed at exactly that frame's end. The one observation died under the latch. The node then replicated at v1 for 90 s.

**The fix.** The sim now emits each observation once per node. It delivers the observation `archive_obs_latency_ns` after the counter moves, as `SimEvent::CfgObsDelivered`. That delay models the archive thread's store-then-send window. The sim consumes the observation on the consensus cadence. It holds the observation across the latch exactly as `uc_node` does since `cc4e321`.

**The pin.** `config_observation_under_the_latch_is_adopted_at_the_ack` stages the interleaving. It uses `set_archive_frozen` plus an injected term map. It pins the adoption.

**The red twin.** `counterfactual_unheld_config_observation_breaks_inv8` deletes the hold. It sets `config_obs_latch_buffer = false`, which is the pre-fix node. It pins that inv8 fires at the ack.

## inv11: set alignment

inv11 shipped in `2.11.0`. A coordinated snapshot instant is a `SNAPSHOT` frame. Every row freezes at that frame's END position, P. The node that completes the whole set at P moves its purge floor there.

An oracle is the rule that decides pass or fail. inv11 is the oracle for those snapshot sets. It indexes sets **by frame end**. It judges three things.

1. **Nothing is listed above the node's own commit.** A row can only freeze at a position it has applied. Apply is gated on `min(commit, durable)`. So a set above commit would claim a floor with uncommitted bytes under it.
2. **Every listed instant is a real `SNAPSHOT` frame inside the cluster-wide genuine committed frontier.** So a truncated instant is never a set.
3. **Two nodes listing the same P must mean the same frame.**

### Why inv11 is not a prefix rule

inv11 is deliberately **not** a prefix rule between nodes. A prefix rule is what §11 of the coordinated-snapshot spec first sketched. Two correct behaviours break it. One is abandonment, where a superseding instant takes over. The other is a row that declines an instant. Both make `[P1, P2]` on one node and `[P2]` on another entirely legitimate. A prefix rule would have failed on correct behaviour. `check_set_alignment` carries the argument in full.

### inv11 is exercised, not vacuously green

An invariant nothing exercises is green for the wrong reason. So the fuzz arms **command instants**. All three fuzz arms of the default tier do it. So does the heavy tier's 5-node arm.

| tier | instants commanded | completed sets judged | inv11 fired |
|---|---|---|---|
| default tier, 150 runs | 5 646 | 16 549 | zero times |
| heavy tier | ~38 000 more | not stated | zero times |

### The truncated-instant scenario

A directed scenario stages a `SNAPSHOT` frame truncated by a leader change. Its red twin is a world model that lists a truncated instant as a set. The scenario's own per-step assertion catches that twin. inv11 does not. The reason is that a listed P is by construction at or below `global_max_commit`. And inv4 already forbids truncating there.

## inv12: two readers

inv12 shipped in `2.11.0`. Membership is carried by one frame. Two consumers read it, at two different time bases.

- **The consensus kernel reads at *durability*.** Raft §4.1 requires the newest configuration in the log, committed or not.
- **The cluster FSM reads at *commit*.** An FSM applies at commit, and the FSM is the snapshot authority.

inv12 pins the relation between them, in two halves. At every step, the FSM's membership equals the kernel's durable-time config at some position at or below commit. At every committed position, the kernel's config equals what the FSM applied there.

### How the sim stands in for the FSM

The sim has no cluster FSM. It has no frames at all. So the FSM's membership is the derived quantity the cluster-FSM spec defines. That quantity is the highest-version config frame the node genuinely holds at or below `min(commit, durable)`.

### Proving inv12 is not vacuous

Each sweep returns how many `(node, frame)` pairs it actually related. So a scenario can prove the sweep was not vacuous. `inv12_the_cluster_fsms_membership_is_a_committed_prefix_of_the_kernels` asserts a non-zero count.

## The red twin for inv12, and an honest note about what it pins

`counterfactual_kernel_on_the_committed_view_is_caught_by_inv6_the_durable_time_oracle` runs behind `mutation-testing`. It feeds the kernel from the **committed** view. That is the reader an FSM-first design reaches for. It is also the one Raft forbids. The test pins that the invariant set sees it.

What fires is **inv6**, config determinism. The wrong reader deletes inv6's durable-time adoption oracle head-on. inv6 fires at the first config frame a follower makes durable. It fires on 64 of 64 seeds.

inv12 itself stays green under that tooth. That is correct, not a weakness in the test. inv12 pins *FSM ⊆ kernel*. A kernel reading the committed view is not ahead of the FSM. It is level with it.

**In the sim, inv12 is therefore largely a corollary of inv6.** With inv6 suppressed, inv12 does fire, on 64/64 seeds. But it fires off the archive-scan lag, not off the prefix relation. inv6 and inv12 together pin the reader. Neither invariant does it alone.

inv7 and inv5 are reachable only when three things hold together. inv6 is suppressed. inv12 is suppressed. The fault rate is raised to the storm settings. Even then they are reachable at 1 seed in 64. The cluster-FSM spec's §11 sentence naming "inv7 or inv4" is an erratum. It is recorded as such.

## The timers oracle

### The reference model

The timers model shipped in `2.11.0`. [`uc_sim::timers`](/uc_sim/src/timers.rs) is a pure, dependency-free reference model of the leader-pass algorithm. That algorithm is §4.3 of the time-and-timers spec. 64 seeds sweep five rules, numbered 1 to 5.

Rule 5 is that lateness must pre-date the pass. A `TIMER` frame's `time_ns` may exceed its `deadline_ns` only when the pass that emitted it was itself running late. It must never be an artefact of decoding order.

### The same oracle over the shipped code

The `uc_sim::timers` reference model has no `use` statements at all. Nothing linked it to `uc_node`. So it could drift from the algorithm it mirrors without any test noticing.

The model's oracle is therefore split out as a predicate, `uc_sim::timers::check_frames`. It is a free function over a frame slice, not a method on the model. `uc_node::node::tests::the_real_leader_pass_satisfies_the_sim_oracle_across_seeds` drives the **real** `Consensus::do_work` one pass at a time. It runs the real pieces:

- real `fire_due_timers`
- real `RowTimers` heap
- real `Appender` clamp
- real step-3-before-3b ordering

The test reads back the frames the pass appended. It hands the sequence to that same predicate.

**The workload.** 24 seeds × 10 passes × 2 timer rows. Client payloads are queued behind the timers. One pass in eight arms a burst past `TIMERS_PER_PASS`.

**The clock.** The pass clock is pinned through a `#[cfg(test)]` override on the single `wall_now_ns()` read. So the release build carries neither the field nor a branch. Rule 5 has one input that is not on the wire, `pass_start_stamp`. It needs no instrumentation. The reason is that a test that calls `do_work()` itself owns the pass boundaries.

### The mutation check

A mutation is a bug injected on purpose. `the_real_leader_pass_satisfies_the_sim_oracle_across_seeds` is **mutation-checked, and the mutation survives everything else.** This mutation reorders `do_work` so `drain_ingress` runs before `fire_due_timers`. That is the clients-before-timers bug that rule 5 exists to name.

The reorder leaves all seven `uc_node/tests/timers.rs` integration tests GREEN. That includes `a_scheduled_timer_fires_at_its_deadline_in_order_and_once` and its `time_ns == deadline_ns` assertion. That assertion does not expose the reorder. Its scenario has no client frame in flight during the timer's pass.

That differential test kills the mutation on seed 1. Its failure names the late timer, its deadline, the pass start stamp and the culprit frame. The mutation was applied by hand and reverted. It is not a shipped feature flag.

## How to run it

```bash
cargo test -p uc_sim                          # standard tier
cargo test -p uc_sim --features sim-heavy     # 1000-seed fuzz
cargo test -p uc_sim --features mutation-testing --test scenarios window_slide  # red twin
```
