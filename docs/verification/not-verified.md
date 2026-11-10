# What is not verified

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This is the most important page of the verification record, and the one most projects omit. Every other page says what a tier proves or checks. This page lists the gaps those tiers leave. Each entry says what is not covered, why, and what covers the nearby ground instead.

## Proofs

These gaps sit in the Lean 4 proofs. Lean is a proof assistant, so a Lean theorem is checked by machine. See [Lean proofs](lean-proofs.md) for what is proved.

### `leader_completeness` is not proved

**`leader_completeness` is not proved.** Election safety and log matching are proved. The `leader_completeness` theorem is reduced to one named obligation, and that obligation is open. See [Lean proofs](lean-proofs.md).

### The model collapses the durable counter's two readers into one value

The real durable counter has two independent readers. The Lean model does not keep them apart. It collapses them into one value. That makes a load-bearing lemma trivially true in the model and false in the real system. See [Lean proofs](lean-proofs.md).

A real acked-write-loss bug lived in exactly that gap. It is Finding #12. It was found from the Rust side, not by the proofs. The Rust is fixed and `uc_sim` is closed. Splitting the Lean model's one value back into two readers is still open. **Until that split is done, proofs composed over that lemma are weaker than they look.**

### State-machine safety is not attempted

**State-machine safety is not attempted.** It is gated on the two Lean gaps above. Those are the open `leader_completeness` obligation and the collapsed durable counter.

### The proofs cover the consensus kernels, not the whole node

**The proofs cover the consensus kernels and the protocol model, not the whole node.** Four parts of the node have no machine-checked proof. They are the reliable-UDP data plane, the archive and journal path, the IPC ring buffers, and the service SDK. The [simulation](simulation.md), [linearizability](linearizability.md) and [crash](hard-crash.md) tiers cover those parts instead.

### There is no Rust-to-Lean extraction

**This repo has no Rust-to-Lean extraction.** Extraction would turn the Rust code itself into Lean terms. The standing link between model and code is the conformance rig plus `#guard` pins. Aeneas extraction is blocked on toolchain versions, not on feasibility. See [Lean proofs](lean-proofs.md).

## Your state machine

### Determinism in `apply` is your responsibility

**Your state machine's determinism is your responsibility.** Nothing here checks it. SMR replicates bytes. It guarantees that every replica applies the same commands in the same order. Nondeterminism in `apply` produces divergence that no layer here can catch. The usual sources are clocks, iteration order, floats and ambient state.

`2.11.0` narrows the two most common sources, a clock reading and an id. It does not remove the rule. `ctx.time_ns` is a replicated clock reading. It is the leader's stamp, carried on the frame. `IdGen` derives ids from the position. So a state machine that needs a clock or an id now has a deterministic one to reach for.

But an ambient `SystemTime::now()` or RNG inside `apply` diverges exactly as it always did. **Nothing in this record would catch it.**

## Simulation and model checking

### Bounded model checks are bounded

Veil is a bounded model checker. It explores every execution up to a fixed number of steps and none beyond. So Veil's clean runs do not cover all executions. They are exhaustive only to a depth. See [Veil](veil.md).

### inv12 is largely a corollary of inv6 in the sim

Membership has two readers on purpose. They are the cluster FSM and the consensus kernel's durable-time config. The two-readers rule says the FSM's membership is always a committed prefix of the kernel's. The sim invariant inv12 checks that rule. inv6 is the durable-time adoption oracle. An oracle is the rule that decides pass or fail. In the sim, inv12 is not independent evidence for the two-readers rule. It is largely a corollary of inv6. See [simulation](simulation.md).

The evidence comes from a red twin. A red twin is a deliberately broken variant that must fail, which proves the test can see the bug. This red twin feeds the consensus kernel from the committed view. **inv6** catches it. inv12 stays green under it, and that is correct. A kernel on the committed view is level with the FSM, not ahead of it.

So inv6 and inv12 **together** pin the reader. The sim's evidence for the two-readers rule is real but not independent. inv12 alone would not catch a design that collapsed the two readers into one. This is recorded rather than resolved.

### The sim does not model datagram size

**The sim does not model datagram size, so path-MTU discovery is not adjudicated there.** This applies from `2.12.0`. See the closing subsection of [fault layers](fault-layers.md). `uc_sim` carries messages, not byte counts. It models loss, duplication, reordering, corruption and replay. It does not model "too large for this path".

Path-MTU discovery climbs a ladder of datagram sizes. Each step on that ladder is a rung. The rung a cluster settles on is ordinary replicated `Settings` data. `inv12` sweeps its FSM-versus-kernel relationship like any other. The **measuring** that picks the rung is proved elsewhere. That covers the ladder, the commit rule and the two startup gates. `uc_node/tests/jumbo.rs` proves it over the real transport with `FaultConfig::{max_datagram, emsgsize_over}`. The unit tier proves it too.

The jumbo-frame discovery fleet arm **ran 2026-09-13**. Its record is `docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md`. Discovery converges. The force gate refuses by name on both arms. A narrow path pins the rung. The rate clause is inconclusive on that rig.

The datagram-size gap is stated here because "the sim covers the transport" would otherwise be read to include it.

### The timer oracle's rule 5 is vacuous over the instants row

**Rule 5 of the spec §4.3 timer oracle checks nothing over the new instants row.** That oracle is described in [simulation](simulation.md). The row is `a_commanded_instant_freezes_every_row_at_one_position_under_load_and_ordering_holds`, listed in [linearizability](linearizability.md). It re-walks a whole log carrying `SNAPSHOT` frames through `uc_sim::timers::check_frames`.

Rule 5 keys on the appender's high-water mark at the moment the producing leader pass **began**. A reader outside the node cannot see a pass boundary. So the row uses the only sound bound the log alone gives. That bound is the previous frame's stamp, which is never below the true pass-start stamp. It makes rule 5's "legitimately late" escape hatch always fire. Rules 1–4 are what the row genuinely runs.

The alternative was to infer pass boundaries from the frame sequence. That was rejected. The natural heuristic is "a `Timer` after a `Client` starts a new pass". That heuristic re-authorises exactly the bug rule 5 exists to catch. Rule 5 over a real leader pass stays owned by the in-crate differential test.

### The leader pass is checked for ordering, not for which occurrence fires

**The leader pass is checked for ORDERING, not for which occurrence fires.** `uc_sim::timers` is a reference model that mirrors the leader-pass algorithm. The differential test in [simulation](simulation.md) puts `uc_sim`'s §4.3 oracle on the real pass. So that mirror can no longer drift on stamp ordering, monotonicity or lateness attribution.

The differential test does **not** cover the schedule table's recurrence choice. That choice is `RowTimers::table_fire_deadline`'s one-tick catch-up. It picks the latest occurrence at or before the leader's clock. Two things cover that choice's arithmetic. One is the `uc_protocol_schedule_table` property fuzz target, which checks totality. See [fuzzing](fuzzing.md). The other is `a_restarted_node_resumes_the_table_with_one_catch_up_tick`, which is one scenario. Nothing seeded covers it. A wrong-but-total occurrence choice would pass both the fuzzer and the ordering oracle.

The cluster FSM narrowed the *input*, not the arithmetic. The table that arithmetic reads is now the cluster FSM's committed record. That is one source of truth, identical on every node. Only the leader holds a heap to arm from it. So the uncovered surface is the occurrence choice alone, on one node. It no longer includes which table a given node happened to be holding.

### One seam survives the node/cluster line, on purpose

Since the cluster FSM, cluster data lives in the cluster FSM rather than in node-local state. One membership file is the exception, by design. `state/config.state` is still node-local durable state. It is the only membership copy that is. All other cluster data moved into the cluster FSM.

`state/config.state` stays because it is the consensus kernel's **durable-time** shadow. Cluster-FSM spec §4.6 explains why. Raft §4.1 requires the newest configuration in the log, committed or not. An FSM cannot answer at that time base.

What relates the FSM and the kernel as readers is **inv12**. See [simulation](simulation.md). The note under "inv12 is largely a corollary of inv6 in the sim" applies to it. In the sim, inv12 is largely a corollary of inv6. So inv6 and inv12 together pin the reader, and neither does it alone.

## Snapshots and instants

### Coordinated snapshots have no fleet numbers

**Coordinated snapshots have no fleet numbers.** The freeze-vs-commit-stall argument is not yet a measurement. The argument says a quorum frozen at an instant's position P caps every durable report at `P + fsm_lag`. That is the reason standby instants exist. What backs it today is reasoning, plus in-process assertions that commit keeps moving on a small state.

Three rows are owed to the time-and-timers gate's throughput arm:

- instants under load, A/B'd per M14a
- a below-floor join with the shipper restarted mid-window
- freeze duration against observed commit pause on a deliberately large state, all-nodes then `--standby`

### "The instant completed on the first attempt" has no watched red

**"The instant completed on the first attempt" is not constructible as a watched red.** A watched red is a deliberately broken variant run to show the test fails on it. Two checks assert first-attempt completion. They are `instant_completes_first_try`'s bar and `learner.rs`'s `attempt == 1`. Both fire only on a regression of ruling P10. P10 says a replayed span acts on its last `SNAPSHOT` frame. So both were shipped without a red twin, and stated as such.

The corollary is that these two checks are a flakiness surface across seven pre-existing `learner.rs` tests. An instant abandonment of the kind spec §10 describes, on a loaded box, now fails loudly instead of retrying. That is the intent. But the failure mode should be recognised for what it is.

### The cluster artifact decoder: only stateful sequences remain uncovered

The cluster FSM's artifact decoder is now fuzzed, so single decodes are covered. **Stateful sequences are not.** The design named the missing decoder fuzz target as a gap, and the plan first left it open. Plan 1's pre-final pass closed it with the `uc_node_cluster_artifact` target. See [fuzzing](fuzzing.md).

17.7 M runs over the target's five seeds found no crash. The seeds include a crc-CONSISTENT body whose membership length prefix lies. That is the shape that would index out of bounds without the decoder's `.get(..)` checks.

Neighbouring decoders have their own targets:

| target | what it decodes |
|---|---|
| `uc_node_cluster_artifact` | the cluster FSM's artifact |
| `uc_protocol_cluster_frame` | the `CLUSTER` frame body |
| `uc_protocol_settings` | the settings record the `CLUSTER` frame wraps |
| `uc_protocol_cluster_image` | the image codec, round-tripped |

Since plan 3 the image **codec** itself is a `core`-only leaf, `uc_protocol::v2::cluster_image`. So its target reaches it without linking `uc_node` at all. Like every target, it still leaves stateful sequences uncovered.

## Rings, fuzzing and platforms

### The IPC rings' interleavings and UB: only MPSC and Broadcast are covered

**The IPC rings' interleavings and undefined behaviour are covered by nothing, except for MPSC and Broadcast.** See [loom](loom.md) and [fuzzing](fuzzing.md). The rings live in `uc_protocol/src/ring/{spsc,mpsc,broadcast,common,futex}.rs`. This is the one place in the system where a Rust memory-safety bug is most plausible.

Loom runs a small concurrent program under every possible interleaving. Here is what does cover the rings:

- Offset-pin tests freeze the **layout** of all four ring kinds.
- MPSC has a commit-protocol loom model. It also has the `ring_mpsc_record` fuzz target, added in 2.7.0.
- Broadcast has a seqlock-barrier loom model, from 2026-08-31. It found and fixed a real weak-memory defect the moment it was written.

**SPSC, the futex layer and the mapping itself remain uncovered.** `svc_sched.<row>.ring` is the service→node schedule ring. It is an **SPSC** ring, so its interleavings are in the uncovered set.

`2.11.0` adds a structural narrowing there, not a proof. Since the cluster FSM, the service writes that ring **only while its node leads**. The gate is `write_sched_if_leader`. It uses the same `NODE_FLAG_LEADER` read the apply loop already does each cycle. Only a leading node drains the ring. So a follower never exercises the ring-full spin at all.

Integration assertions cover that gate, not loom. They are `a_follower_does_not_drain_sched_rings_and_demotion_discards_the_heap` and `timers_pending_on_the_old_leader_fire_exactly_once_on_the_new_one_after_its_announce`.

Miri is an interpreter that detects undefined behaviour. Miri does not support file-backed memory mappings. A Vec-backed ring variant would let Miri run. But it would be checking a different object than the one that ships. It has not been built. That trade-off is recorded rather than resolved.

### Fuzzing is a regression gate, not a proof of totality

**Fuzzing does not prove the decoders total.** It is a regression gate. See [fuzzing](fuzzing.md). Green means no new crash from the committed corpus inside the budget. It does not mean the decoders are total for all inputs. It says nothing about stateful sequences across the receiver's dispatch. It also says nothing about the external crates, such as `bincode`, that the decoders reach.

### The published gate numbers are fleet measurements

**The published gate numbers are not universal.** They are fleet measurements, on the hardware and configuration each record names. They are reproducible. See [benchmark methodology](benchmark-methodology.md).

### CI executes tests on x86_64 only

**CI does not run tests on aarch64.** It executes tests on x86_64 only. aarch64 binaries are built natively, on arm64 runners, but their tests never run in CI.

The full correctness stack has passed on real ARM hardware exactly once. That stack is the workspace, both lincheck capstones and the SIGKILL crash tier. The run was on Graviton/Neoverse-V3, on 2026-08-31. Its record is [`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md). The run also surfaced and fixed one x86-timing assumption in a test's scenario construction.

A one-time pass is a data point, not a regression gate. **ARM stays uncovered between such runs.**

### The flag day's retirements are pinned by a grep, not by the type system

The `2.11.0` flag day retired a set of symbols, listed in spec §7. **The type system does not stop a retired symbol coming back.** A grep does. `uc_node/tests/retired.rs` runs `git grep` for each of spec §7's retired symbols. It fails if any comes back. It deliberately catches a re-introduction in a comment, a script or a doc, as well as in code.

But `retired.rs` is a name check. A symbol reintroduced under a *different* name with the same broken shape passes it.

The test has its own excuse rule. A historical-record file may name a retired symbol on a line that says it is retired or reserved. A `.rs` hit is always a hit, whatever its comment says. That rule is unit-tested by `the_retired_or_reserved_excuse_applies_only_outside_rs_files`.

## Multi-service: the M14 record

M14 added multi-service: one log feeding several FSMs. `2.8.0` disclosed a two-FSM proof gap on this page. **`2.8.1` closes it.** That release is M14c2, under spec §15.1 and §16. The record follows in full, so it can be audited rather than taken on trust.

### Seven two-FSM capstones

Seven capstones run with two FSMs. WGL is the Wing-Gong-Lowe linearizability search. Each capstone checks **one WGL history per FSM** with the untouched `uc_lincheck` checker. See [linearizability](linearizability.md) and [hard crash](hard-crash.md).

**`uc_node/tests/lin_v2.rs`:**

- `two_fsm_bounded`
- `two_fsm_lockstep`
- `two_fsm_slow`
- `two_fsm_slow_lockstep`

**`uc_node/tests/lin_partition_v2.rs`:**

- `minority_partition_and_heal_two_fsm`

**`testing/uc_crashtest/tests/hard_crash.rs`:**

- `two_fsm_service_sigkill`
- `two_fsm_node_sigkill`

### The replication-equivalence oracle bites

The replication-equivalence oracle says every `submit_all`'s per-FSM answers must be byte-equal. All seven capstones assert its failure count at zero.

It is **shown to bite**. `two_fsm_oracle_bites` runs FSM 1 as `Corrupt<RegisterSm>` through the capstones' own worker loop. It dies on the same `equiv_failures == 0` assertion the capstones make.

### The slow-FSM oracle

`two_fsm_slow` and `two_fsm_slow_lockstep` run FSM 1 as `Slow<RegisterSm, 200>`. Two things hold:

- The FSMs' separation stays inside the policy at every 50 ms sample.
- Their second-half apply rates agree within 10 %. The measured ratio is 1.000.

Measurement scopes this result. The separation never approached the bound. `max_lag` was 192 B of 65536, and 64 B of 288. **So this is evidence of equal progress, not of the barrier at the bound.** See [linearizability](linearizability.md).

### The Elle clean tier runs with two FSMs

Elle is a checker for transactional consistency anomalies. `elle_quiet_two_fsm` in `uc_node/tests/elle_v2.rs` runs the Elle clean tier with two FSMs. It checks one history per FSM. Both are clean under `serializable` and `strong-serializable`. See [Elle](elle.md).

Its equivalence check can only fail on a malformed fan-in, because `LaResp` has one variant. The equivalence evidence is the WGL capstones'.

### The M14c deferrals are closed

The M14c deferrals are closed in two places.

In `uc_net`:

- snapshot-session refusal tests
- the NAK-before-BEGIN skip
- three new counters
- a 60 s intake timeout
- a paced publish re-drive

In `uc_service`, `uc_node` and `uc2ctl`:

- `lag_waits` now counts bounded mid-frame stalls
- the learner join pins the installed artifact positions
- the pinned-at-bound alert threshold
- the snapshot-decline latch test

### Lockstep 880x: an operating-envelope fact, not a product defect

The M14 gate reported lockstep at 60× in its row e. **Whether that is a product defect was settled by experiment, not argument.** The decision rule was fixed before any measurement.

The lockstep collapse reproduces at 880× on a deliberately oversubscribed rung. Throughput fell from 624 k to 709 frames/s at 3 threads on 1 CPU. Bounded mode on the same rung is unaffected, at 7.4 M frames/s.

The pre-registered sleep-cascade mechanism was **refuted**. `lag_waits = 0` on every collapsed run.

No candidate fix cleared the pre-committed 50 % recovery bar:

| candidate fix | recovery |
|---|---|
| yield ladder ×4 | 1.00× |
| yield ladder ×16 | 1.00× |
| unbounded yield | 1.00× |
| futex wait on the sibling's `applied` word | 116×, but still only 13 % of the unconstrained rate |

The verdict is therefore an **operating-envelope fact stated with the number, not a product defect**. No behavioural code changed. See [`uc2-m14c2-lockstep-oversubscription-2026-08-30.md`](/docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md) and [Limits](/docs/reference/limits.md).

### The `--pin` rig ran, and pinning was not adopted

**The `--pin` rig has now run, on 2026-08-31, and pinning was NOT adopted.** It ran 16 arms on 4 × `c6id.2xlarge`. It compared one binary against itself, pinned vs unpinned.

The pre-committed bar is spec §16.5: "adopt iff the pinned spread is < 5 %". The pinned pooled spread is **14.3 %**. So the bar is not met, and `--pin` stays opt-in.

Pinning DOES remove the worst mode. Pooled spread falls from 47.7 % to 14.3 %. The 1.12 M collapse arm never appears pinned. So placement is *a* cause, but it is not the only one. Pinning also **costs 9.4 % of mean throughput**. The pinned maximum is below the unpinned mean.

The `c6id.2xlarge` sibling map is no longer an assumption. `lscpu -e=CPU,CORE` on all three voters gives `CORE: 0 1 2 3 0 1 2 3`. That is exactly `EXPECTED_SIBLING_PAIRS`. See [`uc2-m14c2-fleet-pinning-2026-08-30.md`](/docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md).

### Still open: row e, and the residual spread

The M14 fleet gate measures rates, a kill and a join. It is not a correctness substitute. **Two items are still open.** **Row e has still not been re-measured.** The pinning run above did not address it. The residual 14.3 % spread is undiagnosed.

## FSM identity

FSM identity shipped in 2.11.0. It gives rows names, replacing M14's numbered-set bitmask. Its spec is `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md`.

### The multi-service proofs now run with named FSMs

The existing multi-service proof surface above now runs with **named** FSMs. That surface is the seven two-FSM capstones and `elle_quiet_two_fsm`. They use `uc_service::Tagged<const ROW: u8, S>`, whose `NAME` is `fsm{ROW}`. Before, they used a bare row number. The checker is the same. The tests are the same. Underneath them is an identity layer that was not there before.

### A capstone-adjacent bug: `FsmSet::Single`

The FSM identity work found and fixed two capstone-adjacent bugs. They are recorded here rather than left to be re-derived.

`uc_node/tests/lincheck_v2/mod.rs`'s `FsmSet::Single` was hardcoded to `RegisterSm::NAME`. It ignored the test's own state-machine type. So every single-FSM `elle_check.sh` pass and `snapshot_restart_installs_only_with_purge` would have attached under the wrong declared name and refused. It is fixed to `SM::NAME`.

### Two new negative learner scenarios

Two new negative scenarios are in `uc_node/tests/learner.rs`. Both use a real leader and joiner `Node` over loopback UDP.

| test | setup | expected refusal |
|---|---|---|
| `a_joiner_whose_rows_are_named_in_the_other_order_is_refused_by_name_and_stalls` | the leader declares `["sum", "fsm1"]` and the joiner declares `["fsm1", "sum"]`. These are the *same* two names, transposed. A naive set comparison would wrongly accept them. | refused at row 0, `kind == Identity`, named in the log line |
| `a_joiner_running_another_fsm_version_is_refused_with_both_versions` | identical names, with differing packed versions set by a direct cnc `status` poke | refused via `kind == Version`, both versions named |

Both were watched red first. A temporary edit to `uc_net/src/receiver.rs` made them fail. The edit was a set comparison for one and a disabled version check for the other. The hang was observed, and the edit was reverted.

### Dev-box run, not a fleet gate

**The FSM identity proof run was a dev-box run, not a fleet gate**, per the standing rule. These passed:

- `lin_v2`: 14/14
- `lin_partition_v2`: 8/8
- `learner`: 7/7, including the two new scenarios
- the six-pass `elle_check.sh`, all green: `quiet failover partition purge reconfig quiet_two_fsm`

`cargo test -p uc_crashtest --features hard-crash-tests` read 4 passed / 2 failed. These were `ok`: `linearizable_under_service_sigkill`, `two_fsm_service_sigkill`, `node_sigkill_recovery` and `two_fsm_node_sigkill`.

**`sigkill_mid_config_window` and `leader_node_sigkill_recovery_multi` failed reproducibly on this box.** They also failed on the pristine pre-FSM-identity base commit, with none of this work's changes applied. A stash round-trip against `testing/uc_crashtest` verified that. This work does not touch `testing/uc_crashtest`. Both failed with the same `cnc attach error: No such file or directory`. That came after a real SIGKILL and respawn under a tight `UC2_CLIENT_TIMEOUT_MS=1500`.

This is a pre-existing flake, not a regression from FSM identity. `sigkill_mid_config_window` was already documented at ~5%. It is recorded here because this run saw it fail 5/5 times on this box. That is a stronger rate than the documented ~5%. It is worth a look by whoever owns that flake next. It is not recorded because FSM identity caused it.

## Wire crypto

### Wire crypto is opt-in, and some adversaries are out of model

**Wire crypto is opt-in and off by default.** With it disabled, the posture is a trusted network. With it enabled, the threat model is a network-path adversary.

A compromised host and a malicious cluster member are explicitly **out of model**. The group key is symmetric. So any holder can forge fan-out traffic as any node. See the [M8 gate record](../benchmarks/uc2-m8-gate-2026-07-29.md) and [`docs/security/threat-model.md`](/docs/security/threat-model.md).
