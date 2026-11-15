# What is not verified

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

## The short version

Every other page in this record says what a tier establishes. This page says where the tiers stop. It is the most important page here, and the one most projects leave out.

If you read nothing else, read these five gaps:

- **Determinism in your `apply` is your job.** Nothing in UC checks it. A clock read or a hash-map walk inside `apply` makes replicas diverge, and no tier here would notice.
- **The Lean proofs stop at election safety and log matching.** `leader_completeness` has one open obligation. The model also merges two readers of the durable counter into one value, and a real acked-write-loss bug once lived in that gap.
- **The IPC rings are the most plausible home for a memory-safety bug**, and only two of the four ring kinds have an interleaving model.
- **Coordinated snapshots have no fleet numbers.** The claim that a freeze stalls commit by a bounded amount is an argument, not a measurement.
- **CI runs tests on x86_64 only.** The full stack has passed on ARM exactly once.

Each entry below says three things. What is not covered. Why it is not covered. What covers the nearby ground instead. A gap marked "recorded rather than resolved" is one the team knows about and has chosen not to close yet.

## Where the proofs stop

The Lean 4 proofs are machine-checked theorems about a model of the consensus kernel. Lean is a proof assistant, so a theorem it accepts cannot be wrong about the model. It can still be about the wrong model, and most of this section is about exactly that. [Lean proofs](lean-proofs.md) says what is proved.

### `leader_completeness` has one open obligation

Raft's safety argument has three legs. Election safety says two leaders never share a term. Log matching says two logs that agree at a position agree on everything before it. Leader completeness says a new leader's log holds every entry committed in an earlier term. The first two are proved. `leader_completeness` is reduced to one named obligation, and that obligation is open.

### The model's durable counter has one reader where the node has two

Every node keeps a durable counter: the byte position up to which its log is safely on disk. In the real node two different threads read that counter at different moments. The receiver reads it to report to the leader, which ranks those reports into the commit position. The consensus agent reads it a little later to decide how to vote. Between the two reads, the value can move.

The Lean model has one value for both. That makes a load-bearing lemma trivially true in the model, because the two reads are the same number by definition. In the real system they are not.

A real acked-write-loss bug lived in exactly that gap. It is Finding #12. A voter could grant a vote while comparing against a self-view lower than the value it had already reported for commit purposes. It was found from the Rust side, not by the proofs. The Rust is fixed and `uc_sim` is closed, meaning the simulator now models both readers and would catch the bug. Splitting the Lean model's one value back into two readers is still open. **Until that split is done, proofs composed over that lemma are weaker than they look.**

### State-machine safety is not attempted

State-machine safety is the top of the Raft argument: every replica applies the same command at the same position. It rests on both gaps above, the open `leader_completeness` obligation and the collapsed durable counter. It is not attempted until they close.

### The proofs cover the consensus kernels, not the whole node

The proofs cover the consensus kernels and the protocol model. Four parts of the node have no machine-checked proof at all. They are the reliable-UDP data plane, the archive and journal path, the IPC ring buffers, and the service SDK. The [simulation](simulation.md), [linearizability](linearizability.md) and [crash](hard-crash.md) tiers cover those parts instead.

### There is no Rust-to-Lean extraction

Extraction would turn the Rust code itself into Lean terms, so a theorem would be about the shipped code and not about a hand-written model. This repo has none. The standing link between model and code is the conformance rig plus `#guard` pins. The rig runs the same inputs through the model and the Rust and compares the outputs. The pins freeze constants on both sides.

Aeneas is the extraction tool for Rust. It was blocked on toolchain versions until 2026-11-10. A retry on that date showed it works here, and proved one theorem about translated code, but nothing from it is in the repo's proofs yet. See [Lean proofs](lean-proofs.md).

## Your state machine

### Determinism in `apply` is your responsibility

SMR replicates bytes. It guarantees that every replica applies the same commands in the same order. It cannot guarantee that the same command produces the same result on every replica. That is up to your `apply`, and nothing in this record checks it.

Nondeterminism in `apply` produces divergence that no layer here can catch. The usual sources are clocks, iteration order, floats and ambient state.

`2.11.0` narrows the two most common sources, a clock reading and an id. It does not remove the rule. `ctx.time_ns` is a replicated clock reading. It is the leader's stamp, carried on the frame, so every replica sees the same value. `IdGen` derives ids from the position. So a state machine that needs a clock or an id now has a deterministic one to reach for.

But an ambient `SystemTime::now()` or RNG inside `apply` diverges exactly as it always did. **Nothing in this record would catch it.**

## Where the simulator does not look

`uc_sim` is the deterministic simulator. It runs many nodes in virtual time under seeded faults and checks a set of invariants after every step. [Simulation](simulation.md) is its page. This section lists the places it does not reach, starting with Veil, the model checker that sits beside it.

### Bounded model checks are bounded

Veil is a bounded model checker. It explores every execution up to a fixed number of steps and none beyond. So Veil's clean runs do not cover all executions. They are exhaustive only to a depth. See [Veil](veil.md).

### inv12 is largely a corollary of inv6

Membership has two readers on purpose. One is the cluster FSM, which applies membership at commit. The other is the consensus kernel's durable-time config, which must see the newest configuration in the log whether or not it is committed. Raft §4.1 requires that. Cluster-FSM spec §4.6 explains why an FSM cannot answer at that time base.

This is also why one membership file survived the move of cluster data into the cluster FSM. `state/config.state` is the kernel's durable-time shadow. It is still node-local durable state, and it is the only membership copy that is. All other cluster data moved into the cluster FSM. The seam is there by design.

The two-readers rule says the FSM's membership is always a committed prefix of the kernel's. The sim invariant inv12 checks that rule. inv6 is the durable-time adoption oracle. An oracle is the rule that decides pass or fail. inv6 says the config a node has adopted is exactly the one its own log implies.

The evidence that the two overlap comes from a red twin. A red twin is a deliberately broken variant that must fail, which proves the test can see the bug. This red twin feeds the consensus kernel from the committed view. **inv6** catches it. inv12 stays green under it, and that is correct. A kernel on the committed view is level with the FSM, not ahead of it, and a prefix rule allows that.

So in the sim, inv12 is not independent evidence for the two-readers rule. It is largely a corollary of inv6. inv6 and inv12 **together** pin the reader. inv12 alone would not catch a design that collapsed the two readers into one. This is recorded rather than resolved.

### The sim does not model datagram size

`uc_sim` carries messages, not byte counts. It models loss, duplication, reordering, corruption and replay. It does not model "too large for this path". So path-MTU discovery, which `2.12.0` added, is not adjudicated there. See the closing subsection of [fault layers](fault-layers.md).

Path-MTU discovery climbs a ladder of datagram sizes. Each step on that ladder is a rung. The rung a cluster settles on is ordinary replicated `Settings` data, so `inv12` sweeps its FSM-versus-kernel relationship like any other. The **measuring** that picks the rung is proved elsewhere. That covers the ladder, the commit rule and the two startup gates. `uc_node/tests/jumbo.rs` proves it over the real transport with `FaultConfig::{max_datagram, emsgsize_over}`. The unit tier proves it too.

The jumbo-frame discovery fleet arm **ran 2026-09-13**. Its record is `docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md`. Discovery converges. The force gate refuses by name on both arms. A narrow path pins the rung. The rate clause is inconclusive on that rig.

The gap is stated here because "the sim covers the transport" would otherwise be read to include it.

### Rule 5 of the timer oracle checks nothing over the instants row

Since `2.11.0` the leader fires due timers at the start of each pass, before that pass's client frames. The time-and-timers spec §4.3 gives five rules every log must obey, and `uc_sim::timers::check_frames` checks them. [Simulation](simulation.md) describes the oracle. Rule 5 is the one that catches the clients-before-timers bug. It says a timer may fire late only if it was already overdue when its pass began.

To check that, rule 5 needs to know where each leader pass began. Inside the node, the oracle records the appender's high-water mark at the moment the producing leader pass **began**. A reader outside the node cannot see a pass boundary.

The instants row is such a reader. It is `a_commanded_instant_freezes_every_row_at_one_position_under_load_and_ordering_holds`, listed in [linearizability](linearizability.md). It re-walks a whole log carrying `SNAPSHOT` frames through `uc_sim::timers::check_frames`. For the pass-start value it uses the only sound bound the log alone gives. That bound is the previous frame's stamp, which is never below the true pass-start stamp. With that bound, rule 5's "legitimately late" escape hatch always fires. Rules 1–4 are what the row genuinely runs.

The alternative was to infer pass boundaries from the frame sequence. That was rejected. The natural heuristic is "a `Timer` after a `Client` starts a new pass". That heuristic re-authorises exactly the bug rule 5 exists to catch. Rule 5 over a real leader pass stays owned by the in-crate differential test.

### The leader pass is checked for ordering, not for which occurrence fires

`uc_sim::timers` is a reference model that mirrors the leader-pass algorithm. The differential test in [simulation](simulation.md) puts `uc_sim`'s §4.3 oracle on the real pass. So that mirror can no longer drift on stamp ordering, monotonicity or lateness attribution.

The differential test does **not** cover the schedule table's recurrence choice. A schedule table entry such as "every hour" has many occurrences. After downtime, the leader must pick one. `RowTimers::table_fire_deadline` picks the latest occurrence at or before the leader's clock, so a restarted cluster gets one catch-up tick and never a backlog.

Two things cover that choice's arithmetic. One is the `uc_protocol_schedule_table` property fuzz target, which checks totality, meaning the function returns an answer for every input. See [fuzzing](fuzzing.md). The other is `a_restarted_node_resumes_the_table_with_one_catch_up_tick`, which is one scenario. Nothing seeded covers it. A wrong-but-total occurrence choice would pass both the fuzzer and the ordering oracle.

The cluster FSM narrowed the *input*, not the arithmetic. The table that arithmetic reads is now the cluster FSM's committed record. That is one source of truth, identical on every node. Only the leader holds a heap to arm from it. So the uncovered surface is the occurrence choice alone, on one node. It no longer includes which table a given node happened to be holding.

## Snapshots and instants

An instant is a coordinated snapshot. A `SNAPSHOT` frame in the log names a position P, and every row and the cluster FSM freeze their state at exactly P.

### Coordinated snapshots have no fleet numbers

The freeze-vs-commit-stall argument is not yet a measurement. The argument says a quorum frozen at an instant's position P caps every durable report at `P + fsm_lag`, so commit stalls by at most that much. That is the reason standby instants exist, where only learners freeze. What backs the argument today is reasoning, plus in-process assertions that commit keeps moving on a small state.

Three rows are owed to the time-and-timers gate's throughput arm:

- instants under load, A/B'd per M14a
- a below-floor join with the shipper restarted mid-window
- freeze duration against observed commit pause on a deliberately large state, all-nodes then `--standby`

### "The instant completed on the first attempt" has no watched red

A watched red is a deliberately broken variant run to show the test fails on it. Two checks assert that an instant completes on its first attempt. They are `instant_completes_first_try`'s bar and `learner.rs`'s `attempt == 1`. Both fire only on a regression of ruling P10. P10 says a replayed span acts on its last `SNAPSHOT` frame, which removed the one reason a first attempt could be abandoned. There is no way to break P10 on purpose without breaking the fixture around it. So both were shipped without a red twin, and stated as such.

The corollary is that these two checks are a flakiness surface across seven pre-existing `learner.rs` tests. An instant abandonment of the kind spec §10 describes, on a loaded box, now fails loudly instead of retrying. That is the intent. But the failure mode should be recognised for what it is.

### The cluster artifact decoder is fuzzed one decode at a time

The cluster FSM's artifact is its snapshot file. Its decoder is now fuzzed, so single decodes are covered. **Stateful sequences are not.** A stateful sequence is a run of decodes where earlier results shape later inputs, as happens when a node installs an artifact and then applies frames on top of it.

The design named the missing decoder fuzz target as a gap, and the plan first left it open. Plan 1's pre-final pass closed it with the `uc_node_cluster_artifact` target. See [fuzzing](fuzzing.md). 17.7 M runs over the target's five seeds found no crash. The seeds include a crc-CONSISTENT body whose membership length prefix lies. That is the shape that would index out of bounds without the decoder's `.get(..)` checks.

Neighbouring decoders have their own targets:

| target | what it decodes |
|---|---|
| `uc_node_cluster_artifact` | the cluster FSM's artifact |
| `uc_protocol_cluster_frame` | the `CLUSTER` frame body |
| `uc_protocol_settings` | the settings record the `CLUSTER` frame wraps |
| `uc_protocol_cluster_image` | the image codec, round-tripped |

Since plan 3 the image **codec** itself is a `core`-only leaf, `uc_protocol::v2::cluster_image`. So its target reaches it without linking `uc_node` at all. Like every target, it still leaves stateful sequences uncovered.

## Rings, fuzzing and platforms

### Only two of the four ring kinds have an interleaving model

The IPC rings are the shared-memory queues between processes on one host. They live in `uc_protocol/src/ring/{spsc,mpsc,broadcast,common,futex}.rs`. This is the one place in the system where a Rust memory-safety bug is most plausible. The rings are lock-free, mapped from files, and written with raw atomics.

Loom runs a small concurrent program under every possible interleaving. Miri is an interpreter that detects undefined behaviour. See [loom](loom.md) and [fuzzing](fuzzing.md). Here is what does cover the rings:

- Offset-pin tests freeze the **layout** of all four ring kinds.
- MPSC has a commit-protocol loom model. It also has the `ring_mpsc_record` fuzz target, added in 2.7.0.
- Broadcast has a seqlock-barrier loom model, from 2026-08-31. It found and fixed a real weak-memory defect the moment it was written.

**SPSC, the futex layer and the mapping itself remain uncovered.** `svc_sched.<row>.ring` is the service→node schedule ring. It is an **SPSC** ring, so its interleavings are in the uncovered set.

`2.11.0` adds a structural narrowing there, not a proof. Since the cluster FSM, the service writes that ring **only while its node leads**. The gate is `write_sched_if_leader`. It uses the same `NODE_FLAG_LEADER` read the apply loop already does each cycle. Only a leading node drains the ring. So a follower never exercises the ring-full spin at all.

Integration assertions cover that gate, not loom. They are `a_follower_does_not_drain_sched_rings_and_demotion_discards_the_heap` and `timers_pending_on_the_old_leader_fire_exactly_once_on_the_new_one_after_its_announce`.

Miri does not support file-backed memory mappings. A Vec-backed ring variant would let Miri run. But it would be checking a different object than the one that ships. It has not been built. That trade-off is recorded rather than resolved.

### Fuzzing is a regression gate, not a proof of totality

Fuzzing does not prove the decoders total. It is a regression gate. See [fuzzing](fuzzing.md). Green means no new crash from the committed corpus inside the budget. It does not mean the decoders are total for all inputs. It says nothing about stateful sequences across the receiver's dispatch. It also says nothing about the external crates, such as `bincode`, that the decoders reach.

### The published gate numbers are fleet measurements

The published gate numbers are not universal. They are fleet measurements, on the hardware and configuration each record names. They are reproducible there. See [benchmark methodology](benchmark-methodology.md).

### CI executes tests on x86_64 only

CI does not run tests on aarch64. aarch64 binaries are built natively, on arm64 runners, but their tests never run in CI.

The full correctness stack has passed on real ARM hardware exactly once. That stack is the workspace, both lincheck capstones and the SIGKILL crash tier. The run was on Graviton/Neoverse-V3, on 2026-08-31. Its record is [`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md). The run also surfaced and fixed one x86-timing assumption in a test's scenario construction.

A one-time pass is a data point, not a regression gate. **ARM stays uncovered between such runs.**

### The flag day's retirements are pinned by a grep, not by the type system

The `2.11.0` flag day retired a set of symbols, listed in spec §7. The type system does not stop a retired symbol coming back. A grep does. `uc_node/tests/retired.rs` runs `git grep` for each of spec §7's retired symbols. It fails if any comes back. It deliberately catches a re-introduction in a comment, a script or a doc, as well as in code.

But `retired.rs` is a name check. A symbol reintroduced under a *different* name with the same broken shape passes it.

The test has its own excuse rule. A historical-record file may name a retired symbol on a line that says it is retired or reserved. A `.rs` hit is always a hit, whatever its comment says. That rule is unit-tested by `the_retired_or_reserved_excuse_applies_only_outside_rs_files`.

## Multi-service: a gap that closed, and its record

M14 added multi-service: one log feeding several FSMs. `2.8.0` disclosed a two-FSM proof gap on this page. **`2.8.1` closes it.** That release is M14c2, under spec §15.1 and §16. Because the gap was disclosed here, its closing record is kept here too, in full, so it can be audited rather than taken on trust. Two items at the end of this section are still open.

### Seven two-FSM capstones

A capstone is an end-to-end test that drives a real cluster and checks the result with a linearizability checker. WGL is the Wing-Gong-Lowe linearizability search. Seven capstones run with two FSMs. Each checks **one WGL history per FSM** with the untouched `uc_lincheck` checker. See [linearizability](linearizability.md) and [hard crash](hard-crash.md).

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

When two FSMs run the same state machine on the same log, they must give the same answers. The replication-equivalence oracle says every `submit_all`'s per-FSM answers must be byte-equal. All seven capstones assert its failure count at zero.

It is **shown to bite**. `two_fsm_oracle_bites` runs FSM 1 as `Corrupt<RegisterSm>`, a wrapper that answers every compare-and-swap with the opposite result, through the capstones' own worker loop. It dies on the same `equiv_failures == 0` assertion the capstones make.

### The slow-FSM oracle

`two_fsm_slow` and `two_fsm_slow_lockstep` run FSM 1 as `Slow<RegisterSm, 200>`, a wrapper that sleeps 200 microseconds in every `apply` and then answers normally. Two things hold:

- The FSMs' separation stays inside the policy at every 50 ms sample.
- Their second-half apply rates agree within 10 %. The measured ratio is 1.000.

Measurement scopes this result. The separation never approached the bound. `max_lag` was 192 B of 65536, and 64 B of 288. **So this is evidence of equal progress, not of the barrier at the bound.** See [linearizability](linearizability.md).

### The Elle clean tier runs with two FSMs

Elle is a checker for transactional consistency anomalies. `elle_quiet_two_fsm` in `uc_node/tests/elle_v2.rs` runs the Elle clean tier with two FSMs. It checks one history per FSM. Both are clean under `serializable` and `strong-serializable`. See [Elle](elle.md).

Its equivalence check can only fail on a malformed fan-in, because `LaResp` has one variant. The equivalence evidence is the WGL capstones'.

### The M14c deferrals are closed

M14c left a list of deferred items. They are closed in two places.

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

Lockstep is the strictest lag policy: no FSM starts frame k+1 until every FSM has finished frame k. The M14 gate reported lockstep at 60× slower than its bounded twin in its row e. **Whether that is a product defect was settled by experiment, not argument.** The decision rule was fixed before any measurement.

The lockstep collapse reproduces at 880× on a deliberately oversubscribed rung. Throughput fell from 624 k to 709 frames/s at 3 threads on 1 CPU. Bounded mode on the same rung is unaffected, at 7.4 M frames/s.

The pre-registered sleep-cascade mechanism was **refuted**. `lag_waits = 0` on every collapsed run, so no FSM was ever waiting on a sibling.

No candidate fix cleared the pre-committed 50 % recovery bar:

| candidate fix | recovery |
|---|---|
| yield ladder ×4 | 1.00× |
| yield ladder ×16 | 1.00× |
| unbounded yield | 1.00× |
| futex wait on the sibling's `applied` word | 116×, but still only 13 % of the unconstrained rate |

The verdict is therefore an **operating-envelope fact stated with the number, not a product defect**. No behavioural code changed. See [`uc2-m14c2-lockstep-oversubscription-2026-08-30.md`](/docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md) and [Limits](/docs/reference/limits.md).

### The `--pin` rig ran, and pinning was not adopted

Pinning fixes a node's processes to named CPUs instead of letting the scheduler move them. The `--pin` rig ran on 2026-08-31, and pinning was NOT adopted. It ran 16 arms on 4 × `c6id.2xlarge`. It compared one binary against itself, pinned vs unpinned.

The pre-committed bar is spec §16.5: "adopt iff the pinned spread is < 5 %". The pinned pooled spread is **14.3 %**. So the bar is not met, and `--pin` stays opt-in.

Pinning DOES remove the worst mode. Pooled spread falls from 47.7 % to 14.3 %. The 1.12 M collapse arm never appears pinned. So placement is *a* cause, but it is not the only one. Pinning also **costs 9.4 % of mean throughput**. The pinned maximum is below the unpinned mean.

The `c6id.2xlarge` sibling map is no longer an assumption. `lscpu -e=CPU,CORE` on all three voters gives `CORE: 0 1 2 3 0 1 2 3`. That is exactly `EXPECTED_SIBLING_PAIRS`. See [`uc2-m14c2-fleet-pinning-2026-08-30.md`](/docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md).

### Still open: row e, and the residual spread

The M14 fleet gate measures rates, a kill and a join. It is not a correctness substitute. Two items are still open. **Row e has still not been re-measured.** The pinning run above did not address it. The residual 14.3 % spread is undiagnosed.

## FSM identity: a record, not a gap

FSM identity shipped in 2.11.0. It gives rows names, replacing M14's numbered-set bitmask. Its spec is `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md`. This section records what that work did to the multi-service proof surface, and what it found on the way.

### The multi-service proofs now run with named FSMs

The multi-service proof surface above now runs with **named** FSMs. That surface is the seven two-FSM capstones and `elle_quiet_two_fsm`. They use `uc_service::Tagged<const ROW: u8, S>`, whose `NAME` is `fsm{ROW}`. Before, they used a bare row number. The checker is the same. The tests are the same. Underneath them is an identity layer that was not there before.

### Two capstone-adjacent bugs

The FSM identity work found and fixed two bugs next to the capstones. They are recorded here rather than left to be re-derived.

The first was in the test harness. `uc_node/tests/lincheck_v2/mod.rs`'s `FsmSet::Single` was hardcoded to `RegisterSm::NAME`. It ignored the test's own state-machine type. So every single-FSM `elle_check.sh` pass and `snapshot_restart_installs_only_with_purge` would have attached under the wrong declared name and refused. It is fixed to `SM::NAME`.

The second was in a fleet driver. `bench-infra/scripts/m12_fleet_gate.py`'s node role passed no `--services` flag at all, so under named rows its node would have refused to start. It now declares the same names its service role attaches.

### Two new negative learner scenarios

A negative scenario checks that something is refused. Two are in `uc_node/tests/learner.rs`. Both use a real leader and joiner `Node` over loopback UDP.

| test | setup | expected refusal |
|---|---|---|
| `a_joiner_whose_rows_are_named_in_the_other_order_is_refused_by_name_and_stalls` | the leader declares `["sum", "fsm1"]` and the joiner declares `["fsm1", "sum"]`. These are the *same* two names, transposed. A naive set comparison would wrongly accept them. | refused at row 0, `kind == Identity`, named in the log line |
| `a_joiner_running_another_fsm_version_is_refused_with_both_versions` | identical names, with differing packed versions set by a direct cnc `status` poke | refused via `kind == Version`, both versions named |

Both were watched red first. A temporary edit to `uc_net/src/receiver.rs` made them fail. The edit was a set comparison for one and a disabled version check for the other. The hang was observed, and the edit was reverted.

### Dev-box run, not a fleet gate

The FSM identity proof run was a dev-box run, not a fleet gate, per the standing rule that a developer machine's numbers never gate anything. These passed:

- `lin_v2`: 14/14
- `lin_partition_v2`: 8/8
- `learner`: 7/7, including the two new scenarios
- the six-pass `elle_check.sh`, all green: `quiet failover partition purge reconfig quiet_two_fsm`

`cargo test -p uc_crashtest --features hard-crash-tests` read 4 passed / 2 failed. These were `ok`: `linearizable_under_service_sigkill`, `two_fsm_service_sigkill`, `node_sigkill_recovery` and `two_fsm_node_sigkill`.

**`sigkill_mid_config_window` and `leader_node_sigkill_recovery_multi` failed reproducibly on this box.** They also failed on the pristine pre-FSM-identity base commit, with none of this work's changes applied. A stash round-trip against `testing/uc_crashtest` verified that. This work does not touch `testing/uc_crashtest`. Both failed with the same `cnc attach error: No such file or directory`. That came after a real SIGKILL and respawn under a tight `UC2_CLIENT_TIMEOUT_MS=1500`.

This is a pre-existing flake, not a regression from FSM identity. `sigkill_mid_config_window` was already documented at ~5%. It is recorded here because this run saw it fail 5/5 times on this box. That is a stronger rate than the documented ~5%. It is worth a look by whoever owns that flake next. It is not recorded because FSM identity caused it.

## Wire crypto

### Some adversaries are out of model

Wire crypto is opt-in and off by default. With it disabled, the posture is a trusted network. With it enabled, the threat model is a network-path adversary, someone who can read and inject packets between nodes.

A compromised host and a malicious cluster member are explicitly **out of model**. The group key is symmetric, meaning every node holds the same key for fan-out traffic. So any holder can forge fan-out traffic as any node. See the [M8 gate record](../benchmarks/uc2-m8-gate-2026-07-29.md) and [`docs/security/threat-model.md`](/docs/security/threat-model.md).
