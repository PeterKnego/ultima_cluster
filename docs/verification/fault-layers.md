# Fault injection on real nodes: the in-process layer and netfault

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This tier injects network faults into real UC nodes instead of a simulated world. It has two layers. The in-process layer adds faults inside the node's own socket shim. It proves jumbo-frame discovery, which the simulator cannot see. The kernel-level layer, `testing/netfault`, impairs a shipped binary's UDP traffic on a real Linux kernel.

## The in-process fault layer: jumbo-frame discovery, 2.12.0

### What the simulator cannot see

The simulator, [`uc_sim`](simulation.md), models loss, duplication, reordering, corruption and replay. But it does **not model datagram size**. Every datagram in the simulated world is a message, not a byte count.

So path-MTU discovery cannot be adjudicated there. The MTU is the largest datagram a network path carries in one piece. Path-MTU discovery is the jumbo-frames feature of `2.12.0`. This gap is stated here rather than papered over.

Discovery works by climbing a ladder of datagram sizes. Each step on that ladder is a rung. The rung a cluster settles on is ordinary replicated `Settings` data, and the sim's `inv12` already sweeps that data's FSM-versus-kernel invariants. What the sim cannot reach is the measuring that picks the rung.

### Two size-aware faults

Discovery is proved instead on **real nodes over the real transport**. Two size-aware faults were added to `uc_net::fault::FaultConfig` for this:

- **`max_datagram`**: a send longer than this is dropped whole. That is how a do-not-fragment datagram vanishes at a hop that cannot carry it. Do-not-fragment is an IP flag that tells routers to drop a too-large datagram instead of splitting it. This fault is the stand-in for a narrow *path*.
- **`emsgsize_over`**: a send longer than this fails with `EMSGSIZE` instead of leaving the host. `EMSGSIZE` is the kernel's "message too long" error. This fault is the stand-in for the *local* refusal when the route's MTU is already known.

Both faults are checked before the seeded rolls. So neither consumes an RNG draw. And neither perturbs an existing scenario's seed.

### The integration tests: `uc_node/tests/jumbo.rs`

Each case in `uc_node/tests/jumbo.rs` runs three real loopback nodes. Loopback carries 65 536 B, so an uncapped cluster resolves at the ladder's top rung. The cap stands in for the narrow hop. The tests pin the following:

| test | what it proves |
|---|---|
| `discovery_lands_on_the_capped_rung_on_every_node` | Every node commits exactly the capped rung. The committed value is the minimum over paths, not a local guess. |
| `an_uncapped_loopback_cluster_reaches_the_top_rung` | The ladder walks all the way up when nothing blocks it. |
| `the_ceiling_holds_at_baseline_while_one_member_is_silent` | One unanswered member pins the cluster at the baseline. An unresolved peer is *no evidence*, never optimism. |
| `a_client_attached_before_the_raise_sees_it` | A client attached before the raise submits a jumbo command after it. It learns the raise through the live cnc `payload_ceiling` word. |
| `the_force_gate_refuses_a_path_too_narrow` / `..._a_silent_peer` | `force_jumbo_frames` fail-stops by name. The two refusals are distinguished: a peer that answered too small versus one that never answered. |
| `a_restart_below_the_committed_rung_refuses_to_join` | A node restarting against a committed rung its path cannot carry refuses by name instead of joining. |
| `a_healthy_restart_on_a_jumbo_cluster_does_not_refuse` | The companion negative. A mid-ladder peer is *not* degradation, so an ordinary restart serves instead of crash-looping. |
| `a_survivor_restarted_with_one_member_down_passes_on_a_proven_quorum` | The join gate's pass is a QUORUM of voters, never silence or a timer. A survivor restarted beside a dead member proves the rung to the other survivor. That is self plus 1 of 3, and it serves. The record says so through `proven_voters`/`voters`. The test was watched red against the unproven pass it replaced. That means it was first seen to fail against the old code. |

### Why the refusal pair matters most

The two force-gate refusal tests are the proof that matters most. The defect they pin was a state-classification bug, found twice in review. A peer whose jumbo ack has simply not landed yet looks exactly like a narrow one. The refusal check can only tell them apart if it also asks whether the peer's fast probe ladder has been spent.

### The deterministic reds are unit tests

A deterministic red is a test that reliably fails when the bug is put back. For this gate, the deterministic reds live in the unit tier, not in `jumbo.rs`.

**On loopback, the integration test is a guard, not the oracle.** The oracle is the rule that decides pass or fail. On loopback the acks land microseconds apart, so the integration test cannot be the one that catches the race. This is stated here so the proof is not read as stronger than it is.

**The refusal classification** has two reds:

- `narrow_peers_needs_an_answer_and_a_spent_fast_ladder` in `uc_net::probe`
- `a_mid_ladder_answer_holds_serving_but_never_refuses` in `uc_node::node`

**The quorum rule** has seven. One is in `uc_net::probe`, `proven_count_counts_only_the_listed_members_at_or_above_the_rung`. The other six are in `uc_node::node`:

- `silence_holds_the_join_gate_until_a_quorum_of_voters_proves_the_rung`
- `a_two_voter_cluster_needs_the_other_voter_proven`
- `a_four_voter_cluster_needs_two_proven_peers`
- `a_joining_learner_needs_one_proven_voter_not_a_majority`
- `a_learner_peers_proof_does_not_count_toward_the_quorum`
- `a_holding_join_gate_says_so_on_a_slow_cadence`

**Two ordering guards** in `uc_node::node` pin that the refusal is checked before the quorum on every poll. The shipped gate already satisfied them. One is for a voter, `a_proven_narrow_peer_refuses_even_when_the_other_voters_are_a_proven_quorum`. The other is for a learner peer, `a_narrow_learner_peer_refuses_even_under_a_proven_quorum`.

**One freshness guard** in `uc_net::probe`, `a_proof_at_a_lower_rung_survives_the_peer_going_silent`, passed on first run by design. It pins that `proven_count` applies NO freshness test. It does so at the one rung, 8832, where a proven peer keeps being probed and so can age. The mutation it catches is a future "make proofs expire". A mutation is a bug injected on purpose. So this guard does not point to a defect in this branch.

### How to run it

```bash
cargo test -p uc_node --test jumbo
```

## The kernel-level fault layer: `testing/netfault`, unreleased

### Why a second layer

Everything in the in-process layer impairs the network from **inside** the process. `FaultSocket` is a shim the test builds the node with. The sim's world is virtual. Neither can touch a shipped binary on a real kernel, and neither can single out one message kind on one link. [`testing/netfault`](/testing/netfault) can do both.

### What netfault is

netfault is a pair of `tc` eBPF programs. eBPF programs are small programs the Linux kernel verifies and runs, and `tc` is the traffic-control hook that runs them on each packet. They are forked from Fredrik Dahlberg's [faulteron](https://github.com/FredrikJDahlberg/faulteron), which was written for Aeron.

The programs drop, duplicate and delay UDP by peer, port and **UC datagram kind**. The 16-byte datagram header is cleartext even under wire crypto. So `--kind nak` works with `[crypto] enabled`. A NAK is a negative acknowledgement. A receiver sends one to ask for bytes it missed.

netfault is what "drop only NAKs between node 2 and node 3" means on a fleet. It is also what "delay every `SNAP_CHUNK` by 50 ms" means.

### What is proved about the tool itself

These are the tool's own tests, as of today:

| test | how it runs | what it proves |
|---|---|---|
| `testing/netfault/test/netfault-rules.sh` | Rootful docker, about 10 s. | Every rule kind is tested against hand-made datagrams between two containers. It checks which rule each packet went to and what each rule counted. It checks what the receiver saw: delay, duplicates both ways, and reordering. It checks that a term bump is a new frontier and not a repair. It checks that a sealed flow's `retransmit` frontier counts log bytes, not wire bytes. It checks that bad input changes no rule. It checks that `remove` and `detach` undo exactly what they should. |
| `uc_crashtest/tests/netfault_pins.rs` | Plain `cargo test`. | It checks the C header layout, the crypto overhead and the CLI's whole kind table against the `uc_protocol` constants. It checks both ways. So an added kind without a name fails, and a renumbered kind fails too. It was watched red against a renumbered `nak` and a moved `kind` offset before it was trusted. |

### What netfault has proved about UC

The scenarios live in `testing/uc_crashtest/tests/netfault.rs`, since 2026-11-03. They run nightly in two legs, one cleartext and one crypto-on. The design is `docs/superpowers/specs/2026-11-03-uc2-netfault-scenarios-design.md`.

Each scenario runs three real node processes on `127.0.0.2/3/4`. It reuses the shmem workers and the WGL checker of the [hard-crash suite](hard-crash.md). WGL is the Wing-Gong-Lowe linearizability search. A history is linearizable when every operation appears to take effect at one instant between its call and its return.

The faults are ones no in-process layer can express: one kind, one link, one direction. The direction is chosen with `--out --peer`.

Every scenario makes three checks:

- its rule **fired**, because a rule that counted nothing is a failed scenario, not a pass
- the recorded history is linearizable
- its own liveness claim holds

The history check runs first, after the run. Only then are the fired and liveness checks, recorded during the run, asserted.

In the table, L is the leader and F1 and F2 are the two followers. The results are from the first run, 2026-11-03.

| scenario | fault | what it proves |
|---|---|---|
| `nak_loss_on_one_link` | Every NAK F1 sends toward L is dropped. 1 DATA in 10 from L to F1 is lost. | L and F2 commit on: ok 29→882 in 5 s. F1's durable reached L's commit within 10 s of heal. 1 774 ops were linearizable. 946 NAKs were dropped. |
| `votes_silenced` | F1's VOTE datagrams never leave. L is SIGKILLed. | The cluster elects **F1**, the member nobody can hear voting. F2 cannot collect a second vote, but F1 can. 4 votes were dropped when F2 campaigned. Whether F2 campaigns is a timer race. A counting rule on F2's REQUEST_VOTEs makes the assertion exact about it. L restarts and the cluster re-converges on one leader. The history is linearizable across the failover. |
| `one_direction_slow` | Everything F1 sends toward L is held 20–50 ms and reordered. | Commit and linearizable reads keep completing on F2's prompt reports: ok 13→818, and ok reads 5→279. 1 108 datagrams were delayed. |
| `repairs_dropped` | 1 DATA in 10 from L to F1 is lost. AND every repair to F1 is dropped. netfault classifies those repairs as `retransmit`. | F1 froze at durable 1 728 while commit reached 37 728. 1 029 repairs were dropped. F1 caught up within 10 s of heal. This is netfault's `retransmit` classification seen live against UC's own repairs. |

### Two harness lessons from that run

- **A dead leader can look alive.** A SIGKILLed node's cnc page is frozen with `LEADER|CAN_SERVE` still set. This is #35. So a leader wait that spans a kill must name the live nodes, with `await_single_leader_among`. The all-dirs wait returned the dead leader at once.
- **The history is checked first.** The fired and liveness checks are asserted only after the history is checked. So a missed counter or a timeout never hides a `Violation`, which the design spec's §7 ranks above everything else.

### What is not done: the fleet

The netfault scenarios design has a plan 2: the same four scenarios on `bench-infra`'s hosts under load, recorded as a gate doc. It has not been written or run. Its first run is to be a measurement with no bar. That was the maintainer's decision on 2026-11-03. The fleet run is also listed as a direction in `docs/BACKLOG.md` §5.

The fleet run can reach two open items for the first time:

- Jumbo row c's soak. It is NOT RUN for want of exactly this.
- The unexplained 58-minute `remote_lin` hang of 2026-09-08.

### How to run it

```bash
(cd testing/netfault && test/netfault-rules.sh)   # the tool's self-test; DOCKER_CONTEXT=default where the default is rootless
cargo test -p uc_crashtest --test netfault_pins   # the wire pin
make -C testing/netfault && sudo -v && cargo test -p uc_crashtest --features netfault-tests --test netfault -- --test-threads=1   # the scenarios, as root
testing/netfault/scripts/run-suite-in-container.sh [--crypto]   # the scenarios without host root: a privileged container on a rootful docker
```
