# Verification

What is **proved**, what is **checked**, what is only **bug-hunted**, and how to reproduce each one yourself.

"Verified" is a word that hides an enormous range. A machine-checked theorem and a passing fuzz run are both often called verification. They carry very different weight. This page sorts UC's evidence by strength, and each tier has its own page that states what it covers and where it stops. The boundaries between tiers are stated plainly, not blurred.

**How current this is.** The pages were last reconciled as a set at the M14c2 proof pass (`2.8.1`, 2026-08-30). Later work added to individual pages as it shipped, up to the rolling wire-level upgrade test (`2.15.0`, unreleased). Each page cites the dated record it summarizes. Where a page and its dated record disagree, the dated record wins.

## The tiers, strongest first

| Tier | Strength | What it covers | Page |
|---|---|---|---|
| Lean 4 proofs | Machine-checked | The consensus safety kernels. Election safety and log matching over an N-node protocol model. | [Lean proofs](verification/lean-proofs.md) |
| Conformance rig | Executable, exhaustive by sampling | That the Lean model and the real Rust agree, vector by vector. | [Lean proofs](verification/lean-proofs.md) |
| Deterministic simulation | Checked under seeded fuzz | Twelve whole-cluster safety invariants under fault injection. | [Simulation](verification/simulation.md) |
| Upgrade-lifecycle tests | Checked on real nodes and services | Pins, the pinned install at attach, pin-verify, snapshot-hash reports, and the row running version. | [Upgrade lifecycle](verification/upgrade-lifecycle-tests.md) |
| In-process fault layer | Checked on real nodes, deterministically | Datagram-size faults on the real transport: path-MTU discovery, both jumbo startup gates, and a client seeing a raised ceiling (`2.12.0`). | [Fault layers](verification/fault-layers.md) |
| Kernel-level fault layer | Checked on real processes on a real kernel | One datagram kind, one link, one direction, with `testing/netfault` (unreleased). | [Fault layers](verification/fault-layers.md) |
| WGL lincheck capstones | Checked on real processes | Linearizability of a register under leader kills, crashes, partitions and purge. Single and two FSMs. | [Linearizability](verification/linearizability.md) |
| Elle | Checked on real processes | Transactional safety, serializable and strict, single and two FSMs. A mutation tier proves the harness has teeth. | [Elle](verification/elle.md) |
| Multi-process crashtest | Checked on real processes | Recovery correctness under `SIGKILL` mid-load. Single and two FSMs, plus the rolling wire-level upgrade. | [Hard crash](verification/hard-crash.md) |
| loom | Exhaustive over interleavings | The frame-visibility protocol, the MPSC ring's per-record commit, the Broadcast ring's seqlock read barrier, and the follower write-reserve seqlock (#78). | [loom](verification/loom.md) |
| Fuzzing (libFuzzer) | Checked under coverage-guided input search | Totality of the twenty-seven decoders that see bytes the process did not write. | [Fuzzing and Miri](verification/fuzzing.md) |
| Miri | Checked under a symbolic interpreter | Undefined behaviour in the pure wire and journal decoders and `uc_remote`'s Vec-backed SPSC internals. Not the file-backed rings. | [Fuzzing and Miri](verification/fuzzing.md) |
| Veil | Bug-hunting only, never the record | Bounded model checking of the election and reconfiguration planes. | [Veil](verification/veil.md) |
| `log_clock` unit suite | Ordinary unit tests | The monotonic log clock's pure arithmetic. See below. | this page |

**The `log_clock` unit suite** is 17 tests in `uc_node::log_clock`, run with `cargo test -p uc_node --lib log_clock`. They cover the steady state, forward-step adoption, backward-step smear and full retirement, tolerance, and multiple resamples against a smear in flight. One of those, `a_smear_in_flight_is_not_re_reported_and_the_gauge_counts_down`, checks that one step is reported, that the remaining-smear gauge falls strictly, and that it never undershoots, over 3 000 resamples. The rest are a seeded-random step sequence and `bracket_sample` and `resample_slow` against the real clock.

## The headline result

The Lean effort is not decorative. **It found and fixed four real, shipped safety bugs** in code that had already passed the simulation, linearizability and crash-recovery tiers. They are Findings #5, #6b, #8 and #9 in [`docs/benchmarks/uc2-lean-gate-2026-07-16.md`](/docs/benchmarks/uc2-lean-gate-2026-07-16.md). Two of them could lose acknowledged writes.

- **Finding #6b** is a Raft §5.4.2 bug, the class Figure 8 of the Raft paper describes. The §5.4.2 barrier was applied to linearizable reads, to ingress admission and to reconfiguration. It was never applied to the commit advance itself. So a leader routinely committed a range that only an old term had written, below the barrier. A divergent rival with a higher term could then win the next election and truncate those committed bytes on every node. The proof effort drove this end to end with a 46-step, five-node, machine-checked countermodel.
- **Finding #5** is a gate that did not survive a reboot. A voter granted term T, held a divergent tail, and crashed before reconciling. It rebooted with the reconciliation intake gate open. Its stale same-term report then fed the leader's commit tracker, which certified a **phantom commit** over content the voter did not hold.

Both were found by trying to prove a theorem and discovering it was false against the code as shipped. Neither was reachable by the fuzz tiers that had been green for weeks. That is the argument for doing this work, and the reason this page leads with it.

## What is not verified

The most important page is [What is not verified](verification/not-verified.md). The short version:

- `leader_completeness` is not proved, and the Lean model collapses the durable counter's two readers into one, which makes a load-bearing lemma trivially true in the model and false in the real system.
- The proofs cover the consensus kernels, not the whole node. The data plane, the archive, the IPC rings and the service SDK are covered by the simulation, linearizability and crash tiers only.
- Your state machine's determinism is your responsibility. No tier here can catch a clock or a random number inside `apply`.
- The IPC rings' interleavings are covered by loom for MPSC and Broadcast only. SPSC, the futex layer and the mapping itself are covered by nothing.
- Fuzzing is a regression gate, not a proof of totality. CI runs tests on x86_64 only. Coordinated snapshots have no fleet numbers.

The full page lists every gap with its reason and what covers the nearby ground.

## Benchmarks and continuous integration

Performance bars are committed before the run, and a miss is recorded as a FAIL with the bar left where it was. The rules and the hop-isolation harnesses are on [Benchmark methodology](verification/benchmark-methodology.md).

| Workflow | Contents |
|---|---|
| `ci.yml` | The fast gate on every PR: workspace build, tests, clippy with `-D warnings`. |
| `nightly.yml` | The full proof suite. The lincheck capstones, single and two-FSM. `sim-heavy`. loom. The crashtest, single and two-FSM, plus the two-binary rolling wire-level upgrade. The Elle clean tier's six passes. `lean-proofs` with a conformance replay on a date-rotated seed. `fuzz`, six legs at 600 s per target with an asserted run-count floor. `miri` over the pure decoders and `uc_remote`'s SPSC. The netfault scenarios, cleartext and crypto-on. |
| `elle-weekly.yml` | The Elle mutation tier. |

## Reproducing everything

Every tier page ends with its own commands. [Reproducing everything](verification/reproduce-everything.md) collects them in one block.

## All pages

1. [Machine-checked proofs in Lean 4](verification/lean-proofs.md)
2. [Deterministic simulation](verification/simulation.md)
3. [The FSM upgrade lifecycle: what the tests prove](verification/upgrade-lifecycle-tests.md)
4. [Fault injection on real nodes: the in-process layer and netfault](verification/fault-layers.md)
5. [Linearizability: the WGL capstones](verification/linearizability.md)
6. [Transactional safety: Elle](verification/elle.md)
7. [Multi-process hard crash](verification/hard-crash.md)
8. [Memory model: loom](verification/loom.md)
9. [Fuzzing and Miri: decoders on untrusted bytes](verification/fuzzing.md)
10. [Veil: bug-hunting only, never the record](verification/veil.md)
11. [Benchmark methodology](verification/benchmark-methodology.md)
12. [What is not verified](verification/not-verified.md)
13. [Reproducing everything](verification/reproduce-everything.md)
