# Transactional safety: Elle

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This tier checks that UC keeps transactions safe under a list-append workload. It runs real clusters, records every operation, and hands the history to Elle. The evidence is a checker verdict on recorded histories, not a proof. A second tier injects bugs on purpose to show the harness can fail.

**Record:** [`docs/benchmarks/uc2-elle-gate-2026-07-16.md`](/docs/benchmarks/uc2-elle-gate-2026-07-16.md)

## What Elle checks

Elle is Jepsen's consistency checker. It builds a dependency graph from a recorded history and searches it for cycles, which are anomalies. A list-append workload is one where each transaction appends values to lists and reads whole lists back, so the order of every write is visible.

The lincheck capstones check linearizability of a *single register*. See [Linearizability capstones](linearizability.md). Elle checks **transactional safety of a list-append workload** by cycle detection over the recorded history. It catches a class of anomaly the register capstone cannot phrase.

## The checker and its models

The checker is a vendored `elle-cli` 0.1.9, pinned by sha256. Histories are recorded in Jepsen EDN.

Both models are run:

- `serializable` means the outcome matches some order in which the transactions ran one at a time.
- **`strong-serializable`** is the strict, real-time model. It means that one-at-a-time order must also respect real time, so a transaction that finished before another began comes first.

A cycle-search timeout is reported as `unknown`. It is treated as a hard **FAIL**, never a pass.

## Clean tier: six passes, all clean under both models

| Pass | Events | serializable | strong-serializable |
|---|--:|---|---|
| quiet | 100,836 | clean | clean |
| failover | 45,702 | clean | clean |
| partition | 51,770 | clean | clean |
| purge | 54,574 | clean | clean |
| reconfig | 96,714 | clean | clean |
| quiet_two_fsm, FSM 0 | 16,062 | clean | clean |
| quiet_two_fsm, FSM 1 | 16,060 | clean | clean |

The five older rows are the 2026-07-16 gate's default sizing. The two-FSM rows are the 2026-08-30 run at `ELLE_TARGET_OPS=8000`. That is the sizing nightly uses. 100 % of ops were `ok` on both FSMs.

### The two-FSM pass

`quiet_two_fsm` is the M14c2 addition, shipped in `2.8.1`. It runs a quiet two-FSM cluster at `FsmLag::Bounded(64 KiB)`. It records **one history per FSM**, at `$ELLE_DIR/quiet_two_fsm/fsm{0,1}/history.edn`. `scripts/elle_check.sh` adjudicates each history separately under both models.

An oracle is the rule that decides pass or fail. The pass also asserts the replication-equivalence oracle at zero. This is the same oracle the [linearizability capstones](linearizability.md) describe.

**That oracle is structurally weak in this tier and carries no equivalence evidence of its own.** The Elle workload's `LaResp` has a single variant. So the two FSMs' answers can only differ by being malformed, such as a missing id or a non-ascending pair. The check can therefore only fail on a malformed fan-in, never on a genuine state divergence. WGL is the Wing-Gong-Lowe linearizability search. The equivalence evidence comes from the WGL capstones in [Linearizability capstones](linearizability.md) and [Multi-process hard crash](hard-crash.md).

### The checker tests itself first

Before any real verdict is trusted, every invocation of this tier first self-tests the checker against two fixtures:

- a known write-skew cycle that must be rejected under `serializable`
- a real-time violation that plain serializability accepts but the strict model must reject

## Mutation tier: proving the harness can fail

A clean verdict from a harness that cannot detect anything is worthless. So this tier injects mutations, which are bugs added on purpose. Three consensus bugs sit behind a `mutation-testing` cargo feature. The feature is off in every default build. The harness must catch all three. A tooth is one injected bug together with the check that must catch it.

| Injected bug | Oracle | Caught as |
|---|---|---|
| `commit-quorum-minus-one` | Elle INVALID under both models | `incompatible-order`, `strong-PL-1-cycle-exists` |
| `skip-read-barrier` | Elle INVALID under the **strict model only** | `G-single-item-realtime` |
| `skip-vote-order-check` | Driver hard-fails | truncation-below-commit panic |

`skip-read-barrier` is the tooth that proves the strict model earns its keep. Its anomaly is invisible to plain serializability. Only the real-time model catches it.

Feature-off inertness is verified explicitly. With the feature off, the default build is byte-identical. The read-path mutation is `#[cfg]`-shadowed out.

### A finding worth recording: two mutations the natural passes could not expose

The mutation tier's original design mapped two of its mutations to the natural failover and partition passes of the clean tier. Empirically, *neither could be exposed there*, because UC's own layered defenses absorb them.

- `kill_and_restart_leader` restarts the same node on its own disk. So a quorum-minus-one-committed tail never actually dies.
- UC has no check-quorum step-down. So skipping only the read barrier still yields no stale read under gross isolation.

Each tooth therefore uses a dedicated adversary, matched to how UC actually catches that bug. This is a real robustness result about the system, not a harness defect. It is exactly the kind of thing a mutation tier exists to discover.

## How to run it

```bash
scripts/elle_check.sh       # clean tier
scripts/elle_mutation.sh    # mutation tier
```
