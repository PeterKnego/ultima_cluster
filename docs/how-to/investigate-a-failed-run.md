# How to investigate a failed correctness run

What to do when an elle tier, a capstone, or a proof job comes back red.

The first question is always which of two things you are looking at: a real
consistency violation, or a harness that has lost its teeth. They point in
opposite directions, and elle's two tiers fail for opposite reasons.

## A clean-tier elle failure

`scripts/elle_check.sh` runs six passes: quiet, failover, partition, purge,
reconfig, and — since `2.8.1` — `quiet_two_fsm`, a two-FSM cluster that
records **one history per FSM**. The two-FSM pass therefore writes
`$ELLE_DIR/quiet_two_fsm/fsm0/history.edn` and `.../fsm1/history.edn` instead
of a single `history.edn`, and each is adjudicated separately: the FAIL line
names the FSM (`quiet_two_fsm/fsm1`), which is the first thing to read — a
violation on one FSM only is a different bug from one on both.

A FAIL here means elle found a dependency cycle, or an aborted or stale read
that linearizability forbids. **Treat it as a real consistency bug, not flake.**

The reproducible artifact is the history, with its seed alongside:

```
$ELLE_DIR/<pass>/history.edn
$ELLE_DIR/<pass>/seed
```

Re-run the checker by hand for per-anomaly explanations and cycle plots:

```bash
java -Xmx3g -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar \
    --model list-append --consistency-models strong-serializable \
    --verbose --directory out/ "$HOME/.cache/uc2-elle/failover/history.edn"
```

Two verdicts need interpreting before you start debugging:

| Verdict | Meaning |
|---|---|
| `unknown` | a cycle-search timeout, never a pass. Shrink `ELLE_TARGET_OPS` or raise the checker heap, then re-run. |
| `serializable` clean but `strong-serializable` dirty | a real-time violation — a stale read. Suspect the read barrier or the leader-change path. |

Histories go to `$HOME/.cache/uc2-elle*`, on disk. Never point `ELLE_DIR` at
`/tmp`: it is RAM-backed with no swap, and a large history will get the run
OOM-killed rather than failed.

## A mutation-tier failure

`scripts/elle_mutation.sh` proves the harness still catches three injected
consensus bugs. **Its assertions invert.** A mutation that is *not* caught means
the harness has lost its teeth — the product may be fine.

Each tooth has a different oracle, because UC catches each a different way:

| Mutation | Oracle |
|---|---|
| `commit-quorum-minus-one` | elle INVALID under both models |
| `skip-read-barrier` | elle INVALID under the strict model only |
| `skip-vote-order-check` | the driver run hard-fails |

The control arm runs the feature compiled in with `UC2_MUTATION` unset and must
be completely clean; if the control fails, the feature is not inert and that is
the bug.

When a tooth stops biting, raise the dose — `ELLE_MIN_FAULTS`, `ELLE_HOLD_MS`,
worker count. Never weaken the catch.

But first ask **what was detecting it before**. A tooth scored on a symptom
rather than on the property it targets will go quiet the day that symptom is
fixed elsewhere, and raising the dose will not bring it back. That has happened
here: the account is in
[the elle gate record](../benchmarks/uc2-elle-gate-2026-07-16.md).

The vote-order tooth is a timing race and is retried up to
`ELLE_VOTE_ORDER_TRIES` times; it counts as caught if any attempt hard-fails.

## A failure you cannot reproduce under instrumentation

If adding tracing makes the failure disappear, stop adding tracing. Timing-
sensitive races in this codebase have been suppressed outright by per-event
instrumentation on the hot path.

Prefer post-mortem forensics: make the failure path dump what it already has —
counters, frame headers, terms — at zero runtime cost. The archive's
`corrupt_report` is the worked example, and the reasoning is in
[Two framings, one position](../notes/uc2-two-framings-one-position.md).

## Judging whether a fix worked

These failures are probabilistic. A handful of clean runs is not evidence: at a
10% failure rate, twenty clean runs happen 9% of the time.

**Fix the sample size before you read the result**, choosing it from the rate
you need to exclude. This is not a formality — three separate fixes in this
codebase were declared working on 20-to-30-run streaks and were later shown to
have changed nothing.

## After changing a proved kernel

If you touched `commit.rs`, `reconcile.rs`, or `log_ok`, the Lean model mirrors
them one-to-one and the nightly job replays 100,000 conformance vectors.

1. Update the matching `Uc2Model` definition — its doc comments name the Rust
   source.
2. Rebuild the proofs: `cd proofs && lake build`, and repair broken theorems.
   **A theorem that can no longer be proved is a signal**, not an obstacle: the
   change may have broken a safety property. Do not delete or weaken a theorem
   to make the build green without review.
3. Re-run conformance locally, on disk:

```bash
cargo run -p uc2_consensus --release --example conform_gen -- \
    --out $HOME/.cache/uc2-conform/vectors.jsonl --count 100000 --seed 1
cd proofs && lake exe conform $HOME/.cache/uc2-conform/vectors.jsonl
```

## Where to go next

- Re-running a published gate: [Reproduce a published result](reproduce-a-result.md)
- What each tier actually proves: [Verification](../VERIFICATION.md)
