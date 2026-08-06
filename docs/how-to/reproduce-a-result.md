# How to reproduce a published result

Re-run a performance or correctness claim on your own hardware, and get a
number you can compare against the published one.

Every claim in this repository names what it was measured on. Reproducing one
means matching that, or knowing which way your difference cuts.

## Decide what you are reproducing

| Claim | Record | Reproduce with |
|---|---|---|
| Throughput and latency | [`BENCHMARKS.md`](../BENCHMARKS.md) | a gate binary, below |
| A correctness tier | [`VERIFICATION.md`](../VERIFICATION.md) | the tier's own command, below |
| A specific dated run | the file under [`benchmarks/`](../benchmarks/) | that record's own "Reproducing" section |

## Run a performance gate

The `m*_gate` example binaries run the milestone scenarios and exit 1 on an
honest failure, so a zero exit is the pass signal.

```bash
cargo run -p uc2_node --release --example m5_gate -- all
cargo run -p uc2_node --release --example m6_gate -- all --secs 6 --cycles 5
cargo run -p uc2_node --release --example m7_gate -- all --secs 6
```

Put journals on a real disk. The gates guard against `/tmp` because a journal on
RAM makes every `fsync` a no-op and the resulting number means nothing. For a
fleet run, pass an explicit `journal_root` on the real volume.

A local run of `m5_gate` is a smoke test, not the gate — the harness says so
itself. The published headline numbers are fleet numbers, and a single
oversubscribed box cannot reproduce them.

## Run a fleet gate

Fleet runs cost real money and are a deliberate, separately-approved step.
`bench-infra/` provisions the hosts.

```bash
bench-infra/scripts/m6_fleet_gate.py --fleet                  # M6
bench-infra/scripts/m6_fleet_gate.py --fleet --m7             # M7
bench-infra/scripts/m6_fleet_gate.py --fleet --read-profile   # read profile
```

The orchestrator `stat -f`s every instance-directory parent, on every host, and
refuses to run on `tmpfs` or `ramfs`.

Tear the fleet down afterwards and verify it is gone — the terraform state
directory is the authority, not the console.

## Run a correctness tier

```bash
cargo test                                        # in-process integration + sim
cargo test -p uc2_node --test lin_v2              # linearizability under failover + purge churn
cargo test -p uc2_node --test lin_partition_v2    # partition and quorum loss
cargo test -p uc2-crashtest --features hard-crash-tests   # multi-process SIGKILL
scripts/elle_check.sh                             # transactional safety (needs java + jq)
(cd proofs && lake exe cache get && lake build)   # Lean proofs
```

Add `UC2_CRYPTO=1` to run the crypto arm of the crashtest and elle tiers.

Point `ELLE_DIR` at real disk. It defaults to `$HOME/.cache/`, which is correct;
overriding it to `/tmp` will OOM-kill the run rather than fail it.

## Compare honestly

Two things decide whether your number means anything.

**Match the hardware class, or know the direction of the difference.** The
published fleet numbers are `c6id.2xlarge`, single AZ, cluster placement group,
journals on local NVMe, `Durability::Consistent`. A slower disk moves throughput
down; a quieter box moves it up.

**For anything probabilistic, fix your sample size first.** Correctness tiers
that fail intermittently cannot be judged by a few runs — decide how many runs
you need to exclude the rate you care about, then run that many. Reading a
short clean streak as a pass is the most common way to get this wrong.

## If a tier fails

See [Investigate a failed correctness run](investigate-a-failed-run.md).
