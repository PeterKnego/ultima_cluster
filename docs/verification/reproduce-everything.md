# Reproducing everything

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

Each tier's page ends with its own "How to run it" block. This page collects them in one place, in the order the overview lists the tiers.

```bash
# Proofs — from proofs/
lake build                                                  # 3177 jobs, zero sorry
# model vs. real Rust: generate vectors from the Rust, then replay them in Lean
cargo run -p uc_consensus --release --example conform_gen -- \
  --out $HOME/.cache/uc2-conform/vectors.jsonl --count 100000 --seed 1
lake exe conform $HOME/.cache/uc2-conform/vectors.jsonl

# Simulation
cargo test -p uc_sim --features sim-heavy                  # 1000-seed fuzz

# Linearizability + the rest of the suite
cargo test --workspace

# Transactional safety
scripts/elle_check.sh                                       # clean tier
scripts/elle_mutation.sh                                    # the harness's teeth

# Alert rules — every shipped rule fired against a real cluster (needs promtool)
scripts/m10_alert_fire.sh

# Hard crash
cargo test -p uc_crashtest --features hard-crash-tests

# Memory model
RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release

# Fuzzing — needs nightly + cargo-fuzz; CI uses 600 with --min-runs 10000
scripts/fuzz_smoke.sh 30                                    # every target
scripts/fuzz_smoke.sh --min-runs 10000 600 uc_node_toml    # one, at CI budget

# Miri — the pure decoders (needs the miri component on nightly)
cargo +nightly miri test -p uc_protocol --lib -- v2:: version::
cargo +nightly miri test -p uc_journal --lib -- \
  stable_value::tests:: error::tests:: journal::segment::tests::header_ \
  journal::segment::tests::record_ journal::segment::tests::decode_
```

Elle histories must not be written to `/tmp` on a RAM-backed box. Both Elle scripts default to `$HOME/.cache`.

Pages with more commands than the block above: [the fault layers](fault-layers.md) for the jumbo and netfault tests, [hard crash](hard-crash.md) for the rolling wire-level upgrade, and [loom](loom.md) for the Broadcast and write-reserve models.
