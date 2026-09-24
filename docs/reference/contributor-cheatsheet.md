# Contributor command cheatsheet

This page lists the commands that you use to work on `ultima_cluster` (UC)
itself: build, lint, the proof tiers and the benchmark harnesses. Each row
tells you what a command does and why you use it. To build an application on
UC, or to operate a cluster, use the [command cheatsheet](cheatsheet.md).

| Command | What it does | Why you use it |
|---|---|---|
| `cargo build --workspace` | Builds all workspace crates. | The first check after a change. |
| `cargo test` | Runs the unit tests, the in-process integration tests and the simulation tests. | The default test suite. |
| `cargo fmt --all -- --check` | Finds code that `rustfmt` would change. | CI runs this step first, and a difference fails CI. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Runs the linter on the pinned stable toolchain. | CI fails on any warning. |
| `cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings` | Runs the linter on the minimum supported Rust version (MSRV). | Version 1.89 finds lints that the newer toolchain does not. Run it before each push. |
| `cargo test -p uc_node --test lin_v2` | Runs the linearizability capstone under failover, purge and snapshot load. | Prove that a consensus or replication change keeps each history linearizable. |
| `cargo test -p uc_node --test lin_partition_v2` | Runs the linearizability capstone under network partitions and quorum loss. | The same proof, for partitions. |
| `cargo test -p uc_crashtest --features hard-crash-tests` | Starts real node and service processes and kills them with `SIGKILL` under load. | Prove recovery after a real crash. |
| `scripts/fuzz_smoke.sh 60 --min-runs 10000` | Runs each fuzz target for 60 seconds. It needs the nightly toolchain and `cargo-fuzz`. | The regression gate for all decoders of untrusted bytes. |
| `(cd fuzz && cargo +nightly fuzz run <TARGET> -- -max_total_time=600)` | Runs one fuzz target for 10 minutes. | Search one decoder for new faults. [`fuzz/README.md`](../../fuzz/README.md) |
| `scripts/elle_check.sh` | Runs six list-append histories through the Elle consistency checker. It needs Java and `jq`. | Check transactional consistency. Set `ELLE_DIR` to a directory on disk, not on `/tmp`. |
| `scripts/elle_mutation.sh` | Injects three known consensus faults and makes sure that Elle finds each one. | Prove that the Elle tier can find real faults. |
| `(cd proofs && lake exe cache get && lake build)` | Builds the Lean model, the theorems and the conformance checker. It needs `elan`. | Check the formal proofs after a change to the consensus model. |
| `RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --release --test loom_mpsc` | Runs the loom model of the MPSC ring. Use `--test loom_broadcast` for the broadcast ring. | Find memory-order faults that x86 tests cannot show. |
| `python3 scripts/check_doc_links.py` | Examines each internal link and anchor in the Markdown files. | CI fails on a dead link. Run it after a change to documentation. |
| `cargo run -p uc_node --release --example m5_gate` | Runs the throughput gate harness. The `m6_gate` and `m7_gate` examples run the snapshot and reconfiguration gates. | Get a smoke result on a development host. Rate bars are for the fleet only. [Benchmarks](../BENCHMARKS.md) |
| `scripts/apply_ab.sh <BASE> <HEAD>` | Builds and compares the `apply_bench` binaries of two commits. A third arm rebuilds the head commit as a control. | Measure a change in the speed of the apply hop, and the noise of the build. |
| `scripts/hop1_ab.sh --sink <BIN> --a <BIN> --b <BIN>` | Compares two `hop_bench` client drivers that you built. It has no rebuild control, so build the same source two times yourself. | Measure a change in the speed of the client hop. |

`CLAUDE.md` in the repository root has the full list of build and proof
commands.
