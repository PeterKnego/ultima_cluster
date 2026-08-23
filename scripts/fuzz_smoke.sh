#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run every fuzz target (or the named ones) for SECS seconds each on the
# committed corpus. Exit 1 on the first crash. Needs: rustup nightly + cargo-fuzz.
set -euo pipefail
SECS="${1:-30}"; shift || true
cd "$(dirname "$0")/../fuzz"
# `cargo fuzz list` reports every [[bin]] in fuzz/Cargo.toml, which includes the
# `seed-corpus` GENERATOR — fuzzing that would waste a sanitizer build, rewrite
# the committed corpus mid-run, and print a meaningless "clean" line. Skip it.
if [ $# -gt 0 ]; then TARGETS=("$@"); else mapfile -t TARGETS < <(cargo +nightly fuzz list | grep -v '^seed-corpus$'); fi
for t in "${TARGETS[@]}"; do
  echo "== fuzz $t (${SECS}s) =="
  # -print_funcs=0 is NOT cosmetic. libFuzzer symbolizes every newly-discovered
  # function to print a NEW_FUNC line, and the workspace release profile keeps
  # `debug = 1`, so the larger targets are ~27 MB of debug info. llvm-symbolizer
  # needs ~90 s to index one of those for a SINGLE address — more than the whole
  # fuzz budget. Measured on uc2_node_toml: 400 runs took 90,180 ms with
  # symbolization and 57 ms without. Four of the fourteen targets were getting
  # ~20 executions per run instead of tens of millions before this was added.
  # A crash still writes its artifact, which can be symbolized deliberately.
  cargo +nightly fuzz run "$t" -- -max_total_time="$SECS" -timeout=10 -rss_limit_mb=2048 -print_funcs=0 || { echo "CRASH in $t (see fuzz/artifacts/$t/)"; exit 1; }
done
echo "fuzz smoke: all targets clean"
