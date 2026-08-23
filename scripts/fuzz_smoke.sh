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
  cargo +nightly fuzz run "$t" -- -max_total_time="$SECS" -timeout=10 -rss_limit_mb=2048 || { echo "CRASH in $t (see fuzz/artifacts/$t/)"; exit 1; }
done
echo "fuzz smoke: all targets clean"
