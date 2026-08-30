#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run every fuzz target (or the named ones) for SECS seconds each on the
# committed corpus. Exit 1 on the first crash. Needs: rustup nightly + cargo-fuzz.
#
# Usage: fuzz_smoke.sh [--min-runs N] [SECS] [TARGET...]
#
#   --min-runs N  after each target, parse libFuzzer's "Done <N> runs" line and
#                 fail if it executed fewer than N inputs. A target that runs
#                 but executes almost nothing prints a perfectly clean line
#                 (see the -print_funcs=0 comment below for the real incident
#                 that motivated this), so "exit 0" alone does not mean
#                 "actually fuzzed". CI passes this; locally it is optional.
set -euo pipefail

MIN_RUNS=0
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --min-runs)   MIN_RUNS="${2:?--min-runs needs a value}"; shift 2 ;;
    --min-runs=*) MIN_RUNS="${1#*=}"; shift ;;
    *)            ARGS+=("$1"); shift ;;
  esac
done
set -- ${ARGS[@]+"${ARGS[@]}"}

SECS="${1:-30}"; shift || true
cd "$(dirname "$0")/../fuzz"
# `cargo fuzz list` reports every [[bin]] in fuzz/Cargo.toml, which includes the
# `seed-corpus` GENERATOR — fuzzing that would waste a sanitizer build, rewrite
# the committed corpus mid-run, and print a meaningless "clean" line. Skip it.
if [ $# -gt 0 ]; then TARGETS=("$@"); else mapfile -t TARGETS < <(cargo +nightly fuzz list | grep -v '^seed-corpus$'); fi

LOG="$(mktemp -p "${TMPDIR:-$PWD}" fuzz_smoke.XXXXXX)"
trap 'rm -f "$LOG"' EXIT

for t in "${TARGETS[@]}"; do
  echo "== fuzz $t (${SECS}s) =="
  # -print_funcs=0 is NOT cosmetic. libFuzzer symbolizes every newly-discovered
  # function to print a NEW_FUNC line, and cargo-fuzz builds with its own
  # `--config profile.release.debug="line-tables-only"` (the root workspace's
  # [profile.release] does not apply here — fuzz/ is an excluded, separate
  # workspace), so the larger targets are ~27 MB of debug info. llvm-symbolizer
  # needs ~90 s to index one of those for a SINGLE address — more than the whole
  # fuzz budget. Measured on uc_node_toml: 400 runs took 90,180 ms with
  # symbolization and 57 ms without. Four of the fourteen targets were getting
  # ~20 executions per run instead of tens of millions before this was added.
  # A crash still writes its artifact, which can be symbolized deliberately.
  set +e
  cargo +nightly fuzz run "$t" -- -max_total_time="$SECS" -timeout=10 -rss_limit_mb=2048 -print_funcs=0 2>&1 | tee "$LOG"
  rc="${PIPESTATUS[0]}"
  set -e
  if [ "$rc" -ne 0 ]; then echo "CRASH in $t (see fuzz/artifacts/$t/)"; exit 1; fi

  if [ "$MIN_RUNS" -gt 0 ]; then
    # libFuzzer's last line on a clean -max_total_time run is
    #   Done <N> runs in <S> second(s)
    runs="$(sed -nE 's/^Done ([0-9]+) runs in .*/\1/p' "$LOG" | tail -1)"
    if [ -z "$runs" ]; then
      echo "NO RUN COUNT for $t: libFuzzer printed no 'Done N runs' line — the run did not complete normally"
      exit 1
    fi
    if [ "$runs" -lt "$MIN_RUNS" ]; then
      echo "TOO FEW RUNS in $t: $runs < $MIN_RUNS in ${SECS}s — the target built and exited clean but barely executed."
      echo "  This is the symbolizer-stall failure mode (see -print_funcs=0 above), or a target whose harness"
      echo "  is doing per-input setup work. A green run that fuzzes nothing is worse than a red one."
      exit 1
    fi
    echo "-- $t: $runs runs in ${SECS}s (floor $MIN_RUNS)"
  fi
done
echo "fuzz smoke: all targets clean"
