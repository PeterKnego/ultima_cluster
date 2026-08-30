#!/usr/bin/env bash
# lockstep under CPU oversubscription: reproduce M14 gate row e on the dev box.
# Usage: scripts/lockstep_oversub.sh [--cores 0-1] [--spinners N] [--secs 8] [--bin PATH]
# Runs apply_bench --fsms 2 --mode lockstep (a) unconstrained, (b) pinned to
# --cores with N stress-ng cpu spinners on the same cores. Prints both
# `hop: min applied_frames/s` lines. Smoke only — never a bar.
#
# --bin PATH measures a pre-built binary (each bisect variant lives in its own
# target dir); otherwise the script builds into $CARGO_TARGET_DIR. The binary's
# sha256 is printed once per invocation — provenance matters because the
# default ~/.cache/cargo-target is shared with every other checkout
# (CLAUDE.md "Benchmarking discipline").
set -euo pipefail
CORES="0-1"; SPINNERS=2; SECS=8; ROOT="$HOME/.cache/uc2-apply-bench"; BIN=""
while [ $# -gt 0 ]; do case "$1" in
  --cores) CORES="$2"; shift 2;;
  --spinners) SPINNERS="$2"; shift 2;;
  --secs) SECS="$2"; shift 2;;
  --bin) BIN="$2"; shift 2;;
  *) echo "unknown $1" >&2; exit 2;;
esac; done
if [ -z "$BIN" ]; then
  cargo build --release -p uc_node --example apply_bench >/dev/null
  BIN="${CARGO_TARGET_DIR:-$HOME/.cache/cargo-target}/release/examples/apply_bench"
fi
echo "bin: $(sha256sum "$BIN")"
run() { "$BIN" --root "$ROOT" --fsms 2 --mode lockstep --secs "$SECS" --warmup-secs 1 2>&1 | grep -E 'hop: min|lag_waits'; }
echo "== unconstrained"; run
echo "== pinned to $CORES with $SPINNERS spinner(s)"
# Guard the pinned arm's tools BEFORE running it. stress-ng is launched as a
# background job, whose failure `set -e` does not catch: without this check a
# missing stress-ng would silently produce a NON-oversubscribed rung that reads
# like a clean result.
command -v taskset >/dev/null || { echo "taskset required" >&2; exit 2; }
if [ "$SPINNERS" -gt 0 ]; then
  command -v stress-ng >/dev/null || { echo "stress-ng required" >&2; exit 2; }
  taskset -c "$CORES" stress-ng --cpu "$SPINNERS" --timeout $((SECS + 3)) --quiet & SP=$!
  sleep 0.5
  taskset -c "$CORES" "$BIN" --root "$ROOT" --fsms 2 --mode lockstep --secs "$SECS" --warmup-secs 1 2>&1 | grep -E 'hop: min|lag_waits'
  wait $SP || true
else
  taskset -c "$CORES" "$BIN" --root "$ROOT" --fsms 2 --mode lockstep --secs "$SECS" --warmup-secs 1 2>&1 | grep -E 'hop: min|lag_waits'
fi
