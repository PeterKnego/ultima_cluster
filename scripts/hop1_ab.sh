#!/usr/bin/env bash
# UC v2 — hop-1 A/B: two `hop_bench` DRIVER binaries against ONE fixed sink.
#
# SMOKE, NEVER A GATE. Rate bars are fleet-only (CLAUDE.md "Benchmarking
# discipline"; docs/notes/dev-box-not-a-bench.md). This script produces a
# RATIO between two binaries measured back to back on an idle box; it never
# produces a number to compare against a bar, and a red run here is not a
# regression until the fleet says so.
#
# The measured hop is client Engine -> ingress ring -> node -> egress
# broadcast -> Engine, with `dummy-node` standing in for the node (an
# infinitely fast backend). Only the DRIVER differs between the two sides:
# --sink is one fixed binary used for every single run, so a sink-side
# codegen difference can never leak into the delta.
#
# BUILD DISCIPLINE (required, see CLAUDE.md "Benchmarking discipline"):
# ~/.cache/cargo-target is shared by the main checkout and every worktree, so
# another checkout's build silently replaces your binaries mid-measurement.
# Build every side with its own private CARGO_TARGET_DIR, COPY the binary out
# to a stable path, and record its sha256 — then pass the copies here. Never
# point --a/--b at a live target dir.
#
# Usage:
#   scripts/hop1_ab.sh --sink BIN --a BIN --b BIN [--reps N] [--secs S] [--root DIR]
#
#   --sink  hop_bench binary used for `dummy-node` (fixed for every run)
#   --a     hop_bench binary used as driver A (the baseline)
#   --b     hop_bench binary used as driver B (the candidate)
#   --reps  A/B pairs to run (default 6). Odd reps run A then B, even reps
#           run B then A, so a warm-up or thermal drift cannot favour a side.
#   --secs  seconds per run (default 6)
#   --root  scratch dir for the instance dir and logs (default $HOME/m14c-ab).
#           MUST be on real disk — never /tmp (RAM-backed, no swap).
set -euo pipefail

SINK=""; DRIVER_A=""; DRIVER_B=""; REPS=6; SECS=6; ROOT="$HOME/m14c-ab"
PAYLOAD=64; INFLIGHT=4096; ENGINES=1; APP_ID="hop-ab"

while [ $# -gt 0 ]; do
    case "$1" in
        --sink) SINK="$2"; shift 2 ;;
        --a) DRIVER_A="$2"; shift 2 ;;
        --b) DRIVER_B="$2"; shift 2 ;;
        --reps) REPS="$2"; shift 2 ;;
        --secs) SECS="$2"; shift 2 ;;
        --root) ROOT="$2"; shift 2 ;;
        -h|--help) sed -n '2,33p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
for v in SINK DRIVER_A DRIVER_B; do
    [ -n "${!v}" ] || { echo "--${v,,} is required (see --help)" >&2; exit 2; }
done
for b in "$SINK" "$DRIVER_A" "$DRIVER_B"; do
    [ -x "$b" ] || { echo "not an executable: $b" >&2; exit 2; }
done
case "$ROOT" in /tmp/*|/tmp) echo "--root must not be under /tmp (RAM-backed, no swap)" >&2; exit 2 ;; esac

mkdir -p "$ROOT"
TSV="$ROOT/runs.tsv"
: > "$TSV"

echo "== hop-1 A/B (SMOKE, not a gate) — reps=$REPS secs=$SECS payload=${PAYLOAD}B inflight=$INFLIGHT engines=$ENGINES"
echo "   sink   $(sha256sum "$SINK" | cut -c1-12)  $SINK"
echo "   A      $(sha256sum "$DRIVER_A" | cut -c1-12)  $DRIVER_A"
echo "   B      $(sha256sum "$DRIVER_B" | cut -c1-12)  $DRIVER_B"

run_one() { # $1 = side label (A|B), $2 = driver binary, $3 = rep number
    local side="$1" bin="$2" rep="$3" sink_pid waited=0 out
    rm -rf "$ROOT/instance"
    mkdir -p "$ROOT/instance"
    "$SINK" dummy-node --instance-dir "$ROOT/instance" --app-id "$APP_ID" \
        >"$ROOT/sink.log" 2>&1 &
    sink_pid=$!
    until grep -q '^READY$' "$ROOT/sink.log" 2>/dev/null; do
        sleep 0.1
        waited=$((waited + 1))
        if [ "$waited" -gt 300 ]; then
            kill "$sink_pid" 2>/dev/null || true
            echo "sink never printed READY (30 s); see $ROOT/sink.log" >&2
            exit 1
        fi
        kill -0 "$sink_pid" 2>/dev/null || { echo "sink died; see $ROOT/sink.log" >&2; exit 1; }
    done
    # Let the sink settle past READY before attaching, and let the previous
    # run's teardown finish before the next one starts, so the driver does not
    # race the sink's own start-up. The only controlled comparison of these
    # two pauses is R0g (without) vs R0h (with): same runner, same binaries,
    # same sink, -0.69 % vs -1.28 %, BOTH overlapping. So they are worth about
    # 0.6 pp of an overlapping delta — kept as hygiene, not as a fix for a
    # known confound, and they do not explain the larger prior-script gap
    # (docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md, "The runner's own
    # confound").
    sleep 0.5
    out="$("$bin" engine-load --instance-dir "$ROOT/instance" --app-id "$APP_ID" \
        --secs "$SECS" --payload "$PAYLOAD" --inflight "$INFLIGHT" --engines "$ENGINES")"
    kill "$sink_pid" 2>/dev/null || true
    wait "$sink_pid" 2>/dev/null || true
    sleep 1
    printf '%s\n' "$out" | sed -n 's/^RESULT //p' | python3 -c '
import json, sys
rep, side, tsv = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.loads(sys.stdin.readline())
print("rep %2s  %s  %12.0f resp/s   p50 %.3f ms  p90 %.3f ms  p99 %.3f ms  lost %d"
      % (rep, side, d["responses_per_sec"], d["p50_ms"], d["p90_ms"], d["p99_ms"], d["lost"]))
open(tsv, "a").write("%s\t%f\t%f\n" % (side, d["responses_per_sec"], d["p90_ms"]))
' "$rep" "$side" "$TSV"
}

for rep in $(seq 1 "$REPS"); do
    if [ $((rep % 2)) -eq 1 ]; then
        run_one A "$DRIVER_A" "$rep"
        run_one B "$DRIVER_B" "$rep"
    else
        run_one B "$DRIVER_B" "$rep"   # reversed order: drift cannot favour a side
        run_one A "$DRIVER_A" "$rep"
    fi
done

python3 - "$TSV" <<'PY'
import sys
rows = {"A": [], "B": []}
for line in open(sys.argv[1]):
    side, rate, p90 = line.split("\t")
    rows[side].append((float(rate), float(p90)))
print("\n== summary (SMOKE — a ratio, not a gate)")
stat = {}
for side in ("A", "B"):
    r = sorted(x[0] for x in rows[side])
    p = sorted(x[1] for x in rows[side])
    stat[side] = (sum(r) / len(r), r[0], r[-1])
    print("   %s  n=%d  mean %.0f  min %.0f  max %.0f resp/s   p90 median %.3f ms"
          % (side, len(r), stat[side][0], stat[side][1], stat[side][2], p[len(p) // 2]))
delta = (stat["B"][0] - stat["A"][0]) / stat["A"][0] * 100.0
overlap = not (stat["B"][1] > stat["A"][2] or stat["A"][1] > stat["B"][2])
print("   B vs A: %+.2f %%   ranges %s" % (delta, "OVERLAP" if overlap else "disjoint"))
print("   (dev-box smoke; keep a variant only on a disjoint, repeatable delta)")
PY
