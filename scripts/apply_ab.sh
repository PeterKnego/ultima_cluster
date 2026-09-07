#!/usr/bin/env bash
# UC v2 — apply-hop A/B: two commits' `apply_bench` binaries, plus a
# same-source rebuild control.
#
# SMOKE, NEVER A GATE. Rate bars are fleet-only (CLAUDE.md "Benchmarking
# discipline"; docs/notes/dev-box-not-a-bench.md). This script produces a
# RATIO between two builds measured back to back on an idle box, judged
# against a THIRD build of the head source. It never produces a number to
# compare against an absolute bar, and a red verdict here is a prompt to
# measure on the fleet, not a regression on its own.
#
# This is the `apply_bench` sibling of `scripts/hop1_ab.sh` (which drives
# `hop_bench`'s `dummy-node`/`engine-load` subcommands and parses a `RESULT`
# line — neither of which `apply_bench` has). It exists because the
# time-and-timers gate's row d and the coordinated-snapshot gate's row f are
# both apply-hot-loop A/Bs, and had no runner.
#
# THE MEASURED HOP is the FSM apply loop alone: `apply_bench` builds a fake
# node (cnc page, log buffer, per-row rings), appends frames through
# `uc_log::Appender`, and N `uc_service` attaches apply a raw-tier counter.
# The number is `min_rate` from its `APPLY-JSON` line — the SLOWEST FSM's
# applied frames/s, which is the hop.
#
# WHY THE THIRD ARM (CLAUDE.md, "Exact binaries are not enough"): M14b's
# client-hop A/B read −4.2 % on one binary pair; fresh builds of the same two
# commits read ±0.3 %, and two builds of the *SAME* commit differed by 1 %.
# So a candidate delta means nothing until you know what two builds of one
# source measure apart. That is arm B′ here, and it is the bar.
#
# THE VERDICT RULE (pinned by --selftest; quote it whenever you quote a run).
# The rebuild resolution is the ONLY bar. Run noise is a SEPARATE gate on
# whether the run is worth reading at all — it never widens the bar.
#
#   head_vs_base = (mean(B)  - mean(A)) / mean(A) * 100   [the candidate]
#   resolution   = |mean(B') - mean(B)| / mean(B) * 100   [the bar]
#   sem(X)       = stdev(X) / (mean(X) * sqrt(K)) * 100   [per arm, %]
#
#   if K < 2 or max(sem(A), sem(B), sem(B')) > resolution:
#       verdict = "inconclusive (noisy run)"
#   elif |head_vs_base| <= resolution:  verdict = "within resolution"
#   else:                               verdict = "outside resolution"
#
# WHY IT IS SHAPED THIS WAY. Rows d and f are NULL bars — the claim under
# test is "the added code is free". Anything ADDED to the right-hand side
# therefore makes it EASIER to bless a real regression, which is the wrong
# direction to be wrong in for a null bar. So the bar is exactly the M14b
# rule: a candidate delta counts only if it is bigger than what two builds of
# ONE source measure apart.
#
# Noise is handled by refusing to answer instead. `sem` is the standard error
# of the arm's MEAN, which is the quantity the verdict compares — and unlike a
# min/max spread it SHRINKS as 1/sqrt(K), so `--pairs` is a real remedy: a run
# whose arms are noisier than the resolution they are being judged against is
# reported "inconclusive (noisy run)", with the remedy printed. Neither
# "within" nor "outside" is claimed from a run that cannot resolve either.
#
# "outside resolution" means "measure this hop on the fleet", never "this is
# a X % regression".
#
# BUILD DISCIPLINE (required, see CLAUDE.md "Benchmarking discipline"):
# ~/.cache/cargo-target is shared by the main checkout and every worktree, so
# another checkout's build silently replaces your binaries mid-measurement.
# Each arm is built in a TEMPORARY `git worktree` (created and removed here,
# never your checkout) with its OWN private CARGO_TARGET_DIR, `--locked` so
# the commit's own lockfile is used; the binary is then COPIED to the run
# directory and its sha256 recorded. Record those three hashes with any
# number you quote.
#
# Usage:
#   scripts/apply_ab.sh <base-commit-ish> <head-commit-ish> [options]
#   scripts/apply_ab.sh --bin-a A --bin-b B --bin-bp B' [options]
#   scripts/apply_ab.sh --selftest
#
#   --fsms N          FSMs that attach, 1..=8 (default 1)
#   --mode M          bounded | lockstep (default bounded)
#   --secs S          measure seconds per run (default 6)
#   --warmup-secs W   warm-up seconds per run (default 1)
#   --pairs K         runs PER ARM (default 4). Odd pairs run A, B, B'; even
#                     pairs run B', B, A — so no arm is systematically first
#                     and thermal drift cannot favour a side.
#   --settle S        seconds between runs (default 1)
#   --root DIR        run directory root (default $HOME/scratch/apply_ab).
#                     MUST be on real disk — never /tmp (RAM-backed, no swap).
#   --reuse-targets   keep the per-arm CARGO_TARGET_DIRs instead of wiping
#                     them first. Faster re-runs of the SAME comparison, but
#                     the three binaries (and so the resolution) are then the
#                     ones built on some earlier day, not fresh today.
#   --harness FILE    copy FILE over each arm's
#                     `uc_node/examples/apply_bench.rs` before building, so
#                     every arm runs the IDENTICAL harness and only the
#                     library code under it differs (hop1_ab.sh's one-fixed-
#                     sink discipline, generalized). Its sha256 is printed
#                     with the run. Use it when the arms' own in-tree harness
#                     is broken or differs for reasons unrelated to the hop —
#                     and NOT across an API break, where it will simply fail
#                     to compile (see the note below). Default: off; each arm
#                     builds the harness its own commit carries.
#
# WHEN THE HARNESS OVERLAY IS AND IS NOT AVAILABLE. `apply_bench` could not
# run at all between the log-time-and-timers merge and 2026-09-07: attach
# opens a per-row `svc_sched.<row>.ring` that the fake node never created, so
# every arm in that range dies with `ring error: io: No such file or
# directory`. --harness with the FIXED harness is how you A/B a pair inside
# that range. It is NOT available across the `Appender::new`/`append` arity
# change on the same flag day: a pre-change commit cannot compile the current
# harness, so such a pair runs each arm's own harness and the two differ in
# the FAKE DRIVER (which paces the run) though not in the measured apply
# loop. Say which of the two you did whenever you quote a number.
#   --bin-a/--bin-b/--bin-bp
#                     skip the build phase entirely and measure three
#                     binaries you already have. All three or none.
#   --selftest        fake the three binaries and assert the arithmetic and
#                     the verdict on known inputs. No cargo, no git, seconds.
set -euo pipefail

BASE_REF=""
HEAD_REF=""
FSMS=1
MODE="bounded"
SECS=6
WARMUP=1
PAIRS=4
SETTLE=1
ROOT="$HOME/scratch/apply_ab"
REUSE_TARGETS=0
HARNESS=""
BIN_A=""
BIN_B=""
BIN_BP=""
SELFTEST=0

usage() { sed -n '/^# Usage:/,/^#   --selftest/p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
    case "$1" in
        --fsms) FSMS="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --secs) SECS="$2"; shift 2 ;;
        --warmup-secs) WARMUP="$2"; shift 2 ;;
        --pairs) PAIRS="$2"; shift 2 ;;
        --settle) SETTLE="$2"; shift 2 ;;
        --root) ROOT="$2"; shift 2 ;;
        --reuse-targets) REUSE_TARGETS=1; shift ;;
        --harness) HARNESS="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift 2 ;;
        --bin-a) BIN_A="$2"; shift 2 ;;
        --bin-b) BIN_B="$2"; shift 2 ;;
        --bin-bp) BIN_BP="$2"; shift 2 ;;
        --selftest) SELFTEST=1; shift ;;
        -h|--help) usage; exit 0 ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *)
            if [ -z "$BASE_REF" ]; then BASE_REF="$1"
            elif [ -z "$HEAD_REF" ]; then HEAD_REF="$1"
            else echo "unexpected argument: $1" >&2; exit 2
            fi
            shift ;;
    esac
done

case "$ROOT" in
    /tmp|/tmp/*) echo "--root must not be under /tmp (RAM-backed, no swap)" >&2; exit 2 ;;
esac

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------------------------------------------------------------- selftest --
# Three shell stubs that print a fixed `APPLY-JSON` line, cycling a per-stub
# rate list, drive the REAL measure loop and the REAL summary below — so the
# arithmetic under test is the arithmetic that runs on a live A/B, not a copy
# of it. The line's shape is pinned on the Rust side by
# `uc_node/examples/apply_bench.rs`'s `apply_json_line_shape_is_pinned`.
make_stub() { # $1 = path, rest = the min_rate values to cycle
    local path="$1"; shift
    printf '%s\n' "$@" > "$path.rates"
    cat > "$path" <<'STUB'
#!/usr/bin/env bash
# fake apply_bench: ignores every argument, prints one APPLY-JSON line.
set -euo pipefail
n=0
[ -f "$0.n" ] && n="$(cat "$0.n")"
mapfile -t rates < "$0.rates"
r="${rates[$((n % ${#rates[@]}))]}"
echo $((n + 1)) > "$0.n"
echo "== fake apply_bench (selftest stub) =="
echo "APPLY-JSON {\"fsms\":1,\"mode\":\"bounded\",\"lag\":16777216,\"payload\":64,\"frame\":96,\"secs\":1.00,\"min_rate\":$r,\"driver_rate\":$((r * 2)),\"per\":[{\"fsm\":0,\"rate\":$r,\"lag_waits\":0}]}"
STUB
    chmod +x "$path"
}

selftest_case() { # $1 = label, $2 = expected AB-JSON (python dict literal), rest = 6 rates
    local label="$1" expect="$2"; shift 2
    local dir="$ROOT/selftest/$label"
    rm -rf "$dir"; mkdir -p "$dir"
    make_stub "$dir/a" "$1" "$2"
    make_stub "$dir/b" "$3" "$4"
    make_stub "$dir/bp" "$5" "$6"
    local out
    out="$("$0" --bin-a "$dir/a" --bin-b "$dir/b" --bin-bp "$dir/bp" \
        --pairs 2 --secs 1 --settle 0 --root "$dir/run")"
    printf '%s\n' "$out"
    printf '%s\n' "$out" | sed -n 's/^AB-JSON //p' | python3 -c '
import json, sys
got = json.loads(sys.stdin.readline())
want = json.loads(sys.argv[1])
bad = []
for k, v in want.items():
    g = got.get(k)
    if isinstance(v, float):
        if g is None or round(float(g), 6) != round(v, 6):
            bad.append("%s: got %r want %r" % (k, g, v))
    elif g != v:
        bad.append("%s: got %r want %r" % (k, g, v))
if bad:
    print("SELFTEST FAIL %s\n  %s" % (sys.argv[2], "\n  ".join(bad)))
    sys.exit(1)
print("selftest ok: %s -> %s" % (sys.argv[2], got["verdict"]))
' "$expect" "$label"
}

if [ "$SELFTEST" -eq 1 ]; then
    echo "== apply_ab.sh --selftest (no cargo, no git) =="
    # Case 1 — a candidate inside the rebuild resolution, arms quiet.
    #   A 1000000 x2 ; B 1002000 x2 ; B' 1005000 x2, every sem 0
    #   head_vs_base +0.200 ; resolution 0.299401 -> within
    selftest_case within \
        '{"a_mean":1000000.0,"b_mean":1002000.0,"bp_mean":1005000.0,
          "a_sem_pct":0.0,"b_sem_pct":0.0,"bp_sem_pct":0.0,
          "head_vs_base_pct":0.2,"resolution_pct":0.2994011976047904,
          "verdict":"within resolution","runs_per_arm":2}' \
        1000000 1000000  1002000 1002000  1005000 1005000
    # Case 2 — a candidate an order of magnitude outside a tight control.
    #   A 1000000 ; B 900000 ; B' 901000, every sem 0
    #   head_vs_base -10.0 ; resolution 0.111111 -> outside
    selftest_case outside \
        '{"a_mean":1000000.0,"b_mean":900000.0,"bp_mean":901000.0,
          "a_sem_pct":0.0,"b_sem_pct":0.0,"bp_sem_pct":0.0,
          "head_vs_base_pct":-10.0,"resolution_pct":0.1111111111111111,
          "verdict":"outside resolution","runs_per_arm":2}' \
        1000000 1000000  900000 900000  901000 901000
    # Case 3 — a null candidate the run cannot resolve: arm A's standard
    #   error of the mean (1.960784 %) is twenty times the resolution
    #   (0.098039 %), so neither "within" nor "outside" is claimed.
    selftest_case inconclusive \
        '{"a_mean":1020000.0,"b_mean":1020000.0,"bp_mean":1021000.0,
          "a_sem_pct":1.9607843137254901,"b_sem_pct":0.0,"bp_sem_pct":0.0,
          "head_vs_base_pct":0.0,"resolution_pct":0.09803921568627451,
          "verdict":"inconclusive (noisy run)","runs_per_arm":2}' \
        1000000 1040000  1020000 1020000  1021000 1021000
    echo "== selftest PASSED (all three verdicts)"
    exit 0
fi

# ------------------------------------------------------------ argument gate --
PREBUILT=0
if [ -n "$BIN_A$BIN_B$BIN_BP" ]; then
    if [ -z "$BIN_A" ] || [ -z "$BIN_B" ] || [ -z "$BIN_BP" ]; then
        echo "--bin-a/--bin-b/--bin-bp must be given together" >&2; exit 2
    fi
    if [ -n "$BASE_REF" ] || [ -n "$HEAD_REF" ]; then
        echo "pass commit-ishes OR --bin-*, not both" >&2; exit 2
    fi
    PREBUILT=1
elif [ -z "$BASE_REF" ] || [ -z "$HEAD_REF" ]; then
    usage >&2; exit 2
fi
case "$MODE" in bounded|lockstep) ;; *) echo "--mode must be bounded|lockstep" >&2; exit 2 ;; esac
[ "$PAIRS" -ge 1 ] || { echo "--pairs must be >= 1" >&2; exit 2; }

# --------------------------------------------------------------- the builds --
WORKTREES=()
cleanup() {
    local wt
    for wt in "${WORKTREES[@]:-}"; do
        [ -n "$wt" ] || continue
        git -C "$REPO" worktree remove --force "$wt" >/dev/null 2>&1 || rm -rf "$wt"
    done
    [ "${#WORKTREES[@]}" -eq 0 ] || git -C "$REPO" worktree prune >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ "$PREBUILT" -eq 1 ]; then
    RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-prebuilt"
else
    BASE_SHA="$(git -C "$REPO" rev-parse --verify "$BASE_REF^{commit}")"
    HEAD_SHA="$(git -C "$REPO" rev-parse --verify "$HEAD_REF^{commit}")"
    RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${BASE_SHA:0:7}-${HEAD_SHA:0:7}"
fi
RUN_DIR="$ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"

build_arm() { # $1 = arm label (a|b|bp), $2 = sha, $3 = target-dir suffix
    local arm="$1" sha="$2" suffix="$3"
    local wt="$RUN_DIR/wt-$arm" target="$HOME/.cache/cargo-target-ab-$suffix"
    echo "-- building arm ${arm^^}: ${sha:0:12}  target=$target" >&2
    [ "$REUSE_TARGETS" -eq 1 ] || rm -rf "$target"
    rm -rf "$wt"
    git -C "$REPO" worktree add --detach --quiet "$wt" "$sha"
    WORKTREES+=("$wt")
    [ -z "$HARNESS" ] || cp "$HARNESS" "$wt/uc_node/examples/apply_bench.rs"
    ( cd "$wt" && CARGO_TARGET_DIR="$target" \
        cargo build --release --locked -p uc_node --example apply_bench >&2 )
    cp "$target/release/examples/apply_bench" "$RUN_DIR/apply_bench.$arm"
    git -C "$REPO" worktree remove --force "$wt"
    # Drop it from the cleanup list BY INDEX. `${WORKTREES[@]/$wt}` would
    # substring-replace inside every element and leave an empty one behind,
    # which the EXIT trap would then try to `worktree remove`.
    local i
    for i in "${!WORKTREES[@]}"; do
        if [ "${WORKTREES[i]}" = "$wt" ]; then
            unset 'WORKTREES[i]'
        fi
    done
}

if [ "$PREBUILT" -eq 0 ]; then
    # B′ is built from the SAME sha as B into a SEPARATE target dir and a
    # SEPARATE worktree: different absolute paths get baked into the binary
    # (panic locations, and whatever else the compiler happens to lay out
    # differently), which is exactly the build-to-build variation the control
    # arm is here to measure.
    build_arm a "$BASE_SHA" "${BASE_SHA:0:12}"
    build_arm b "$HEAD_SHA" "${HEAD_SHA:0:12}"
    build_arm bp "$HEAD_SHA" "${HEAD_SHA:0:12}-ctl"
    BIN_A="$RUN_DIR/apply_bench.a"
    BIN_B="$RUN_DIR/apply_bench.b"
    BIN_BP="$RUN_DIR/apply_bench.bp"
fi
for b in "$BIN_A" "$BIN_B" "$BIN_BP"; do
    [ -x "$b" ] || { echo "not an executable: $b" >&2; exit 2; }
done

# Three release builds have just saturated every core. Rep 1 starts with arm A
# (and with an odd --pairs, A is first in a majority of pairs), so let the box
# come back down rather than charging the build's heat to one side.
if [ "$PREBUILT" -eq 0 ]; then
    echo "-- cooldown 10 s after the builds" >&2
    sleep 10
fi

TSV="$RUN_DIR/runs.tsv"
: > "$TSV"

echo "== apply-hop A/B (SMOKE, not a gate) — run $RUN_ID"
echo "   fsms=$FSMS mode=$MODE secs=$SECS warmup=$WARMUP pairs=$PAIRS (runs per arm)"
if [ "$PREBUILT" -eq 0 ]; then
    echo "   A  base $BASE_REF  ${BASE_SHA:0:12}"
    echo "   B  head $HEAD_REF  ${HEAD_SHA:0:12}"
    echo "   B' head $HEAD_REF  ${HEAD_SHA:0:12}  (same source, second build — the control)"
    if [ -n "$HARNESS" ]; then
        echo "   harness overlay (identical on every arm)  $(sha256sum "$HARNESS" | cut -d' ' -f1)"
        echo "                                             $HARNESS"
    else
        echo "   harness: each arm's own uc_node/examples/apply_bench.rs"
    fi
fi
echo "   sha256 A  $(sha256sum "$BIN_A" | cut -d' ' -f1)"
echo "   sha256 B  $(sha256sum "$BIN_B" | cut -d' ' -f1)"
echo "   sha256 B' $(sha256sum "$BIN_BP" | cut -d' ' -f1)"

# ---- idle-box guard (warn, never refuse) --------------------------------
# A busy box does not bias one arm — the interleave sees to that — but it
# inflates every arm's standard error, and an arm noisier than the resolution
# makes the whole run "inconclusive". Cheaper to know before the run than
# after it. `pgrep -c` prints 0 and exits 1 when nothing matches.
LOAD1="$(cut -d' ' -f1 /proc/loadavg)"
N_CARGO="$(pgrep -xc cargo 2>/dev/null)" || N_CARGO=0
N_RUSTC="$(pgrep -xc rustc 2>/dev/null)" || N_RUSTC=0
OTHER_BUILDS=$((N_CARGO + N_RUSTC))
BOX_BUSY=0
if python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 1.0 else 1)' "$LOAD1"; then
    BOX_BUSY=1
fi
[ "$OTHER_BUILDS" -eq 0 ] || BOX_BUSY=1
echo "   box: /proc/loadavg 1-min $LOAD1, other cargo/rustc $OTHER_BUILDS"
if [ "$BOX_BUSY" -eq 1 ]; then
    echo "   *** WARNING: the box is not idle (load > 1.0 and/or another build is"
    echo "   *** running). Measuring anyway — but expect a wide standard error and"
    echo "   *** an 'inconclusive (noisy run)' verdict. CLAUDE.md: a dev box is"
    echo "   *** noisy whatever its size."
fi

# ------------------------------------------------------------- the measures --
run_one() { # $1 = arm label, $2 = binary, $3 = rep number
    local arm="$1" bin="$2" rep="$3" out line
    rm -rf "$RUN_DIR/instance"
    out="$("$bin" --root "$RUN_DIR/instance" --fsms "$FSMS" --mode "$MODE" \
        --secs "$SECS" --warmup-secs "$WARMUP")" || {
        echo "arm $arm rep $rep: apply_bench failed" >&2; exit 1; }
    line="$(printf '%s\n' "$out" | sed -n 's/^APPLY-JSON //p')"
    [ -n "$line" ] || { echo "arm $arm rep $rep: no APPLY-JSON line" >&2; exit 1; }
    printf '%s\n' "$line" | python3 -c '
import json, sys
rep, arm, tsv = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.loads(sys.stdin.readline())
print("  rep %2s  %-2s  %12.0f applied frames/s   driver %12.0f"
      % (rep, arm, d["min_rate"], d["driver_rate"]))
open(tsv, "a").write("%s\t%f\t%f\n" % (arm, d["min_rate"], d["driver_rate"]))
' "$rep" "$arm" "$TSV"
    [ "$SETTLE" = "0" ] || sleep "$SETTLE"
}

for rep in $(seq 1 "$PAIRS"); do
    if [ $((rep % 2)) -eq 1 ]; then
        run_one A "$BIN_A" "$rep"
        run_one B "$BIN_B" "$rep"
        run_one Bp "$BIN_BP" "$rep"
    else
        run_one Bp "$BIN_BP" "$rep"   # reversed: drift cannot favour a side
        run_one B "$BIN_B" "$rep"
        run_one A "$BIN_A" "$rep"
    fi
done

# ---------------------------------------------------------------- the verdict --
python3 - "$TSV" "$LOAD1" "$OTHER_BUILDS" <<'PY'
import json, math, sys

rows = {"A": [], "B": [], "Bp": []}
for line in open(sys.argv[1]):
    arm, rate, driver = line.split("\t")
    rows[arm].append((float(rate), float(driver)))
load1, other_builds = float(sys.argv[2]), int(sys.argv[3])


def stats(v):
    r = sorted(x[0] for x in v)
    d = [x[1] for x in v]
    n = len(r)
    mean = sum(r) / n
    # Standard error of the MEAN, in percent of the mean. This — not a
    # min/max spread — is the run-quality number, because the mean is what
    # the verdict compares and because it shrinks as 1/sqrt(n), so --pairs
    # is a real remedy. n < 2 has no spread estimate at all.
    if n < 2:
        sem_pct = float("inf")
    else:
        var = sum((x - mean) ** 2 for x in r) / (n - 1)
        sem_pct = math.sqrt(var) / (mean * math.sqrt(n)) * 100.0
    return {
        "n": n, "mean": mean,
        # p50: the middle element of the sorted list (the UPPER median for an
        # even n) — hop1_ab.sh's convention, kept so the two runners read alike.
        "p50": r[n // 2], "min": r[0], "max": r[-1],
        "spread_pct": (r[-1] - r[0]) / mean * 100.0,
        "sem_pct": sem_pct,
        "driver_mean": sum(d) / n,
        "driver_over_min": (sum(d) / n) / mean,
    }


st = {k: stats(v) for k, v in rows.items()}
head_vs_base = (st["B"]["mean"] - st["A"]["mean"]) / st["A"]["mean"] * 100.0
resolution = abs(st["Bp"]["mean"] - st["B"]["mean"]) / st["B"]["mean"] * 100.0
worst_sem = max(st[k]["sem_pct"] for k in ("A", "B", "Bp"))

# The rebuild resolution is the ONLY bar (these are NULL bars: adding noise to
# the right-hand side would only make it easier to bless a real regression).
# Noise gets its own gate, and its answer is "no answer".
if worst_sem > resolution:
    verdict = "inconclusive (noisy run)"
elif abs(head_vs_base) <= resolution:
    verdict = "within resolution"
else:
    verdict = "outside resolution"

print("\n== summary (SMOKE — a ratio against a control arm, not a gate)")
for k, label in (("A", "A  base      "), ("B", "B  head      "),
                 ("Bp", "B' head-again")):
    s = st[k]
    print("   %s  n=%d  mean %12.0f  p50 %12.0f  min %12.0f  max %12.0f  "
          "spread %5.2f %%  sem %6.3f %%"
          % (label, s["n"], s["mean"], s["p50"], s["min"], s["max"],
             s["spread_pct"], s["sem_pct"]))
    print("                 driver mean %12.0f  driver/min %.3f"
          % (s["driver_mean"], s["driver_over_min"]))
print("   box: loadavg 1-min %.2f, other cargo/rustc %d" % (load1, other_builds))
print("   head vs base     %+7.3f %%   (the candidate)" % head_vs_base)
print("   head vs head'    %7.3f %%   (the RESOLUTION — the bar, build noise alone)"
      % resolution)
print("   worst arm sem    %7.3f %%   (run quality; must be <= the resolution)"
      % worst_sem)
if verdict == "inconclusive (noisy run)":
    print("   verdict: %s" % verdict)
    print("          the arms are noisier (%.3f %%) than the resolution they are"
          % worst_sem)
    print("          judged against (%.3f %%), so NEITHER 'within' nor 'outside'"
          % resolution)
    print("          is claimed. Remedy: a quieter box, or more --pairs (sem")
    print("          shrinks as 1/sqrt(K); %d more pairs would roughly halve it)."
          % (3 * st["A"]["n"]))
else:
    print("   verdict: %s  (|%.3f| %s %.3f)"
          % (verdict, head_vs_base,
             "<=" if verdict == "within resolution" else ">", resolution))
print("   (dev-box smoke. 'outside resolution' means MEASURE IT ON THE FLEET,")
print("    never 'this is a regression of that size'.)")
print("AB-JSON " + json.dumps({
    "runs_per_arm": st["A"]["n"],
    "a_mean": st["A"]["mean"], "b_mean": st["B"]["mean"], "bp_mean": st["Bp"]["mean"],
    "a_p50": st["A"]["p50"], "b_p50": st["B"]["p50"], "bp_p50": st["Bp"]["p50"],
    "a_spread_pct": st["A"]["spread_pct"], "b_spread_pct": st["B"]["spread_pct"],
    "bp_spread_pct": st["Bp"]["spread_pct"],
    "a_sem_pct": st["A"]["sem_pct"], "b_sem_pct": st["B"]["sem_pct"],
    "bp_sem_pct": st["Bp"]["sem_pct"],
    "a_driver_mean": st["A"]["driver_mean"], "b_driver_mean": st["B"]["driver_mean"],
    "bp_driver_mean": st["Bp"]["driver_mean"],
    "a_driver_over_min": st["A"]["driver_over_min"],
    "b_driver_over_min": st["B"]["driver_over_min"],
    "bp_driver_over_min": st["Bp"]["driver_over_min"],
    "load1": load1, "other_builds": other_builds,
    "head_vs_base_pct": head_vs_base,
    "resolution_pct": resolution,
    "worst_sem_pct": worst_sem,
    "verdict": verdict,
}))
PY
