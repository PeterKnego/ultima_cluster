#!/usr/bin/env bash
# UC v2 elle consistency check (design spec 2026-07-15). Generates missing pass
# histories via the elle_v2 driver, then asserts each is valid with an EMPTY
# anomaly set under BOTH serializable and the strict (real-time) model.
# Usage: scripts/elle_check.sh [pass ...]     (default: all six passes)
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
FIX_CYCLE="$ROOT/tools/elle-cli/fixtures/known_bad.edn"
FIX_RT="$ROOT/tools/elle-cli/fixtures/realtime_violation.edn"
# Histories go to DISK, never /tmp: /tmp is RAM-backed tmpfs with no swap on this
# box, and the clean-tier histories (quiet ~50k events) OOM-kill the run there.
ELLE_DIR="${ELLE_DIR:-$HOME/.cache/uc2-elle}"
# Pinned by the Task-3 probe (see tools/elle-cli/README.md).
STRICT_MODEL="${ELLE_STRICT_MODEL:-strong-serializable}"
# Bound elle-cli's JVM heap so a pathological history fails loudly, not via the
# OOM killer (see CLAUDE.md — this box has no swap).
JAVA_XMX="${ELLE_JAVA_XMX:-2g}"
# Escape hatch for manually generating a MUTATED clean-tier history (e.g.
# ELLE_CARGO_FEATURES=--features\ mutation-testing). The mutation PROOF lives in
# scripts/elle_mutation.sh; this stays empty for the normal clean tier.
CARGO_FEATURES="${ELLE_CARGO_FEATURES:-}"
# M8 Task 15: UC2_CRYPTO=1 re-runs every pass below with wire crypto Enabled
# on every node (see uc_node/tests/elle_v2.rs's crypto_from_env / lincheck_v2's
# ClusterCfg::crypto). Inherited by the `cargo test` subshell below like any
# other exported var — named here only so it shows up in the run's own log line
# and so a history cached under a DIFFERENT ELLE_DIR is never silently reused
# across the crypto/no-crypto boundary (the caller must point ELLE_DIR at a
# fresh directory to force regeneration either way — see the pass-generation
# loop's cached-history check below, which keys on
# `$ELLE_DIR/$pass/history.edn` / `$ELLE_DIR/$pass/fsm0/history.edn` plus the
# `crypto` sidecar beside them).
UC2_CRYPTO="${UC2_CRYPTO:-0}"

PASSES=("$@")
[ ${#PASSES[@]} -eq 0 ] && PASSES=(quiet failover partition purge reconfig quiet_two_fsm)

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found (set JAVA=)" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "error: jq not found" >&2; exit 1; }
[ -f "$JAR" ] || { echo "error: missing $JAR" >&2; exit 1; }
if [ ! -f "$FIX_CYCLE" ] || [ ! -f "$FIX_RT" ]; then
    echo "error: missing fixtures" >&2
    exit 1
fi

# verdict <model> <history>: echoes true|false|unknown (exit code untrusted).
verdict() {
    local out v
    out="$("$JAVA" "-Xmx$JAVA_XMX" -jar "$JAR" --model list-append --consistency-models "$1" "$2")" || true
    v="$(printf '%s\n' "$out" | awk 'END { print $NF }')"
    case "$v" in
        true|false|unknown) printf '%s\n' "$v" ;;
        *) echo "error: no verdict from elle-cli on $2 (output: '$out')" >&2; exit 1 ;;
    esac
}

# classify <model> <history>: echoes "<valid?>|<sorted,joined anomaly-types>".
classify() {
    local out
    out="$("$JAVA" "-Xmx$JAVA_XMX" -jar "$JAR" --model list-append --consistency-models "$1" --verbose "$2")" || true
    printf '%s' "$out" \
        | jq -r '((.["valid?"])|tostring) + "|" + (((.["anomaly-types"]) // []) | sort | join(","))' 2>/dev/null \
        || { echo "error: elle-cli produced no JSON report for $2" >&2; exit 1; }
}

require() { # <expected> <actual> <label>
    if [ "$2" != "$1" ]; then
        echo "FAIL: $3 (got: '$2', expected '$1')" >&2
        case "$2" in unknown*) echo "hint: shrink the history (ELLE_TARGET_OPS) — unknown never passes" >&2 ;; esac
        exit 1
    fi
    echo "OK: $3"
}

echo "== fixture self-tests (checker teeth before any real verdict) =="
require false "$(verdict serializable "$FIX_CYCLE")" "cycle fixture rejected under serializable"
require true  "$(verdict serializable "$FIX_RT")"    "realtime fixture accepted under plain serializable"
require false "$(verdict "$STRICT_MODEL" "$FIX_RT")" "realtime fixture rejected under $STRICT_MODEL"

for pass in "${PASSES[@]}"; do
    # M14c2 Task 7: a pass's history is either one file
    # ($ELLE_DIR/$pass/history.edn, every pre-M14c2 pass) or one per FSM
    # ($ELLE_DIR/$pass/fsm{0,1}/history.edn, quiet_two_fsm) — the generation
    # trigger below fires only when NEITHER shape is present on disk.
    crypto_sidecar="$ELLE_DIR/$pass/crypto"
    if [ -f "$ELLE_DIR/$pass/history.edn" ] || [ -f "$ELLE_DIR/$pass/fsm0/history.edn" ]; then
        # M8 Task 15 review fix: a cached history under a reused ELLE_DIR can
        # predate this posture check entirely (pre-Task-15 dir) or have been
        # generated under the OTHER crypto posture — either way, adjudicating
        # it and printing the REQUESTED posture (rather than the one it was
        # actually generated under) is exactly the vacuity risk this whole
        # task exists to close, one layer up: a UC2_CRYPTO=1 run against a
        # directory of cleartext histories would otherwise print "crypto=1"
        # having sealed nothing. Refuse rather than silently trust the ask.
        # The sidecar lives at the PASS level even for the per-FSM shape
        # (elle_v2.rs's run_pass2 writes it beside fsm0/ and fsm1/, not
        # inside either) — one posture per pass, both FSMs generated together.
        cached_crypto="0"
        if [ -f "$crypto_sidecar" ]; then
            cached_crypto="$(cat "$crypto_sidecar")"
        fi
        if [ "$cached_crypto" != "$UC2_CRYPTO" ]; then
            echo "error: $ELLE_DIR/$pass was generated under crypto=$cached_crypto but UC2_CRYPTO=$UC2_CRYPTO was requested." >&2
            echo "hint: point ELLE_DIR at a fresh directory, or delete $ELLE_DIR/$pass to regenerate under the requested posture." >&2
            exit 1
        fi
    else
        echo "== generating $pass history (elle_v2 driver, crypto=$UC2_CRYPTO) =="
        # shellcheck disable=SC2086
        (cd "$ROOT" && ELLE_DIR="$ELLE_DIR" UC2_CRYPTO="$UC2_CRYPTO" cargo test -p uc_node --release $CARGO_FEATURES \
            --test elle_v2 -- --ignored --exact "elle_$pass" --nocapture)
    fi
    # Count what we actually adjudicate. Without this the script is FAIL-OPEN:
    # a generation step that exits 0 without writing a history (a renamed or
    # mistyped `#[ignore]` test name makes `--exact` run 0 tests and exit 0; a
    # partial write that produced only fsm0/) leaves the loop body unentered
    # and the script goes on to print PASS having checked nothing.
    n=0
    for hist in "$ELLE_DIR/$pass"/history.edn "$ELLE_DIR/$pass"/fsm*/history.edn; do
        [ -f "$hist" ] || continue
        label="$pass"
        case "$hist" in
            */fsm*/history.edn) label="$pass/$(basename "$(dirname "$hist")")" ;;
        esac
        echo "== $label: $(wc -l < "$hist") events =="
        require "true|" "$(classify serializable "$hist")"    "$label clean under serializable"
        require "true|" "$(classify "$STRICT_MODEL" "$hist")" "$label clean under $STRICT_MODEL"
        n=$((n + 1))
    done
    [ "$n" -ge 1 ] || { echo "FAIL: $pass adjudicated no history" >&2; exit 1; }
    # The two-FSM shape must produce BOTH halves: a fan-in that wrote only
    # fsm0/ is precisely the partial write this tier exists to catch.
    case "$pass" in
        *two_fsm) [ "$n" -eq 2 ] || { echo "FAIL: $pass needs both FSM histories (got $n)" >&2; exit 1; } ;;
    esac
done

echo "elle consistency check passed (${PASSES[*]}, crypto=$UC2_CRYPTO)"
