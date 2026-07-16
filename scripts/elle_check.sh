#!/usr/bin/env bash
# UC v2 elle consistency check (design spec 2026-07-15). Generates missing pass
# histories via the elle_v2 driver, then asserts each is valid with an EMPTY
# anomaly set under BOTH serializable and the strict (real-time) model.
# Usage: scripts/elle_check.sh [pass ...]     (default: all five passes)
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

PASSES=("$@")
[ ${#PASSES[@]} -eq 0 ] && PASSES=(quiet failover partition purge reconfig)

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
    hist="$ELLE_DIR/$pass/history.edn"
    if [ ! -f "$hist" ]; then
        echo "== generating $pass history (elle_v2 driver) =="
        # shellcheck disable=SC2086
        (cd "$ROOT" && ELLE_DIR="$ELLE_DIR" cargo test -p uc2_node --release $CARGO_FEATURES \
            --test elle_v2 -- --ignored --exact "elle_$pass" --nocapture)
    fi
    echo "== $pass: $(wc -l < "$hist") events =="
    require "true|" "$(classify serializable "$hist")"    "$pass clean under serializable"
    require "true|" "$(classify "$STRICT_MODEL" "$hist")" "$pass clean under $STRICT_MODEL"
done

echo "elle consistency check passed (${PASSES[*]})"
