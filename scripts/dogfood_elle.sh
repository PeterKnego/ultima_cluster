#!/usr/bin/env bash
# Adjudicate a history written by `uc2-adjudicate elle` (gate row B2-v2.iv)
# under BOTH consistency models `scripts/elle_check.sh` checks, with the same
# vendored elle-cli. Usage: scripts/dogfood_elle.sh <history.edn>
# Exit 0 = clean under both, 1 = an anomaly under either, 2 = setup error.
#
# The verdict is elle-cli's LAST output token (`true`/`false`/`unknown`), the
# same parse `elle_check.sh`'s verdict() uses — NOT the `true|...` shape, which
# only appears under `--verbose | jq` (classify()).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
JAVA="${JAVA:-java}"
JAVA_XMX="${JAVA_XMX:-2g}"
STRICT_MODEL="${ELLE_STRICT_MODEL:-strong-serializable}"
HIST="${1:?usage: $0 <history.edn>}"
[ -f "$JAR" ] || { echo "missing $JAR" >&2; exit 2; }
[ -f "$HIST" ] || { echo "missing $HIST" >&2; exit 2; }
rc=0
for model in serializable "$STRICT_MODEL"; do
  out="$("$JAVA" "-Xmx$JAVA_XMX" -jar "$JAR" --model list-append --consistency-models "$model" "$HIST" 2>&1)" || true
  v="$(printf '%s\n' "$out" | awk 'END { print $NF }')"
  case "$v" in
    true)  echo "$model: PASS" ;;
    false) echo "$model: FAIL (anomaly)"; rc=1 ;;
    *)     echo "$model: FAIL (no verdict: '$v')"; rc=1 ;;
  esac
done
exit $rc
