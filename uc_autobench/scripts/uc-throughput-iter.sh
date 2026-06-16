#!/usr/bin/env bash
# One uc-throughput iteration: cheap local gates first (compile, lincheck), then the
# expensive cloud fitness (make iterate) only if both pass. Emits ONE JSON line on
# stdout. Exit 0 even on gate failure (status carries the verdict -- matches run-iter).
#
# Commands are overridable via env for testing:
#   UC_BUILD_CMD     (default: cargo build the bench bins)
#   UC_LINCHECK_CMD  (default: cargo test the lincheck capstone)
#   UC_ITER_CMD      (default: make -C bench-infra iterate)
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

BUILD_CMD="${UC_BUILD_CMD:-cargo build -p uc_autobench --bin uc-node-launch --bin commit-path-load --release}"
LINCHECK_CMD="${UC_LINCHECK_CMD:-cargo test -p uc_node --test lin_register linearizable_under_failover --release -- --test-threads=1}"
# shellcheck disable=SC2086
ITER_CMD="${UC_ITER_CMD:-make -C $ROOT/bench-infra iterate}"

emit() { # emit <status> <lincheck_passed> [fitness_json]
  local status="$1" lp="$2" fit="${3:-{\}}"
  python3 - "$status" "$lp" "$fit" <<'PY'
import json,sys
status, lp, fit = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    metrics = json.loads(fit)
except Exception:
    metrics = {}
print(json.dumps({
    "status": status,
    "gate": {"lincheck_passed": lp == "true"},
    "metrics": {
        "uc_throughput_msgs": metrics.get("uc_throughput_msgs", 0),
        "knee_rate": metrics.get("knee_rate", 0),
        "p99_at_knee_ms": metrics.get("p99_at_knee_ms", 0),
    },
}))
PY
}

# 1. compile check (cheap)
if ! eval "$BUILD_CMD" >/tmp/uc-iter-build.log 2>&1; then
  emit build_failed false; exit 0
fi

# 2. correctness gate (local lincheck capstone, ~40s) -- before any cloud spend
if ! eval "$LINCHECK_CMD" >/tmp/uc-iter-lincheck.log 2>&1; then
  emit lincheck_failed false; exit 0
fi

# 3. cloud fitness (make iterate prints the fitness JSON as its last line containing uc_throughput_msgs)
iter_out="$(eval "$ITER_CMD" 2>/tmp/uc-iter-cloud.log)"
# Extract fitness: try valid JSON first (real make iterate), then key:value pairs (test stubs).
fit="$(UC_ITER_OUT="$iter_out" python3 -c '
import os, json, re, sys
text = os.environ.get("UC_ITER_OUT", "")
for line in reversed(text.splitlines()):
    if "uc_throughput_msgs" not in line:
        continue
    try:
        d = json.loads(line)
        print(json.dumps(d))
        sys.exit(0)
    except ValueError:
        pass
    pairs = re.findall(r"\"([\w]+)\":([\d.]+)", line)
    if pairs:
        print(json.dumps({k: float(v) if "." in v else int(v) for k, v in pairs}))
        sys.exit(0)
sys.exit(1)
')" || { emit iterate_failed true; exit 0; }
emit pass true "$fit"
