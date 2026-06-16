#!/usr/bin/env bash
# Tests the driver's gate ordering + JSON output with the cargo/lincheck/cloud
# commands stubbed (no real build, no cloud spend).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
D="$HERE/uc-throughput-iter.sh"

# Case 1: build fails -> status=build_failed, no lincheck/cloud run.
out="$(UC_BUILD_CMD=false UC_LINCHECK_CMD='echo SHOULD_NOT_RUN; false' \
       UC_ITER_CMD='echo SHOULD_NOT_RUN' bash "$D")"
echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);assert d["status"]=="build_failed",d;print("c1 ok")'

# Case 2: build ok, lincheck fails -> status=lincheck_failed, no cloud run.
out="$(UC_BUILD_CMD=true UC_LINCHECK_CMD=false \
       UC_ITER_CMD='echo SHOULD_NOT_RUN' bash "$D")"
echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);assert d["status"]=="lincheck_failed",d;assert d["gate"]["lincheck_passed"]==False,d;print("c2 ok")'

# Case 3: gates pass, cloud returns fitness -> status=pass, metric threaded through.
out="$(UC_BUILD_CMD=true UC_LINCHECK_CMD=true \
       UC_ITER_CMD='echo {\"uc_throughput_msgs\":812.5,\"knee_rate\":600,\"p99_at_knee_ms\":3.1}' \
       bash "$D")"
echo "$out" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["status"]=="pass",d
assert d["gate"]["lincheck_passed"]==True,d
assert abs(d["metrics"]["uc_throughput_msgs"]-812.5)<0.01,d
assert d["metrics"]["knee_rate"]==600,d
print("c3 ok")'
echo "DRIVER OK"
