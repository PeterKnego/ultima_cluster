#!/usr/bin/env bash
# Verifies uc_fitness.py extracts the throughput ceiling + knee from a UC sweep CSV.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$(python3 "$HERE/uc_fitness.py" "$HERE/testdata/uc_sweep_sample.csv")"
echo "got: $out"
echo "$out" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert abs(d["uc_throughput_msgs"]-804.3)<0.01, d
assert d["knee_rate"]==500, d
assert abs(d["p99_at_knee_ms"]-2.865151)<0.001, d
print("UC_FITNESS OK")
'
