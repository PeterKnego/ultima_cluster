#!/usr/bin/env bash
# UC v2 M10 Task 9: fire every shipped alert rule (packaging/prometheus/uc2-alerts.yml)
# against a deliberately broken cluster, and let promtool adjudicate.
#
# Method: `uc2_node/examples/m10_alerts.rs` builds/breaks a real or (where
# disclosed) synthetic cluster, scrapes every node's REAL /metrics HTTP
# endpoint once per second over a short bounded real window, and writes
# `<scenario>.series` files (raw scraped values, unmodified). This script
# TIME-DILATES those raw values into promtool `input_series` on a synthetic
# 30s-interval timeline sized to each rule's `for:` clause and any delta
# range window — holding a series' last real value constant for as long as
# the window needs, or (for delta rules) placing the real observed jump
# inside the evaluated range and continuing a small synthetic ramp so the
# condition stays true for the rest of the `for:` sustain. This mirrors
# scripts/elle_check.sh's tool-probe style (see its `command -v java` check).
#
# Usage: scripts/m10_alert_fire.sh [--out DIR]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RULES_FILE="$ROOT/packaging/prometheus/uc2-alerts.yml"
OUT="${OUT:-$HOME/.cache/uc2-m10-alerts}"

while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT="$2"; shift 2 ;;
        *) echo "error: unknown argument $1" >&2; exit 2 ;;
    esac
done

PROMTOOL="${PROMTOOL:-$(command -v promtool || echo "$HOME/.local/bin/promtool")}"
if [ ! -x "$PROMTOOL" ] && ! command -v "$PROMTOOL" >/dev/null 2>&1; then
    echo "error: promtool not found (checked PATH and \$HOME/.local/bin/promtool)." >&2
    echo "hint: download a Prometheus release tarball from https://prometheus.io/download/" >&2
    echo "      and extract promtool to \$HOME/.local/bin/, or set PROMTOOL=/path/to/promtool." >&2
    exit 2
fi

[ -f "$RULES_FILE" ] || { echo "error: missing $RULES_FILE" >&2; exit 1; }

mkdir -p "$OUT"

echo "== running m10_alerts (builds/breaks clusters, scrapes real /metrics) =="
cargo run --manifest-path "$ROOT/Cargo.toml" -p uc2_node --release --example m10_alerts -- --all --out "$OUT"

echo
echo "== time-dilating scrapes into promtool input_series + adjudicating =="
PROMTOOL="$PROMTOOL" RULES_FILE="$RULES_FILE" OUT="$OUT" python3 - <<'PYEOF'
import os
import re
import subprocess
import sys

PROMTOOL = os.environ["PROMTOOL"]
RULES_FILE = os.environ["RULES_FILE"]
OUT = os.environ["OUT"]
TEST_DIR = os.path.join(OUT, "_promtool_tests")
os.makedirs(TEST_DIR, exist_ok=True)

INTERVAL = 30  # synthetic dilation interval, seconds — see module docs.

LABEL_RE = re.compile(r'(\w+)="([^"]*)"')


def parse_series_file(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            name, rest = line.split("{", 1)
            labels_str, values_str = rest.split("}", 1)
            values = values_str.strip().split()
            labels = dict(LABEL_RE.findall(labels_str))
            rows.append(
                {"name": name, "labels": labels, "labels_str": labels_str, "values": values}
            )
    return rows


def load_scenario(name):
    path = os.path.join(OUT, f"{name}.series")
    if not os.path.isfile(path):
        sys.exit(f"error: missing {path} — run m10_alerts --all first")
    return parse_series_file(path)


def select(rows, name, filt):
    matches = [
        r for r in rows if r["name"] == name and all(r["labels"].get(k) == v for k, v in filt.items())
    ]
    if not matches:
        sys.exit(f"error: no series matching {name}{filt} in loaded rows")
    return matches[0]


def fmt_num(x):
    xf = float(x)
    return str(int(xf)) if xf.is_integer() else str(xf)


def hold_last(row, total):
    """LEVEL policy: hold the series' last real scraped value constant for
    `total` synthetic samples — the controller-ruling default."""
    v = row["values"][-1]
    return f"{v}x{total}"


def literal(row):
    """DELTA-jump policy for `for: 0m` rules: the raw captured samples
    (a real 0->N transition) are already inside the range window at the
    eval_time this caller picks — use them verbatim, no padding needed."""
    return " ".join(row["values"])


def jump_then_ramp(row, total, step=1):
    """DELTA policy for `for: >0` rules: replay the real captured JUMP
    (baseline -> peak — exactly 2 anchor points, regardless of how many raw
    samples the scenario captured either side of it) verbatim, then
    continue a small synthetic ramp so delta(...) stays > 0 for the rest of
    the `for:` sustain window. A flat-after-jump series — which is what the
    real capture usually looks like once the tiny admission window
    saturates within the first round — would fall back to delta==0 once
    the jump ages out of the range window, well before `for:` is
    satisfied; replaying the RAW trace's flat tail verbatim (instead of
    just its two anchor points) would silently swallow the ramp entirely
    once the raw sample count exceeds this rule's own `total`."""
    raw = row["values"]
    baseline, jumped = raw[0], raw[-1]
    prefix = f"{baseline} {jumped}"
    remaining = total - 2
    if remaining <= 0:
        return prefix
    start = float(jumped) + step
    m = remaining - 1
    if m <= 0:
        return f"{prefix} {fmt_num(start)}"
    return f"{prefix} {fmt_num(start)}+{step}x{m}"


def total_for(for_secs, range_secs=0, margin=60):
    eval_time = for_secs + range_secs + margin
    return eval_time, (eval_time // INTERVAL) + 3


# ---------------------------------------------------------------- rule specs
#
# Each entry builds the promtool `input_series` list and the `exp_samples`
# label set (the PromQL AND/group_left/aggregation semantics of the SHIPPED
# expr in uc2-alerts.yml, worked out by hand and cross-checked against a
# scratch promtool run — see the task report). `real` mirrors the Task 9
# brief's scenario table disclosure.

RULES = {}


def rule(name, severity, real, method):
    RULES[name] = {"severity": severity, "real": real, "method": method, "series": [], "labels_from": None}
    return RULES[name]


# Uc2AgentDead — synthetic, for: 0m.
r = rule("Uc2AgentDead", "critical", False, "agent_dead")
rows = load_scenario("agent_dead")
row = select(rows, "uc2_agent_alive", {"agent": "archive"})
_, total = total_for(0)
r["series"].append((f'uc2_agent_alive{{{row["labels_str"]}}}', hold_last(row, total)))
r["labels_from"] = row
r["eval_time"] = total_for(0)[0]

# Uc2NoLeader — real, for: 30s. max(uc2_is_leader) strips ALL labels.
r = rule("Uc2NoLeader", "critical", True, "no_leader")
rows = load_scenario("no_leader")
row = select(rows, "uc2_is_leader", {})
_, total = total_for(30)
r["series"].append((f'uc2_is_leader{{{row["labels_str"]}}}', hold_last(row, total)))
r["labels_from"] = None  # aggregation strips labels
r["eval_time"] = total_for(30)[0]

# Uc2LeaderNotServing — synthetic, for: 30s.
r = rule("Uc2LeaderNotServing", "critical", False, "leader_not_serving")
rows = load_scenario("leader_not_serving")
leader_row = select(rows, "uc2_is_leader", {})
serve_row = select(rows, "uc2_can_serve", {})
_, total = total_for(30)
r["series"].append((f'uc2_is_leader{{{leader_row["labels_str"]}}}', hold_last(leader_row, total)))
r["series"].append((f'uc2_can_serve{{{serve_row["labels_str"]}}}', hold_last(serve_row, total)))
r["labels_from"] = leader_row  # LHS of `and`
r["eval_time"] = total_for(30)[0]

# Uc2ServiceWedged — real, for: 1m.
r = rule("Uc2ServiceWedged", "critical", True, "service_wedged")
rows = load_scenario("service_wedged")
svc_row = select(rows, "uc2_service_heartbeat_age_seconds", {})
node_row = select(rows, "uc2_node_heartbeat_age_seconds", {})
_, total = total_for(60)
r["series"].append((f'uc2_service_heartbeat_age_seconds{{{svc_row["labels_str"]}}}', hold_last(svc_row, total)))
r["series"].append((f'uc2_node_heartbeat_age_seconds{{{node_row["labels_str"]}}}', hold_last(node_row, total)))
r["labels_from"] = svc_row  # LHS of `and`
r["eval_time"] = total_for(60)[0]

# Uc2ReplicationStalled — real, delta over [1m], for: 1m.
r = rule("Uc2ReplicationStalled", "critical", True, "leader_isolated")
rows = load_scenario("leader_isolated")
commit_row = select(rows, "uc2_commit_bytes", {})
append_row = select(rows, "uc2_append_bytes", {})
eval_time, total = total_for(60, range_secs=60)
r["series"].append((f'uc2_commit_bytes{{{commit_row["labels_str"]}}}', hold_last(commit_row, total)))
r["series"].append((f'uc2_append_bytes{{{append_row["labels_str"]}}}', jump_then_ramp(append_row, total)))
r["labels_from"] = commit_row  # LHS: delta(uc2_commit_bytes[1m])
r["eval_time"] = eval_time

# Uc2AdmissionSaturated — real (same scenario), level, for: 1m.
r = rule("Uc2AdmissionSaturated", "warning", True, "leader_isolated")
rows = load_scenario("leader_isolated")
sat_row = select(rows, "uc2_admission_saturation", {})
_, total = total_for(60)
r["series"].append((f'uc2_admission_saturation{{{sat_row["labels_str"]}}}', hold_last(sat_row, total)))
r["labels_from"] = sat_row
r["eval_time"] = total_for(60)[0]

# Uc2PeerNeverHeard — real, for: 2m.
r = rule("Uc2PeerNeverHeard", "warning", True, "peer_never_heard")
rows = load_scenario("peer_never_heard")
row = select(rows, "uc2_peer_reported_durable_bytes", {"peer": "2"})
_, total = total_for(120)
r["series"].append((f'uc2_peer_reported_durable_bytes{{{row["labels_str"]}}}', hold_last(row, total)))
r["labels_from"] = row
r["eval_time"] = total_for(120)[0]

# Uc2PeerLagging — real, for: 5m. group_left keeps LHS's extra labels.
r = rule("Uc2PeerLagging", "warning", True, "follower_partitioned")
rows = load_scenario("follower_partitioned")
lag_row = select(rows, "uc2_peer_replication_lag_bytes", {"peer": "0"})
adm_row = select(rows, "uc2_admission_bytes", {})
_, total = total_for(300)
r["series"].append((f'uc2_peer_replication_lag_bytes{{{lag_row["labels_str"]}}}', hold_last(lag_row, total)))
r["series"].append((f'uc2_admission_bytes{{{adm_row["labels_str"]}}}', hold_last(adm_row, total)))
r["labels_from"] = lag_row
r["eval_time"] = total_for(300)[0]

# Uc2PurgeStalled — synthetic, for: 10m.
r = rule("Uc2PurgeStalled", "warning", False, "purge_stalled")
rows = load_scenario("purge_stalled")
pe_row = select(rows, "uc2_purge_enabled", {})
floor_row = select(rows, "uc2_node_snapshot_floor_bytes", {})
base_row = select(rows, "uc2_archive_first_base_bytes", {})
seg_row = select(rows, "uc2_journal_segment_bytes", {})
_, total = total_for(600)
r["series"].append((f'uc2_purge_enabled{{{pe_row["labels_str"]}}}', hold_last(pe_row, total)))
r["series"].append((f'uc2_node_snapshot_floor_bytes{{{floor_row["labels_str"]}}}', hold_last(floor_row, total)))
r["series"].append((f'uc2_archive_first_base_bytes{{{base_row["labels_str"]}}}', hold_last(base_row, total)))
r["series"].append((f'uc2_journal_segment_bytes{{{seg_row["labels_str"]}}}', hold_last(seg_row, total)))
r["labels_from"] = pe_row  # LHS of `and`
r["eval_time"] = total_for(600)[0]

# Uc2RepeatedWipes — synthetic (real transition, injected trigger), delta over [10m], for: 0m.
r = rule("Uc2RepeatedWipes", "warning", False, "repeated_wipes")
rows = load_scenario("repeated_wipes")
row = select(rows, "uc2_wipes_total", {})
r["series"].append((f'uc2_wipes_total{{{row["labels_str"]}}}', literal(row)))
r["labels_from"] = row
r["eval_time"] = (len(row["values"]) - 1) * INTERVAL

# Uc2UnattestedReports — synthetic (real transition), delta over [5m], for: 0m.
r = rule("Uc2UnattestedReports", "critical", False, "crypto_counters")
rows = load_scenario("crypto_counters")
row = select(rows, "uc2_reports_unattested_total", {})
r["series"].append((f'uc2_reports_unattested_total{{{row["labels_str"]}}}', literal(row)))
r["labels_from"] = row
r["eval_time"] = (len(row["values"]) - 1) * INTERVAL

# Uc2CleartextPeer — synthetic (real transition), delta over [5m], for: 0m.
r = rule("Uc2CleartextPeer", "critical", False, "crypto_counters")
rows = load_scenario("crypto_counters")
row = select(rows, "uc2_cleartext_peer_datagrams_total", {})
r["series"].append((f'uc2_cleartext_peer_datagrams_total{{{row["labels_str"]}}}', literal(row)))
r["labels_from"] = row
r["eval_time"] = (len(row["values"]) - 1) * INTERVAL

# Uc2FollowerSealFailures — synthetic (real transition), delta over [5m] AND is_leader==0, for: 0m.
r = rule("Uc2FollowerSealFailures", "warning", False, "crypto_counters")
rows = load_scenario("crypto_counters")
seal_row = select(rows, "uc2_receiver_seal_failures_total", {})
leader_row = select(rows, "uc2_is_leader", {})
r["series"].append((f'uc2_receiver_seal_failures_total{{{seal_row["labels_str"]}}}', literal(seal_row)))
r["series"].append((f'uc2_is_leader{{{leader_row["labels_str"]}}}', literal(leader_row)))
r["labels_from"] = seal_row  # LHS of `and`
r["eval_time"] = (len(seal_row["values"]) - 1) * INTERVAL

# ------------------------------------------------------------ test generation


def exp_labels_block(alertname, severity, labels_from):
    labels = {"alertname": alertname, "alertstate": "firing", "severity": severity}
    if labels_from is not None:
        labels.update(labels_from["labels"])
    body = ", ".join(f'{k}="{v}"' for k, v in labels.items())
    return "{" + body + "}"


def write_test_yaml(name, spec):
    input_series_yaml = "\n".join(
        f'      - series: \'{series}\'\n        values: "{values}"' for series, values in spec["series"]
    )
    exp = exp_labels_block(name, spec["severity"], spec["labels_from"])
    yaml_text = f"""rule_files:
  - {RULES_FILE}
evaluation_interval: {INTERVAL}s
tests:
  - interval: {INTERVAL}s
    input_series:
{input_series_yaml}
    promql_expr_test:
      - expr: 'ALERTS{{alertname="{name}",alertstate="firing"}}'
        eval_time: {spec["eval_time"]}s
        exp_samples:
          - labels: 'ALERTS{exp}'
            value: 1
"""
    path = os.path.join(TEST_DIR, f"{name}.yml")
    with open(path, "w") as f:
        f.write(yaml_text)
    return path


overall_ok = True
for name in sorted(RULES):
    spec = RULES[name]
    path = write_test_yaml(name, spec)
    proc = subprocess.run([PROMTOOL, "test", "rules", path], capture_output=True, text=True)
    disclosure = "real" if spec["real"] else "synthetic"
    if proc.returncode == 0:
        print(f"PASS rule={name} scenario={spec['method']} state={disclosure}")
    else:
        overall_ok = False
        print(f"FAIL rule={name} scenario={spec['method']} state={disclosure}")
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)

sys.exit(0 if overall_ok else 1)
PYEOF
