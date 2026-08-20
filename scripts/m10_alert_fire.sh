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
# Fix round 1, Finding 1: `m10_alerts --all` now isolates each scenario behind
# `catch_unwind` internally and exits nonzero if ANY scenario panicked, but a
# panicking scenario still means fewer `.series` files got written — under
# `set -e` a bare nonzero exit here would abort this script BEFORE the
# per-rule adjudication step below ever ran, which is exactly the "bare
# backtrace instead of per-rule verdicts" failure mode being fixed. Consume
# the exit code explicitly instead: continue to adjudication regardless, and
# let the Python step's per-rule ScenarioMissing handling turn each missing
# `.series` file into its own `FAIL rule=... (scenario did not produce
# series)` line.
m10_status=0
cargo run --manifest-path "$ROOT/Cargo.toml" -p uc2_node --release --example m10_alerts -- --all --out "$OUT" || m10_status=$?
if [ "$m10_status" -ne 0 ]; then
    echo "warning: m10_alerts exited $m10_status (at least one scenario panicked — see above)." >&2
    echo "         continuing to per-rule adjudication; affected rules will show as FAIL." >&2
fi

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


class ScenarioMissing(Exception):
    """Raised when a scenario's `.series` file (or an expected series inside
    it) isn't there — Fix round 1, Finding 1: this used to be a hard
    `sys.exit` that aborted the WHOLE adjudication run on the first missing
    file (e.g. because `m10_alerts --all` panicked partway through). Every
    call site below is now wrapped per-rule (see the `RULE_BUILDERS` loop),
    so one scenario's failure surfaces as that scenario's rules FAILing —
    by name, with a reason — while every other rule still gets adjudicated
    normally."""


def load_scenario(name):
    path = os.path.join(OUT, f"{name}.series")
    if not os.path.isfile(path):
        raise ScenarioMissing(f"{path} does not exist (scenario did not produce series)")
    return parse_series_file(path)


def select(rows, name, filt):
    matches = [
        r for r in rows if r["name"] == name and all(r["labels"].get(k) == v for k, v in filt.items())
    ]
    if not matches:
        raise ScenarioMissing(f"no series matching {name}{filt} in loaded rows")
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


def add_hold_last(r, row, metric, for_secs):
    """LEVEL policy, tracked: append the input_series entry AND a
    human-readable dilation-disclosure line (Fix round 1, Finding 2 — the
    policy has to be visible at runtime, not just in code comments)."""
    _, total = total_for(for_secs)
    r["series"].append((f'{metric}{{{row["labels_str"]}}}', hold_last(row, total)))
    r["dilation"].append(
        f"series={metric} policy=hold_last for={for_secs}s samples={total} "
        f"(last real scraped value \"{row['values'][-1]}\" held constant)"
    )
    return total


def add_literal(r, row, metric, range_secs):
    """DELTA `for: 0m` policy, tracked: the raw captured jump is used
    verbatim inside the range window, no padding."""
    r["series"].append((f'{metric}{{{row["labels_str"]}}}', literal(row)))
    n = len(row["values"])
    r["dilation"].append(
        f"series={metric} policy=literal for=0s range={range_secs}s samples={n} "
        f"(raw captured jump \"{' '.join(row['values'])}\" used verbatim, no padding)"
    )


def add_jump_then_ramp(r, row, metric, for_secs, range_secs, step=1):
    """DELTA `for: >0` policy, tracked: real jump preserved, then a small
    synthetic ramp keeps the condition true for the rest of the `for:`
    sustain window (see `jump_then_ramp`'s own docstring for why)."""
    eval_time, total = total_for(for_secs, range_secs=range_secs)
    r["series"].append((f'{metric}{{{row["labels_str"]}}}', jump_then_ramp(row, total, step=step)))
    r["dilation"].append(
        f"series={metric} policy=jump_then_ramp for={for_secs}s range={range_secs}s samples={total} "
        f"(real jump {row['values'][0]}->{row['values'][-1]} preserved, ramp synthetic +{step}/sample)"
    )
    return eval_time, total


def new_rule(severity, labels_from=None):
    return {"severity": severity, "series": [], "dilation": [], "labels_from": labels_from, "eval_time": None}


# ---------------------------------------------------------------- rule specs
#
# Static per-rule metadata (severity from the rule's own `labels:` block,
# `real` mirroring the Task 9 brief's scenario table disclosure, and which
# scenario feeds it) is separated from the DYNAMIC construction below so
# that even a rule whose scenario failed to produce series can still be
# reported with the right `scenario=`/`state=` in its FAIL line — Fix round
# 1, Finding 1.
RULE_META = {
    "Uc2AgentDead": {"severity": "critical", "real": False, "scenario": "agent_dead"},
    "Uc2NoLeader": {"severity": "critical", "real": True, "scenario": "no_leader"},
    "Uc2LeaderNotServing": {"severity": "critical", "real": False, "scenario": "leader_not_serving"},
    "Uc2ServiceWedged": {"severity": "critical", "real": True, "scenario": "service_wedged"},
    "Uc2ReplicationStalled": {"severity": "critical", "real": True, "scenario": "leader_isolated"},
    "Uc2AdmissionSaturated": {"severity": "warning", "real": True, "scenario": "leader_isolated"},
    "Uc2PeerNeverHeard": {"severity": "warning", "real": True, "scenario": "peer_never_heard"},
    "Uc2PeerLagging": {"severity": "warning", "real": True, "scenario": "follower_partitioned"},
    "Uc2PurgeStalled": {"severity": "warning", "real": False, "scenario": "purge_stalled"},
    "Uc2RepeatedWipes": {"severity": "warning", "real": False, "scenario": "repeated_wipes"},
    "Uc2UnattestedReports": {"severity": "critical", "real": False, "scenario": "crypto_counters"},
    "Uc2CleartextPeer": {"severity": "critical", "real": False, "scenario": "crypto_counters"},
    "Uc2FollowerSealFailures": {"severity": "warning", "real": False, "scenario": "crypto_counters"},
}


def build_Uc2AgentDead():
    rows = load_scenario("agent_dead")
    row = select(rows, "uc2_agent_alive", {"agent": "archive"})
    r = new_rule("critical", labels_from=row)
    add_hold_last(r, row, "uc2_agent_alive", 0)
    r["eval_time"] = total_for(0)[0]
    return r


def build_Uc2NoLeader():
    rows = load_scenario("no_leader")
    row = select(rows, "uc2_is_leader", {})
    r = new_rule("critical", labels_from=None)  # max() strips ALL labels
    add_hold_last(r, row, "uc2_is_leader", 30)
    r["eval_time"] = total_for(30)[0]
    return r


def build_Uc2LeaderNotServing():
    rows = load_scenario("leader_not_serving")
    leader_row = select(rows, "uc2_is_leader", {})
    serve_row = select(rows, "uc2_can_serve", {})
    r = new_rule("critical", labels_from=leader_row)  # LHS of `and`
    add_hold_last(r, leader_row, "uc2_is_leader", 30)
    add_hold_last(r, serve_row, "uc2_can_serve", 30)
    r["eval_time"] = total_for(30)[0]
    return r


def build_Uc2ServiceWedged():
    rows = load_scenario("service_wedged")
    svc_row = select(rows, "uc2_service_heartbeat_age_seconds", {})
    node_row = select(rows, "uc2_node_heartbeat_age_seconds", {})
    r = new_rule("critical", labels_from=svc_row)  # LHS of `and`
    add_hold_last(r, svc_row, "uc2_service_heartbeat_age_seconds", 60)
    add_hold_last(r, node_row, "uc2_node_heartbeat_age_seconds", 60)
    r["eval_time"] = total_for(60)[0]
    return r


def build_Uc2ReplicationStalled():
    rows = load_scenario("leader_isolated")
    commit_row = select(rows, "uc2_commit_bytes", {})
    append_row = select(rows, "uc2_append_bytes", {})
    r = new_rule("critical", labels_from=commit_row)  # LHS: delta(uc2_commit_bytes[1m])
    add_hold_last(r, commit_row, "uc2_commit_bytes", 60)
    eval_time, _ = add_jump_then_ramp(r, append_row, "uc2_append_bytes", 60, 60)
    r["eval_time"] = eval_time
    return r


def build_Uc2AdmissionSaturated():
    rows = load_scenario("leader_isolated")  # shared real scenario
    sat_row = select(rows, "uc2_admission_saturation", {})
    r = new_rule("warning", labels_from=sat_row)
    add_hold_last(r, sat_row, "uc2_admission_saturation", 60)
    r["eval_time"] = total_for(60)[0]
    return r


def build_Uc2PeerNeverHeard():
    rows = load_scenario("peer_never_heard")
    # The fixed rule is leader-scoped (`and on(instance) uc2_is_leader == 1`),
    # so the leader's OWN row is the one that must be adjudicated, not
    # whichever of n0/n1 `select()` happens to hit first: both nodes show
    # peer="2" == 0 in the real capture, but only the leader's zero is the
    # semantically meaningful "never reported" — a follower reads 0 for
    # every peer regardless, because it never tracks peer-reported-durable
    # the way the leader's quorum tracker does (that asymmetry is exactly
    # the bug this leader-scoping fixes). Find the leader from the real
    # uc2_is_leader scrapes and select ITS peer=2 row.
    leader_rows = [r for r in rows if r["name"] == "uc2_is_leader"]
    leader_row = next((r for r in leader_rows if r["values"][-1] == "1"), None)
    if leader_row is None:
        raise ScenarioMissing("no node reported uc2_is_leader=1 in peer_never_heard capture")
    leader_instance = leader_row["labels"]["instance"]
    row = select(
        rows, "uc2_peer_reported_durable_bytes", {"peer": "2", "instance": leader_instance}
    )
    r = new_rule("warning", labels_from=row)
    add_hold_last(r, row, "uc2_peer_reported_durable_bytes", 120)
    add_hold_last(r, leader_row, "uc2_is_leader", 120)
    r["eval_time"] = total_for(120)[0]
    return r


def build_Uc2PeerLagging():
    rows = load_scenario("follower_partitioned")
    # follower_partitioned only ever scrapes the LEADER's /metrics (see the
    # Rust scenario), so every row here is already the leader's — the
    # is_leader series added for the leader-scoped rule is just that same
    # instance's uc2_is_leader, real and held at 1 the whole window.
    lag_row = select(rows, "uc2_peer_replication_lag_bytes", {"peer": "0"})
    adm_row = select(rows, "uc2_admission_bytes", {})
    leader_row = select(rows, "uc2_is_leader", {})
    r = new_rule("warning", labels_from=lag_row)  # group_left keeps LHS's extra labels
    add_hold_last(r, lag_row, "uc2_peer_replication_lag_bytes", 300)
    add_hold_last(r, adm_row, "uc2_admission_bytes", 300)
    add_hold_last(r, leader_row, "uc2_is_leader", 300)
    r["eval_time"] = total_for(300)[0]
    return r


def build_Uc2PurgeStalled():
    rows = load_scenario("purge_stalled")
    pe_row = select(rows, "uc2_purge_enabled", {})
    floor_row = select(rows, "uc2_node_snapshot_floor_bytes", {})
    base_row = select(rows, "uc2_archive_first_base_bytes", {})
    seg_row = select(rows, "uc2_journal_segment_bytes", {})
    r = new_rule("warning", labels_from=pe_row)  # LHS of `and`
    add_hold_last(r, pe_row, "uc2_purge_enabled", 600)
    add_hold_last(r, floor_row, "uc2_node_snapshot_floor_bytes", 600)
    add_hold_last(r, base_row, "uc2_archive_first_base_bytes", 600)
    add_hold_last(r, seg_row, "uc2_journal_segment_bytes", 600)
    r["eval_time"] = total_for(600)[0]
    return r


def build_Uc2RepeatedWipes():
    rows = load_scenario("repeated_wipes")
    row = select(rows, "uc2_wipes_total", {})
    r = new_rule("warning", labels_from=row)
    add_literal(r, row, "uc2_wipes_total", 600)
    r["eval_time"] = (len(row["values"]) - 1) * INTERVAL
    return r


def build_Uc2UnattestedReports():
    rows = load_scenario("crypto_counters")
    row = select(rows, "uc2_reports_unattested_total", {})
    r = new_rule("critical", labels_from=row)
    add_literal(r, row, "uc2_reports_unattested_total", 300)
    r["eval_time"] = (len(row["values"]) - 1) * INTERVAL
    return r


def build_Uc2CleartextPeer():
    rows = load_scenario("crypto_counters")
    row = select(rows, "uc2_cleartext_peer_datagrams_total", {})
    r = new_rule("critical", labels_from=row)
    add_literal(r, row, "uc2_cleartext_peer_datagrams_total", 300)
    r["eval_time"] = (len(row["values"]) - 1) * INTERVAL
    return r


def build_Uc2FollowerSealFailures():
    rows = load_scenario("crypto_counters")
    seal_row = select(rows, "uc2_receiver_seal_failures_total", {})
    leader_row = select(rows, "uc2_is_leader", {})
    r = new_rule("warning", labels_from=seal_row)  # LHS of `and`
    add_literal(r, seal_row, "uc2_receiver_seal_failures_total", 300)
    add_literal(r, leader_row, "uc2_is_leader", 300)
    r["eval_time"] = (len(seal_row["values"]) - 1) * INTERVAL
    return r


RULE_BUILDERS = {
    "Uc2AgentDead": build_Uc2AgentDead,
    "Uc2NoLeader": build_Uc2NoLeader,
    "Uc2LeaderNotServing": build_Uc2LeaderNotServing,
    "Uc2ServiceWedged": build_Uc2ServiceWedged,
    "Uc2ReplicationStalled": build_Uc2ReplicationStalled,
    "Uc2AdmissionSaturated": build_Uc2AdmissionSaturated,
    "Uc2PeerNeverHeard": build_Uc2PeerNeverHeard,
    "Uc2PeerLagging": build_Uc2PeerLagging,
    "Uc2PurgeStalled": build_Uc2PurgeStalled,
    "Uc2RepeatedWipes": build_Uc2RepeatedWipes,
    "Uc2UnattestedReports": build_Uc2UnattestedReports,
    "Uc2CleartextPeer": build_Uc2CleartextPeer,
    "Uc2FollowerSealFailures": build_Uc2FollowerSealFailures,
}

# Fix round 1, Finding 1: build each rule's input series independently, so
# one rule's scenario being missing (panicked upstream, or — in principle —
# a series this particular rule needs being absent from an otherwise-present
# scenario file) never stops the rest of the rules from being adjudicated.
RULES = {}
BUILD_ERRORS = {}
for _name, _builder in RULE_BUILDERS.items():
    try:
        RULES[_name] = _builder()
    except ScenarioMissing as e:
        BUILD_ERRORS[_name] = str(e)

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
for name in sorted(RULE_META):
    meta = RULE_META[name]
    disclosure = "real" if meta["real"] else "synthetic"
    scenario = meta["scenario"]

    if name in BUILD_ERRORS:
        # Fix round 1, Finding 1: the scenario that was supposed to feed this
        # rule never produced usable series (its `m10_alerts` process either
        # panicked — isolated by catch_unwind, see the Rust harness — or, in
        # principle, was missing an expected series inside an otherwise-
        # present file). Report it as a named FAIL instead of aborting the
        # whole adjudication run.
        overall_ok = False
        print(
            f"FAIL rule={name} scenario={scenario} state={disclosure} "
            f"(scenario did not produce series: {BUILD_ERRORS[name]})"
        )
        continue

    spec = RULES[name]
    # Fix round 1, Finding 2: the time-dilation policy applied to each input
    # series must be disclosed at RUNTIME, not just in code comments/the task
    # report — print one line per dilated series before adjudicating.
    for line in spec["dilation"]:
        print(f"  dilate rule={name} {line}")

    path = write_test_yaml(name, spec)
    proc = subprocess.run([PROMTOOL, "test", "rules", path], capture_output=True, text=True)
    if proc.returncode == 0:
        print(f"PASS rule={name} scenario={scenario} state={disclosure}")
    else:
        overall_ok = False
        print(f"FAIL rule={name} scenario={scenario} state={disclosure}")
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)

sys.exit(0 if overall_ok else 1)
PYEOF
