#!/usr/bin/env python3
"""UC v2 M14 fleet-gate driver — spec §15 rows a–g.

Topology (4 hosts): hosts[0..3] voters, hosts[3] the learner (idle until row
f). The direct Engine client is shmem-attached and runs ON THE LEADER HOST.

Arms (each a fresh cluster generation unless noted):
  calib   FSM 0 alone, SpinCountSm at a K ladder → pick K (spec §15.3)
  n1      {0} CountSm                                → rate(n1)
  n2eq    {0,1} CountSm + CountSm, bounded           → rate(n2eq)      row a
  slow1   {0} SpinCountSm(K)                         → rate(slow1)
  pair    {0,1} CountSm + SpinCountSm(K), bounded    → rate(pair)      row b
  n2eq-ls / pair-ls  the same two pairs in lockstep  → reported        row e
  kill    pair under load; SIGKILL FSM 1 on the leader host; restart   row d
  join    pair + purge + snapshots; add-learner on hosts[3] under load row f
  row c   check-fsms after EVERY arm above (leader: linearizable; every
          host: snapshot) — any mismatch FAILs the gate.

Every row verdict is a PURE function of recorded numbers, so `--selftest`
replays canned inputs through them with no fleet. Bars are the constants
below; they are printed beside each verdict as a GATE-JSON line. The exit
code is the verdict: a green terminal is not a PASS.
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
from m12_fleet_gate import (  # noqa: E402
    ssh, start_unit, kill_unit, truncate_log, tail_log, run_foreground,
    parse_result, echo, Verdict, APP, PORT, REMOTE_ROOT, UNIT_PREFIX,
    BUILT_GATE, BUILT_PROBE, BOOT_SETTLE_SECS, CLIENT_SLACK_SECS,
    LEADER_WAIT_SECS,
)
from m13_hop_bench import sync_tree  # noqa: E402

BUILT_CTL = "/opt/bench/uc/target/release/uc2ctl"

# ------------------------------------------------------------------ bars
# Spec §15.4, verbatim. Committed before any run; never edited to fit one.
BAR_A_RATIO = 0.90          # rate(n2eq) / rate(n1)
BAR_B_LO, BAR_B_HI = 0.90, 1.10   # rate(pair) / rate(slow1)
BAR_D_SECS = 15.0           # M9's bar: recovered AND attached+lag≤bound by then
BAR_D_FRACTION = 0.80       # M9's rule: a 2 s window at ≥ 80 % of baseline …
BAR_D_WINDOW_SECS = 2       # … confirmed by the next such window
BAR_F_JOIN_SECS = 60.0      # M6's JOIN_BUDGET
CALIB_TARGET = 0.5          # slow-solo ≈ 0.5 × rate(n1)

# ------------------------------------------------------------- arm shape
ARM_SECS = 12               # 2 s warm-up + 8 s window + 2 s tail (spec §15.3)
WARMUP_SECS, MEASURE_SECS = 2, 8
KILL_ARM_SECS = 45          # row d: baseline [2,10) s, kill at ~12 s, 30 s to recover
JOIN_ARM_SECS = 90          # row f: load for the whole join
JOIN_AT_SECS = 10           # row f: add-learner this long after load starts
STATUS_RE = re.compile(
    r"id=(\d+) attached=(true|false) epoch=(\d+) incarnation=(\d+) "
    r"applied=(\d+) lag=(\d+) snapshot_pos=(\d+)")
TL_RE = re.compile(r'^TL\s+(\{.*\})\s*$', re.M)
FSMS_OK_RE = re.compile(r'^FSMS-OK\s+(\{.*\})\s*$', re.M)
STATS_RE = re.compile(r"reports_unattested=(\d+) snap_refusals=\((\d+),(\d+)\)")

M14_SEGMENT_BYTES = 16 * 1024          # M7's value: purge inside one arm
M14_SNAPSHOT_INTERVAL_BYTES = 32 * 1024


def gate_json(row, passed, **fields):
    print("GATE-JSON " + json.dumps({"row": row, "pass": passed, **fields}), flush=True)


# ------------------------------------------------------ pure verdicts
def pick_k(calib):
    """`calib` = [(K, rate)], the ladder. Return the (K, rate) whose rate is
    nearest CALIB_TARGET × n1_rate — the caller passes the ladder already
    scaled (rate / n1_rate) as `calib[i] = (K, ratio)`."""
    if not calib:
        raise ValueError("empty calibration ladder")
    return min(calib, key=lambda kr: abs(kr[1] - CALIB_TARGET))


def verdict_row_a(rates):
    n1, n2 = rates.get("n1"), rates.get("n2eq")
    ok = bool(n1 and n2) and (n2 / n1) >= BAR_A_RATIO
    ratio = (n2 / n1) if n1 and n2 else None
    gate_json("a", ok, n1=n1, n2eq=n2, ratio=ratio, bar=BAR_A_RATIO)
    return Verdict("a equal-speed pair vs N=1", ok,
                   f"n2eq/n1 = {ratio:.3f} (bar ≥ {BAR_A_RATIO})" if ratio else "missing rate")


def verdict_row_b(rates):
    s, p = rates.get("slow1"), rates.get("pair")
    ratio = (p / s) if s and p else None
    ok = ratio is not None and BAR_B_LO <= ratio <= BAR_B_HI
    gate_json("b", ok, slow1=s, pair=p, ratio=ratio, bar=[BAR_B_LO, BAR_B_HI])
    return Verdict("b bounded pair converges to the slow FSM", ok,
                   f"pair/slow1 = {ratio:.3f} (bar [{BAR_B_LO}, {BAR_B_HI}])" if ratio else "missing rate")


def verdict_row_c(checks):
    """`checks` = [(arm, host, mode, ok, count)] — one per check-fsms run.
    Every one must be ok AND, per arm, every host's count must agree."""
    bad = [c for c in checks if not c[3]]
    by_arm = {}
    for arm, host, mode, ok, count in checks:
        by_arm.setdefault(arm, set()).add(count)
    disagree = {arm: sorted(cs) for arm, cs in by_arm.items() if len(cs) > 1}
    ok = not bad and not disagree and bool(checks)
    gate_json("c", ok, checks=len(checks), failed=[c[:3] for c in bad], cross_host=disagree)
    detail = f"{len(checks)} checks; " + ("all agree" if ok else f"failed={bad} cross-host={disagree}")
    return Verdict("c zero divergence", ok, detail)


def recovery_time(timeline, t0_ms, base_lo_ms, base_hi_ms):
    """M9's rule over 1 s buckets `[(unix_ms, responses)]`: baseline = mean
    rate over [base_lo, base_hi); recovered = the first 2 s window at ≥ 80 %
    of baseline whose END is after t0, confirmed by the NEXT 2 s window.
    Returns (baseline_rps, recovered_at_secs_after_t0 | None, windows)."""
    base = [r for ms, r in timeline if base_lo_ms <= ms < base_hi_ms]
    baseline = (sum(base) / len(base)) if base else 0.0
    after = [(ms, r) for ms, r in timeline if ms + 1000 > t0_ms]
    windows = []
    for i in range(0, len(after) - BAR_D_WINDOW_SECS + 1):
        w = after[i:i + BAR_D_WINDOW_SECS]
        end_ms = w[-1][0] + 1000
        rate = sum(r for _, r in w) / BAR_D_WINDOW_SECS
        windows.append((end_ms, rate))
    recovered = None
    for i in range(len(windows) - BAR_D_WINDOW_SECS):
        end_ms, rate = windows[i]
        nxt = windows[i + BAR_D_WINDOW_SECS][1]
        if baseline > 0 and rate >= BAR_D_FRACTION * baseline and nxt >= BAR_D_FRACTION * baseline:
            recovered = (end_ms - t0_ms) / 1000.0
            break
    return baseline, recovered, windows


def verdict_row_d(kill):
    """`kill` = {"baseline": rps, "recovered_at": s|None, "attached_at": s|None}."""
    r, a = kill.get("recovered_at"), kill.get("attached_at")
    ok = r is not None and a is not None and r <= BAR_D_SECS and a <= BAR_D_SECS
    gate_json("d", ok, **kill, bar=BAR_D_SECS)
    return Verdict("d FSM kill on the leader host recovers", ok,
                   f"rate back at {r}s, attached+lag≤bound at {a}s (bar ≤ {BAR_D_SECS}s), "
                   f"baseline {kill.get('baseline', 0):.0f}/s")


def verdict_row_e(rates):
    pairs = [("n2eq-ls", "n2eq"), ("pair-ls", "pair")]
    out = {}
    for ls, base in pairs:
        if rates.get(ls) and rates.get(base):
            out[ls] = rates[ls] / rates[base]
    gate_json("e", True, ratios=out, bar=None)
    return Verdict("e lockstep cost (reported, no bar)", True,
                   ", ".join(f"{k} = {v:.3f}× bounded" for k, v in out.items()) or "no lockstep rates")


def verdict_row_f(join):
    """`join` = {"joined_at": s|None, "refusals": {host: (legacy, mismatch)},
    "artifacts": {0: n, 1: n}, "check_ok": bool}."""
    j = join.get("joined_at")
    refusals_zero = all(tuple(v) == (0, 0) for v in join.get("refusals", {}).values()) and bool(join.get("refusals"))
    both = all(join.get("artifacts", {}).get(i, 0) > 0 for i in (0, 1))
    ok = j is not None and j <= BAR_F_JOIN_SECS and refusals_zero and both and join.get("check_ok", False)
    gate_json("f", ok, **{k: (v if k != "refusals" else {h: list(t) for h, t in v.items()}) for k, v in join.items()},
              bar=BAR_F_JOIN_SECS)
    return Verdict("f two-FSM learner join over wire 0.6.0", ok,
                   f"joined at {j}s (bar ≤ {BAR_F_JOIN_SECS}s), refusals zero={refusals_zero}, "
                   f"both artifacts={both}, divergence check={join.get('check_ok')}")


# ------------------------------------------------------------------ fleet
def prepare_host_m14(host):
    """m12's build plus the uc2ctl binary (rows d/f drive real admin ops)."""
    m12.prepare_host(host, apply_profile=False)
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    cmd = (f"{env} {m6.SshHost.CARGO} build --release --manifest-path {m6.SshHost.UC_SRC}/Cargo.toml "
           f"-p uc2ctl && test -x {BUILT_CTL} && echo CTL-OK")
    r = ssh(host, cmd, label="build-ctl")
    if "CTL-OK" not in (r.stdout or ""):
        raise RuntimeError(f"uc2ctl build on {host.public_ip}: {r.stderr or r.stdout}")


def setup_fleet(a):
    hosts = m6.build_fleet_hosts(BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=4,
                                 ctl_bin=BUILT_CTL, unit_prefix=UNIT_PREFIX,
                                 remote_root=REMOTE_ROOT, probe_bin=BUILT_PROBE)
    if not a.no_sync:
        sync_tree(hosts, a.local_tree)
    for h in hosts:
        prepare_host_m14(h)
        stop_cluster_m14([h])
    voters, learner = hosts[:3], hosts[3]
    print(f"INFO topology: voters {[h.public_ip for h in voters]}, learner {learner.public_ip}; "
          f"the direct client runs on the leader host", flush=True)
    return hosts, voters, learner


SERVICE_UNITS = ("service0", "service1")


def stop_cluster_m14(hosts):
    for h in hosts:
        for u in ("client",) + SERVICE_UNITS + ("node",):
            kill_unit(h, u)


def node_args(h, node_id, members, fsms, lag, purge, snap):
    args = ["node", "--id", str(node_id), "--bind", f"{h.private_ip}:{PORT}",
            "--instance-dir", h.dir, "--members", members, "--app-id", APP,
            "--admission-kib", str(ADMISSION_KIB),
            "--services", ",".join(str(i) for i, _ in fsms)]
    if lag is not None:
        args += ["--fsm-lag", lag]
    if purge:
        args += ["--purge-below-snapshot", "--journal-segment-bytes", str(M14_SEGMENT_BYTES)]
    return args


def service_args(h, sid, spin, snap):
    args = ["service", "--instance-dir", h.dir, "--app-id", APP, "--envelope", "on",
            "--service-id", str(sid), "--work-spin", str(spin)]
    if snap:
        args += ["--snapshot-interval-bytes", str(snap)]
    return args


ADMISSION_KIB = 256


def start_cluster_m14(voters, fsms, lag=None, purge=False, snap=0):
    """`fsms` = [(id, spin)], e.g. [(0, 0)] or [(0, 0), (1, K)]. A FRESH
    generation: dirs wiped, nodes then services, settle after each."""
    m12.wipe_dirs(voters)
    ms = m12.members_str(voters)
    for i, h in enumerate(voters):
        start_unit(h, "node", node_args(h, i, ms, fsms, lag, purge, snap), nofile=True)
    time.sleep(BOOT_SETTLE_SECS)
    for h in voters:
        for sid, spin in fsms:
            truncate_log(h, f"service{sid}")
            start_unit(h, f"service{sid}", service_args(h, sid, spin, snap))
    time.sleep(BOOT_SETTLE_SECS)
    leader = m6.wait_leader(voters, list(range(len(voters))), LEADER_WAIT_SECS)
    if leader is None:
        raise RuntimeError("no single serving leader")
    return leader


def run_rate_arm(voters, leader, a, label, fan_in, secs=ARM_SECS, timeline=False, unit=False):
    """The direct client on the leader host. Foreground (returns the RESULT
    dict) unless `unit`, in which case it is started as a transient unit and
    the caller reads the log later (row d/f keep it running across an action)."""
    h = voters[leader]
    args = ["client-direct", "--instance-dir", h.dir, "--app-id", APP,
            "--secs", str(secs), "--payload", str(a.payload), "--inflight", str(a.inflight),
            "--envelope", "on", "--warmup-secs", str(WARMUP_SECS), "--measure-secs", str(MEASURE_SECS)]
    if fan_in:
        args.append("--fan-in")
    if timeline:
        args.append("--timeline")
    if unit:
        truncate_log(h, "client")
        start_unit(h, "client", args)
        return None
    rc, out = run_foreground(h, args, timeout=secs + CLIENT_SLACK_SECS)
    echo(label, out)
    d = parse_result(out, "direct")
    if d is None:
        raise RuntimeError(f"{label}: no RESULT line (rc={rc})")
    return d


def check_fsms(h, mode, expect=None, expect_min=None):
    args = ["check-fsms", "--instance-dir", h.dir, "--app-id", APP, "--mode", mode]
    if expect is not None:
        args += ["--expect", str(expect)]
    if expect_min is not None:
        args += ["--expect-min", str(expect_min)]
    rc, out = run_foreground(h, args, timeout=60)
    echo(f"check-fsms {h.public_ip} {mode}", out, lines=6)
    m = FSMS_OK_RE.search(out)
    count = json.loads(m.group(1))["count"] if m else None
    return rc == 0 and m is not None, count


def check_all(hosts, leader, arm, checks, expect=None, expect_min=None):
    """Row c after an arm: linearizable on the leader, snapshot on every host.
    Appends (arm, host, mode, ok, count) tuples; never raises — the verdict
    function judges."""
    ok, c = check_fsms(hosts[leader], "linearizable", expect, expect_min)
    checks.append((arm, hosts[leader].public_ip, "linearizable", ok, c))
    for h in hosts:
        ok, c = check_fsms(h, "snapshot", expect, expect_min)
        checks.append((arm, h.public_ip, "snapshot", ok, c))


# ------------------------------------------------------------- rate arms
def rate_of(d):
    return float(d["window_rps"])


def one_arm(voters, a, label, fsms, lag, rates, checks, fan_in):
    leader = start_cluster_m14(voters, fsms, lag=lag)
    print(f"INFO arm {label}: leader n{leader} on {voters[leader].public_ip}", flush=True)
    d = run_rate_arm(voters, leader, a, label, fan_in)
    rates[label] = rate_of(d)
    print(f"INFO arm {label}: window_rps={rates[label]:.0f} responses={d['responses']} lost={d['lost']}", flush=True)
    check_all(voters, leader, label, checks, expect=int(d["responses"]))
    stop_cluster_m14(voters)
    return d


def arm_calib(voters, a, rates, checks):
    """FSM 0 alone as SpinCountSm over a K ladder; pick the K nearest 0.5 × n1."""
    ladder = []
    for k in [int(x) for x in a.calib_ks.split(",")]:
        d = one_arm(voters, a, f"calib-{k}", [(0, k)], None, rates, checks, fan_in=False)
        ladder.append((k, rate_of(d) / rates["n1"]))
        print(f"INFO calib K={k}: {ladder[-1][1]:.3f} × n1", flush=True)
    k, ratio = pick_k(ladder)
    gate_json("calib", True, ladder=ladder, K=k, ratio=ratio)
    return k


def arm_rates(voters, a, rates, checks):
    one_arm(voters, a, "n1", [(0, 0)], None, rates, checks, fan_in=False)
    K = a.k if a.k else arm_calib(voters, a, rates, checks)
    print(f"INFO slow FSM K = {K}", flush=True)
    one_arm(voters, a, "n2eq", [(0, 0), (1, 0)], None, rates, checks, fan_in=True)
    one_arm(voters, a, "slow1", [(0, K)], None, rates, checks, fan_in=False)
    one_arm(voters, a, "pair", [(0, 0), (1, K)], None, rates, checks, fan_in=True)
    one_arm(voters, a, "n2eq-ls", [(0, 0), (1, 0)], "lockstep", rates, checks, fan_in=True)
    one_arm(voters, a, "pair-ls", [(0, 0), (1, K)], "lockstep", rates, checks, fan_in=True)
    return K


def status_slots(h):
    """`uc2ctl status` per-FSM rows (M14c) → {id: {...}}; also returns the
    node's fsm_lag bound (bytes; 0 = lockstep) from the `services:` line."""
    r = ssh(h, f"sudo {BUILT_CTL} status --instance-dir {h.dir} --app-id {APP}", label="uc2ctl")
    out = (r.stdout or "") + (r.stderr or "")
    slots = {}
    for m in STATUS_RE.finditer(out):
        slots[int(m.group(1))] = {
            "attached": m.group(2) == "true", "applied": int(m.group(5)),
            "lag": int(m.group(6)), "snapshot_pos": int(m.group(7)),
        }
    lm = re.search(r"fsm_lag=(\d+) bytes|fsm_lag=(lockstep)", out)
    bound = 0 if (lm is None or lm.group(2)) else int(lm.group(1))
    return slots, bound


def node_stats(h):
    """Last `stats:` line of the node unit's log → (unattested, legacy, mismatch)."""
    out = tail_log(h, "node", lines=400)
    hits = STATS_RE.findall(out or "")
    if not hits:
        return None
    u, l, m = hits[-1]
    return int(u), int(l), int(m)


def parse_timeline(out):
    return [(int(json.loads(m)["unix_ms"]), int(json.loads(m)["responses"])) for m in TL_RE.findall(out)]


def bound_timeline(tl, end_ms):
    """Drop trailing timeline buckets published after the client's own run
    ended. `--timeline` prints one TL line per bucket for `secs + 40`
    buckets, so ~40 s of zero-response buckets trail the run and would
    otherwise read as an outage to `recovery_time`. Buckets strictly before
    `end_ms` are kept unchanged."""
    return [(ms, r) for ms, r in tl if ms < end_ms]


def arm_kill(voters, a, K, checks):
    """Row d: the bounded pair under fan-in load; SIGKILL FSM 1's unit on the
    leader host; start it again at once. Recovery is judged twice — the
    client's own per-second timeline (M9's window rule, same host as the
    kill so no clock skew) and `uc2ctl status` showing FSM 1 attached with
    lag ≤ bound."""
    leader = start_cluster_m14(voters, [(0, 0), (1, K)])
    h = voters[leader]
    run_rate_arm(voters, leader, a, "kill", fan_in=True, secs=KILL_ARM_SECS, timeline=True, unit=True)
    t_start = time.time()
    time.sleep(12.0)                       # 2 s ramp + [2,10) s baseline + slack
    t0 = time.time()
    ssh(h, f"sudo systemctl kill --signal=SIGKILL {UNIT_PREFIX}-service1", label="SIGKILL")
    start_unit(h, "service1", service_args(h, 1, K, 0))
    attached_at = None
    deadline = t0 + 30.0
    _, bound = status_slots(h)
    while time.time() < deadline:
        slots, _ = status_slots(h)
        s1 = slots.get(1)
        if s1 and s1["attached"] and (bound == 0 or s1["lag"] <= bound):
            attached_at = round(time.time() - t0, 2)
            break
        time.sleep(0.25)
    # let the client finish, then read its timeline
    time.sleep(max(0.0, (t_start + KILL_ARM_SECS + 8) - time.time()))
    out = tail_log(h, "client", lines=2000) or ""
    d = parse_result(out, "direct")
    tl = parse_timeline(out)
    if d:
        # Amendment 1: bound the timeline by the client's own run so the ~40
        # trailing zero-response buckets --timeline keeps emitting past
        # RESULT don't read as an outage. t_start_ms is the client's own
        # clock (the first TL line's unix_ms), not the driver's local time,
        # so this is immune to driver/host clock skew.
        t_start_ms = tl[0][0] if tl else int(t_start * 1000)
        tl = bound_timeline(tl, t_start_ms + int(d["elapsed_secs"] * 1000) + 1000)
    t0_ms = int(t0 * 1000)
    base_lo, base_hi = int((t_start + 2) * 1000), int((t_start + 10) * 1000)
    baseline, recovered, windows = recovery_time(tl, t0_ms, base_lo, base_hi)
    print("INFO recovery timeline (ops/s per 2 s window, end-relative to t0): " +
          ", ".join(f"{(e - t0_ms) / 1000:.1f}s:{r:.0f}" for e, r in windows[:25]), flush=True)
    print(f"INFO row d: baseline {baseline:.0f}/s, rate recovered at {recovered}s, "
          f"FSM 1 attached+lag≤{bound} at {attached_at}s; client lost={d['lost'] if d else '?'}", flush=True)
    kill_unit(h, "client")
    check_all(voters, leader, "kill", checks, expect_min=int(d["responses"]) if d else None)
    stop_cluster_m14(voters)
    return {"baseline": baseline, "recovered_at": recovered, "attached_at": attached_at,
            "bound": bound, "client_lost": d["lost"] if d else None}


def arm_join(voters, learner, a, K, checks):
    raise NotImplementedError("Task 4d")


# ---------------------------------------------------------------- selftest
def selftest():
    fails = 0

    def expect(name, cond):
        nonlocal fails
        print(f"  [{'ok' if cond else 'FAIL'}] {name}")
        fails += 0 if cond else 1

    expect("pick_k nearest 0.5", pick_k([(500, 0.9), (2000, 0.52), (8000, 0.2)])[0] == 2000)
    expect("row a pass at 0.95", verdict_row_a({"n1": 1000.0, "n2eq": 950.0}).passed)
    expect("row a fail at 0.85", not verdict_row_a({"n1": 1000.0, "n2eq": 850.0}).passed)
    expect("row a fail on missing", not verdict_row_a({"n1": 1000.0}).passed)
    expect("row b pass at 1.05", verdict_row_b({"slow1": 500.0, "pair": 525.0}).passed)
    expect("row b fail at 0.85", not verdict_row_b({"slow1": 500.0, "pair": 425.0}).passed)
    expect("row b fail at 1.2 (outran the bound)", not verdict_row_b({"slow1": 500.0, "pair": 600.0}).passed)
    expect("row c pass", verdict_row_c([("n1", "h0", "lin", True, 10), ("n1", "h1", "snap", True, 10)]).passed)
    expect("row c fail on one bad check", not verdict_row_c([("n1", "h0", "lin", False, 10)]).passed)
    expect("row c fail on cross-host disagreement",
           not verdict_row_c([("n1", "h0", "lin", True, 10), ("n1", "h1", "snap", True, 9)]).passed)
    expect("row c fail on no checks", not verdict_row_c([]).passed)
    # recovery: 1 s buckets, baseline 1000/s over [2000,10000) ms, kill at
    # 12000 ms, zero until 20000, back to 900 from 20000 on → recovered when the
    # window ending at 22000 (rates 900,900) is confirmed by [22000,24000).
    tl = [(ms, 1000) for ms in range(0, 12000, 1000)] + \
         [(ms, 0) for ms in range(12000, 20000, 1000)] + \
         [(ms, 900) for ms in range(20000, 30000, 1000)]
    base, rec, _ = recovery_time(tl, 12000, 2000, 10000)
    expect("recovery baseline 1000", abs(base - 1000) < 1e-9)
    expect("recovery at 10 s", rec == 10.0)
    _, rec2, _ = recovery_time([(ms, 1000) for ms in range(0, 12000, 1000)] +
                               [(ms, 0) for ms in range(12000, 40000, 1000)], 12000, 2000, 10000)
    expect("no recovery → None", rec2 is None)
    lucky = [(ms, 1000) for ms in range(0, 12000, 1000)] + [(ms, 0) for ms in range(12000, 20000, 1000)] + \
            [(20000, 900), (21000, 900), (22000, 0), (23000, 0)] + [(ms, 900) for ms in range(24000, 30000, 1000)]
    _, rec3, _ = recovery_time(lucky, 12000, 2000, 10000)
    expect("one lucky window is not recovery", rec3 == 14.0)
    expect("row d pass", verdict_row_d({"baseline": 1000, "recovered_at": 9.5, "attached_at": 3.0}).passed)
    expect("row d fail late attach", not verdict_row_d({"baseline": 1000, "recovered_at": 9.5, "attached_at": 16.0}).passed)
    expect("row d fail never", not verdict_row_d({"baseline": 1000, "recovered_at": None, "attached_at": 3.0}).passed)
    expect("row e always passes", verdict_row_e({"n2eq": 100.0, "n2eq-ls": 40.0}).passed)
    tl_in = [(ms, 100) for ms in range(0, 5000, 1000)] + [(ms, 0) for ms in range(5000, 45000, 1000)]
    expect("bound_timeline drops trailing buckets past end_ms, keeps earlier",
           bound_timeline(tl_in, 5000) == [(ms, 100) for ms in range(0, 5000, 1000)])
    good = {"joined_at": 30.0, "refusals": {"h0": (0, 0), "h1": (0, 0), "h2": (0, 0), "h3": (0, 0)},
            "artifacts": {0: 1, 1: 1}, "check_ok": True}
    expect("row f pass", verdict_row_f(good).passed)
    expect("row f fail on a refusal", not verdict_row_f({**good, "refusals": {**good["refusals"], "h3": (1, 0)}}).passed)
    expect("row f fail on one artifact", not verdict_row_f({**good, "artifacts": {0: 1, 1: 0}}).passed)
    expect("row f fail late", not verdict_row_f({**good, "joined_at": 61.0}).passed)
    expect("row f fail divergence", not verdict_row_f({**good, "check_ok": False}).passed)
    print(f"selftest: {'PASS' if fails == 0 else f'FAIL ({fails})'}")
    return 0 if fails == 0 else 1


def main():
    ap = argparse.ArgumentParser(description="UC v2 M14 fleet-gate driver (spec §15 rows a–g)")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--fleet", action="store_true")
    ap.add_argument("--hosts", default="", help="pub/priv,... (else terraform output); 4 needed")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--local-tree", default=str(Path(__file__).resolve().parent.parent.parent))
    ap.add_argument("--no-sync", action="store_true")
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--inflight", type=int, default=4096)
    ap.add_argument("--calib-ks", default="250,500,1000,2000,4000,8000",
                    help="SpinCountSm K ladder for the calibration arm")
    ap.add_argument("--k", type=int, default=0, help="skip calibration and use this K")
    ap.add_argument("--rows", default="abcdef", help="subset of a b c d e f (c runs with every arm)")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    if not a.fleet:
        ap.error("one of --fleet or --selftest is required")
    hosts, voters, learner = setup_fleet(a)
    rates, checks, verdicts = {}, [], []
    kill = join = None
    try:
        if any(r in a.rows for r in "abe"):
            K = arm_rates(voters, a, rates, checks)
        else:
            K = a.k
        if "d" in a.rows:
            kill = arm_kill(voters, a, K, checks)
        if "f" in a.rows:
            join = arm_join(voters, learner, a, K, checks)
    finally:
        stop_cluster_m14(hosts)
    print("\nM14 gate — FLEET (rates in ops/s over the 8 s window)")
    for k, v in rates.items():
        print(f"  {k:10s} {v:12.0f}")
    if "a" in a.rows: verdicts.append(verdict_row_a(rates))
    if "b" in a.rows: verdicts.append(verdict_row_b(rates))
    verdicts.append(verdict_row_c(checks))
    if kill is not None: verdicts.append(verdict_row_d(kill))
    if "e" in a.rows: verdicts.append(verdict_row_e(rates))
    if join is not None: verdicts.append(verdict_row_f(join))
    for v in verdicts:
        print(f"  [{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    failed = [v for v in verdicts if not v.passed]
    if failed:
        print(f"RESULT: FAIL (honest) — {len(failed)} of {len(verdicts)} rows missed: {[v.row for v in failed]}")
        sys.exit(1)
    print(f"RESULT: PASS — {len(verdicts)} rows")
    sys.exit(0)


if __name__ == "__main__":
    main()
