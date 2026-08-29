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
    ap.add_argument("--selftest", action="store_true", help="replay canned rows through the verdicts; no fleet")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    ap.error("--fleet is added in the next task; only --selftest exists yet")


if __name__ == "__main__":
    main()
