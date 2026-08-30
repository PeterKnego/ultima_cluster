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
  kill    pair with snapshots on BOTH FSMs AND purge on (so a restart can
          install), load submitted to FSM 0 only; SIGKILL FSM 1 on the leader
          host; restart it with the same snapshot policy
          (procedure re-specified 2026-08-29)                      row d
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
import shlex
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
    LEADER_WAIT_SECS, PIN_MAP_C6ID_2XL, EXPECTED_SIBLING_PAIRS,
    sibling_pairs, require_pin_layout,
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
# The ladder must actually STRADDLE the target. `pick_k` returns the nearest
# rung whatever the ladder holds, so a ladder that never slows the FSM below
# ~0.85 × n1 would still yield a K — and row b would then compare two
# consensus-bound arms and pass vacuously (the slow FSM was never the
# limiter). Outside this band the run FAILS at calibration, before row b.
CALIB_LO, CALIB_HI = 0.35, 0.65

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

# Journal/snapshot sizing for row f. The m6/m7-era values (16 KiB / 32 KiB)
# were written for arms whose client wrote kilobytes per second; the M13-class
# direct client on this shape writes ~100 MB/s (≈ 1.5 M ops/s × 64 B), which
# at 16 KiB segments is ~6 000 segment rolls AND ~3 000 snapshot builds every
# second — an untested churn regime that would red row f for harness reasons.
# At 16 MiB / 32 MiB the same 90 s arm still rolls the journal ~500 times and
# builds ~250 snapshots, so the learner is still far below the purge floor and
# must still converge by a snapshot session.
M14_SEGMENT_BYTES = 16 << 20
M14_SNAPSHOT_INTERVAL_BYTES = 32 << 20


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


def calib_ok(ratio):
    """True when the picked rung's ratio lands inside [CALIB_LO, CALIB_HI] —
    i.e. the ladder really produced a slow FSM. Pure, so the selftest can
    reject a ladder that never slowed down."""
    return ratio is not None and CALIB_LO <= ratio <= CALIB_HI


def fsm_reattached(pre_inc, slot, bound):
    """Row d's attach clause, as a pure predicate.

    The `attached` bit alone is NOT evidence of a reattach: only the service
    writes it (`uc2_service/src/attach.rs:159` sets it, `uc2_service/src/
    lib.rs:388-389` clears it on an orderly stop), so a SIGKILL leaves the
    KILLED incarnation's bit set and the first poll after the kill would read
    `attached=true` for the corpse. `uc2_service::attach` bumps the slot's
    incarnation exactly once per attach (same line 159,
    `incarnation.wrapping_add(1)`), and the node is NOT restarted in row d, so
    the counter survives the kill — a STRICTLY greater incarnation than the
    pre-kill reading is the new life.

    `bound is None` means the `services:` line was unreadable, so the lag
    clause cannot be judged: never satisfied (keep polling)."""
    if pre_inc is None or slot is None or bound is None:
        return False
    return bool(slot["attached"]) and slot["incarnation"] > pre_inc \
        and (bound == 0 or slot["lag"] <= bound)


def baseline_clean(t0_ms, base_hi_ms):
    """True when row d's baseline window closed BEFORE the kill instant. If
    the client unit took longer than the driver's pre-kill sleep to reach its
    first completion, the baseline window is still open when the SIGKILL
    lands, and the outage deflates the baseline — which errs toward PASS. Pure
    so the selftest can pin the comparison."""
    return t0_ms >= base_hi_ms


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
    "artifacts": {0: n, 1: n}, "installs": n, "check_ok": bool}.

    `installs` is the anti-vacuity clause. Everything else row f checks is
    satisfiable by a learner that caught up by PLAIN JOURNAL REPLAY and then
    built its own snapshots on its own interval: it would be attached, at the
    target `applied`, with `snapshot_pos > 0` and an artifact under both ids,
    having never opened a snapshot session at all. At least one
    `snapshot_installed` record on the learner is the positive evidence that
    the wire-0.6.0 two-artifact session actually ran."""
    j = join.get("joined_at")
    refusals_zero = all(tuple(v) == (0, 0) for v in join.get("refusals", {}).values()) and bool(join.get("refusals"))
    both = all(join.get("artifacts", {}).get(i, 0) > 0 for i in (0, 1))
    installs = int(join.get("installs") or 0)
    ok = j is not None and j <= BAR_F_JOIN_SECS and refusals_zero and both \
        and installs >= 1 and join.get("check_ok", False)
    gate_json("f", ok, **{k: (v if k != "refusals" else {h: list(t) for h, t in v.items()}) for k, v in join.items()},
              bar=BAR_F_JOIN_SECS)
    return Verdict("f two-FSM learner join over wire 0.6.0", ok,
                   f"joined at {j}s (bar ≤ {BAR_F_JOIN_SECS}s), refusals zero={refusals_zero}, "
                   f"both artifacts={both}, snapshot installs={installs} (need ≥ 1), "
                   f"divergence check={join.get('check_ok')}")


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


def service_cpu(pins, sid):
    """The CPU pin for FSM `sid`'s service unit, from a role -> CPU-list
    `pins` dict (e.g. `PIN_MAP_C6ID_2XL`): id 0 gets `service0`'s dedicated
    thread, id 1 gets `service1`'s. `PIN_MAP_C6ID_2XL` has no dedicated pin
    past id 1 (M14 allows up to 8 FSMs total), so id >= 2 shares
    `service1`'s thread — those extra FSMs are unpinned load on top of it,
    not isolated. `None` (unpinned) if `pins` is falsy or has no entry."""
    if not pins:
        return None
    if sid == 0:
        return pins.get("service0")
    return pins.get(f"service{sid}", pins.get("service1"))


def start_cluster_m14(voters, fsms, lag=None, purge=False, snap=0, pins=None):
    """`fsms` = [(id, spin)], e.g. [(0, 0)] or [(0, 0), (1, K)]. A FRESH
    generation: dirs wiped, nodes then services, settle after each.

    `pins` (role -> CPU-list dict, e.g. `PIN_MAP_C6ID_2XL`), when given,
    pins every node unit to its `node` entry and every service unit per
    `service_cpu`; `None` (the default) pins nothing."""
    m12.wipe_dirs(voters)
    ms = m12.members_str(voters)
    node_cpus = (pins or {}).get("node")
    for i, h in enumerate(voters):
        # Node units append to a per-unit log that is NOT wiped by
        # `systemd-run`, so without this every grep over the node log (row d's
        # attach/detach transitions, row f's snapshot installs) would also see
        # every earlier arm's records. Service units already did this.
        truncate_log(h, "node")
        start_unit(h, "node", node_args(h, i, ms, fsms, lag, purge, snap), nofile=True,
                  cpus=node_cpus)
    time.sleep(BOOT_SETTLE_SECS)
    for h in voters:
        for sid, spin in fsms:
            truncate_log(h, f"service{sid}")
            start_unit(h, f"service{sid}", service_args(h, sid, spin, snap),
                      cpus=service_cpu(pins, sid))
    time.sleep(BOOT_SETTLE_SECS)
    leader = m6.wait_leader(voters, list(range(len(voters))), LEADER_WAIT_SECS)
    if leader is None:
        raise RuntimeError("no single serving leader")
    return leader


def run_rate_arm(voters, leader, a, label, fan_in, secs=ARM_SECS, timeline=False, unit=False,
                 measure=True, pins=None):
    """The direct client on the leader host. Foreground (returns the RESULT
    dict) unless `unit`, in which case it is started as a transient unit and
    the caller reads the log later (row d/f keep it running across an action).

    `measure=False` passes `--measure-secs 0`, which switches the client's
    per-completion `done_ns` Vec off entirely. Rows d and f never read
    `window_rps` (row d judges recovery from the per-second TL buckets, row f
    from `uc2ctl status`), and at ~1 M ops/s over a 45 s / 90 s arm that Vec
    would grow to hundreds of MB by doubling INSIDE the poll thread — a
    ~200 MB memcpy that can land inside the 2 s recovery window row d is
    trying to measure. Rows a/b/e keep the window.

    `pins` (role -> CPU-list dict), when given, pins the client to its
    `client` entry — as a `-p CPUAffinity=` on the transient unit (`unit`
    path) or a `taskset -c` prefix on the foreground ssh (the other path);
    `None` (the default) pins nothing."""
    h = voters[leader]
    client_cpus = (pins or {}).get("client")
    args = ["client-direct", "--instance-dir", h.dir, "--app-id", APP,
            "--secs", str(secs), "--payload", str(a.payload), "--inflight", str(a.inflight),
            "--envelope", "on",
            "--warmup-secs", str(WARMUP_SECS if measure else 0),
            "--measure-secs", str(MEASURE_SECS if measure else 0)]
    if fan_in:
        args.append("--fan-in")
    if timeline:
        args.append("--timeline")
    if unit:
        truncate_log(h, "client")
        start_unit(h, "client", args, cpus=client_cpus)
        return None
    rc, out = run_foreground(h, args, timeout=secs + CLIENT_SLACK_SECS, cpus=client_cpus)
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


def one_arm(voters, a, label, fsms, lag, rates, checks, fan_in, pins=None):
    leader = start_cluster_m14(voters, fsms, lag=lag, pins=pins)
    print(f"INFO arm {label}: leader n{leader} on {voters[leader].public_ip}", flush=True)
    d = run_rate_arm(voters, leader, a, label, fan_in, pins=pins)
    rates[label] = rate_of(d)
    print(f"INFO arm {label}: window_rps={rates[label]:.0f} responses={d['responses']} lost={d['lost']}", flush=True)
    check_all(voters, leader, label, checks, expect=int(d["responses"]))
    stop_cluster_m14(voters)
    return d


def arm_calib(voters, a, rates, checks, pins=None):
    """FSM 0 alone as SpinCountSm over a K ladder; pick the K nearest 0.5 × n1."""
    ladder = []
    for k in [int(x) for x in a.calib_ks.split(",")]:
        d = one_arm(voters, a, f"calib-{k}", [(0, k)], None, rates, checks, fan_in=False, pins=pins)
        ladder.append((k, rate_of(d) / rates["n1"]))
        print(f"INFO calib K={k}: {ladder[-1][1]:.3f} × n1", flush=True)
    k, ratio = pick_k(ladder)
    if not calib_ok(ratio):
        gate_json("calib", False, ladder=ladder, K=k, ratio=ratio, band=[CALIB_LO, CALIB_HI])
        print(f"FAIL calib: the nearest rung is K={k} at {ratio:.3f} × n1, outside "
              f"[{CALIB_LO}, {CALIB_HI}] — the ladder never made FSM 0 the limiter, so row b "
              f"would compare two consensus-bound arms and pass vacuously. Extend the ladder "
              f"(--calib-ks) past K={max(kk for kk, _ in ladder)} and re-run.", flush=True)
        raise RuntimeError(f"calibration ratio {ratio:.3f} outside [{CALIB_LO}, {CALIB_HI}]")
    gate_json("calib", True, ladder=ladder, K=k, ratio=ratio, band=[CALIB_LO, CALIB_HI])
    return k


def arm_rates(voters, a, rates, checks, pins=None):
    one_arm(voters, a, "n1", [(0, 0)], None, rates, checks, fan_in=False, pins=pins)
    K = a.k if a.k else arm_calib(voters, a, rates, checks, pins=pins)
    print(f"INFO slow FSM K = {K}", flush=True)
    one_arm(voters, a, "n2eq", [(0, 0), (1, 0)], None, rates, checks, fan_in=True, pins=pins)
    one_arm(voters, a, "slow1", [(0, K)], None, rates, checks, fan_in=False, pins=pins)
    one_arm(voters, a, "pair", [(0, 0), (1, K)], None, rates, checks, fan_in=True, pins=pins)
    one_arm(voters, a, "n2eq-ls", [(0, 0), (1, 0)], "lockstep", rates, checks, fan_in=True, pins=pins)
    one_arm(voters, a, "pair-ls", [(0, 0), (1, K)], "lockstep", rates, checks, fan_in=True, pins=pins)
    return K


def status_slots(h):
    """`uc2ctl status` per-FSM rows (M14c) → {id: {...}}; also returns the
    node's fsm_lag bound from the `services:` line.

    `bound` is bytes, `0` for a genuine `fsm_lag=lockstep`, and **None when
    the `services:` line is absent** — a status this driver could not read.
    It is also None for a PRESENT `fsm_lag=n/a`, which `uc2ctl` prints since
    2.8.1 for a node that declares nothing (a harness page): there is no lag
    policy to report, so "not known" is the right reading. Fleet arms always
    declare FSMs, so that case should not arise here.
    Mapping "unreadable" onto 0 would silently drop the lag clause from row
    d's attach condition (0 reads as lockstep, which needs no lag check), so
    `None` is kept distinct and every consumer treats it as not-yet-known.

    `epoch` and `incarnation` are returned too: row d's attach clause needs a
    BUMPED incarnation, not just the `attached` bit (see `fsm_reattached`)."""
    r = ssh(h, f"sudo {BUILT_CTL} status --instance-dir {h.dir} --app-id {APP}", label="uc2ctl")
    out = (r.stdout or "") + (r.stderr or "")
    slots = {}
    for m in STATUS_RE.finditer(out):
        slots[int(m.group(1))] = {
            "attached": m.group(2) == "true", "epoch": int(m.group(3)),
            "incarnation": int(m.group(4)), "applied": int(m.group(5)),
            "lag": int(m.group(6)), "snapshot_pos": int(m.group(7)),
        }
    lm = re.search(r"fsm_lag=(\d+) bytes|fsm_lag=(lockstep)", out)
    if lm is None:
        bound = None
    elif lm.group(2):
        bound = 0
    else:
        bound = int(lm.group(1))
    return slots, bound


def log_lines(h, unit, pattern, lines=200):
    """Lines of a unit's log matching an extended regex, newest last.

    `obs_event!` renders one JSON line per record and writes it to STDERR
    (`uc2_node/src/obs/log.rs:227-243`, sink defaults to stderr, default level
    Info) — no subscriber is installed or needed — and every transient unit
    appends BOTH stdout and stderr to the same file
    (`m12_fleet_gate.unit_start_cmd`), so the node role's structured records
    are in its unit log next to its own printlns."""
    r = ssh(h, f"sudo grep -E {shlex.quote(pattern)} {m12.unit_log(h, unit)} 2>/dev/null | tail -n {lines}",
            label="grep")
    return [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]


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


def arm_kill(voters, a, K, checks, pins=None):
    """Row d: the bounded pair under load submitted to FSM 0; SIGKILL FSM 1's
    unit on the leader host; start it again at once. Recovery is judged twice
    — the client's own per-second timeline (M9's window rule) and `uc2ctl
    status` showing FSM 1 attached with lag ≤ bound.

    Single-clock discipline (fix round 1): the kill instant (`t0_ms`) and the
    baseline window are both taken on the HOST clock (the same clock that
    stamps every TL line's `unix_ms`), so no driver/host skew term can enter
    the recovery judgement. The driver's own `time.time()` (`t0`) is kept
    only for `attached_at` (a driver-clock delta at both ends of the
    `uc2ctl status` poll, so it stays internally consistent) and the poll
    deadline."""
    # ---------------------------------------------------------------------
    # PROCEDURE RE-SPECIFIED 2026-08-29, after run 1 of the M14 fleet gate
    # (docs/benchmarks/uc2-m14-gate-2026-08-29.md, "Re-specification —
    # applied 2026-08-29 (run 2)"). THE BAR IS UNCHANGED (BAR_D_* above, M9's
    # rule plus the attach clause); run 1's FAIL and its numbers stay in the
    # record. Two things changed, both about what the row can measure:
    #
    #  (1) BOTH FSMs run with `snap=M14_SNAPSHOT_INTERVAL_BYTES` (32 MiB) —
    #      the cluster below and the restarted FSM 1 further down — AND the
    #      cluster runs with PURGE ON (`purge=True`, which also gives
    #      `node_args` the 16 MiB `--journal-segment-bytes`), the same shape
    #      row f uses. Both halves are required, and the second is the one
    #      that actually does the work: reconstruction installs the newest
    #      artifact only inside the gap guard
    #      `if first > start_pos` (uc2_service/src/replay.rs:73-78), where
    #      `first` is the base of the OLDEST RETAINED journal segment
    #      (`reader.first_meta()`, 0 while nothing has been purged) and a
    #      fresh process's `start_pos` is 0
    #      (uc2_service/src/attach.rs:153, `last_applied().unwrap_or(0)`).
    #      With purge off, `first` stays 0 for the whole arm, `0 > 0` is
    #      false, and the FSM falls through to `scan_from(0)` — the full
    #      replay run 1 diagnosed — NO MATTER what snapshot interval it was
    #      given. Only `PurgePolicy::BelowSnapshot` dispatches
    #      `ArchiveCmd::Purge` and lifts `first` above 0
    #      (uc2_node/src/node.rs:3261-3270). Run 1 gave the FSMs neither, so
    #      the restart replayed the WHOLE journal (~11.9 M commands, ~1.3 GB)
    #      and `attached_at` was a replay-completion clock (21.6 s). A
    #      deployed service installs its newest artifact and tail-replays one
    #      interval; that is what M9's 15 s budget was itemised against.
    #  (2) The measuring client submits to FSM 0 ONLY (`fan_in=False`). Under
    #      fan-in a submit completes only when EVERY declared FSM answers, and
    #      journal replay is publish-silent (uc2_service/src/replay.rs:44-46),
    #      so in run 1 all 4 096 in-flight requests could retire only on the
    #      client's 30 s `request_timeout` — the rate read 0 for the rest of
    #      the arm regardless of how fast FSM 1 recovered, and `lost` came out
    #      at exactly `--inflight`. Submitting to FSM 0 alone removes that
    #      artifact and still measures the recovery the row wants: FSM 0 is the
    #      default responder, and its apply is held back by the bounded lag
    #      barrier (64 MiB) once FSM 1 is dead, so the client's completion rate
    #      falls at the kill and rises again exactly when FSM 1 has caught up
    #      enough to release the barrier.
    #
    # Row c's checks after this arm still verify BOTH FSMs (`expect_min =
    # responses`), unchanged: FSM 1 must still agree with FSM 0 and with every
    # remote host in both read modes.
    # ---------------------------------------------------------------------
    leader = start_cluster_m14(voters, [(0, 0), (1, K)], purge=True, snap=M14_SNAPSHOT_INTERVAL_BYTES,
                               pins=pins)
    h = voters[leader]
    run_rate_arm(voters, leader, a, "kill", fan_in=False, secs=KILL_ARM_SECS, timeline=True, unit=True,
                 measure=False, pins=pins)
    t_start = time.time()
    time.sleep(12.0)                       # 2 s ramp + [2,10) s baseline + slack
    t0 = time.time()                       # driver clock: attached_at + poll deadline only
    # The pre-kill reading of FSM 1's slot, taken BEFORE the SIGKILL: its
    # incarnation is what the reattach must exceed (`fsm_reattached`).
    pre_slots, bound = status_slots(h)
    pre = pre_slots.get(1)
    if pre is None or bound is None:
        print("WARN row d: FSM 1's status row (or the services: line) is unreadable before the "
              "kill — the attach clause cannot be adjudicated and will fail closed", flush=True)
    pre_inc = pre["incarnation"] if pre else None
    r = ssh(h, f"date +%s%3N; sudo systemctl kill --signal=SIGKILL {UNIT_PREFIX}-service1", label="SIGKILL")
    t0_ms = int((r.stdout or "").strip().splitlines()[0])   # host clock: the SIGKILL instant
    start_unit(h, "service1", service_args(h, 1, K, M14_SNAPSHOT_INTERVAL_BYTES),
              cpus=service_cpu(pins, 1))
    attached_at = None
    deadline = t0 + 30.0
    while time.time() < deadline:
        slots, _ = status_slots(h)
        if fsm_reattached(pre_inc, slots.get(1), bound):
            attached_at = round(time.time() - t0, 2)
            break
        time.sleep(0.25)
    # Wait for the client UNIT to exit (the sibling drivers' full ssh+attach+drain
    # budget) rather than sleeping a fixed margin, so the log read never races the
    # client's last write and silently skips the timeline trim.
    m12.wait_units_done([(h, ["client"])], t_start + KILL_ARM_SECS + CLIENT_SLACK_SECS)
    out = tail_log(h, "client", lines=2000) or ""
    kill_unit(h, "client")
    d = parse_result(out, "direct")
    tl = parse_timeline(out)
    if d is None:
        print("WARN row d: client RESULT missing — log read raced or the client died; "
              "timeline NOT trimmed", flush=True)
    if not tl:
        print("WARN row d: empty timeline — no baseline, no recovery window", flush=True)
        baseline, recovered, windows = 0.0, None, []
    else:
        # The client's own first TL line is its own t0 (unix_ms = t0_unix_ms +
        # sec*1000) — host clock throughout, matching t0_ms above.
        t_start_ms = tl[0][0]
        if d is not None:
            # Amendment 1: bound the timeline by the client's own run so the ~40
            # trailing zero-response buckets --timeline keeps emitting past
            # RESULT don't read as an outage.
            tl = bound_timeline(tl, t_start_ms + int(d["elapsed_secs"] * 1000) + 1000)
        base_lo, base_hi = t_start_ms + 2000, t_start_ms + 10000
        baseline, recovered, windows = recovery_time(tl, t0_ms, base_lo, base_hi)
        if not baseline_clean(t0_ms, base_hi):
            print(f"WARN row d: baseline window overlapped the kill "
                  f"(t0 - base_hi = {t0_ms - base_hi}ms) — recovery NOT adjudicated", flush=True)
            recovered = None
    print("INFO recovery timeline (ops/s per 2 s window, end-relative to t0): " +
          ", ".join(f"{(e - t0_ms) / 1000:.1f}s:{r:.0f}" for e, r in windows[:25]), flush=True)
    print(f"INFO row d: baseline {baseline:.0f}/s, rate recovered at {recovered}s, "
          f"FSM 1 attached+lag≤{bound} at {attached_at}s; client lost={d['lost'] if d else '?'}", flush=True)
    # Spec §15.5: the transitions the LEADER's node actually observed. Attach
    # dominates and the pair is not symmetric — a restart inside the ~3 s
    # heartbeat bar shows `service_attached` twice with no `service_detached`
    # between (uc2_node/src/node.rs:2854-2888). Recorded, never adjudicated.
    transitions = log_lines(h, "node", "service_(de|at)tached")
    print(f"INFO row d: leader service transitions ({len(transitions)}):", flush=True)
    for ln in transitions:
        print(f"  {ln}", flush=True)
    check_all(voters, leader, "kill", checks, expect_min=int(d["responses"]) if d else None)
    stop_cluster_m14(voters)
    return {"baseline": baseline, "recovered_at": recovered, "attached_at": attached_at,
            "bound": bound, "pre_incarnation": pre_inc, "transitions": transitions,
            "client_lost": d["lost"] if d else None}


def arm_join(voters, learner, a, K, checks, pins=None):
    """Row f: voters run the bounded pair with purge ON and snapshots every
    `M14_SNAPSHOT_INTERVAL_BYTES`; fan-in load runs for the whole arm; 10 s in, a learner declared
    {0,1} is admitted (`uc2ctl add-learner` on the leader — M7's pattern:
    the learner boots as a plain node with the CURRENT voters as its seed
    members) and must reach both voters' `applied` within 60 s via a
    two-artifact snapshot session (wire 0.6.0), with zero refusals."""
    leader = start_cluster_m14(voters, [(0, 0), (1, K)], purge=True, snap=M14_SNAPSHOT_INTERVAL_BYTES,
                               pins=pins)
    h = voters[leader]
    run_rate_arm(voters, leader, a, "join", fan_in=True, secs=JOIN_ARM_SECS, timeline=False, unit=True,
                 measure=False, pins=pins)
    t_client_start = time.time()
    time.sleep(JOIN_AT_SECS)
    new_id, addr = 3, f"{learner.private_ip}:{PORT}"
    m12.wipe_dirs([learner])
    rc, out = h.ctl("add-learner", new_id, addr)
    if rc != 0:
        raise RuntimeError(f"add-learner refused: {out.strip()}")
    # Capture the target at add-learner time (fix round 1): under continuous
    # fan-in load, `applied` keeps advancing, so reading it after the
    # learner's node+service units boot (several ssh round trips) would
    # inflate the target past "join start" and, with it, the 60 s bar for a
    # reason unrelated to join speed. Spec §15.4 row f is explicit: the
    # learner must reach both voters' `applied` AT ADD-LEARNER TIME.
    target = {i: s["applied"] for i, s in status_slots(h)[0].items()}
    print(f"INFO row f: leader applied at join start {target}", flush=True)
    # t0 is a driver-clock delta at both ends (here and the status poll below),
    # so no host clock is needed for joined_at (amendment 2).
    t0 = time.time()
    truncate_log(learner, "node")          # the snapshot-install grep must be arm-scoped
    start_unit(learner, "node", node_args(learner, new_id, m12.members_str(voters), [(0, 0), (1, K)], None,
                                          True, M14_SNAPSHOT_INTERVAL_BYTES), nofile=True,
              cpus=(pins or {}).get("node"))
    time.sleep(2.0)
    for sid, spin in [(0, 0), (1, K)]:
        truncate_log(learner, f"service{sid}")
        start_unit(learner, f"service{sid}", service_args(learner, sid, spin, M14_SNAPSHOT_INTERVAL_BYTES),
                  cpus=service_cpu(pins, sid))
    joined_at = None
    while time.time() < t0 + BAR_F_JOIN_SECS + 5:
        slots, _ = status_slots(learner)
        if all(i in slots and slots[i]["attached"] and slots[i]["applied"] >= target.get(i, 0) for i in (0, 1)) \
                and all(slots[i]["snapshot_pos"] > 0 for i in (0, 1)):
            joined_at = round(time.time() - t0, 2)
            break
        time.sleep(0.5)
    # Wait for the client UNIT to exit rather than sleeping a fixed margin, so
    # the log read never races the client's last write (amendment 1).
    m12.wait_units_done([(h, ["client"])], t_client_start + JOIN_ARM_SECS + CLIENT_SLACK_SECS)
    out = tail_log(h, "client", lines=200) or ""
    d = parse_result(out, "direct")
    if d is None:
        print("WARN row f: client RESULT missing — log read raced or the client died", flush=True)
    kill_unit(h, "client")
    # Spec §15.5 wants the per-id artifact LENGTHS, not just a count: one
    # `find` prints a size per complete artifact, the count is its length.
    artifacts, artifact_bytes = {}, {}
    for i in (0, 1):
        r = ssh(learner, f"sudo find {learner.dir}/snapshots/{i} -type f ! -name '*.part' "
                         f"-printf '%s\\n' 2>/dev/null", label="ls")
        sizes = [int(x) for x in (r.stdout or "").split() if x.isdigit()]
        artifacts[i], artifact_bytes[i] = len(sizes), sizes
    # Anti-vacuity for row f: positive evidence that a snapshot SESSION ran on
    # the learner, rather than a plain journal catch-up plus its own snapshot
    # builds (which would satisfy attached + applied + snapshot_pos > 0 on
    # their own). `snapshot_installed` is emitted at Info by
    # `uc2_node/src/node.rs:3168`.
    installs = len(log_lines(learner, "node", '"event":"snapshot_installed"'))
    refusals = {}
    for hh in voters + [learner]:
        st = node_stats(hh)
        refusals[hh.public_ip] = (st[1], st[2]) if st else (-1, -1)
    hosts_all = voters + [learner]
    before = len(checks)
    check_all(hosts_all, leader, "join", checks, expect_min=int(d["responses"]) if d else None)
    check_ok = all(c[3] for c in checks[before:]) and len({c[4] for c in checks[before:]}) == 1
    print(f"INFO row f: joined_at={joined_at}s artifacts={artifacts} bytes={artifact_bytes} "
          f"snapshot_installs={installs} refusals={refusals} check_ok={check_ok}", flush=True)
    stop_cluster_m14(hosts_all)
    return {"joined_at": joined_at, "refusals": refusals, "artifacts": artifacts,
            "artifact_bytes": artifact_bytes, "installs": installs, "check_ok": check_ok,
            "client_lost": d["lost"] if d else None}


# ---------------------------------------------------------------- selftest
def selftest():
    fails = 0

    def expect(name, cond):
        nonlocal fails
        print(f"  [{'ok' if cond else 'FAIL'}] {name}")
        fails += 0 if cond else 1

    expect("pick_k nearest 0.5", pick_k([(500, 0.9), (2000, 0.52), (8000, 0.2)])[0] == 2000)
    expect("calib band accepts a straddling ladder",
           calib_ok(pick_k([(500, 0.9), (2000, 0.52), (8000, 0.2)])[1]))
    expect("calib band rejects a ladder that never slowed the FSM down",
           not calib_ok(pick_k([(250, 0.98), (8000, 0.85)])[1]))
    expect("calib band rejects an over-slow ladder", not calib_ok(pick_k([(8000, 0.10)])[1]))
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
    # The killed FSM's stale `attached` bit must not satisfy the attach clause.
    live = {"attached": True, "incarnation": 4, "lag": 100}
    expect("row d attach: stale incarnation is the corpse, not a reattach",
           not fsm_reattached(4, live, 4096))
    expect("row d attach: bumped incarnation + lag inside the bound",
           fsm_reattached(3, live, 4096))
    expect("row d attach: bumped incarnation but lag over the bound",
           not fsm_reattached(3, live, 10))
    expect("row d attach: lockstep (bound 0) skips the lag clause",
           fsm_reattached(3, live, 0))
    expect("row d attach: unreadable status (bound None) never satisfies",
           not fsm_reattached(3, live, None))
    expect("row d attach: detached never satisfies",
           not fsm_reattached(3, {**live, "attached": False}, 4096))
    expect("row d baseline window closed before the kill", baseline_clean(12000, 10000))
    expect("row d baseline window still open at the kill", not baseline_clean(9000, 10000))
    expect("row e always passes", verdict_row_e({"n2eq": 100.0, "n2eq-ls": 40.0}).passed)
    tl_in = [(ms, 100) for ms in range(0, 5000, 1000)] + [(ms, 0) for ms in range(5000, 45000, 1000)]
    expect("bound_timeline drops trailing buckets past end_ms, keeps earlier",
           bound_timeline(tl_in, 5000) == [(ms, 100) for ms in range(0, 5000, 1000)])
    good = {"joined_at": 30.0, "refusals": {"h0": (0, 0), "h1": (0, 0), "h2": (0, 0), "h3": (0, 0)},
            "artifacts": {0: 1, 1: 1}, "artifact_bytes": {0: [4096], 1: [4096]},
            "installs": 2, "check_ok": True}
    expect("row f pass", verdict_row_f(good).passed)
    expect("row f fail on a refusal", not verdict_row_f({**good, "refusals": {**good["refusals"], "h3": (1, 0)}}).passed)
    expect("row f fail on one artifact", not verdict_row_f({**good, "artifacts": {0: 1, 1: 0}}).passed)
    expect("row f fail late", not verdict_row_f({**good, "joined_at": 61.0}).passed)
    expect("row f fail divergence", not verdict_row_f({**good, "check_ok": False}).passed)
    expect("row f fail on no snapshot install (journal-replay join)",
           not verdict_row_f({**good, "installs": 0}).passed)
    # --pin (Task 9 step 1): unit_start_cmd's CPUAffinity threading, and the
    # pure sibling-pairs parser verify_pin_layout is built on. No fleet, no
    # ssh — a fake host object is enough since unit_start_cmd never touches
    # the network.
    class _FakeHost:
        public_ip = "10.0.0.1"
        private_ip = "10.0.0.1"
        gate = "/opt/bench/uc/target/release/examples/m6_gate"

    _fh = _FakeHost()
    _cmd_unpinned = m12.unit_start_cmd(_fh, "node", ["node"], cpus=None)
    _cmd_pinned = m12.unit_start_cmd(_fh, "node", ["node"], cpus="0,1,4,5")
    expect("unit_start_cmd cpus=None has no CPUAffinity", "CPUAffinity" not in _cmd_unpinned)
    expect("unit_start_cmd cpus set adds -p CPUAffinity=<list> ",
           "-p CPUAffinity=0,1,4,5 " in _cmd_pinned)
    expect("unit_start_cmd cpus=None is byte-identical to the pre-cpus call shape",
           _cmd_unpinned == m12.unit_start_cmd(_fh, "node", ["node"]))
    expect("service_cpu id 0 -> service0's dedicated pin",
           service_cpu(PIN_MAP_C6ID_2XL, 0) == PIN_MAP_C6ID_2XL["service0"])
    expect("service_cpu id 1 -> service1's dedicated pin",
           service_cpu(PIN_MAP_C6ID_2XL, 1) == PIN_MAP_C6ID_2XL["service1"])
    expect("service_cpu id >= 2 shares service1's pin (no dedicated pin past id 1)",
           service_cpu(PIN_MAP_C6ID_2XL, 2) == PIN_MAP_C6ID_2XL["service1"])
    expect("service_cpu with no pins is unpinned", service_cpu(None, 0) is None)
    # sibling_pairs: canned `lscpu -p=CPU,CORE` text, both the assumed
    # layout (siblings i,i+4) and a WRONG layout (siblings i,i+1) that must
    # be rejected by verify_pin_layout's comparison.
    LSCPU_EXPECTED = (
        "# The following is the parsable format, which can be fed to other\n"
        "# programs. Each different item in every column has an unique ID\n"
        "# starting usually from zero.\n"
        "# CPU,CORE\n"
        "0,0\n1,1\n2,2\n3,3\n4,0\n5,1\n6,2\n7,3\n"
    )
    LSCPU_WRONG = (
        "# CPU,CORE\n"
        "0,0\n1,0\n2,1\n3,1\n4,2\n5,2\n6,3\n7,3\n"
    )
    expect("sibling_pairs on the assumed c6id.2xlarge layout matches EXPECTED_SIBLING_PAIRS",
           sibling_pairs(LSCPU_EXPECTED) == EXPECTED_SIBLING_PAIRS)
    expect("sibling_pairs on a (i, i+1) layout is rejected (not EXPECTED_SIBLING_PAIRS)",
           sibling_pairs(LSCPU_WRONG) != EXPECTED_SIBLING_PAIRS)
    expect("sibling_pairs on a (i, i+1) layout is exactly {(0,1),(2,3),(4,5),(6,7)}",
           sibling_pairs(LSCPU_WRONG) == {(0, 1), (2, 3), (4, 5), (6, 7)})
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
    ap.add_argument("--pin", action="store_true",
                    help="pin every node/service/client unit to PIN_MAP_C6ID_2XL's CPUs "
                         "(default off); verifies the assumed hyperthread-sibling layout "
                         "on every host the run starts units on (voters + the row-f "
                         "learner) first and refuses to run (SystemExit) if it doesn't "
                         "hold — see m12_fleet_gate.verify_pin_layout")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    if not a.fleet:
        ap.error("one of --fleet or --selftest is required")
    hosts, voters, learner = setup_fleet(a)
    if a.pin:
        # `hosts` = voters + the learner (`setup_fleet` returns all 4); row f
        # starts node/service units on the learner too (arm_join), so the
        # sibling layout must be verified there as well, not just on the
        # voters — a wrong-layout learner would otherwise pin onto siblings
        # silently the one time this run touches a 4th host.
        require_pin_layout(hosts)
    pins = PIN_MAP_C6ID_2XL if a.pin else None
    rates, checks, verdicts = {}, [], []
    kill = join = None
    try:
        if any(r in a.rows for r in "abe"):
            K = arm_rates(voters, a, rates, checks, pins=pins)
        else:
            K = a.k
        if "d" in a.rows:
            kill = arm_kill(voters, a, K, checks, pins=pins)
        if "f" in a.rows:
            join = arm_join(voters, learner, a, K, checks, pins=pins)
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
