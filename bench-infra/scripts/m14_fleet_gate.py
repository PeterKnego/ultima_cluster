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

TIME AND TIMERS (`--tt-rows`, 2026-09-07)
-----------------------------------------
This driver also carries the fleet rows of
`docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md`. They are a SEPARATE
namespace from the M14 rows above, on a separate flag: `--rows` keeps its
`abcdef` meaning and `--tt-rows` takes the letters `a b c e g h`.

  tt-a  the rate arms (n1/n2eq/slow1/pair) with every service wrapped in
        `Timed<..>` and NO timers scheduled, A/B'd against the SAME arms on
        the pre-time-and-timers binary (`--base-tree`), interleaved per arm on
        fresh clusters. Bar: within `--resolution-pct` (the day's
        `scripts/hop1_ab.sh` same-source rebuild number).
  tt-b  the same A/B with FSM 0 sustaining `--timers-per-sec` (the gate's
        number is 1000), plus `uc2_timers_late_total == 0` on every node for
        every row after every timers-on arm.
  tt-c  timer precision under row b's load: p99 of `uc2_timer_lateness_ns`
        (row 0) against 2 x the mean of `uc2_consensus_pass_ns`, both scraped
        off the LEADER. Needs >= 10 000 fires or the row is inconclusive.
  tt-e  the same arms again with a live `--schedule-table N` (the gate's
        number is 32) applied by `uc2ctl schedule apply` BEFORE each arm's
        client starts; bar is row a's resolution plus the same late == 0.
  tt-g  row f's below-floor join with the LEADER's node unit restarted ~2 s
        after `add-learner`; bar <= 60 s to converge, `snapshot_installed`
        seen, `uc2_snapshot_set_position` equal cluster-wide.
  tt-h  freeze duration vs commit stall: a 256 MiB-state FSM under load, an
        all-nodes instant then a `--standby` one. The standby arm's commit
        gap must be <= the pass length; the all-nodes gap is reported bare.

The leaf helpers those rows stand on (the Prometheus parser, the histogram
quantile/mean, the stall measure, the table file, the A/B arithmetic) are in
`tt_fleet_gate.py`, stdlib-only so they are selftestable in isolation.
"""

import argparse
import base64
import contextlib
import json
import re
import shlex
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
import tt_fleet_gate as tt  # noqa: E402
from m12_fleet_gate import (  # noqa: E402
    ssh, start_unit, kill_unit, truncate_log, tail_log, run_foreground,
    parse_result, echo, Verdict, APP, PORT, REMOTE_ROOT, UNIT_PREFIX,
    BUILT_GATE, BUILT_PROBE, BOOT_SETTLE_SECS, CLIENT_SLACK_SECS,
    LEADER_WAIT_SECS, PIN_MAP_C6ID_2XL, EXPECTED_SIBLING_PAIRS,
    sibling_pairs, require_pin_layout,
)
from m13_hop_bench import sync_tree  # noqa: E402
from m14_ab_27_vs_28 import sync_tree_to  # noqa: E402

BUILT_CTL = "/opt/bench/uc/target/release/uc2ctl"

# The time-and-timers A/B's SECOND tree: the pre-time-and-timers checkout
# (`17d5c6b`), rsynced and built beside the head one exactly as
# `m14_ab_27_vs_28.py` does. Each version is probed by ITS OWN `m6_gate`,
# because the two trees' cnc page layouts need not agree.
BASE_SRC = "/opt/bench/uc-base"
BASE_GATE = f"{BASE_SRC}/target/release/examples/m12_gate"
BASE_PROBE = f"{BASE_SRC}/target/release/examples/m6_gate"

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
# `uc2ctl status` (FSM identity, cnc 3.1): one row per declared FSM, field-keyed
# so a reordered/extended line doesn't silently misalign the tuple. Example line
# (uc_ctl/src/main.rs's `println!` in the status command):
#   row=0 name=count version=1.0.0 hash=0x0123456789abcdef attached=true epoch=5 \
#     incarnation=2 applied=1000 lag=50 snapshot_pos=900 heartbeat_age=0.123s
STATUS_RE = re.compile(
    r"row=(\d+) name=(\S*) version=(\S+) hash=0x([0-9a-f]+) attached=(true|false) "
    r"epoch=(\d+) incarnation=(\d+) applied=(\d+) lag=(\d+) snapshot_pos=(\d+) "
    r"heartbeat_age=(\S+)")
TL_RE = re.compile(r'^TL\s+(\{.*\})\s*$', re.M)
FSMS_OK_RE = re.compile(r'^FSMS-OK\s+(\{.*\})\s*$', re.M)
# `m12_gate.rs`'s node role prints three snapshot-refusal counters since FSM
# identity: legacy (peer wire <= 0.6.0), identity (positional name mismatch),
# version (both sides versioned, differ) — e.g.
#   m12_gate node 0 stats: reports_unattested=0 snap_refusals=(0,0,0)
STATS_RE = re.compile(r"reports_unattested=(\d+) snap_refusals=\((\d+),(\d+),(\d+)\)")

# Journal/snapshot sizing for row f. The m6/m7-era values (16 KiB / 32 KiB)
# were written for arms whose client wrote kilobytes per second; the M13-class
# direct client on this shape writes ~100 MB/s (≈ 1.5 M ops/s × 64 B), which
# at 16 KiB segments is ~6 000 segment rolls AND ~3 000 snapshot builds every
# second — an untested churn regime that would red row f for harness reasons.
# At 16 MiB / 32 MiB the same 90 s arm still rolls the journal ~500 times and
# builds ~250 snapshots, so the learner is still far below the purge floor and
# must still converge by a snapshot session.
M14_SEGMENT_BYTES = 16 << 20
# Passed to BOTH roles of `m12_gate`: the `node` role seeds it into the
# cluster's `[settings] snapshot.interval_bytes` at genesis (the replicated
# cadence, coordinated-snapshot spec §5.5/§6) and the `service` role reads any
# positive value as "start snapshot-capable". Rows d and f of
# docs/benchmarks/uc2-m14-gate-2026-08-29.md were measured at this cadence.
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
    writes it (`uc_service/src/attach.rs:159` sets it, `uc_service/src/
    lib.rs:388-389` clears it on an orderly stop), so a SIGKILL leaves the
    KILLED incarnation's bit set and the first poll after the kill would read
    `attached=true` for the corpse. `uc_service::attach` bumps the slot's
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
    """`join` = {"joined_at": s|None, "refusals": {host: (legacy, identity, version)},
    "artifacts": {0: n, 1: n}, "installs": n, "check_ok": bool}.

    `installs` is the anti-vacuity clause. Everything else row f checks is
    satisfiable by a learner that caught up by PLAIN JOURNAL REPLAY and then
    built its own snapshots on its own interval: it would be attached, at the
    target `applied`, with `snapshot_pos > 0` and an artifact under both ids,
    having never opened a snapshot session at all. At least one
    `snapshot_installed` record on the learner is the positive evidence that
    the wire-0.6.0 two-artifact session actually ran."""
    j = join.get("joined_at")
    refusals_zero = all(tuple(v) == (0, 0, 0) for v in join.get("refusals", {}).values()) and bool(join.get("refusals"))
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
           f"-p uc_ctl && test -x {BUILT_CTL} && echo CTL-OK")
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


def fsm_name(i, spin):
    """FSM identity (Tasks 4/5): the `--fsm`/`--services` NAME row `i` (0-based,
    == the `fsms` list position — every caller in this file builds `fsms` in
    row order, so `sid` and `i` always agree) presents, given whether it runs
    the deliberately slow variant. `m12_gate`'s service role maps `count` ->
    `CountSm` (row 0's default), `spin` -> `SpinCountSm` (paced by
    `--work-spin`), and `fsm<N>` -> `Tagged<N, CountSm>` for any other row
    (`uc_gateway/examples/m12_gate.rs`'s `ServiceArgs::fsm` doc)."""
    if spin:
        return "spin"
    return "count" if i == 0 else f"fsm{i}"


class TtOpts:
    """The time-and-timers role flags that must reach `node_args`/
    `service_args`, which every arm builds without seeing the parsed argparse
    namespace.

    A module-level singleton rather than four more parameters threaded through
    `one_arm`/`arm_kill`/`arm_join`, because those signatures are pinned by the
    existing selftest and by three other rows that have nothing to do with
    this flag day. `main()` sets it once; individual arms narrow it with the
    `tt_options`/`tt_disabled` context managers.

    Every field defaults OFF, so an untouched import builds byte-identical
    role arguments to the pre-2026-09-07 driver — which is what lets the
    BASELINE tree's arms (whose `m12_gate` has none of these flags) run
    through the very same `node_args`/`service_args`.
    """

    def __init__(self, metrics_port=0, timed=False, timers_per_sec=0, state_bytes=0):
        self.metrics_port = int(metrics_port or 0)
        self.timed = bool(timed)
        self.timers_per_sec = int(timers_per_sec or 0)
        self.state_bytes = int(state_bytes or 0)

    def replace(self, **over):
        d = {"metrics_port": self.metrics_port, "timed": self.timed,
             "timers_per_sec": self.timers_per_sec, "state_bytes": self.state_bytes}
        d.update(over)
        return TtOpts(**d)

    def __repr__(self):
        return (f"TtOpts(metrics_port={self.metrics_port}, timed={self.timed}, "
                f"timers_per_sec={self.timers_per_sec}, state_bytes={self.state_bytes})")


TT = TtOpts()


@contextlib.contextmanager
def tt_options(**over):
    """Narrow `TT` for the duration of one arm (e.g. timers on for row b's
    head arms only), then restore it. Nested use is fine — each level restores
    the value it found."""
    global TT
    saved = TT
    TT = saved.replace(**over)
    try:
        yield TT
    finally:
        TT = saved


def tt_disabled():
    """Every T&T role flag off — what the BASELINE tree's arms run under, since
    `--metrics-listen`/`--timed`/`--timers-per-sec`/`--state-bytes` are all
    flags this flag day added and its `m12_gate` would refuse by name."""
    return tt_options(metrics_port=0, timed=False, timers_per_sec=0, state_bytes=0)


# Node unit args as last started, per host — row g restarts the LEADER's node
# unit mid-window and must start it with the SAME arguments (a restart that
# quietly changed the member list or the purge policy would be measuring a
# different cluster).
LAST_NODE_ARGS = {}


def node_args(h, node_id, members, fsms, lag, purge, snap):
    args = ["node", "--id", str(node_id), "--bind", f"{h.private_ip}:{PORT}",
            "--instance-dir", h.dir, "--members", members, "--app-id", APP,
            "--admission-kib", str(ADMISSION_KIB),
            "--services", ",".join(fsm_name(sid, spin) for sid, spin in fsms)]
    # Every node this driver starts serves Prometheus text on its PRIVATE NIC
    # when a port is configured (`--metrics-port`, default 9310): the T&T rows
    # read all of their evidence off `/metrics`, and there is no second way to
    # get `uc2_timers_late_total` or the two new histograms out of a node.
    # `0` = do not pass the flag at all, which is what the baseline tree's
    # arms run under (`tt_disabled`).
    if TT.metrics_port:
        args += ["--metrics-listen", f"{h.private_ip}:{TT.metrics_port}"]
    if lag is not None:
        args += ["--fsm-lag", lag]
    if purge:
        args += ["--purge-below-snapshot", "--journal-segment-bytes", str(M14_SEGMENT_BYTES)]
        # The CADENCE this gate measures, stated here and nowhere else.
        # Since 2.11.0 the cadence is a replicated setting seeded into
        # `[settings] snapshot.interval_bytes` at genesis (coordinated-
        # snapshot spec §5.5/§6), not the per-service byte policy spec §5.2
        # deleted — so it has to reach the NODE role, not just the service's.
        # `snap` was already a parameter here and had never been used; that
        # gap briefly let `m12_gate`'s own hardcoded 32 KiB (m6/m9's smoke
        # number) stand in for M14_SNAPSHOT_INTERVAL_BYTES, ~1024x more
        # often, which under this gate's load degenerates into continuous
        # freeze-and-abandon on every row. `m12_gate node` now REFUSES
        # `--purge-below-snapshot` without an explicit number.
        assert snap, "purge rows must state a snapshot cadence (M14_SNAPSHOT_INTERVAL_BYTES)"
        args += ["--snapshot-interval-bytes", str(snap)]
    return args


def service_args(h, sid, spin, snap):
    name = fsm_name(sid, spin)
    args = ["service", "--instance-dir", h.dir, "--app-id", APP, "--envelope", "on",
            "--fsm", name]
    if spin > 0:
        args += ["--work-spin", str(spin)]
    if snap:
        args += ["--snapshot-interval-bytes", str(snap)]
    # Time and timers. `--timed` wraps the state machine in `uc_service::
    # Timed<S>` (exactly-once delivery from the log-derived pending set) and is
    # rows a/b/c/e's standing condition — the gate measures the shipped
    # wrapper, not a bare SM.
    if TT.timed:
        args.append("--timed")
    # The timer LOAD and the 256 MiB ballast both attach to FSM 0's `count`
    # row only, per the Rust contract: `--timers-per-sec` is valid only with
    # `--fsm count` and requires `--timed`. Row 0 is `spin` in the `slow1`
    # arm, so that arm deliberately carries no timer load — the late == 0
    # sweep still runs over it, and the arms that DO carry timers are the
    # three the bar reads.
    if name == "count":
        if TT.timed and TT.timers_per_sec > 0:
            args += ["--timers-per-sec", str(TT.timers_per_sec)]
        if TT.state_bytes > 0:
            args += ["--state-bytes", str(TT.state_bytes)]
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
        args = node_args(h, i, ms, fsms, lag, purge, snap)
        LAST_NODE_ARGS[h.public_ip] = list(args)
        start_unit(h, "node", args, nofile=True, cpus=node_cpus)
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


# -------------------------------------------------- /metrics (time+timers)
def scrape_prom(h, label="metrics"):
    """This host's `/metrics`, parsed by `tt.parse_prom`.

    `{}` when no metrics port is configured (`--metrics-port 0`, and every
    baseline arm) or the endpoint does not answer — with a WARN, because every
    clause that reads a metric treats an unreadable one as NOT satisfied
    rather than as a pass."""
    if not TT.metrics_port:
        return {}
    r = ssh(h, f"curl -s --max-time {tt.SCRAPE_TIMEOUT_SECS} "
               f"http://{h.private_ip}:{TT.metrics_port}/metrics", label=label)
    m = tt.parse_prom(r.stdout or "")
    if not m:
        print(f"WARN scrape {h.public_ip}: /metrics empty or unreachable "
              f"(port {TT.metrics_port})", flush=True)
    return m


def late_sweep(hosts, arm, late):
    """Rows b and e's second clause: `uc2_timers_late_total == 0` on EVERY node
    for EVERY row. Appends `(arm, host, service, row, late)` per sample.

    A host whose scrape carries no `uc2_timers_late_total` at all is recorded
    as an offender with count `-1`: the clause was not readable, and an
    unreadable clause is not a pass (the same posture `status_slots`' `bound
    is None` takes in row d)."""
    for h in hosts:
        m = scrape_prom(h, label="late")
        hits = tt.prom_find(m, "uc2_timers_late_total")
        if not hits:
            print(f"WARN {arm}: no uc2_timers_late_total on {h.public_ip} — recorded "
                  f"as an offender (an unreadable clause is not a pass)", flush=True)
            late.append((arm, h.public_ip, "?", "?", -1))
            continue
        fired = {d.get("row"): v for d, v in tt.prom_find(m, "uc2_timers_fired_total")}
        for d, v in hits:
            row = d.get("row", "?")
            late.append((arm, h.public_ip, d.get("service", "?"), row, int(v)))
            print(f"INFO {arm} timers {h.public_ip} row={row} service={d.get('service')}: "
                  f"fired={fired.get(row)} late={int(v)}", flush=True)


def fold_freeze(ip, metrics, acc):
    """Fold one host's per-row `uc2_snapshot_freeze_seconds_max` samples into
    `acc` (host -> the largest value seen). Pure over a parsed scrape, so the
    selftest can pin it.

    Sampled DURING the instant poll, never once at the end: the gauge is reset
    to 0 on the scrape after the node's instant position advances
    (`uc_node/src/obs/metrics.rs`), so a single read taken after the instant
    completed can legitimately read 0 and would report "no freeze" for a
    freeze that did happen."""
    for _, v in tt.prom_find(metrics, "uc2_snapshot_freeze_seconds_max"):
        acc[ip] = max(acc.get(ip, 0.0), float(v))
    return acc


# ------------------------------------------------------------- rate arms
def rate_of(d):
    return float(d["window_rps"])


def one_arm(voters, a, label, fsms, lag, rates, checks, fan_in, pins=None,
            pre_client=None, late=None, scrapes=None, check=True):
    """One rate arm on a fresh cluster generation.

    The three keyword hooks are the time-and-timers rows' whole footprint on
    this function; every one of them is a no-op when unset, so the M14 rows
    call it unchanged.

      `pre_client(voters, leader)` runs after the cluster is up and BEFORE the
          client starts — row e applies its schedule table there, because a
          table adopted mid-window would put an un-tabled prefix inside the
          measured window.
      `late`, when a list, collects this arm's `uc2_timers_late_total` sweep
          over every voter (rows b and e's second clause), taken right after
          the client's window closes and BEFORE the divergence checks, which
          take tens of seconds.
      `scrapes`, when a list, appends `(label, leader_metrics)` — row c's
          histogram evidence, which only the leader has (it is the node that
          fires timers and runs the passes).
      `check=False` skips the row-c divergence checks, which is what the
          BASELINE arms of the A/B do: they are a rate measurement on a
          different binary, not a claim about this branch's correctness.
    """
    leader = start_cluster_m14(voters, fsms, lag=lag, pins=pins)
    print(f"INFO arm {label}: leader n{leader} on {voters[leader].public_ip}", flush=True)
    if pre_client is not None:
        pre_client(voters, leader)
    d = run_rate_arm(voters, leader, a, label, fan_in, pins=pins)
    rates[label] = rate_of(d)
    print(f"INFO arm {label}: window_rps={rates[label]:.0f} responses={d['responses']} lost={d['lost']}", flush=True)
    if late is not None:
        late_sweep(voters, label, late)
    if scrapes is not None:
        scrapes.append((label, scrape_prom(voters[leader], label="row-c")))
    if check:
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
            "name": m.group(2), "version": m.group(3), "hash": m.group(4),
            "attached": m.group(5) == "true", "epoch": int(m.group(6)),
            "incarnation": int(m.group(7)), "applied": int(m.group(8)),
            "lag": int(m.group(9)), "snapshot_pos": int(m.group(10)),
            "heartbeat_age": m.group(11),
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
    (`uc_node/src/obs/log.rs:227-243`, sink defaults to stderr, default level
    Info) — no subscriber is installed or needed — and every transient unit
    appends BOTH stdout and stderr to the same file
    (`m12_fleet_gate.unit_start_cmd`), so the node role's structured records
    are in its unit log next to its own printlns."""
    r = ssh(h, f"sudo grep -E {shlex.quote(pattern)} {m12.unit_log(h, unit)} 2>/dev/null | tail -n {lines}",
            label="grep")
    return [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]


def node_stats(h):
    """Last `stats:` line of the node unit's log → (unattested, legacy, identity, version)."""
    out = tail_log(h, "node", lines=400)
    hits = STATS_RE.findall(out or "")
    if not hits:
        return None
    u, l, i, v = hits[-1]
    return int(u), int(l), int(i), int(v)


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
    #      `if first > start_pos` (uc_service/src/replay.rs:73-78), where
    #      `first` is the base of the OLDEST RETAINED journal segment
    #      (`reader.first_meta()`, 0 while nothing has been purged) and a
    #      fresh process's `start_pos` is 0
    #      (uc_service/src/attach.rs:153, `last_applied().unwrap_or(0)`).
    #      With purge off, `first` stays 0 for the whole arm, `0 > 0` is
    #      false, and the FSM falls through to `scan_from(0)` — the full
    #      replay run 1 diagnosed — NO MATTER what snapshot interval it was
    #      given. Only `PurgePolicy::BelowSnapshot` dispatches
    #      `ArchiveCmd::Purge` and lifts `first` above 0
    #      (uc_node/src/node.rs:3261-3270). Run 1 gave the FSMs neither, so
    #      the restart replayed the WHOLE journal (~11.9 M commands, ~1.3 GB)
    #      and `attached_at` was a replay-completion clock (21.6 s). A
    #      deployed service installs its newest artifact and tail-replays one
    #      interval; that is what M9's 15 s budget was itemised against.
    #  (2) The measuring client submits to FSM 0 ONLY (`fan_in=False`). Under
    #      fan-in a submit completes only when EVERY declared FSM answers, and
    #      journal replay is publish-silent (uc_service/src/replay.rs:44-46),
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
    # between (uc_node/src/node.rs:2854-2888). Recorded, never adjudicated.
    transitions = log_lines(h, "node", "service_(de|at)tached")
    print(f"INFO row d: leader service transitions ({len(transitions)}):", flush=True)
    for ln in transitions:
        print(f"  {ln}", flush=True)
    check_all(voters, leader, "kill", checks, expect_min=int(d["responses"]) if d else None)
    stop_cluster_m14(voters)
    return {"baseline": baseline, "recovered_at": recovered, "attached_at": attached_at,
            "bound": bound, "pre_incarnation": pre_inc, "transitions": transitions,
            "client_lost": d["lost"] if d else None}


def restart_node(h, pins=None):
    """Kill and start THIS host's node unit with the args it was last started
    with, leaving the instance dir and the SERVICE units in place — row g's
    mid-window shipper restart.

    The args come from `LAST_NODE_ARGS` rather than being rebuilt: a restart
    that quietly changed the member list, the purge policy or the metrics port
    would be measuring a different cluster from the one that was running. The
    node log is deliberately NOT truncated — row g wants both lives of the
    leader in one file."""
    args = LAST_NODE_ARGS.get(h.public_ip)
    if args is None:
        raise RuntimeError(f"restart_node: no recorded node args for {h.public_ip}")
    kill_unit(h, "node")
    start_unit(h, "node", args, nofile=True, cpus=(pins or {}).get("node"))


def arm_join(voters, learner, a, K, checks, pins=None, restart_leader_after=None):
    """Row f: voters run the bounded pair with purge ON and snapshots every
    `M14_SNAPSHOT_INTERVAL_BYTES`; fan-in load runs for the whole arm; 10 s in, a learner declared
    {0,1} is admitted (`uc2ctl add-learner` on the leader — M7's pattern:
    the learner boots as a plain node with the CURRENT voters as its seed
    members) and must reach both voters' `applied` within 60 s via a
    two-artifact snapshot session (wire 0.6.0), with zero refusals.

    `restart_leader_after`, when set, makes this the time-and-timers gate's
    ROW G instead: that many seconds after `add-learner` — i.e. before the
    joiner's first commit advance — the LEADER's node unit is killed and
    started again with the same args, the service units left alone. That is
    the fleet form of `learner.rs::a_joiner_served_by_a_leader_restarted_
    before_its_first_commit_advance_still_installs_the_table`; the residual
    the coordinated-snapshot spec was written to close was a restarted shipper
    serving `(0, 0, [])`, so the joiner installed nothing. The bar is
    deliberately row f's 60 s: nothing about the join path's MECHANICS
    changed, only what it carries.

    Either way the result dict now also carries `set_positions` — every host's
    `uc2_snapshot_set_position` at the end of the arm, which row g requires to
    agree cluster-wide."""
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
    t_add = time.time()
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
    leader_restarted = False
    if restart_leader_after is not None:
        # Row g: land the restart `restart_leader_after` seconds after
        # add-learner, whatever the intervening ssh round trips cost, so the
        # instant is a property of the row rather than of the driver's own
        # latency on the day.
        wait = restart_leader_after - (time.time() - t_add)
        if wait > 0:
            time.sleep(wait)
        print(f"INFO row g: restarting the LEADER's node unit on {h.public_ip} "
              f"{time.time() - t_add:.2f}s after add-learner (services stay up)", flush=True)
        restart_node(h, pins=pins)
        leader_restarted = True
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
    # `uc_node/src/node.rs:3168`.
    installs = len(log_lines(learner, "node", '"event":"snapshot_installed"'))
    refusals = {}
    for hh in voters + [learner]:
        st = node_stats(hh)
        refusals[hh.public_ip] = (st[1], st[2], st[3]) if st else (-1, -1, -1)
    hosts_all = voters + [learner]
    # Row g: the newest COMPLETE snapshot set every host holds. It must agree
    # cluster-wide once caught up (coordinated-snapshot spec §5.3/§9) — which
    # is the clause that says the joiner adopted the shipper's set rather than
    # merely catching up beside it.
    set_positions = {}
    for hh in hosts_all:
        v = tt.prom_get(scrape_prom(hh, label="set-pos"), "uc2_snapshot_set_position")
        if v is not None:
            set_positions[hh.public_ip] = int(v)
    before = len(checks)
    check_all(hosts_all, leader, "join", checks, expect_min=int(d["responses"]) if d else None)
    check_ok = all(c[3] for c in checks[before:]) and len({c[4] for c in checks[before:]}) == 1
    print(f"INFO row f: joined_at={joined_at}s artifacts={artifacts} bytes={artifact_bytes} "
          f"snapshot_installs={installs} refusals={refusals} check_ok={check_ok} "
          f"set_positions={set_positions} leader_restarted={leader_restarted}", flush=True)
    stop_cluster_m14(hosts_all)
    return {"joined_at": joined_at, "refusals": refusals, "artifacts": artifacts,
            "artifact_bytes": artifact_bytes, "installs": installs, "check_ok": check_ok,
            "client_lost": d["lost"] if d else None, "set_positions": set_positions,
            "leader_restarted": leader_restarted}


# ============================================ time and timers (--tt-rows)
# Rows a/b/c/e/g/h of docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md.
# The bars are that document's, pre-committed; nothing below may be edited to
# fit a run. Every verdict is pure over recorded numbers, exactly like the M14
# rows above, and every one prints a single GATE-JSON line.

# The four rate arms the T&T rows re-use from `arm_rates`. The two LOCKSTEP
# arms are deliberately absent: they are the M14 gate's row e, "reported, no
# bar", so A/B-ing them would invent a bar the gate doc does not carry.
TT_RATE_ARMS = ("n1", "n2eq", "slow1", "pair")

ROW_H_ARM_SECS = 360          # two 120 s instant budgets + a learner join + baseline
ROW_H_BASELINE_SECS = 12      # 2 s ramp + [2,10) s baseline + slack (row d's shape)
ROW_H_GAP_TAIL_MS = 2000      # a stall may outlive the instant by a bucket or two
ROW_H_LEARNER_SERVE_SECS = 120.0


def tt_arm_fsms(label, K):
    return {"n1": [(0, 0)], "n2eq": [(0, 0), (1, 0)],
            "slow1": [(0, K)], "pair": [(0, 0), (1, K)]}[label]


def tt_fan_in(label):
    """Fan-in whenever two FSMs are declared (the M14 rate conventions)."""
    return label in ("n2eq", "pair")


# ------------------------------------------------------- pure T&T verdicts
def _ab_fields(stats):
    """`tt.ab_stats` output flattened for a GATE-JSON line."""
    out = {}
    for label, row in sorted(stats.items()):
        f = {}
        for side in ("base", "head"):
            if side in row:
                f[side] = {k: row[side][k]
                           for k in ("n", "mean", "min", "max", "spread_pct", "sem_pct")}
        if "delta_pct" in row:
            f["delta_pct"] = row["delta_pct"]
        out[label] = f
    return out


def _ab_row(row, title, rates_base, rates_head, resolution_pct, late_counts=None, **extra):
    """The shared body of rows a, b and e: an interleaved A/B read against a
    recorded resolution, plus (rows b and e) the `uc2_timers_late_total == 0`
    sweep.

    `rates_base` / `rates_head` are `{arm: [rate per rep]}`. Only
    `within resolution` passes — `inconclusive (noisy run)` is a third answer
    and never a soft pass (gate doc, "Rows d and f: the verdict rule").
    `late_counts=None` means the row has no late clause; an EMPTY sweep is a
    failure, not a pass, because it is the absence of evidence."""
    arms = {label: {"base": rates_base.get(label, []), "head": rates_head.get(label, [])}
            for label in sorted(set(rates_base) | set(rates_head))}
    stats = tt.ab_stats(arms)
    reading, worst_delta, worst_sem = tt.ab_reading(stats, resolution_pct)
    offenders = tt.late_offenders(late_counts) if late_counts is not None else []
    late_ok = None if late_counts is None else (bool(late_counts) and not offenders)
    ok = (reading == "within resolution") and (late_ok is not False)
    detail = (f"{reading}"
              + (f": worst arm delta {worst_delta:+.3f} % vs resolution "
                 f"{resolution_pct} %" if worst_delta is not None else "")
              + (f" (worst sem {worst_sem:.3f} %)" if worst_sem is not None else ""))
    if late_ok is not None:
        detail += (f"; timers_late == 0 on {len(late_counts)} samples" if late_ok
                   else f"; TIMERS LATE: {offenders[:8]}")
    for arm, host, service, srow, count in offenders:
        print(f"FAIL {row}: uc2_timers_late_total={count} on {host} arm={arm} "
              f"service={service} row={srow}", flush=True)
    gate_json(row, ok, reading=reading, worst_delta_pct=worst_delta, worst_sem_pct=worst_sem,
              resolution_pct=resolution_pct, arms=_ab_fields(stats),
              late_samples=(len(late_counts) if late_counts is not None else None),
              late_offenders=[list(o) for o in offenders], **extra)
    return Verdict(title, ok, detail)


def verdict_tt_a(rates_base, rates_head, resolution_pct):
    """Row a: every service wrapped in `Timed<..>`, NO timers scheduled, against
    the same arms on the pre-time-and-timers binary. Bar: within the day's
    `scripts/hop1_ab.sh` same-source rebuild resolution — a NULL bar ("the
    stamp is free"), not a ratio against a target."""
    return _ab_row("tt-a", "tt-a Timed<..> services, no timers, vs the pre-T&T binary",
                   rates_base, rates_head, resolution_pct)


def verdict_tt_b(rates_base, rates_head, resolution_pct, late_counts):
    """Row b: the same A/B with FSM 0 sustaining the timer load, AND
    `uc2_timers_late_total == 0` on every node for every row after the warm-up
    window. Both clauses must hold."""
    return _ab_row("tt-b", "tt-b sustained timer load vs the pre-T&T binary",
                   rates_base, rates_head, resolution_pct, late_counts=late_counts)


def verdict_tt_e(rates_row_a, rates_row_e, resolution_pct, late_counts, table_ok=None):
    """Row e: the 32-entry, 100 ms table live through the same arms.

    The comparison is row a's OWN head rates against row e's — the same
    binary with and without a live table — because the bar is "throughput
    within row a's resolution" and what row e adds is the table, not a
    version change. `table_ok` is the per-arm convergence flag from
    `apply_schedule_table`; a row whose table never converged on every voter
    is not a pass whatever its rate did."""
    converged = bool(table_ok) and all(table_ok.values())
    v = _ab_row("tt-e", "tt-e 32-entry 100 ms schedule table live through the rate arms",
                rates_row_a, rates_row_e, resolution_pct, late_counts=late_counts,
                table_converged=converged, table_ok=table_ok)
    if not converged:
        return Verdict(v.row, False, v.detail + f"; TABLE DID NOT CONVERGE: {table_ok}")
    return v


def verdict_tt_c(p99_ns, pass_ns, count):
    """Row c: `p99 <= 2 x the measured consensus-pass length on the rig`.

    An on-time fire is stamped with its deadline, so the histogram measures
    WALL-CLOCK lateness — the delay between the deadline passing and the pass
    that notices it. That is bounded below by the pass length, which is why
    the bar is a multiple of the measured pass and not a fixed number, and why
    a p99 above it points at pass scheduling rather than at the timer heap.

    Fewer than `ROW_C_MIN_FIRES` fires is `inconclusive (too few fires)` —
    reported as such and NOT as a pass."""
    bar_ns = tt.ROW_C_BAR_MULTIPLE * pass_ns if pass_ns else None
    enough = count is not None and count >= tt.ROW_C_MIN_FIRES
    ok = bool(enough and p99_ns is not None and bar_ns is not None and p99_ns <= bar_ns)
    if not enough:
        detail = (f"inconclusive (too few fires): count={count}, "
                  f"need >= {tt.ROW_C_MIN_FIRES}")
    elif p99_ns is None or bar_ns is None:
        detail = f"inconclusive (missing histogram): p99={p99_ns} ns, mean pass={pass_ns} ns"
    else:
        detail = (f"p99 {p99_ns:.0f} ns over {count:.0f} fires vs bar {bar_ns:.0f} ns "
                  f"({tt.ROW_C_BAR_MULTIPLE:g} x mean pass {pass_ns:.0f} ns)")
    gate_json("tt-c", ok, p99_ns=p99_ns, pass_ns=pass_ns, count=count, bar_ns=bar_ns,
              min_fires=tt.ROW_C_MIN_FIRES, multiple=tt.ROW_C_BAR_MULTIPLE)
    return Verdict("tt-c timer precision under row b's load", ok, detail)


def verdict_tt_g(g):
    """Row g: `g` = `{"joined_at": s|None, "installs": n, "set_positions":
    {host: pos}, "leader_restarted": bool}`.

    Four clauses, and the last two are the anti-vacuity ones. `installs` is
    row f's: at least one `snapshot_installed` record is the positive evidence
    that a snapshot SESSION ran, rather than a plain journal catch-up.
    `leader_restarted` is row g's own: without the mid-window restart this is
    row f again, and the residual the row exists to close was never exercised.
    `set_positions` must AGREE across every host and be non-zero — a complete
    set at one position, cluster-wide."""
    j = g.get("joined_at")
    positions = g.get("set_positions") or {}
    values = set(positions.values())
    agree = len(positions) >= 1 and len(values) == 1 and all(p > 0 for p in values)
    installs = int(g.get("installs") or 0)
    restarted = bool(g.get("leader_restarted"))
    ok = (j is not None and j <= tt.BAR_TT_G_CONVERGE_SECS and installs >= 1
          and agree and restarted)
    gate_json("tt-g", ok, joined_at=j, installs=installs, set_positions=positions,
              set_agree=agree, leader_restarted=restarted, bar=tt.BAR_TT_G_CONVERGE_SECS)
    return Verdict("tt-g below-floor join with the shipper restarted mid-window", ok,
                   f"joined at {j}s (bar <= {tt.BAR_TT_G_CONVERGE_SECS}s), "
                   f"snapshot installs={installs} (need >= 1), leader restarted={restarted}, "
                   f"snapshot set agrees cluster-wide={agree} {sorted(positions.items())}")


def verdict_tt_h(all_arm, standby_arm, pass_ns):
    """Row h: freeze duration vs commit stall.

    ONE bar, and it is the standby arm's: its commit gap must be `<=` the pass
    length measured on the day (row c's mean), i.e. no stall attributable to
    the instant. The ALL-NODES arm's gap is reported bare — it is the cost the
    operator is choosing between, not a target to hit, and a freeze on a
    quorum that outlasts `fsm_lag` of appended log stalls commit BY DESIGN.

    Both instants must have completed: an instant that never landed makes the
    row unreadable rather than passed."""
    bar_secs = (pass_ns / 1e9) if pass_ns else None
    a_arm, s_arm = all_arm or {}, standby_arm or {}
    gap = s_arm.get("gap_secs")
    both_done = bool(a_arm.get("completed")) and bool(s_arm.get("completed"))
    ok = bool(both_done and bar_secs is not None and gap is not None and gap <= bar_secs)
    if not both_done:
        detail = (f"inconclusive (an instant did not complete): all-nodes="
                  f"{a_arm.get('completed')} standby={s_arm.get('completed')}")
    elif bar_secs is None:
        detail = "inconclusive (no pass length recorded — run tt-c, or pass --pass-ns)"
    else:
        detail = (f"standby commit gap {gap}s vs bar {bar_secs * 1e3:.3f} ms "
                  f"(the measured pass length); all-nodes gap {a_arm.get('gap_secs')}s "
                  f"(reported, no bar), freeze max all-nodes "
                  f"{max(a_arm.get('freeze_max', {}).values(), default=None)}s / standby "
                  f"{max(s_arm.get('freeze_max', {}).values(), default=None)}s")
    gate_json("tt-h", ok, all_nodes=a_arm, standby=s_arm, pass_ns=pass_ns,
              bar_secs=bar_secs, stall_fraction=tt.STALL_FRACTION)
    return Verdict("tt-h freeze duration vs commit stall", ok, detail)


def row_c_reading(scrapes):
    """`scrapes` = `[(arm, leader_metrics)]` from the TIMERS-ON arms ->
    `(arm, p99_ns, pass_ns, count, per_arm)`.

    Adjudicated on the arm with the MOST fires rather than on a pool of all of
    them: 1 000 timers/s over one 12 s arm only just clears the 10 000-fire
    floor, and pooling arms whose pass lengths differ would mix distributions
    into a histogram that describes none of them. Every arm's reading is
    returned too, and printed, so a run can be read even when the chosen arm
    is inconclusive."""
    per_arm = []
    for arm, m in scrapes:
        buckets, _, count = tt.hist_series(m, "uc2_timer_lateness_ns", row="0")
        pb, psum, pcount = tt.hist_series(m, "uc2_consensus_pass_ns")
        per_arm.append({
            "arm": arm,
            "count": count,
            "p99_ns": tt.hist_quantile(buckets, 0.99),
            "p50_ns": tt.hist_quantile(buckets, 0.50),
            "lateness_max_ns": tt.prom_get(m, "uc2_timer_lateness_ns_max", row="0"),
            "pass_ns": tt.hist_mean(psum, pcount),
            "pass_p99_ns": tt.hist_quantile(pb, 0.99),
            "pass_max_ns": tt.prom_get(m, "uc2_consensus_pass_ns_max"),
        })
    if not per_arm:
        return None, None, None, None, per_arm
    best = max(per_arm, key=lambda r: (r["count"] or 0))
    return best["arm"], best["p99_ns"], best["pass_ns"], best["count"], per_arm


# ------------------------------------------------------------- T&T fleet
def prepare_base_tree(hosts, base_tree):
    """rsync the pre-time-and-timers checkout to `BASE_SRC` on every host and
    build ITS `m6_gate` + `m12_gate` there — `m14_ab_27_vs_28.py`'s
    second-tree pattern, whose `sync_tree_to` this reuses rather than
    re-deriving. Each version is probed by its own `m6_gate` because the two
    trees' cnc page layouts need not agree."""
    sync_tree_to(hosts, base_tree, BASE_SRC)
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    cargo = m6.SshHost.CARGO
    for h in hosts:
        cmd = (f"sudo mkdir -p {BASE_SRC} && "
               f"{env} {cargo} build --release --manifest-path {BASE_SRC}/Cargo.toml "
               f"-p uc_node --example m6_gate && "
               f"{env} {cargo} build --release --manifest-path {BASE_SRC}/Cargo.toml "
               f"-p uc_gateway --example m12_gate && "
               f"test -x {BASE_GATE} && test -x {BASE_PROBE} && echo PREPARED-BASE")
        r = ssh(h, cmd, label="build-base")
        if "PREPARED-BASE" not in (r.stdout or ""):
            raise RuntimeError(f"baseline build on {h.public_ip}: {(r.stderr or r.stdout)[-2000:]}")
    # Provenance beside every number, the M14b rule: which binaries these were.
    r = ssh(hosts[0], f"sha256sum {BUILT_GATE} {BASE_GATE}; "
                      f"git -C {m6.SshHost.UC_SRC} rev-parse --short HEAD 2>/dev/null; "
                      f"git -C {BASE_SRC} rev-parse --short HEAD 2>/dev/null; true",
            label="provenance")
    print("INFO A/B provenance (head, base):\n" + (r.stdout or ""), flush=True)


def base_fleet_hosts(a):
    """The same 4 machines, addressed through the BASELINE tree's binaries."""
    return m6.build_fleet_hosts(BASE_GATE, a.ssh_user, a.ssh_key, a.hosts, count=4,
                                ctl_bin=BUILT_CTL, unit_prefix=UNIT_PREFIX,
                                remote_root=REMOTE_ROOT, probe_bin=BASE_PROBE)


def ensure_k(voters, a, rates, checks, pins=None):
    """The slow-FSM `K` the rate arms need. Free when the M14 rows already ran
    (they leave `n1` in `rates` and return K); otherwise `--k`, else one `n1`
    arm plus the calibration ladder — the same two steps `arm_rates` takes,
    and the same `calib_ok` band refusal if the ladder never slowed FSM 0
    down."""
    if a.k:
        return a.k
    if "n1" not in rates:
        one_arm(voters, a, "n1", [(0, 0)], None, rates, checks, fan_in=False, pins=pins)
    return arm_calib(voters, a, rates, checks, pins=pins)


def arm_tt_ab(voters, base_voters, a, K, checks, timers_per_sec, tag, pins=None):
    """Rows a and b: the four rate arms A/B'd against the baseline tree,
    INTERLEAVED per arm on fresh clusters (A base, B head, A, B …).

    Interleaved so a drift in the rig across the run lands on both versions
    equally rather than on whichever ran second; fresh clusters per arm so no
    mixed-version cluster ever exists (the baseline predates this flag day's
    header relayout, and a relaid header is the SAME LENGTH — a mixed cluster
    would parse each other's frames and mean something different, which is the
    one failure mode the wire's length checks cannot catch).

    Every BASE arm runs under `tt_disabled()`: its `m12_gate` has none of
    `--metrics-listen`/`--timed`/`--timers-per-sec`, and would refuse them by
    name. That is also why the base arms carry no late sweep, no metrics
    scrape and no divergence check — they are a rate on another binary, not a
    claim about this branch."""
    base_rates = {label: [] for label in TT_RATE_ARMS}
    head_rates = {label: [] for label in TT_RATE_ARMS}
    late, scrapes = [], []
    for label in TT_RATE_ARMS:
        fsms = tt_arm_fsms(label, K)
        row0 = fsm_name(0, fsms[0][1])
        timers_here = timers_per_sec > 0 and row0 == "count"
        if timers_per_sec > 0 and not timers_here:
            print(f"INFO {tag} {label}: row 0 declares '{row0}', not 'count' — no timer load on "
                  f"this arm (the Rust contract allows --timers-per-sec only with --fsm count). "
                  f"The late == 0 sweep still runs over it.", flush=True)
        for rep in range(1, a.ab_reps + 1):
            with tt_disabled():
                db = one_arm(base_voters, a, f"{tag} base {label} rep{rep}", fsms, None, {},
                             checks, fan_in=tt_fan_in(label), pins=pins, check=False)
            base_rates[label].append(rate_of(db))
            with tt_options(timers_per_sec=(timers_per_sec if timers_here else 0)):
                dh = one_arm(voters, a, f"{tag} head {label} rep{rep}", fsms, None, {},
                             checks, fan_in=tt_fan_in(label), pins=pins,
                             late=(late if timers_per_sec > 0 else None),
                             scrapes=(scrapes if timers_here else None))
            head_rates[label].append(rate_of(dh))
            print(f"INFO {tag} {label} rep{rep}: base {base_rates[label][-1]:.0f} ops/s, "
                  f"head {head_rates[label][-1]:.0f} ops/s", flush=True)
    return {"base": base_rates, "head": head_rates, "late": late, "scrapes": scrapes}


def apply_schedule_table(voters, leader, count, row0_name, arm):
    """Row e: stage a `count`-entry table naming `row0_name`, apply it against
    the LEADER, and wait until every voter's `uc2_schedule_table_position`
    agrees and is non-zero.

    `uc2ctl schedule apply` is leader-only by construction — it stages the
    encoded table as a node-local file and signs its digest into the admin
    request, so a follower has nothing to forward — which is why this runs on
    `voters[leader]` and never retries elsewhere. No admin key is passed: this
    gate's clusters run `m12_gate`'s default admin policy, exactly as
    `arm_join`'s `add-learner` and `status_slots` do.

    The table names the row's DECLARED name rather than a fixed `count`: an
    entry naming an undeclared FSM refuses the WHOLE table (refusal 43), and
    the `slow1` arm declares row 0 as `spin`.

    Returns the per-host positions, or `None` on refusal or on the 30 s
    convergence timeout. The arm still runs either way and `verdict_tt_e`
    fails the row on the flag — an arm that produced evidence is worth more to
    the run than a traceback."""
    h = voters[leader]
    path = f"/opt/bench/{UNIT_PREFIX}-schedule.toml"
    body = tt.schedule_toml(count, row0_name)
    blob = base64.b64encode(body.encode()).decode()
    ssh(h, f"echo {blob} | base64 -d | sudo tee {path} >/dev/null && sudo sha256sum {path}",
        label="schedule-stage")
    r = ssh(h, f"sudo {BUILT_CTL} schedule apply --instance-dir {h.dir} --app-id {APP} {path}",
            label="uc2ctl schedule")
    out = ((r.stdout or "") + (r.stderr or "")).strip()
    print(f"INFO {arm}: schedule apply ({count} entries on '{row0_name}') "
          f"rc={r.returncode}: {out[:400]}", flush=True)
    if r.returncode != 0:
        print(f"FAIL {arm}: `uc2ctl schedule apply` was refused — row e cannot be "
              f"adjudicated on this arm", flush=True)
        return None
    deadline = time.time() + tt.SCHEDULE_CONVERGE_SECS
    positions = {}
    while time.time() < deadline:
        positions = {hh.public_ip: tt.prom_get(scrape_prom(hh, label="table-pos"),
                                               "uc2_schedule_table_position")
                     for hh in voters}
        vals = [v for v in positions.values() if v is not None]
        if len(vals) == len(voters) and len(set(vals)) == 1 and vals[0] > 0:
            print(f"INFO {arm}: schedule table converged at position {vals[0]:.0f} on all "
                  f"{len(vals)} voters", flush=True)
            return positions
        time.sleep(0.5)
    print(f"FAIL {arm}: schedule table did not converge within "
          f"{tt.SCHEDULE_CONVERGE_SECS}s — positions {positions}", flush=True)
    return None


def arm_tt_e(voters, a, K, checks, pins=None):
    """Row e: the same four arms again with the table live from before the
    client starts, `--ab-reps` reps each so the comparison against row a's
    head rates has the same statistical shape as rows a and b."""
    rates = {label: [] for label in TT_RATE_ARMS}
    late, table_ok, positions = [], {}, {}
    for label in TT_RATE_ARMS:
        fsms = tt_arm_fsms(label, K)
        row0 = fsm_name(0, fsms[0][1])
        for rep in range(1, a.ab_reps + 1):
            state = {}

            def pre_client(vs, leader, _row0=row0, _label=label, _rep=rep, _state=state):
                _state["positions"] = apply_schedule_table(
                    vs, leader, a.schedule_table, _row0, f"tt-e {_label} rep{_rep}")

            scratch = {}
            with tt_options(timers_per_sec=0):
                one_arm(voters, a, f"tt-e {label} rep{rep}", fsms, None, scratch, checks,
                        fan_in=tt_fan_in(label), pins=pins, pre_client=pre_client, late=late)
            rates[label].append(scratch[f"tt-e {label} rep{rep}"])
            key = f"{label} rep{rep}"
            table_ok[key] = state.get("positions") is not None
            positions[key] = state.get("positions")
    return {"rates": rates, "late": late, "table_ok": table_ok, "positions": positions}


def take_instant(voters, leader, watchers, standby, tag):
    """Command one coordinated snapshot instant and wait for it to land.

    Two different completion signals, deliberately:

      all-nodes  `uc2_snapshot_instant_position` carries P — but it is
                 documented LEADER-LOCAL ("the last snapshot instant this node
                 COMMANDED as leader"; a follower exports whatever it last
                 commanded in some earlier term), so polling it on every voter
                 would never agree. The cluster-wide signal is
                 `uc2_snapshot_set_position` reaching P on every voter, which
                 IS "every row plus the cluster FSM froze at P and the set is
                 complete" — the thing row h wants to know.
      standby    the LEARNER's `uc2_snapshot_standby_instant_position`, which
                 is per-node "the instant this node's uc2-cluster agent ACTED
                 on" and learner-only by design (a voter skips every standby
                 instant, so it exports 0 forever).

    One scrape per watcher per poll serves both the completion test and the
    freeze fold, because `uc2_snapshot_freeze_seconds_max` is reset on the
    scrape after the instant advances and must be sampled during the wait."""
    h = voters[leader]
    metric = ("uc2_snapshot_standby_instant_position" if standby
              else "uc2_snapshot_instant_position")
    pre = tt.prom_get(scrape_prom(h if not standby else watchers[0], label="instant"), metric) or 0.0
    flag = " --standby" if standby else ""
    r = ssh(h, f"date +%s%3N; sudo {BUILT_CTL} snapshot --instance-dir {h.dir} "
               f"--app-id {APP}{flag}", label="uc2ctl snapshot")
    lines = [ln.strip() for ln in ((r.stdout or "") + (r.stderr or "")).splitlines() if ln.strip()]
    # The instant's t0 is taken on the HOST clock, the same clock that stamps
    # every TL bucket's unix_ms, so no driver/host skew enters the commit-gap
    # measurement (row d's single-clock discipline).
    t0_ms = int(lines[0]) if lines and lines[0].isdigit() else None
    t0 = time.time()
    res = {"standby": standby, "commanded": r.returncode == 0, "t0_ms": t0_ms,
           "position": None, "completed": False, "secs": None, "freeze_max": {},
           "pre_position": pre}
    print(f"INFO {tag}: uc2ctl snapshot{flag} rc={r.returncode} t0_ms={t0_ms} "
          f":: {' | '.join(lines[1:])[:400]}", flush=True)
    if r.returncode != 0:
        print(f"FAIL {tag}: the instant was refused", flush=True)
        return res
    deadline = t0 + tt.INSTANT_WAIT_SECS
    while time.time() < deadline:
        snaps = {w.public_ip: scrape_prom(w, label="instant") for w in watchers}
        for ip, m in snaps.items():
            fold_freeze(ip, m, res["freeze_max"])
        if standby:
            vals = [tt.prom_get(snaps[w.public_ip], metric) for w in watchers]
            if vals and all(v is not None and v > pre for v in vals):
                res["position"], res["completed"] = int(max(vals)), True
                break
        else:
            p = tt.prom_get(snaps.get(h.public_ip, {}), metric)
            if p is not None and p > pre:
                res["position"] = int(p)
                sets = [tt.prom_get(snaps[w.public_ip], "uc2_snapshot_set_position")
                        for w in watchers]
                if sets and all(s is not None and s >= p for s in sets):
                    res["completed"] = True
                    break
        time.sleep(1.0)
    res["secs"] = round(time.time() - t0, 2)
    if not res["completed"]:
        print(f"FAIL {tag}: the instant did not complete within {tt.INSTANT_WAIT_SECS}s "
              f"(position={res['position']}, freeze_max={res['freeze_max']})", flush=True)
    else:
        print(f"INFO {tag}: instant complete at position {res['position']} in {res['secs']}s, "
              f"freeze_max={res['freeze_max']}", flush=True)
    return res


def wait_learner_serving(learner, budget=ROW_H_LEARNER_SERVE_SECS):
    """Row h: the learner must be attached and applying before a standby
    instant means anything (`49 snapshot_no_learner` is about MEMBERSHIP, not
    about readiness, so a commanded standby instant would otherwise be
    accepted against a learner that has not caught up)."""
    deadline = time.time() + budget
    while time.time() < deadline:
        slots, _ = status_slots(learner)
        s = slots.get(0)
        if s and s["attached"] and s["applied"] > 0:
            print(f"INFO tt-h: learner serving after {budget - (deadline - time.time()):.1f}s "
                  f"(applied={s['applied']})", flush=True)
            return True
        time.sleep(0.5)
    print(f"WARN tt-h: the learner was not serving within {budget}s — the standby instant "
          f"runs anyway and the row records what it finds", flush=True)
    return False


def arm_tt_h(voters, learner, a, checks, pins=None):
    """Row h: freeze duration vs commit stall on a 256 MiB-state FSM.

    One declared FSM (`--state-bytes` attaches to row 0's `count` only), purge
    OFF and no replicated cadence — so the ONLY instants that happen are the
    two this arm commands, which is what makes the commit gaps attributable.
    `--snapshot-interval-bytes` still reaches the SERVICE role, where since
    coordinated snapshots it is nothing but the capability switch
    (`start_with_snapshots`, `CNC_SVC_STATUS_SNAPSHOT_CAPABLE`)."""
    with tt_options(state_bytes=a.state_bytes):
        leader = start_cluster_m14(voters, [(0, 0)], purge=False,
                                   snap=M14_SNAPSHOT_INTERVAL_BYTES, pins=pins)
        h = voters[leader]
        print(f"INFO tt-h: leader n{leader} on {h.public_ip}, "
              f"state_bytes={a.state_bytes}", flush=True)
        run_rate_arm(voters, leader, a, "tt-h", fan_in=False, secs=ROW_H_ARM_SECS,
                     timeline=True, unit=True, measure=False, pins=pins)
        t_client = time.time()
        time.sleep(ROW_H_BASELINE_SECS)
        all_arm = take_instant(voters, leader, voters, False, "tt-h all-nodes")
        # The standby arm needs a learner in the COMMITTED membership; added
        # here rather than at the top so the all-nodes arm measures a plain
        # three-voter cluster, and without purge so the join is an ordinary
        # catch-up rather than row g's below-floor one.
        new_id, addr = 3, f"{learner.private_ip}:{PORT}"
        m12.wipe_dirs([learner])
        rc, out = h.ctl("add-learner", new_id, addr)
        if rc != 0:
            raise RuntimeError(f"tt-h add-learner refused: {out.strip()}")
        truncate_log(learner, "node")
        start_unit(learner, "node",
                   node_args(learner, new_id, m12.members_str(voters), [(0, 0)], None, False, 0),
                   nofile=True, cpus=(pins or {}).get("node"))
        time.sleep(2.0)
        truncate_log(learner, "service0")
        start_unit(learner, "service0",
                   service_args(learner, 0, 0, M14_SNAPSHOT_INTERVAL_BYTES),
                   cpus=service_cpu(pins, 0))
        served = wait_learner_serving(learner)
        standby_arm = take_instant(voters, leader, [learner], True, "tt-h standby")
        standby_arm["learner_serving"] = served
        # The client ran across both instants; its per-second buckets are the
        # visible end of the commit stream.
        m12.wait_units_done([(h, ["client"])], t_client + ROW_H_ARM_SECS + CLIENT_SLACK_SECS)
        out = tail_log(h, "client", lines=4000) or ""
        kill_unit(h, "client")
        d = parse_result(out, "direct")
        tl = parse_timeline(out)
        if d is None:
            print("WARN tt-h: client RESULT missing — timeline NOT trimmed", flush=True)
        if not tl:
            print("WARN tt-h: empty timeline — no baseline, no commit gap", flush=True)
        baseline = 0.0
        if tl:
            t_start_ms = tl[0][0]
            if d is not None:
                tl = bound_timeline(tl, t_start_ms + int(d["elapsed_secs"] * 1000) + 1000)
            base = [r for ms, r in tl if t_start_ms + 2000 <= ms < t_start_ms + 10000]
            baseline = (sum(base) / len(base)) if base else 0.0
        for arm in (all_arm, standby_arm):
            arm["baseline_rps"] = baseline
            arm["gap_secs"] = None
            if tl and arm["t0_ms"] is not None and arm["secs"] is not None:
                hi = arm["t0_ms"] + int(arm["secs"] * 1000) + ROW_H_GAP_TAIL_MS
                arm["gap_secs"] = tt.longest_stall(tl, baseline, lo_ms=arm["t0_ms"], hi_ms=hi)
        print(f"INFO tt-h: baseline {baseline:.0f}/s; all-nodes gap {all_arm['gap_secs']}s "
              f"freeze_max {all_arm['freeze_max']}; standby gap {standby_arm['gap_secs']}s "
              f"freeze_max {standby_arm['freeze_max']}", flush=True)
        check_all(voters + [learner], leader, "tt-h", checks,
                  expect_min=int(d["responses"]) if d else None)
        stop_cluster_m14(voters + [learner])
    return {"all_nodes": all_arm, "standby": standby_arm, "baseline_rps": baseline,
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
    good = {"joined_at": 30.0, "refusals": {"h0": (0, 0, 0), "h1": (0, 0, 0), "h2": (0, 0, 0), "h3": (0, 0, 0)},
            "artifacts": {0: 1, 1: 1}, "artifact_bytes": {0: [4096], 1: [4096]},
            "installs": 2, "check_ok": True}
    expect("row f pass", verdict_row_f(good).passed)
    expect("row f fail on a refusal", not verdict_row_f({**good, "refusals": {**good["refusals"], "h3": (1, 0, 0)}}).passed)
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
        dir = "/opt/bench/uc/instance"

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
    # fsm_name (Task 9, FSM identity): row 0's default is "count", a spun row
    # is always "spin" regardless of position, and any other row is "fsm<i>".
    expect("fsm_name row 0 no spin -> count", fsm_name(0, 0) == "count")
    expect("fsm_name row 0 spun -> spin", fsm_name(0, 200) == "spin")
    expect("fsm_name row 1 no spin -> fsm1", fsm_name(1, 0) == "fsm1")
    expect("fsm_name row 1 spun -> spin", fsm_name(1, 200) == "spin")
    expect("fsm_name row 2 no spin -> fsm2", fsm_name(2, 0) == "fsm2")
    _na = node_args(_fh, 0, "0@h", [(0, 0), (1, 300)], None, False, 0)
    expect("node_args joins names, not row numbers",
           _na[_na.index("--services") + 1] == "count,spin")
    expect("node_args passes no cadence without purge", "--snapshot-interval-bytes" not in _na)
    # The cadence a purge row measures must reach the NODE role: since
    # 2.11.0 it is a replicated setting seeded at genesis, and `m12_gate`'s
    # `service` flag no longer configures one.
    _na_purge = node_args(_fh, 0, "0@h", [(0, 0)], None, True, M14_SNAPSHOT_INTERVAL_BYTES)
    expect("node_args carries M14_SNAPSHOT_INTERVAL_BYTES to the node role on a purge row",
           _na_purge[_na_purge.index("--snapshot-interval-bytes") + 1] == str(32 << 20))
    _sa_plain = service_args(_fh, 0, 0, 0)
    _sa_spin = service_args(_fh, 1, 300, 0)
    expect("service_args omits --work-spin at spin=0", "--work-spin" not in _sa_plain)
    expect("service_args --fsm count at spin=0", _sa_plain[_sa_plain.index("--fsm") + 1] == "count")
    expect("service_args passes --fsm spin + --work-spin when spun",
           _sa_spin[_sa_spin.index("--fsm") + 1] == "spin" and "--work-spin" in _sa_spin)
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
    # FSM identity: STATUS_RE/STATS_RE against literal lines copied from the
    # producers' own format strings (uc_ctl/src/main.rs's status println!,
    # m12_gate.rs's node-role stats println!) rather than re-derived text.
    _status_line = ("  row=1 name=spin version=unversioned hash=0x0123456789abcdef "
                     "attached=true epoch=5 incarnation=2 applied=1000 lag=50 "
                     "snapshot_pos=900 heartbeat_age=0.123s")
    _sm = STATUS_RE.search(_status_line)
    expect("STATUS_RE matches a literal uc2ctl status row", _sm is not None)
    if _sm:
        expect("STATUS_RE row/name/version/hash",
               (_sm.group(1), _sm.group(2), _sm.group(3), _sm.group(4)) == ("1", "spin", "unversioned", "0123456789abcdef"))
        expect("STATUS_RE attached/epoch/incarnation/applied/lag/snapshot_pos/heartbeat_age",
               (_sm.group(5), _sm.group(6), _sm.group(7), _sm.group(8), _sm.group(9), _sm.group(10), _sm.group(11))
               == ("true", "5", "2", "1000", "50", "900", "0.123s"))
    _stats_line = "m12_gate node 0 stats: reports_unattested=0 snap_refusals=(1,2,3)"
    _tm = STATS_RE.search(_stats_line)
    expect("STATS_RE matches a literal m12_gate node-role stats line", _tm is not None)
    if _tm:
        expect("STATS_RE unattested/legacy/identity/version",
               _tm.groups() == ("0", "1", "2", "3"))

    # ================================= time and timers (--tt-rows) =========
    print("  -- tt_fleet_gate leaf helpers --")
    fails += tt.selftest()
    print("  -- time-and-timers rows --")
    # Role args: the four new flags, on and off, through the SAME node_args /
    # service_args every arm builds — including the BASELINE arms, whose
    # m12_gate has none of them and which therefore run under tt_disabled().
    with tt_options(metrics_port=9310, timed=True, timers_per_sec=1000, state_bytes=1 << 20):
        _na_tt = node_args(_fh, 0, "0@h", [(0, 0)], None, False, 0)
        _sa_row0 = service_args(_fh, 0, 0, 0)
        _sa_row1 = service_args(_fh, 1, 0, 0)
        _sa_spin0 = service_args(_fh, 0, 300, 0)
        with tt_disabled():
            _na_base = node_args(_fh, 0, "0@h", [(0, 0)], None, False, 0)
            _sa_base = service_args(_fh, 0, 0, 0)
        _na_after = node_args(_fh, 0, "0@h", [(0, 0)], None, False, 0)
    _na_off = node_args(_fh, 0, "0@h", [(0, 0)], None, False, 0)
    expect("node_args carries --metrics-listen on the private NIC when a port is set",
           _na_tt[_na_tt.index("--metrics-listen") + 1] == f"{_fh.private_ip}:9310")
    expect("node_args passes no --metrics-listen at port 0 (the default TT)",
           "--metrics-listen" not in _na_off)
    expect("tt_disabled strips every T&T flag from the node role (the baseline arms)",
           "--metrics-listen" not in _na_base)
    expect("tt_options restores the outer options after a nested tt_disabled",
           _na_after == _na_tt)
    expect("service_args wraps row 0 in Timed<..> under --timed", "--timed" in _sa_row0)
    expect("service_args gives the timer load to row 0's `count` only",
           _sa_row0[_sa_row0.index("--timers-per-sec") + 1] == "1000"
           and "--timers-per-sec" not in _sa_row1)
    expect("service_args gives a spun row 0 (`spin`, not `count`) no timer load",
           "--timers-per-sec" not in _sa_spin0 and "--timed" in _sa_spin0)
    expect("service_args gives the row-h ballast to row 0 only",
           _sa_row0[_sa_row0.index("--state-bytes") + 1] == str(1 << 20)
           and "--state-bytes" not in _sa_row1)
    expect("tt_disabled strips every T&T flag from the service role",
           "--timed" not in _sa_base and "--timers-per-sec" not in _sa_base
           and "--state-bytes" not in _sa_base)
    _sa_plain_tt = service_args(_fh, 0, 0, 0)
    expect("service_args with T&T off is byte-identical to the pre-2026-09-07 shape",
           _sa_plain_tt == _sa_plain)
    # The arm map rows a/b/e re-use, and the fan-in convention.
    expect("tt_arm_fsms n1/n2eq/slow1/pair", (
        tt_arm_fsms("n1", 700) == [(0, 0)] and tt_arm_fsms("n2eq", 700) == [(0, 0), (1, 0)]
        and tt_arm_fsms("slow1", 700) == [(0, 700)]
        and tt_arm_fsms("pair", 700) == [(0, 0), (1, 700)]))
    expect("tt_fan_in only where two FSMs are declared",
           [tt_fan_in(x) for x in TT_RATE_ARMS] == [False, True, False, True])
    expect("the T&T A/B deliberately excludes the lockstep arms (M14 row e has no bar)",
           "n2eq-ls" not in TT_RATE_ARMS and "pair-ls" not in TT_RATE_ARMS)
    # Row a/b/e verdicts: the A/B reading plus (b/e) the late == 0 sweep.
    _base = {"n1": [1000.0, 1000.0, 1000.0], "pair": [500.0, 500.0, 500.0]}
    _head_ok = {"n1": [1002.0, 998.0, 1000.0], "pair": [501.0, 499.0, 500.0]}
    _head_bad = {"n1": [900.0, 900.0, 900.0], "pair": [500.0, 500.0, 500.0]}
    _clean = [("tt-b head n1 rep1", "h0", "count", "0", 0),
              ("tt-b head n1 rep1", "h1", "count", "0", 0)]
    expect("row a passes inside the recorded resolution",
           verdict_tt_a(_base, _head_ok, 1.0).passed)
    expect("row a fails a real regression outside the resolution",
           not verdict_tt_a(_base, _head_bad, 1.0).passed)
    expect("row a with no resolution recorded is not a pass",
           not verdict_tt_a(_base, _head_ok, None).passed)
    expect("row a is not a pass when the run is noisier than its own bar",
           not verdict_tt_a(_base, _head_ok, 0.001).passed)
    expect("row b passes with a clean late sweep",
           verdict_tt_b(_base, _head_ok, 1.0, _clean).passed)
    expect("row b fails on one late timer anywhere",
           not verdict_tt_b(_base, _head_ok, 1.0,
                            _clean + [("tt-b head pair rep2", "h2", "count", "0", 7)]).passed)
    expect("row b fails on an unreadable late clause (count -1)",
           not verdict_tt_b(_base, _head_ok, 1.0,
                            _clean + [("tt-b head n1 rep2", "h2", "?", "?", -1)]).passed)
    expect("row b fails on an EMPTY late sweep (absence of evidence is not evidence)",
           not verdict_tt_b(_base, _head_ok, 1.0, []).passed)
    expect("row b fails the throughput clause even with a clean sweep",
           not verdict_tt_b(_base, _head_bad, 1.0, _clean).passed)
    _tok = {"n1 rep1": True, "pair rep1": True}
    expect("row e passes with a converged table and a clean sweep",
           verdict_tt_e(_base, _head_ok, 1.0, _clean, table_ok=_tok).passed)
    expect("row e fails when the table never converged on every voter",
           not verdict_tt_e(_base, _head_ok, 1.0, _clean,
                            table_ok={**_tok, "pair rep1": False}).passed)
    expect("row e fails with no table result at all",
           not verdict_tt_e(_base, _head_ok, 1.0, _clean, table_ok={}).passed)
    # Row c: p99 <= 2 x the mean pass, over >= 10 000 fires.
    expect("row c passes at p99 = 2 x the mean pass exactly",
           verdict_tt_c(20000.0, 10000.0, 12000).passed)
    expect("row c fails just past 2 x the mean pass",
           not verdict_tt_c(20001.0, 10000.0, 12000).passed)
    expect("row c is inconclusive (not a pass) under 10 000 fires",
           not verdict_tt_c(1000.0, 10000.0, 9999).passed)
    expect("row c is inconclusive with no pass-length reading",
           not verdict_tt_c(1000.0, None, 12000).passed)
    expect("row c fails a p99 that landed in the +Inf bucket",
           not verdict_tt_c(float("inf"), 10000.0, 12000).passed)
    # row_c_reading over LITERAL scrapes: the arm with the most fires wins.
    _thin = tt.parse_prom(
        'uc2_timer_lateness_ns_bucket{service="count",row="0",le="10000"} 10\n'
        'uc2_timer_lateness_ns_bucket{service="count",row="0",le="+Inf"} 10\n'
        'uc2_timer_lateness_ns_count{service="count",row="0"} 10\n'
        'uc2_consensus_pass_ns_sum 1000\nuc2_consensus_pass_ns_count 100\n')
    _fat = tt.parse_prom(
        'uc2_timer_lateness_ns_bucket{service="count",row="0",le="10000"} 11000\n'
        'uc2_timer_lateness_ns_bucket{service="count",row="0",le="20000"} 12000\n'
        'uc2_timer_lateness_ns_bucket{service="count",row="0",le="+Inf"} 12000\n'
        'uc2_timer_lateness_ns_count{service="count",row="0"} 12000\n'
        'uc2_consensus_pass_ns_sum 2400000\nuc2_consensus_pass_ns_count 200000\n')
    _arm, _p99, _pass, _cnt, _per = row_c_reading([("n1", _thin), ("pair", _fat)])
    expect("row_c_reading adjudicates on the arm with the MOST fires",
           _arm == "pair" and _cnt == 12000.0)
    expect("row_c_reading reads the p99 off that arm's buckets", _p99 == 20000.0)
    expect("row_c_reading reads the mean pass length off the node histogram", _pass == 12.0)
    expect("row_c_reading keeps every arm's reading for the record", len(_per) == 2)
    expect("row_c_reading on no scrapes is all-None", row_c_reading([])[1] is None)
    # Row g: joined in time, a real snapshot install, the leader really
    # restarted, and one agreed set position cluster-wide.
    _g = {"joined_at": 30.0, "installs": 2, "leader_restarted": True,
          "set_positions": {"h0": 4096, "h1": 4096, "h2": 4096, "h3": 4096}}
    expect("row g passes", verdict_tt_g(_g).passed)
    expect("row g fails past the 60 s bar", not verdict_tt_g({**_g, "joined_at": 61.0}).passed)
    expect("row g fails when the join never completed",
           not verdict_tt_g({**_g, "joined_at": None}).passed)
    expect("row g fails a journal-replay join (no snapshot_installed)",
           not verdict_tt_g({**_g, "installs": 0}).passed)
    expect("row g fails when the snapshot sets disagree",
           not verdict_tt_g({**_g, "set_positions": {**_g["set_positions"], "h3": 2048}}).passed)
    expect("row g fails on an all-zero set position (nothing was ever complete)",
           not verdict_tt_g({**_g, "set_positions": {h: 0 for h in _g["set_positions"]}}).passed)
    expect("row g fails when the leader was NOT restarted (that is row f, not row g)",
           not verdict_tt_g({**_g, "leader_restarted": False}).passed)
    # Row h: only the STANDBY arm has a bar, and it is the measured pass length.
    _all = {"completed": True, "gap_secs": 4.0, "freeze_max": {"h0": 3.2}}
    _sb = {"completed": True, "gap_secs": 0.0, "freeze_max": {"h3": 3.4}}
    expect("row h passes when the standby instant did not stall commit",
           verdict_tt_h(_all, _sb, 8000.0).passed)
    expect("row h fails when the standby instant DID stall commit",
           not verdict_tt_h(_all, {**_sb, "gap_secs": 2.0}, 8000.0).passed)
    expect("row h does not judge the all-nodes gap (reported, no bar)",
           verdict_tt_h({**_all, "gap_secs": 30.0}, _sb, 8000.0).passed)
    expect("row h is inconclusive when an instant never completed",
           not verdict_tt_h({**_all, "completed": False}, _sb, 8000.0).passed)
    expect("row h is inconclusive with no measured pass length",
           not verdict_tt_h(_all, _sb, None).passed)
    # fold_freeze: the gauge is per row and resets, so the fold keeps the max.
    _acc = {}
    fold_freeze("h0", tt.parse_prom(
        'uc2_snapshot_freeze_seconds_max{service="count",row="0"} 1.5\n'
        'uc2_snapshot_freeze_seconds_max{service="spin",row="1"} 3.25\n'), _acc)
    expect("fold_freeze takes the largest row on a host", _acc == {"h0": 3.25})
    fold_freeze("h0", tt.parse_prom(
        'uc2_snapshot_freeze_seconds_max{service="count",row="0"} 0\n'), _acc)
    expect("fold_freeze keeps the max across polls (the gauge resets to 0)",
           _acc == {"h0": 3.25})
    expect("fold_freeze on a scrape without the family leaves the accumulator alone",
           fold_freeze("h1", {}, dict(_acc)) == _acc)
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
    ap.add_argument("--rows", default="abcdef",
                    help="subset of a b c d e f — the M14 gate's own rows (c runs with every "
                         "arm). The time-and-timers rows are a SEPARATE namespace on --tt-rows; "
                         "pass --rows '' to run only those.")
    # ------------------------------------------------ time and timers rows
    ap.add_argument("--tt-rows", default="",
                    help="subset of a b c e g h — the fleet rows of "
                         "docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md, a separate "
                         "namespace from --rows: a = Timed<..> services, no timers, A/B'd "
                         "against --base-tree; b = the same with --timers-per-sec sustained, "
                         "plus uc2_timers_late_total == 0 everywhere; c = timer precision "
                         "(p99 lateness vs 2 x the mean consensus pass) off row b's arms; "
                         "e = the same arms with a live --schedule-table; g = row f's join "
                         "with the LEADER's node restarted mid-window; h = an all-nodes then "
                         "a --standby snapshot instant on a --state-bytes FSM under load")
    ap.add_argument("--metrics-port", type=int, default=tt.METRICS_PORT_DEFAULT,
                    help="every node this driver starts gets --metrics-listen "
                         "<private_ip>:PORT and the T&T rows scrape it (0 = do not pass the "
                         "flag; the baseline tree's arms always run without it)")
    ap.add_argument("--timed", action="store_true",
                    help="wrap every service in uc_service::Timed<..> (--timed on the service "
                         "role); rows a/b/c/e are defined on the wrapped service and require it")
    ap.add_argument("--timers-per-sec", type=int, default=0,
                    help="FSM 0 sustains this many timer fires/s through the window (the gate's "
                         "number for rows b and c is 1000); applied to row 0's `count` service "
                         "only, since the Rust contract allows it only with --fsm count")
    ap.add_argument("--schedule-table", type=int, default=0,
                    help=f"row e: apply this many [[schedule]] entries (the gate's number is "
                         f"{tt.MAX_SCHEDULE_ENTRIES} = MAX_SCHEDULE_ENTRIES) on row 0 before "
                         f"each arm's client starts; 0 = off")
    ap.add_argument("--state-bytes", type=int, default=tt.ROW_H_STATE_BYTES,
                    help="row h: the ballast row 0's FSM carries into its snapshot (the gate's "
                         "number is 256 MiB); applied by row h's arm only")
    ap.add_argument("--base-tree", default="",
                    help="rows a and b: local checkout of the PRE-time-and-timers binary "
                         "(17d5c6b), rsynced to /opt/bench/uc-base and built there; its arms "
                         "are the A of the interleaved A/B")
    ap.add_argument("--ab-reps", type=int, default=3,
                    help="interleaved reps per A/B arm (rows a, b and e)")
    ap.add_argument("--resolution-pct", type=float, default=None,
                    help="the same-source rebuild resolution measured by scripts/hop1_ab.sh on "
                         "the rig ON THE DAY — rows a/b/e's bar. Required with --base-tree; "
                         "record it BEFORE comparing anything to it")
    ap.add_argument("--pass-ns", type=float, default=0.0,
                    help="row h: the consensus pass length in ns, when row c is not being run "
                         "in the same invocation (0 = take it from row c)")
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
    # ------------------------------------------------ T&T door checks
    # Every one of these would otherwise surface AFTER a fleet trip had been
    # spent, as an unadjudicable row. The gate doc's own "When this gate is
    # run" order (record the resolution first, measure the pass length first)
    # is what they enforce.
    global TT
    unknown = sorted(set(a.tt_rows) - set("abcegh"))
    if unknown:
        ap.error(f"--tt-rows: unknown row letter(s) {unknown} (the T&T rows are a b c e g h)")
    if any(r in a.tt_rows for r in "ab") and not a.base_tree:
        ap.error("--tt-rows a/b are an A/B against the pre-time-and-timers binary: "
                 "--base-tree is required")
    if a.base_tree and a.resolution_pct is None:
        ap.error("--base-tree without --resolution-pct: rows a/b/e are judged against the "
                 "same-source rebuild resolution scripts/hop1_ab.sh measures ON THE DAY, and "
                 "the gate doc requires it recorded BEFORE anything is compared to it")
    if any(r in a.tt_rows for r in "abce") and not a.timed:
        ap.error("--tt-rows a/b/c/e are defined on Timed<..>-wrapped services: --timed is required")
    if any(r in a.tt_rows for r in "bc") and a.timers_per_sec <= 0:
        ap.error("--tt-rows b/c need a sustained timer load: pass --timers-per-sec "
                 "(the gate's number is 1000)")
    if "e" in a.tt_rows:
        if a.schedule_table <= 0:
            ap.error(f"--tt-rows e needs --schedule-table N (the gate's number is "
                     f"{tt.MAX_SCHEDULE_ENTRIES} = MAX_SCHEDULE_ENTRIES)")
        if a.schedule_table > tt.MAX_SCHEDULE_ENTRIES:
            ap.error(f"--schedule-table {a.schedule_table} exceeds MAX_SCHEDULE_ENTRIES "
                     f"({tt.MAX_SCHEDULE_ENTRIES})")
        if "a" not in a.tt_rows:
            ap.error("--tt-rows e compares against row a's OWN head rates (the same binary "
                     "with and without a live table), so row a must run in the same invocation")
    if "c" in a.tt_rows and "b" not in a.tt_rows:
        ap.error("--tt-rows c reads its histograms off row b's arms: run b in the same invocation")
    if "h" in a.tt_rows and "c" not in a.tt_rows and a.pass_ns <= 0:
        ap.error("--tt-rows h is judged against the consensus pass length measured on the day: "
                 "run row c in the same invocation, or pass --pass-ns")
    # The role flags every HEAD arm carries. Timer load and the row-h ballast
    # are narrowed per arm (`tt_options`), so the M14 rows never carry either.
    TT = TtOpts(metrics_port=a.metrics_port, timed=a.timed)
    print(f"INFO time-and-timers options: {TT}, tt_rows={a.tt_rows or '(none)'}, "
          f"ab_reps={a.ab_reps}, resolution_pct={a.resolution_pct}", flush=True)
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
    tt_a = tt_b = tt_e = tt_g = tt_h = None
    base_voters = None
    try:
        if any(r in a.rows for r in "abe"):
            K = arm_rates(voters, a, rates, checks, pins=pins)
        else:
            K = a.k
        if "d" in a.rows:
            kill = arm_kill(voters, a, K, checks, pins=pins)
        if "f" in a.rows:
            join = arm_join(voters, learner, a, K, checks, pins=pins)
        if a.tt_rows:
            if a.base_tree:
                prepare_base_tree(hosts, a.base_tree)
                base_voters = base_fleet_hosts(a)[:3]
            if any(r in a.tt_rows for r in "abce"):
                K = ensure_k(voters, a, rates, checks, pins=pins)
                print(f"INFO T&T rows: slow FSM K = {K}", flush=True)
            if "a" in a.tt_rows:
                tt_a = arm_tt_ab(voters, base_voters, a, K, checks, 0, "tt-a", pins=pins)
            if "b" in a.tt_rows:
                tt_b = arm_tt_ab(voters, base_voters, a, K, checks, a.timers_per_sec,
                                 "tt-b", pins=pins)
            if "e" in a.tt_rows:
                tt_e = arm_tt_e(voters, a, K, checks, pins=pins)
            if "g" in a.tt_rows:
                tt_g = arm_join(voters, learner, a, K, checks, pins=pins,
                                restart_leader_after=2.0)
            if "h" in a.tt_rows:
                tt_h = arm_tt_h(voters, learner, a, checks, pins=pins)
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
    # ------------------------------------------------ time and timers rows
    pass_ns = a.pass_ns or None
    if tt_a is not None:
        verdicts.append(verdict_tt_a(tt_a["base"], tt_a["head"], a.resolution_pct))
    if tt_b is not None:
        verdicts.append(verdict_tt_b(tt_b["base"], tt_b["head"], a.resolution_pct, tt_b["late"]))
        if "c" in a.tt_rows:
            arm, p99_ns, mean_pass_ns, count, per_arm = row_c_reading(tt_b["scrapes"])
            print("\ntt-c per-arm readings (adjudicated on the arm with the most fires)")
            for r in per_arm:
                print(f"  {r['arm']:28s} fires={r['count']} p99={r['p99_ns']} ns "
                      f"p50={r['p50_ns']} ns max={r['lateness_max_ns']} ns | mean pass="
                      f"{r['pass_ns']} ns p99 pass={r['pass_p99_ns']} ns max={r['pass_max_ns']} ns")
            print(f"  adjudicated on: {arm}")
            pass_ns = mean_pass_ns or pass_ns
            verdicts.append(verdict_tt_c(p99_ns, mean_pass_ns, count))
    if tt_e is not None:
        # Row e's comparison is row a's OWN head rates against row e's — the
        # same binary with and without a live table.
        verdicts.append(verdict_tt_e((tt_a or {}).get("head", {}), tt_e["rates"],
                                     a.resolution_pct, tt_e["late"], table_ok=tt_e["table_ok"]))
    if tt_g is not None:
        verdicts.append(verdict_tt_g(tt_g))
    if tt_h is not None:
        verdicts.append(verdict_tt_h(tt_h["all_nodes"], tt_h["standby"], pass_ns))
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
