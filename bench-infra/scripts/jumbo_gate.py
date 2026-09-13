#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""Jumbo-frame path-MTU discovery — fleet gate driver.

The pre-committed bars this driver adjudicates live in
`docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md` (renamed to
`-<run date>.md` when it actually runs), copied VERBATIM from
`docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md` §10,
with the two plan-1 errata folded in (errata 1: a permanently narrow path
makes every node probe forever, so row b's `uc2_probe_sent_total` is expected
to KEEP CLIMBING, never a leak; errata 4: a solo cluster never raises its
rung, so row a needs >= 2 nodes before any raise is observable) and row d's
refusal names corrected to what plan 2's Task 2 actually ships
(`jumbo_path_too_narrow`, `jumbo_peer_silent`, `path_below_committed_mtu` —
lower snake_case `obs_event!` reason strings, not the spec table's
CamelCase enum-variant prose).

Rows a-d are fleet rows (row c also carries UC's own path-MTU ladder as a
pre-arm blackhole probe, spec §10's closing paragraph). Rows e and f are
**reported, no bar** (`m5_gate` on the fleet paired against the base tree,
and `scripts/hop1_ab.sh` dev-box smoke respectively) — CLAUDE.md
"Benchmarking discipline": a dev-box run is smoke, never a gate, and a rate
bar this rig cannot resolve is reported honestly rather than forced.

`--selftest` exercises every row's PURE arithmetic — the required-pairs rule,
the >=15%/-3% envelope-map comparison, the 10 s adoption window, the 30 s
force-gate window and refusal-naming rule, and the path-MTU blackhole-probe
abort — against canned inputs. No fleet, no ssh, no cargo; modelled on
`bench-infra/scripts/m13_hop_bench.py --selftest` and the paired-statistic
module `bench-infra/scripts/tt_fleet_gate.py`.

`--arms {a,b,c,d,e,f}` (comma-separated, e.g. `--arms a,b,d`) runs the named
arms on a real fleet, reusing `m12_fleet_gate.py`'s ssh/systemd-run plumbing
and `tt_fleet_gate.py`'s Prometheus-text reader rather than reinventing
either. No fleet arm runs as a side effect of `--selftest`, and this driver
never runs anything by itself: it is the pre-commitment plan 2 Task 5 exists
to write, not the run — that is a separate, user-gated fleet trip.
"""

import argparse
import json
import math
import statistics
import sys
import time
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
import tt_fleet_gate as tt  # noqa: E402
import m14_fleet_gate as m14  # noqa: E402
from m12_fleet_gate import ssh, start_unit, kill_unit, tail_log, truncate_log, Verdict  # noqa: E402

# ------------------------------------------------------------------ knobs
#
# Every constant below is either the spec's own value (§4.1's RUNGS, §6's
# JUMBO_GATE_WINDOW) or copied verbatim from spec §10's bar table / the
# envelope-map brief §6. None is overridable from the command line — a bar
# you can move from the shell is not a bar.

RUNGS = (1408, 8832, 8960)
MTU_DEFAULT = RUNGS[0]
JUMBO_MIN_RUNG = RUNGS[1]           # spec §4.1: "jumbo" for force_jumbo_frames
TOP_RUNG = RUNGS[-1]                # spec §4.1: MTU_BOUND, the AWS IPv4 rung

ROW_A_ADOPTION_WINDOW_SECS = 10.0   # row a: within 10 s of the last node's start
ROW_A_MIN_REPS = 3                  # row a: 3 of 3 reps
ROW_A_MIN_NODES = 2                 # errata 4: a solo cluster never raises

ROW_B_RUNG_BAR_PCT = -3.0           # row b: 64 B throughput within -3%
ROW_B_MIN_PAIRS = 5                 # row b: "minimum 5 pairs"
ROW_B_NARROW_MTU = 1500             # row b/d: the interface MTU forced by ssh
ROW_B_SAMPLE_SECS = 60.0            # row b: how long the three series are sampled
ROW_B_PRELIM_REPS = 4               # row b: base-tree reps that fix the pair count
ROW_B_PAIRS_MAX_DEFAULT = 12        # row b: cap on what required_pairs may ask for
ROW_D_POLL_SECS = 45.0              # row d: how long to wait for a refusal (bar is 30)

ROW_C_PLATEAU_BAR_PCT = 15.0        # envelope-map brief §6: >= 15% over standard
ROW_C_RUNG_BAR_PCT = 3.0            # envelope-map brief §6: within -3% (magnitude)
ROW_C_BORDERLINE_LO_PCT = 10.0      # brief §6: 10-20% on the plateau clause is
ROW_C_BORDERLINE_HI_PCT = 20.0      # "resolved only by a re-run ... never by local smoke"
ROW_C_BLACKHOLE_WINDOW_SECS = 30.0  # spec §10 closing paragraph

ROW_D_FORCE_WINDOW_SECS = 30.0      # spec §6 JUMBO_GATE_WINDOW / row d's bar
ROW_D_FORCE_REASON_NARROW = "jumbo_path_too_narrow"
ROW_D_FORCE_REASON_SILENT = "jumbo_peer_silent"
ROW_D_JOIN_REASON = "path_below_committed_mtu"   # named for completeness only —
                                                  # row d does not exercise this gate

# Process exit codes (fix round 1: a verdict must reach the exit code, and
# "not run" must be distinct from "passed" — see `exit_code_for_results`).
# FAIL takes precedence over NOT-RUN, which takes precedence over PASS, so a
# wrapper reading `$?` alone still gets the worst finding across every arm.
#
# NOT-RUN is 3, not 2, deliberately: argparse exits 2 on a USAGE error (a bad
# `--arms`, an unknown flag), so a 2 would have made "you typed it wrong" and
# "an arm produced no verdict" the same code to any wrapper reading `$?`.
EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_USAGE = 2  # argparse's own, never returned by this driver
EXIT_NOT_RUN = 3

METRICS_PORT_DEFAULT = tt.METRICS_PORT_DEFAULT

# Jumbo needs a REAL `uc2-node` daemon configured by a TOML file, not a gate
# harness's CLI-flag `node` role (m9_fleet_gate.py's reasoning for the same
# choice: force_jumbo_frames is a node.toml/env key, not something a harness
# role can set any other way). `m12.BUILT_GATE` below is reused only for its
# `probe`/`ctl` roles via `m6.build_fleet_hosts`, never started as the node.
UC_NODE_BUILT_DEFAULT = "/opt/bench/uc/target/release/uc2-node"


# =========================================================== row arithmetic
#
# Every function below is a PURE transformation of a small evidence dict/list
# into a `Verdict` (row, passed, detail) or a plain `(ok, detail)` pair, so
# `selftest()` can adjudicate canned inputs with no fleet, no ssh, no cargo —
# the m13_hop_bench.py idiom.


def required_pairs(observed_n, observed_stat_pct, target_stat_pct, min_pairs=5):
    """The rep-count rule (CLAUDE.md's 2026-08-31 core-count-sweep lesson —
    "fix a spread bar's rep count from observed arm-to-arm variance" — applied
    the same way the time-and-timers gate derived its "~430 reps" note: a
    spread/sem statistic falls as `1/sqrt(n)`, so resolving an
    `observed_stat_pct` measured at `observed_n` reps down to a
    `target_stat_pct` bar needs

        observed_n * (observed_stat_pct / target_stat_pct) ** 2

    reps, rounded up, never fewer than `min_pairs` (row b's floor). A
    non-positive `observed_stat_pct` (a perfectly quiet preliminary run) is
    already at or below any target, so the floor alone applies."""
    if observed_stat_pct <= 0:
        return min_pairs
    needed = math.ceil(observed_n * (observed_stat_pct / target_stat_pct) ** 2)
    return max(needed, min_pairs)


def spread_stat_pct(rates):
    """The base tree's own arm-to-arm spread as a percentage: sem / mean. This
    is the `observed_stat_pct` that `required_pairs` turns into a pair count
    (the 2026-08-31 lesson: fix the count from what the rig actually did,
    never from a hoped-for noise figure)."""
    if len(rates) < 2:
        return None
    mean = statistics.fmean(rates)
    if mean <= 0:
        return None
    sem = statistics.stdev(rates) / math.sqrt(len(rates))
    return 100.0 * sem / mean


def paired_delta_pct(base_rate, head_rate):
    """One interleaved pair's delta, head relative to base, in percent."""
    return 100.0 * (head_rate - base_rate) / base_rate


def refusal_from_log(text, reasons):
    """The FIRST obs record in `text` whose `event` is one of `reasons`, as
    `(reason, ts_ns, peer)` — or `None`. The daemon writes one JSON object per
    line (`uc_obs`); a fail-stop's reason record carries `peer`."""
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("event") in reasons:
            return rec["event"], int(rec.get("ts_ns", 0)), str(rec.get("peer", ""))
    return None


def paired_stats(deltas_pct):
    """`(mean, sem)` of a list of per-pair deltas, in percentage points.
    `sem` is `None` below 2 samples (the same convention as
    `tt_fleet_gate.ab_stats`'s `paired.sem_pp`)."""
    n = len(deltas_pct)
    mean = statistics.mean(deltas_pct) if n else None
    sd = statistics.stdev(deltas_pct) if n > 1 else 0.0
    sem = (sd / math.sqrt(n)) if n > 1 else None
    return mean, sem


# --------------------------------------------------------------------- row a

def verdict_row_a(reps, window_secs=ROW_A_ADOPTION_WINDOW_SECS, top_rung=TOP_RUNG,
                   min_reps=ROW_A_MIN_REPS, min_nodes=ROW_A_MIN_NODES):
    """`reps`: one dict per repetition,
    `{"last_start_ns": int, "nodes": {name: {"mtu_bytes": int, "observed_ns": int}}}`.

    Bar (spec §10 row a): every node reports `uc2_datagram_mtu_bytes = 8960`
    within 10 s of the LAST node's start, 3 of 3 reps. Errata 4: a solo
    cluster never raises (`ProbeTable::table_min(&[])` is `None`, not
    `Some(MTU_BOUND)`), so a rep with fewer than 2 nodes is not evidence of
    anything and fails outright rather than passing vacuously."""
    row = f"a every node adopts {top_rung} within {window_secs:.0f}s of the last start, {min_reps}/{min_reps} reps"
    if len(reps) < min_reps:
        return Verdict(row, False, f"only {len(reps)} rep(s), need {min_reps}")
    bad = []
    for i, rep in enumerate(reps[:min_reps] if len(reps) > min_reps else reps):
        nodes = rep.get("nodes", {})
        if len(nodes) < min_nodes:
            bad.append(f"rep {i}: {len(nodes)} node(s) < {min_nodes} "
                       f"(errata 4 — a solo cluster never raises; no evidence)")
            continue
        last_start = rep["last_start_ns"]
        for name, info in sorted(nodes.items()):
            dt_secs = (info["observed_ns"] - last_start) / 1e9
            if dt_secs < 0:
                bad.append(f"rep {i}/{name}: observed before the last node's start "
                           f"({dt_secs:.3f}s)")
            elif dt_secs > window_secs:
                bad.append(f"rep {i}/{name}: adopted at {dt_secs:.3f}s > {window_secs:.0f}s")
            if info["mtu_bytes"] != top_rung:
                bad.append(f"rep {i}/{name}: mtu_bytes={info['mtu_bytes']} != {top_rung}")
    detail = "; ".join(bad) if bad else f"{len(reps)}/{len(reps)} reps, every node at {top_rung} within {window_secs:.0f}s"
    return Verdict(row, not bad, detail)


# --------------------------------------------------------------------- row b

def verdict_row_b(mtu_samples, emsgsize_samples, probe_sent_series,
                   paired_deltas_pct, base_prelim_n, base_prelim_stat_pct,
                   bar_pct=ROW_B_RUNG_BAR_PCT, min_pairs=ROW_B_MIN_PAIRS,
                   expected_mtu=MTU_DEFAULT):
    """The 1500 B arm, four clauses (spec §10 row b + errata 1):

    1. `mtu_samples`: every node's `uc2_datagram_mtu_bytes` reading, every
       sample, stays at `expected_mtu` (1408) — the rung never rises on a
       path that cannot carry a jumbo rung.
    2. `emsgsize_samples`: `uc2_send_emsgsize_total` is 0 throughout (spec
       §4.3: probe EMSGSIZEs count only in `uc2_probe_sent_total`, never
       here — a non-zero reading here is a DATA send refused for size, which
       cannot happen at the baseline rung).
    3. `probe_sent_series`: `uc2_probe_sent_total` over the run — errata 1
       says a permanently narrow path makes every node probe every peer
       forever (2-3 datagrams/30s/peer), so this series is expected to be
       NON-DECREASING with a NET INCREASE over the window, and that is not a
       leak; a series that goes flat is the actual anomaly (probing stopped,
       which should never happen on an unresolved peer).
    4. `paired_deltas_pct`: the 64 B throughput paired delta against the base
       tree (same construction as `tt_fleet_gate.ab_reading`'s paired
       statistic) must clear `bar_pct` (-3%, i.e. no more than 3% slower),
       decisively — `required_pairs` sets the floor on how many pairs the
       run needs, from the base tree's own preliminarily observed spread,
       and the paired mean must clear the bar by at least 2*sem to count as
       resolved rather than noise-inconclusive.
    """
    row = "b 1500 B arm: rung pinned, no EMSGSIZE, probes climb (errata 1), throughput within -3%"
    bad = []

    offenders = {n: v for n, v in mtu_samples.items() if v != expected_mtu} \
        if isinstance(mtu_samples, dict) else \
        [v for v in mtu_samples if v != expected_mtu]
    if offenders:
        bad.append(f"rung rose above {expected_mtu}: {offenders}")

    if any(v != 0 for v in emsgsize_samples):
        bad.append(f"uc2_send_emsgsize_total nonzero: {[v for v in emsgsize_samples if v != 0]}")

    if len(probe_sent_series) >= 2:
        non_decreasing = all(b >= a for a, b in zip(probe_sent_series, probe_sent_series[1:]))
        net_increase = probe_sent_series[-1] > probe_sent_series[0]
        if not non_decreasing:
            bad.append(f"uc2_probe_sent_total decreased mid-run: {probe_sent_series}")
        elif not net_increase:
            bad.append("uc2_probe_sent_total went flat — errata 1 says a narrow path "
                       f"probes forever, so a flat series is the anomaly: {probe_sent_series}")

    need = required_pairs(base_prelim_n, base_prelim_stat_pct, abs(bar_pct), min_pairs)
    n = len(paired_deltas_pct)
    if n < need:
        bad.append(f"only {n} pair(s), need >= {need} (from base spread "
                   f"{base_prelim_stat_pct:.3f}% at n={base_prelim_n})")
    else:
        mean, sem = paired_stats(paired_deltas_pct)
        if sem is None:
            bad.append("cannot compute sem with < 2 pairs")
        elif abs(mean - bar_pct) < 2 * sem:
            bad.append(f"inconclusive (noisy): mean {mean:.3f}% within 2*sem "
                       f"({sem:.3f}%) of the {bar_pct:.0f}% bar")
        elif mean < bar_pct:
            bad.append(f"throughput {mean:.3f}% worse than the {bar_pct:.0f}% bar "
                       f"(sem {sem:.3f}%)")

    detail = "; ".join(bad) if bad else \
        f"rung pinned at {expected_mtu}, 0 EMSGSIZE, probes climbing " \
        f"({probe_sent_series[0]}->{probe_sent_series[-1]}), " \
        f"{n} pairs (>= {need}) within {bar_pct:.0f}%"
    return Verdict(row, not bad, detail)


# --------------------------------------------------------------------- row c

def verdict_row_c(plateau_delta_pct, rung_throughput_delta_pct, rung_p99_delta_pct,
                   plateau_bar_pct=ROW_C_PLATEAU_BAR_PCT, rung_bar_pct=ROW_C_RUNG_BAR_PCT,
                   borderline_lo=ROW_C_BORDERLINE_LO_PCT, borderline_hi=ROW_C_BORDERLINE_HI_PCT):
    """The envelope-map brief §6 disposition, quoted in the gate doc verbatim:
    "Jumbo MTU (9001) becomes the recommended fleet configuration in the
    runbook iff both hold: the jumbo arm's soak-sustained bytes-bound plateau
    exceeds the standard arm's by >= 15%, and the jumbo 64 B rung is within
    -3% of the standard 64 B rung (throughput and p99). Otherwise jumbo
    remains a documented knob." "Within -3%" reads, for each metric, as "no
    worse than 3% in that metric's bad direction": throughput must not be
    more than 3% LOWER (`rung_throughput_delta_pct >= -rung_bar_pct`), p99
    latency must not be more than 3% HIGHER
    (`rung_p99_delta_pct <= rung_bar_pct`) — both deltas defined as
    `(jumbo - standard) / standard * 100`.

    This decides whether the runbook RECOMMENDS jumbo — it never decides
    whether the feature ships (spec §10). Borderline (10-20% on the plateau
    clause) is called out separately per the brief: "resolved only by a
    re-run or treated as not justified — never by local smoke.\""""
    row = "c envelope-map brief §6: jumbo recommended in the runbook iff plateau >= 15% and rung within -3%"
    plateau_ok = plateau_delta_pct >= plateau_bar_pct
    throughput_ok = rung_throughput_delta_pct >= -rung_bar_pct
    p99_ok = rung_p99_delta_pct <= rung_bar_pct
    recommend = plateau_ok and throughput_ok and p99_ok
    borderline = borderline_lo <= plateau_delta_pct < borderline_hi and not plateau_ok
    detail = (f"plateau {plateau_delta_pct:+.2f}% (bar >= {plateau_bar_pct:.0f}%), "
              f"rung throughput {rung_throughput_delta_pct:+.2f}% (bar >= {-rung_bar_pct:.0f}%), "
              f"rung p99 {rung_p99_delta_pct:+.2f}% (bar <= {rung_bar_pct:.0f}%) "
              f"-> {'RECOMMEND jumbo' if recommend else 'jumbo stays a documented knob'}")
    if borderline:
        detail += (" — BORDERLINE on the plateau clause "
                   f"({borderline_lo:.0f}-{borderline_hi:.0f}%): resolved only by a "
                   "re-run or treated as not justified, never by local smoke")
    # This row never fails the gate outright (it decides a doc recommendation,
    # not shipping); `passed` records whether jumbo is recommended, and the
    # detail carries the full reasoning either way.
    return Verdict(row, recommend, detail)


def check_blackhole_probe(observed_mtu_by_node, window_secs=ROW_C_BLACKHOLE_WINDOW_SECS,
                           min_rung=JUMBO_MIN_RUNG, baseline=MTU_DEFAULT,
                           expected=None):
    """Row c's pre-arm gate (spec §10 closing paragraph): with do-not-fragment
    in place (§4.3), UC's own probe ladder IS the path-MTU blackhole probe.
    `observed_mtu_by_node`: `{name: mtu_bytes}` sampled after `window_secs`.
    Returns `(ok, message)`; `ok=False` means "ABORT THE ARM LOUDLY" — a
    jumbo arm whose nodes still report the baseline after the window is not
    a slow-converging cluster, it is infrastructure that never carried jumbo
    at all, and running the soak on it would silently measure the baseline.

    `expected`: every host name the arm SHOULD have heard from (row c passes
    its whole host list). A name absent from `observed_mtu_by_node` — or
    present with `None` — never answered `/metrics` at all: a crashed node, a
    wedged one, an unreachable host. That is a FAILURE, not a pass. Judging
    only what was PRESENT (the shape this had before) let such a host drop out
    of the stuck set entirely, so a two-of-three arm with one dead node read
    as "every node cleared the rung" and the soak ran on it. An empty
    measurement is likewise an abort: nothing reporting is not everything
    passing."""
    names = list(expected) if expected is not None else list(observed_mtu_by_node)
    missing = sorted(n for n in names if observed_mtu_by_node.get(n) is None)
    stuck = {n: v for n, v in observed_mtu_by_node.items()
             if v is not None and v < min_rung}
    if not names:
        return False, (f"ABORT: after {window_secs:.0f}s, no host reported "
                       "uc2_datagram_mtu_bytes at all — nothing was measured; "
                       "do not run the soak")
    if missing or stuck:
        why = []
        if missing:
            why.append(f"{missing} never reported uc2_datagram_mtu_bytes "
                       "(/metrics unreachable — a dead or wedged node, not a "
                       "narrow path)")
        if stuck:
            why.append(f"{stuck} still below the jumbo rung {min_rung} "
                       f"(baseline {baseline}) — the arm's path is a blackhole, "
                       "not a slow cluster")
        return False, (f"ABORT: after {window_secs:.0f}s, " + "; ".join(why)
                       + "; do not run the soak")
    return True, f"every node cleared {min_rung} within {window_secs:.0f}s: {observed_mtu_by_node}"


# --------------------------------------------------------------------- row d

def verdict_row_d(force_arm_refusals, silent_arm_refusals,
                   window_secs=ROW_D_FORCE_WINDOW_SECS,
                   narrow_reason=ROW_D_FORCE_REASON_NARROW,
                   silent_reason=ROW_D_FORCE_REASON_SILENT):
    """Two arms of `force_jumbo_frames` (spec §10 row d; the `Forcing` gate
    only — row d does not exercise the `Joining`/`path_below_committed_mtu`
    gate at all, so that name is documented in this driver and the gate doc
    for completeness, never adjudicated here).

    `force_arm_refusals`: the 1500 B arm, `{node: {"reason": str,
    "elapsed_secs": float, "peer": str}}` — ALL THREE nodes must refuse
    `jumbo_path_too_narrow` naming a peer within `window_secs`.

    `silent_arm_refusals`: the 9001 arm with one node held down, same shape
    — the TWO LIVE nodes must refuse `jumbo_peer_silent` naming the held-down
    third node, within `window_secs`.

    Refusal-name note (as-built, not spec's pre-fix wording — plan 2 Task 2,
    `progress.md` rulings + `task-2-report.md`): the code emits the
    lower-`snake_case` `obs_event!` reason string, not spec §10's table
    CamelCase (`JumboPathTooNarrow`/`JumboPeerSilent`), which mirrors §6's
    Rust enum-variant prose rather than what ships on the wire/in the log."""
    row = ("d force gate: 1500B arm all-three jumbo_path_too_narrow naming a peer; "
           "9001-minus-one arm two-live jumbo_peer_silent naming the third, both <= 30s")
    bad = []

    if len(force_arm_refusals) != 3:
        bad.append(f"1500B arm: {len(force_arm_refusals)} refusal(s), need 3")
    for node, r in sorted(force_arm_refusals.items()):
        if r["reason"] != narrow_reason:
            bad.append(f"1500B/{node}: reason={r['reason']!r} != {narrow_reason!r}")
        if r["elapsed_secs"] > window_secs:
            bad.append(f"1500B/{node}: {r['elapsed_secs']:.1f}s > {window_secs:.0f}s")
        if not r.get("peer"):
            bad.append(f"1500B/{node}: no peer named")

    if len(silent_arm_refusals) != 2:
        bad.append(f"9001-minus-one arm: {len(silent_arm_refusals)} refusal(s), need 2")
    named_peers = set()
    for node, r in sorted(silent_arm_refusals.items()):
        if r["reason"] != silent_reason:
            bad.append(f"9001/{node}: reason={r['reason']!r} != {silent_reason!r}")
        if r["elapsed_secs"] > window_secs:
            bad.append(f"9001/{node}: {r['elapsed_secs']:.1f}s > {window_secs:.0f}s")
        if not r.get("peer"):
            bad.append(f"9001/{node}: no peer named")
        else:
            named_peers.add(r["peer"])
    if len(named_peers) > 1:
        bad.append(f"9001 arm: live nodes named different peers {named_peers}, "
                   "expected both to name the same silent third node")

    detail = "; ".join(bad) if bad else \
        f"1500B: 3/3 {narrow_reason}; 9001-minus-one: 2/2 {silent_reason} naming {named_peers}"
    return Verdict(row, not bad, detail)


# ------------------------------------------------------------- rows e and f
#
# No bar (spec §10): both are REPORTED. These helpers only format the number
# and its resolution — there is nothing to pass or fail.

def report_row_e(paired_delta_pct, sem_pp, resolution_pct):
    return (f"row e (appender relaxed load, m5_gate on the fleet, standard arm, "
           f"paired against the base tree): {paired_delta_pct:+.3f}% "
           f"(sem {sem_pp:.3f}pp) against a same-source rebuild resolution of "
           f"{resolution_pct:.3f}% — REPORTED, NO BAR (spec §10: a rate bar for "
           "a one-load change cannot be resolved by this rig)")


def report_row_f(ratio_pct, control_ratio_pct):
    return (f"row f (client-hop cost of the per-submit cnc load, "
           f"scripts/hop1_ab.sh): {ratio_pct:+.3f}% against a same-source "
           f"rebuild control of {control_ratio_pct:+.3f}% — dev-box SMOKE, "
           "REPORTED, NO BAR")


# ============================================================== exit code
#
# Fix round 1 (Important 1): no arm's verdict used to reach the process exit
# code, so `--fleet --arms b,c,d` (three print-only stubs) and `--arms a`
# with a genuine miss were both invisible to `$?` — a wrapper or a human
# reading only the exit code could not tell a no-op stub run from a passing
# gate. `exit_code_for_results` is the pure mapping from "one outcome per
# requested arm" to a process exit code, so `--selftest` can pin the mapping
# itself with no fleet involved.

def exit_code_for_results(results):
    """`results`: `{arm: Verdict | None}`, one entry per REQUESTED arm.
    `None` means "no verdict was produced" (a stub, or a probe that aborted
    before producing one) — NOT-RUN, distinct from both PASS and FAIL.

    Precedence, worst finding wins, so `$?` alone carries the whole story:

        any Verdict with passed=False  -> EXIT_FAIL (1)
        else any arm is None           -> EXIT_NOT_RUN (3)
        else (every arm passed, or none were requested) -> EXIT_PASS (0)

    2 is argparse's usage-error code and is never returned here.
    """
    if any(v is not None and not v.passed for v in results.values()):
        return EXIT_FAIL
    if any(v is None for v in results.values()):
        return EXIT_NOT_RUN
    return EXIT_PASS


def print_summary(results):
    print("\nJUMBO GATE — SUMMARY")
    for arm, v in sorted(results.items()):
        if v is None:
            print(f"  [NOT RUN] arm {arm}")
        else:
            print(f"  [{'PASS' if v.passed else 'FAIL'}] arm {arm}: {v.row} — {v.detail}")
    code = exit_code_for_results(results)
    label = {EXIT_PASS: "PASS", EXIT_FAIL: "FAIL", EXIT_NOT_RUN: "NOT RUN"}[code]
    print(f"RESULT: {label} (exit {code})")
    return code


# ================================================================ selftest

def selftest():
    fails = []

    def check(name, got, want):
        if got != want:
            fails.append(f"{name}: got {got!r}, expected {want!r}")

    # ---------------------------------------------------------- required_pairs
    # The time-and-timers gate's own derivation: observed sem 13.364% at n=3,
    # target 1.12% -> ceil(3 * (13.364/1.12)^2) = 428 (doc's "~430" is a
    # rounded quote of the same arithmetic).
    check("required_pairs worked example", required_pairs(3, 13.364, 1.12), 428)
    check("required_pairs floor", required_pairs(3, 0.01, 3.0, min_pairs=5), 5)
    check("required_pairs zero spread", required_pairs(3, 0.0, 3.0), 5)
    check("required_pairs exact n", required_pairs(4, 6.0, 3.0), 16)  # 4*(6/3)^2=16

    # ---------------------------------------------------------- paired_stats
    m, s = paired_stats([1.0, 2.0, 3.0])
    check("paired_stats mean", round(m, 6), 2.0)
    check("paired_stats sem", round(s, 6), round(statistics.stdev([1.0, 2.0, 3.0]) / math.sqrt(3), 6))
    check("paired_stats single", paired_stats([5.0]), (5.0, None))
    check("paired_stats empty", paired_stats([]), (None, None))

    # ---------------------------------------------------------------- row a
    def rep(last_start_ns, nodes):
        return {"last_start_ns": last_start_ns, "nodes": nodes}

    def node(mtu, dt_secs):
        return {"mtu_bytes": mtu, "observed_ns": int(dt_secs * 1e9)}

    good_reps = [rep(0, {"n0": node(TOP_RUNG, 3.0), "n1": node(TOP_RUNG, 5.0),
                         "n2": node(TOP_RUNG, 9.9)}) for _ in range(3)]
    check("row a pass", verdict_row_a(good_reps).passed, True)

    late_reps = good_reps[:2] + [rep(0, {"n0": node(TOP_RUNG, 3.0),
                                          "n1": node(TOP_RUNG, 10.1)})]
    check("row a late", verdict_row_a(late_reps).passed, False)

    wrong_rung_reps = good_reps[:2] + [rep(0, {"n0": node(TOP_RUNG, 1.0),
                                                "n1": node(1408, 1.0)})]
    check("row a wrong rung", verdict_row_a(wrong_rung_reps).passed, False)

    solo_reps = good_reps[:2] + [rep(0, {"n0": node(TOP_RUNG, 1.0)})]
    check("row a solo (errata 4)", verdict_row_a(solo_reps).passed, False)

    check("row a too few reps", verdict_row_a(good_reps[:2]).passed, False)

    # ---------------------------------------------------------------- row b
    # A clean 1500 B arm: rung pinned, no EMSGSIZE, probes climbing forever
    # (errata 1), throughput comfortably within -3%.
    clean_deltas = [0.4, -0.2, 0.1, -0.5, 0.3, 0.0]
    v = verdict_row_b(
        mtu_samples={"n0": MTU_DEFAULT, "n1": MTU_DEFAULT, "n2": MTU_DEFAULT},
        emsgsize_samples=[0, 0, 0, 0],
        probe_sent_series=[10, 12, 14, 20, 26, 32],
        paired_deltas_pct=clean_deltas,
        base_prelim_n=3, base_prelim_stat_pct=1.0,
    )
    check("row b pass", v.passed, True)

    v = verdict_row_b(
        mtu_samples={"n0": MTU_DEFAULT, "n1": 8832, "n2": MTU_DEFAULT},
        emsgsize_samples=[0, 0], probe_sent_series=[1, 2],
        paired_deltas_pct=clean_deltas, base_prelim_n=3, base_prelim_stat_pct=1.0,
    )
    check("row b rung rose", v.passed, False)

    v = verdict_row_b(
        mtu_samples={"n0": MTU_DEFAULT}, emsgsize_samples=[0, 3, 0],
        probe_sent_series=[1, 2], paired_deltas_pct=clean_deltas,
        base_prelim_n=3, base_prelim_stat_pct=1.0,
    )
    check("row b emsgsize nonzero", v.passed, False)

    # errata 1: a FLAT probe_sent series is the anomaly, not a pass.
    v = verdict_row_b(
        mtu_samples={"n0": MTU_DEFAULT}, emsgsize_samples=[0, 0],
        probe_sent_series=[10, 10, 10, 10], paired_deltas_pct=clean_deltas,
        base_prelim_n=3, base_prelim_stat_pct=1.0,
    )
    check("row b flat probes fails (errata 1)", v.passed, False)

    v = verdict_row_b(
        mtu_samples={"n0": MTU_DEFAULT}, emsgsize_samples=[0, 0],
        probe_sent_series=[10, 12, 14], paired_deltas_pct=[0.1, 0.2],
        base_prelim_n=3, base_prelim_stat_pct=1.0,
    )
    check("row b too few pairs", v.passed, False)

    v = verdict_row_b(
        mtu_samples={"n0": MTU_DEFAULT}, emsgsize_samples=[0, 0],
        probe_sent_series=[10, 12, 14], paired_deltas_pct=[-10.0, -9.5, -10.2, -9.8, -10.1, -9.9],
        base_prelim_n=3, base_prelim_stat_pct=1.0,
    )
    check("row b clearly worse than -3%", v.passed, False)

    # ---------------------------------------------------------------- row c
    v = verdict_row_c(20.0, -1.0, 1.0)
    check("row c recommend", v.passed, True)
    v = verdict_row_c(12.0, -1.0, 1.0)
    check("row c borderline plateau fails", v.passed, False)
    check("row c borderline noted", "BORDERLINE" in v.detail, True)
    v = verdict_row_c(20.0, -5.0, 1.0)
    check("row c throughput regression fails", v.passed, False)
    v = verdict_row_c(20.0, -1.0, 5.0)
    check("row c p99 regression fails", v.passed, False)
    v = verdict_row_c(5.0, -1.0, 1.0)
    check("row c low plateau, not borderline (below 10%)", "BORDERLINE" in v.detail, False)

    # --------------------------------------------------- blackhole probe
    ok, _ = check_blackhole_probe({"n0": TOP_RUNG, "n1": 8832})
    check("blackhole probe clears", ok, True)
    ok, msg = check_blackhole_probe({"n0": TOP_RUNG, "n1": MTU_DEFAULT})
    check("blackhole probe aborts", ok, False)
    check("blackhole probe message says ABORT", msg.startswith("ABORT"), True)
    # A host whose /metrics never answered is absent from `observed`: without
    # `expected` it silently left the stuck set, so a dead node read as a pass.
    ok, msg = check_blackhole_probe({"n0": TOP_RUNG}, expected=["n0", "n1"])
    check("blackhole probe aborts on an unreachable host", ok, False)
    check("unreachable host is named", "'n1'" in msg, True)
    check("unreachable message says unreachable", "unreachable" in msg, True)
    ok, _ = check_blackhole_probe({"n0": TOP_RUNG, "n1": TOP_RUNG},
                                  expected=["n0", "n1"])
    check("blackhole probe clears with every expected host", ok, True)
    ok, msg = check_blackhole_probe({"n0": None}, expected=["n0"])
    check("a None reading counts as unreachable", ok, False)
    ok, msg = check_blackhole_probe({}, expected=[])
    check("an empty measurement aborts", ok, False)
    check("empty measurement says nothing was measured",
          "nothing was measured" in msg, True)

    # ---------------------------------------------------------------- row d
    def refusal(reason, elapsed, peer):
        return {"reason": reason, "elapsed_secs": elapsed, "peer": peer}

    force_ok = {
        "n0": refusal(ROW_D_FORCE_REASON_NARROW, 12.0, "n1"),
        "n1": refusal(ROW_D_FORCE_REASON_NARROW, 13.5, "n2"),
        "n2": refusal(ROW_D_FORCE_REASON_NARROW, 11.0, "n0"),
    }
    silent_ok = {
        "n0": refusal(ROW_D_FORCE_REASON_SILENT, 30.0, "n2"),
        "n1": refusal(ROW_D_FORCE_REASON_SILENT, 29.9, "n2"),
    }
    check("row d pass", verdict_row_d(force_ok, silent_ok).passed, True)

    force_late = dict(force_ok)
    force_late["n0"] = refusal(ROW_D_FORCE_REASON_NARROW, 31.0, "n1")
    check("row d force arm late", verdict_row_d(force_late, silent_ok).passed, False)

    force_wrong_reason = dict(force_ok)
    force_wrong_reason["n0"] = refusal(ROW_D_JOIN_REASON, 12.0, "n1")
    check("row d wrong reason (join gate name leaking into the force arm)",
          verdict_row_d(force_wrong_reason, silent_ok).passed, False)

    silent_disagree = dict(silent_ok)
    silent_disagree["n1"] = refusal(ROW_D_FORCE_REASON_SILENT, 29.9, "n0")
    check("row d silent arm disagrees on the named peer",
          verdict_row_d(force_ok, silent_disagree).passed, False)

    check("row d missing a node in the force arm",
          verdict_row_d({k: v for k, v in list(force_ok.items())[:2]}, silent_ok).passed, False)

    # ------------------------------------------------------- exit code
    # Fix round 1 (Important 1): the mapping from per-arm results to a
    # process exit code, pinned directly — a FAIL verdict anywhere maps to
    # 1, a NOT-RUN (None) anywhere (with no FAIL) maps to 2, and an
    # all-pass (or empty) result set maps to 0. FAIL beats NOT-RUN beats
    # PASS, so the worst finding always wins the exit code.
    pass_v = Verdict("x", True, "ok")
    fail_v = Verdict("y", False, "bad")
    check("exit code: all pass", exit_code_for_results({"a": pass_v, "e": pass_v}), EXIT_PASS)
    check("exit code: no arms requested", exit_code_for_results({}), EXIT_PASS)
    check("exit code: one fail", exit_code_for_results({"a": pass_v, "b": fail_v}), EXIT_FAIL)
    check("exit code: one not-run, no fail", exit_code_for_results({"a": pass_v, "b": None}), EXIT_NOT_RUN)
    check("exit code: all not-run", exit_code_for_results({"b": None, "c": None}), EXIT_NOT_RUN)
    check("exit code: fail beats not-run",
          exit_code_for_results({"a": fail_v, "b": None}), EXIT_FAIL)
    check("exit code labels distinct", len({EXIT_PASS, EXIT_FAIL, EXIT_NOT_RUN}), 3)
    # argparse exits 2 on a usage error, so no verdict code may collide with it.
    check("not-run does not collide with argparse's usage code",
          EXIT_NOT_RUN != EXIT_USAGE, True)
    check("no verdict code is argparse's usage code",
          EXIT_USAGE in {EXIT_PASS, EXIT_FAIL, EXIT_NOT_RUN}, False)

    # A stub arm (returns None, per the fix-round finding that b/c/d/e/f
    # print-only stubs must never be mistaken for a pass) must map to
    # NOT-RUN on its own, with no other arms requested.
    check("exit code: a single stub arm alone is NOT-RUN",
          exit_code_for_results({"b": None}), EXIT_NOT_RUN)

    # Row b's fleet helpers (the run, 2026-09-12): the base spread that fixes
    # the pair count, the per-pair delta, and the refusal-record reader row d
    # shares.
    check("spread_stat_pct: identical rates spread 0",
          spread_stat_pct([100.0, 100.0, 100.0, 100.0]), 0.0)
    check("spread_stat_pct: one rate is no spread", spread_stat_pct([100.0]), None)
    sp = spread_stat_pct([90.0, 110.0, 100.0, 100.0])
    check("spread_stat_pct: sem/mean in percent (sem 4.08 of mean 100)",
          round(sp, 2), 4.08)
    check("paired_delta_pct: head 97 of base 100 is -3", paired_delta_pct(100.0, 97.0), -3.0)
    log = ('{"ts_ns":1,"level":"info","event":"became_follower","node":0,"term":1}\n'
           'garbage line\n'
           '{"ts_ns":31000000000,"level":"error","event":"jumbo_path_too_narrow",'
           '"node":0,"peer":2,"carried":1408,"required":8832,"waited_secs":30}\n'
           '{"ts_ns":32000000000,"level":"error","event":"agent_failstopped","agent":"uc2-consensus"}\n')
    check("refusal_from_log: first matching record, with peer",
          refusal_from_log(log, (ROW_D_FORCE_REASON_NARROW, ROW_D_FORCE_REASON_SILENT)),
          (ROW_D_FORCE_REASON_NARROW, 31000000000, "2"))
    check("refusal_from_log: nothing matching is None",
          refusal_from_log(log, ("some_other_event",)), None)

    for f in fails:
        print(f"SELFTEST FAIL {f}")
    print(f"SELFTEST: {len(fails)} failure(s)")
    return 1 if fails else 0


# ================================================================ fleet arms
#
# Reuses m12_fleet_gate's ssh/systemd-run plumbing and tt_fleet_gate's
# Prometheus reader rather than reinventing either (per the brief). These
# functions are exercised by a real fleet run, never by `--selftest`, and
# THIS TASK RUNS NONE OF THEM — the pre-commitment is the deliverable; the
# run is a separate, user-gated fleet trip.

def render_node_toml(node_id, bind_addr, instance_dir, members, force_jumbo_frames=False,
                      metrics_addr=None):
    """A minimal real `node.toml` for the jumbo fleet arms — `[crypto]`/
    `[admin]`/`[services]` are explicit-choice config since 2.6.0/2.11.0
    (a node.toml missing any of them refuses to start by name), and
    `force_jumbo_frames` is the one key this feature adds (spec §6; refused
    by env override name `UC2_FORCE_JUMBO_FRAMES` if misspelled, not
    exercised here). `max_payload` is NOT written: spec §1 refuses it by
    name — the ceiling is discovered, never configured."""
    lines = [
        f"id = {node_id}",
        f'bind = "{bind_addr}"',
        f'instance_dir = "{instance_dir}"',
        'app_id = "uc2-jumbo-gate"',
        "buffer_bytes = 4194304",
        f"force_jumbo_frames = {'true' if force_jumbo_frames else 'false'}",
        "",
        "[crypto]",
        "enabled = false",
        "",
        "[admin]",
        'auth = "none"',
        "",
        "[services]",
        'names = ["reg"]',
        "",
    ]
    if metrics_addr:
        lines += ["[metrics]", f'bind = "{metrics_addr}"', ""]
    for mid, maddr in members:
        lines += ["[[members]]", f"id = {mid}", f'addr = "{maddr}"', ""]
    return "\n".join(lines)


def scrape_prom(host, metrics_port=METRICS_PORT_DEFAULT, label="metrics"):
    """This host's `/metrics`, parsed by `tt_fleet_gate.parse_prom` — the
    same reader `m14_fleet_gate.py`'s `scrape_prom` uses, so a metric name
    typo cannot drift between the two gates."""
    r = ssh(host, f"curl -s --max-time {tt.SCRAPE_TIMEOUT_SECS} "
                  f"http://{host.private_ip}:{metrics_port}/metrics", label=label)
    m = tt.parse_prom(r.stdout or "")
    if not m:
        print(f"WARN scrape {host.public_ip}: /metrics empty or unreachable "
              f"(port {metrics_port})", flush=True)
    return m


def read_gauge(metrics, name, **labels):
    return tt.prom_get(metrics, name, **labels)


def force_interface_mtu(host, iface, mtu):
    """Row b's precondition: "interface MTU forced to 1500 by ansible on the
    same fleet" (spec §10 row b). This driver issues the same `ip link`
    change directly over ssh rather than adding a separate ansible role —
    still a code path only, never invoked by this task. Idempotent and
    reversible with the same call at `mtu=9001` (or whatever the fleet's
    provisioned interface MTU is, passed back in by the caller)."""
    r = ssh(host, f"sudo ip link set dev {iface} mtu {mtu}", label="force-mtu")
    if r.returncode != 0:
        raise RuntimeError(f"set mtu {mtu} on {host.public_ip}/{iface}: {r.stderr}")


def build_uc_node(hosts):
    """The ansible provision builds only the gate EXAMPLES; the jumbo rows
    run the real `uc2-node` daemon (a node.toml is the only way to set
    `force_jumbo_frames`), so build it once per host, on the host, from the
    rsynced head tree — the same discipline `m14.prepare_base_tree` uses."""
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    for h in hosts:
        cmd = (f"{env} {m6.SshHost.CARGO} build --release "
               f"--manifest-path {m6.SshHost.UC_SRC}/Cargo.toml -p uc_node --bin uc2-node "
               f"&& test -x {h.gate} && sha256sum {h.gate} && echo NODE-OK")
        r = ssh(h, cmd, label="build-node", timeout=1800)
        if "NODE-OK" not in (r.stdout or ""):
            raise RuntimeError(f"uc2-node build on {h.public_ip}: {(r.stderr or r.stdout)[-2000:]}")
        print(f"INFO uc2-node on {h.public_ip}: " + (r.stdout or "").strip().splitlines()[-2],
              flush=True)


def stop_jumbo_cluster(hosts):
    for h in hosts:
        kill_unit(h, "jumbo-node")


def start_jumbo_cluster(hosts, args, force=False, skip=()):
    """A FRESH cluster of real `uc2-node` daemons on `hosts` (instance dirs
    wiped), every member in every node.toml, the units in `skip` (by index)
    never started — row d's silent arm. Returns the controller wall-clock
    ns at which the last unit was started."""
    members = [(idx, f"{h.private_ip}:19200") for idx, h in enumerate(hosts)]
    last_start_ns = 0
    for idx, h in enumerate(hosts):
        cfg = f"{h.dir}/node.toml"
        body = render_node_toml(idx, members[idx][1], h.dir, members, force_jumbo_frames=force,
                                metrics_addr=f"{h.private_ip}:{METRICS_PORT_DEFAULT}")
        ssh(h, f"sudo rm -rf {h.dir} && sudo mkdir -p {h.dir} && "
               f"sudo tee {cfg} >/dev/null <<'JCFG'\n{body}\nJCFG",
            label="write-config")
        truncate_log(h, "jumbo-node")
        if idx in skip:
            print(f"INFO n{idx} ({h.public_ip}) deliberately NOT started", flush=True)
            continue
        start_unit(h, "jumbo-node", ["--config", cfg], nofile=True)
        last_start_ns = time.time_ns()
    return last_start_ns


def sample_series(hosts, names, secs, period=1.0):
    """Every host's `names` gauges/counters, sampled every `period` for
    `secs`: `{name: {public_ip: [values...]}}`. A scrape that does not answer
    contributes nothing for that tick (so a series is never padded with a
    made-up value) and is counted in `misses`."""
    out = {n: {h.public_ip: [] for h in hosts} for n in names}
    misses = 0
    deadline = time.time() + secs
    while time.time() < deadline:
        for h in hosts:
            m = scrape_prom(h)
            if not m:
                misses += 1
                continue
            for n in names:
                v = read_gauge(m, n)
                if v is not None:
                    out[n][h.public_ip].append(int(v))
        time.sleep(period)
    return out, misses


def host_mtu(host, iface):
    """The interface's current MTU, or a loud failure — never a silent 0: the
    rows' `finally` blocks restore the MTU from this reading, and a falsy
    placeholder would leave a host at 1500 for every later arm."""
    r = ssh(host, f"cat /sys/class/net/{iface}/mtu", label="mtu")
    text = (r.stdout or "").strip()
    if r.returncode != 0 or not text.isdigit():
        raise RuntimeError(f"could not read {iface}'s MTU on {host.public_ip}: {r.stderr or text!r}")
    return int(text)


def run_arm_a(hosts, args):
    """Row a: 3 reps, every node's `uc2_datagram_mtu_bytes` == 8960 within
    10s of the last node's start. Needs >= 2 members (errata 4)."""
    if len(hosts) < ROW_A_MIN_NODES:
        raise SystemExit(f"row a needs >= {ROW_A_MIN_NODES} nodes (errata 4)")
    reps = []
    for i in range(ROW_A_MIN_REPS):
        stop_jumbo_cluster(hosts)
        last_start_ns = start_jumbo_cluster(hosts, args)
        deadline = time.time() + ROW_A_ADOPTION_WINDOW_SECS
        nodes = {}
        while time.time() < deadline and len(nodes) < len(hosts):
            for h in hosts:
                if h.public_ip in nodes:
                    continue
                m = scrape_prom(h)
                v = read_gauge(m, "uc2_datagram_mtu_bytes")
                if v is not None and int(v) == TOP_RUNG:
                    nodes[h.public_ip] = {"mtu_bytes": int(v), "observed_ns": time.time_ns()}
            time.sleep(0.5)
        # A node that never reached the top rung inside the window is
        # recorded with whatever it last read, so the verdict names it.
        for h in hosts:
            if h.public_ip not in nodes:
                m = scrape_prom(h)
                v = read_gauge(m, "uc2_datagram_mtu_bytes")
                nodes[h.public_ip] = {"mtu_bytes": int(v) if v is not None else -1,
                                      "observed_ns": time.time_ns()}
        print(f"INFO row a rep{i + 1}: " + ", ".join(
            f"{ip}={n['mtu_bytes']}@+{(n['observed_ns'] - last_start_ns) / 1e9:.1f}s"
            for ip, n in nodes.items()), flush=True)
        reps.append({"last_start_ns": last_start_ns, "nodes": nodes})
    stop_jumbo_cluster(hosts)
    print("ROW-A-JSON " + json.dumps(reps), flush=True)
    v = verdict_row_a(reps)
    print(f"[{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    return v


def m14_namespace(args):
    """The argparse fields `m14_fleet_gate`'s rate-arm path reads
    (`setup_fleet`, `one_arm` -> `run_rate_arm`), with this driver's values.
    `no_sync`: the provision already rsynced the head tree; `m12.prepare_host`
    (inside `setup_fleet`) builds its gate examples on the host."""
    return types.SimpleNamespace(
        hosts=args.hosts, ssh_user=args.ssh_user, ssh_key=args.ssh_key,
        no_sync=True, local_tree=args.local_tree, payload=64, inflight=4096, pin=False,
    )


def paired_64b(head_voters, base_voters, ns, pairs, label):
    """`pairs` interleaved base/head n1 arms (64 B, direct client on the
    leader host), the m14 driver's own `one_arm` each time — fresh cluster
    per arm, so no mixed-version cluster ever exists. Odd pairs run base
    first, even pairs head first, so a drift across the run lands on both.
    Returns (base_rates, head_rates, deltas_pct)."""
    base_rates, head_rates, deltas = [], [], []
    for k in range(1, pairs + 1):
        order = (("base", base_voters), ("head", head_voters)) if k % 2 else \
                (("head", head_voters), ("base", base_voters))
        got = {}
        for arm, voters in order:
            with m14.tt_disabled():
                d = m14.one_arm(voters, ns, f"{label} {arm} n1 pair{k}", [(0, 0)], None, {},
                                [], fan_in=False, check=False)
            got[arm] = m14.rate_of(d)
        base_rates.append(got["base"])
        head_rates.append(got["head"])
        deltas.append(paired_delta_pct(got["base"], got["head"]))
        print(f"INFO {label} pair{k}: base {got['base']:.0f} head {got['head']:.0f} "
              f"delta {deltas[-1]:+.3f}%", flush=True)
    return base_rates, head_rates, deltas


def run_arm_b(hosts, args):
    """Row b: the 1500 B arm (spec §10 row b, errata 1). Force every host's
    replication interface to 1500, restart the real-daemon cluster from
    cold, sample `uc2_datagram_mtu_bytes` / `uc2_send_emsgsize_total` /
    `uc2_probe_sent_total` for `ROW_B_SAMPLE_SECS`, then — with the
    interface still at 1500 — the 64 B paired throughput against the
    pre-jumbo base tree: `ROW_B_PRELIM_REPS` base-only arms fix the pair
    count through `required_pairs` (floor `ROW_B_MIN_PAIRS`, cap
    `--pairs-max`), then that many interleaved pairs. The interface MTU is
    restored to what it was in a `finally`."""
    if not args.base_tree:
        raise SystemExit("row b needs --base-tree <pre-jumbo checkout> for its paired 64 B arm")
    iface = args.iface or m12.detect_iface(hosts[0])
    original = {h.public_ip: host_mtu(h, iface) for h in hosts}
    print(f"INFO row b: iface {iface}, provisioned MTU {original}", flush=True)
    try:
        for h in hosts:
            force_interface_mtu(h, iface, ROW_B_NARROW_MTU)
        stop_jumbo_cluster(hosts)
        start_jumbo_cluster(hosts, args)
        names = ("uc2_datagram_mtu_bytes", "uc2_send_emsgsize_total", "uc2_probe_sent_total")
        series, misses = sample_series(hosts, names, ROW_B_SAMPLE_SECS)
        stop_jumbo_cluster(hosts)
        mtu_samples = [v for vs in series["uc2_datagram_mtu_bytes"].values() for v in vs]
        emsg_samples = [v for vs in series["uc2_send_emsgsize_total"].values() for v in vs]
        # Errata 1's clause is PER NODE and the verdict takes ONE series. A
        # fleet-wide sum is non-decreasing iff every node's is, but it can
        # RISE while one node's series is flat — the very anomaly clause 3
        # exists to catch — so judge each node first and hand the verdict
        # the first offending node's series if there is one, else the sum.
        per_node = {ip: vs for ip, vs in series["uc2_probe_sent_total"].items() if vs}
        flat = {ip: vs for ip, vs in per_node.items()
                if len(vs) >= 2 and (any(b < a for a, b in zip(vs, vs[1:])) or vs[-1] <= vs[0])}
        if flat:
            ip, vs = sorted(flat.items())[0]
            print(f"INFO row b: uc2_probe_sent_total NOT climbing on {ip}: {vs}", flush=True)
            probe_series = vs
        else:
            n_min = min((len(vs) for vs in per_node.values()), default=0)
            probe_series = [sum(vs[i] for vs in per_node.values()) for i in range(n_min)]
        print(f"INFO row b: {len(mtu_samples)} mtu samples, {misses} scrape misses, "
              f"probe_sent fleet-wide {probe_series[:1]}->{probe_series[-1:]}", flush=True)
        print("ROW-B-SERIES-JSON " + json.dumps(series), flush=True)

        # The paired 64 B arm, on m12_gate clusters (the same client and
        # cluster shape every rate gate uses), head tree vs base tree.
        ns = m14_namespace(args)
        m14_hosts, head_voters, _ = m14.setup_fleet(ns)
        m14.prepare_base_tree(m14_hosts, args.base_tree)
        base_voters = m14.base_fleet_hosts(ns)[:3]
        prelim = []
        for k in range(1, ROW_B_PRELIM_REPS + 1):
            with m14.tt_disabled():
                d = m14.one_arm(base_voters, ns, f"row b prelim base n1 rep{k}", [(0, 0)],
                                None, {}, [], fan_in=False, check=False)
            prelim.append(m14.rate_of(d))
        stat = spread_stat_pct(prelim)
        need = required_pairs(len(prelim), stat, abs(ROW_B_RUNG_BAR_PCT), ROW_B_MIN_PAIRS)
        pairs = min(need, args.pairs_max)
        print(f"INFO row b: base prelim {[round(r) for r in prelim]} spread {stat:.3f}% -> "
              f"need {need} pairs, running {pairs} (cap {args.pairs_max})", flush=True)
        base_rates, head_rates, deltas = paired_64b(head_voters, base_voters, ns, pairs, "row b")
        print("ROW-B-PAIRS-JSON " + json.dumps({"prelim": prelim, "base": base_rates,
                                                  "head": head_rates, "deltas_pct": deltas}),
              flush=True)
        v = verdict_row_b(mtu_samples, emsg_samples, probe_series, deltas, len(prelim), stat)
        print(f"[{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
        return v
    finally:
        for h in hosts:
            if original.get(h.public_ip) is not None:
                force_interface_mtu(h, iface, original[h.public_ip])
        print(f"INFO row b: iface {iface} restored to {original}", flush=True)


def await_refusals(hosts, live, start_ns, secs=ROW_D_POLL_SECS):
    """Poll each live host's unit log until every one carries a force-gate
    refusal record or `secs` elapse. `elapsed_secs` is measured from the
    controller's start instant to the record's own `ts_ns` (host wall
    clock; the fleet runs chrony, so the skew is milliseconds against a
    30 s bar)."""
    reasons = (ROW_D_FORCE_REASON_NARROW, ROW_D_FORCE_REASON_SILENT)
    found = {}
    deadline = time.time() + secs
    while time.time() < deadline and len(found) < len(live):
        for idx in live:
            if idx in found:
                continue
            got = refusal_from_log(tail_log(hosts[idx], "jumbo-node", lines=400), reasons)
            if got:
                reason, ts_ns, peer = got
                found[idx] = {"reason": reason, "elapsed_secs": (ts_ns - start_ns) / 1e9,
                              "peer": peer}
                print(f"INFO n{idx}: {reason} peer={peer} at +{found[idx]['elapsed_secs']:.1f}s",
                      flush=True)
        time.sleep(1.0)
    return {f"n{idx}": r for idx, r in found.items()}


def run_arm_d(hosts, args):
    """Row d: the two `force_jumbo_frames` arms (spec §10 row d). Arm 1: all
    three nodes forced, interface at 1500 -> every node refuses
    `jumbo_path_too_narrow` naming a peer. Arm 2: interface restored, two
    nodes forced, the third never started -> both refuse
    `jumbo_peer_silent` naming it. A fresh cluster each arm; the MTU is
    restored in a `finally`."""
    if len(hosts) < 3:
        raise SystemExit("row d needs 3 nodes")
    hosts = hosts[:3]
    iface = args.iface or m12.detect_iface(hosts[0])
    original = {h.public_ip: host_mtu(h, iface) for h in hosts}
    try:
        for h in hosts:
            force_interface_mtu(h, iface, ROW_B_NARROW_MTU)
        stop_jumbo_cluster(hosts)
        start_ns = start_jumbo_cluster(hosts, args, force=True)
        force_arm = await_refusals(hosts, [0, 1, 2], start_ns)
        stop_jumbo_cluster(hosts)
        for h in hosts:
            force_interface_mtu(h, iface, original[h.public_ip])
        start_ns = start_jumbo_cluster(hosts, args, force=True, skip=(2,))
        silent_arm = await_refusals(hosts, [0, 1], start_ns)
        stop_jumbo_cluster(hosts)
    finally:
        for h in hosts:
            if original.get(h.public_ip) is not None:
                force_interface_mtu(h, iface, original[h.public_ip])
    print("ROW-D-JSON " + json.dumps({"force_1500": force_arm, "silent_9001": silent_arm}),
          flush=True)
    v = verdict_row_d(force_arm, silent_arm)
    print(f"[{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    return v


def run_arm_c(hosts, args, window_secs=ROW_C_BLACKHOLE_WINDOW_SECS, min_rung=JUMBO_MIN_RUNG):
    """Row c: the envelope-map brief's own soak (spec §10 row c).

    The pre-arm blackhole probe is real and wired first: this samples every
    host's `uc2_datagram_mtu_bytes` over `window_secs` on a fresh cluster
    and calls `check_blackhole_probe`; a jumbo arm that never clears the
    jumbo minimum returns a FAIL `Verdict` (reaching the exit code via
    `exit_code_for_results`) instead of falling through. The soak beyond it
    is the brief's own multi-rung instrument (`uc_node/examples/envelope_map.rs`),
    which does not exist in the tree, so this arm returns NOT-RUN after the
    probe; the gate doc records that as such."""
    stop_jumbo_cluster(hosts)
    start_jumbo_cluster(hosts, args)
    deadline = time.time() + window_secs
    observed = {}
    while time.time() < deadline:
        for h in hosts:
            m = scrape_prom(h)
            v = read_gauge(m, "uc2_datagram_mtu_bytes")
            if v is not None:
                observed[h.public_ip] = int(v)
        if len(observed) == len(hosts) and all(v >= min_rung for v in observed.values()):
            break
        time.sleep(1.0)
    stop_jumbo_cluster(hosts)
    # `expected` is the arm's WHOLE host list, not just the hosts that
    # answered: a host whose /metrics never answers is absent from `observed`,
    # and without this it would be absent from the stuck set too — i.e. a dead
    # node would read as a pass.
    ok, msg = check_blackhole_probe(observed, window_secs=window_secs,
                                    min_rung=min_rung,
                                    expected=[h.public_ip for h in hosts])
    if not ok:
        print(f"[FAIL] row c blackhole probe (pre-arm) — {msg}", flush=True)
        return Verdict("c blackhole probe (pre-arm)", False, msg)
    print(f"[OK] row c blackhole probe (pre-arm) — {msg}", flush=True)
    print("row c: blackhole probe cleared; the envelope-map brief's soak instrument "
          "(uc_node/examples/envelope_map.rs) does not exist, so the soak is NOT RUN.")
    return None  # NOT-RUN: the probe is real; the soak beyond it is not.


def run_arm_e(args):
    """Row e: `m5_gate` on the fleet, standard arm, paired against the base
    tree — reported, no bar. Left as a thin wrapper around `m5_fleet_gate.py`
    (which already builds/runs `m5_gate` on a real cluster) rather than a
    reimplementation; this driver only reads its paired-delta output and
    formats `report_row_e`."""
    print("row e reuses bench-infra/scripts/m5_fleet_gate.py's own runner; "
          "see docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md "
          "'When this gate is run' for the exact invocation. Not run by this "
          "task.")


def run_arm_f(args):
    """Row f: `scripts/hop1_ab.sh` dev-box smoke, reported, no bar."""
    print("row f: scripts/hop1_ab.sh --sink <bin> --a <base> --b <head> "
          "--reps 6 --secs 6 --root $HOME/scratch/jumbo-hop1-ab — see the gate "
          "doc's 'When this gate is run'. Not run by this task.")


ARM_RUNNERS = {
    "a": run_arm_a,
    "b": run_arm_b,
    "c": run_arm_c,
    "d": run_arm_d,
    "e": run_arm_e,
    "f": run_arm_f,
}


# Arms b/c/d need real fleet hosts (a node.toml, a systemd unit, a
# /metrics scrape); e/f run a local/dev-box harness against binaries the
# caller names and need no host discovery at all — so `--fleet --arms f`
# must not pay for (or require) terraform state it will never use.
ARMS_NEEDING_HOSTS = frozenset({"a", "b", "c", "d"})


def main():
    ap = argparse.ArgumentParser(
        description="Jumbo-frame discovery fleet gate driver",
        epilog="Exit codes: 0 = every requested arm PASSED (or none were "
               "requested); 1 = FAIL — at least one requested arm's Verdict "
               "had passed=False (a bar was missed, or row c's pre-arm "
               "blackhole probe aborted); 3 = NOT RUN — at least one "
               "requested arm produced no Verdict (a print-only stub) and "
               "none FAILED. 2 is argparse's USAGE error (a bad --arms, an "
               "unknown flag) and is never a gate verdict, which is why NOT "
               "RUN is 3. FAIL always outranks NOT RUN, which always "
               "outranks PASS, so a wrapper reading $? alone gets the worst "
               "finding across every requested arm — see exit_code_for_results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--selftest", action="store_true",
                    help="adjudicate canned inputs through the row arithmetic and exit "
                         "(no fleet, no ssh, no cargo)")
    ap.add_argument("--fleet", action="store_true")
    ap.add_argument("--arms", default="", help="comma-separated subset of: a,b,c,d,e,f")
    ap.add_argument("--hosts", default="", help="pub/priv,... (else terraform output)")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--uc-node-bin", default=UC_NODE_BUILT_DEFAULT,
                    help="path to the real uc2-node binary on the fleet hosts")
    ap.add_argument("--iface", default="",
                    help="replication interface on the hosts (else detected from the default route)")
    ap.add_argument("--base-tree", default="",
                    help="row b: local checkout of the pre-jumbo tree, rsynced and built on "
                         "the hosts beside the head tree (m14_fleet_gate's --base-tree)")
    ap.add_argument("--local-tree", default=str(Path(__file__).resolve().parent.parent.parent))
    ap.add_argument("--pairs-max", type=int, default=ROW_B_PAIRS_MAX_DEFAULT,
                    help="row b: cap on the interleaved pair count required_pairs may ask for")
    ap.add_argument("--no-build", action="store_true",
                    help="skip building uc2-node on the hosts (already built this session)")
    a = ap.parse_args()

    if a.selftest:
        sys.exit(selftest())
    if not a.fleet:
        ap.error("one of --fleet or --selftest is required")
    arms = [x.strip() for x in a.arms.split(",") if x.strip()]
    unknown = [x for x in arms if x not in ("a", "b", "c", "d", "e", "f")]
    if unknown:
        ap.error(f"unknown arm(s): {unknown} (valid: a,b,c,d,e,f)")
    if not arms:
        ap.error("--fleet requires --arms (comma-separated subset of: a,b,c,d,e,f)")

    # MINOR (fix round 1): skip host discovery entirely when no requested
    # arm needs it, so `--fleet --arms f` neither pays for terraform-output
    # discovery nor fails when no fleet state exists.
    hosts = None
    if ARMS_NEEDING_HOSTS.intersection(arms):
        # `gate_bin` is the real daemon: `start_unit` runs `host.gate <args>`,
        # so the units below are `uc2-node --config <node.toml>`. (The first
        # revision passed m12_gate here and the daemon path as an ARGUMENT,
        # which would have run the gate example with a stray positional.)
        hosts = m6.build_fleet_hosts(a.uc_node_bin, a.ssh_user, a.ssh_key, a.hosts,
                                     count=a.nodes, unit_prefix="jumbo",
                                     remote_root="/opt/bench/jumbo", probe_bin=m12.BUILT_PROBE)
        if not a.no_build:
            build_uc_node(hosts)

    # Important 1 (fix round 1): collect every requested arm's outcome and
    # let it reach the exit code — a runner that returns `None` (a
    # print-only stub, or row c's probe having nothing further to run) is
    # NOT-RUN, distinct from both PASS and FAIL; a runner that returns a
    # `Verdict` contributes its `passed` bit. Previously no return value was
    # ever inspected, so a stub run and a passing gate both exited 0.
    results = {}
    for arm in arms:
        runner = ARM_RUNNERS[arm]
        results[arm] = runner(a) if arm in ("e", "f") else runner(hosts, a)

    sys.exit(print_summary(results))


if __name__ == "__main__":
    main()
