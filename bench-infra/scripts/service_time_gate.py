#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
Service-time measurement — PRE-REGISTERED 2026-09-16 in
docs/benchmarks/uc2-service-time-2026-09-16.md (written before this ran).

THE QUESTION. Every published UC v2 latency is a saturation point: the
client's window binds and p50 = window / throughput to three figures
(docs/notes/uc2-latency-throughput-explained.md). Nobody has measured the
commit path's SERVICE TIME — what one command costs when nothing is queued
behind it — which is the number Aeron's 100 k msg/s rows and ABTRDA3's
one-in-flight soaks both lead with. This driver produces it.

WHAT IT DOES. On a fresh 3-voter cluster per arm, the direct shmem client on
the leader host runs a CLOSED LOOP at inflight 1 (send, wait for the commit
+ apply + response, send the next) and reports the round-trip distribution.
A short inflight ladder (1, 2, 4, 8, 16, 64, 256) shows where queueing takes
over. Arms, in this fixed order:

    A  consistent  unpinned  sleep-apply   — the shipped posture, as an operator gets it
    B  consistent  pinned    sleep-apply   — placement noise removed
    C  consistent  pinned    spin-apply    — UC2_APPLY_IDLE=spin: the service's 50 µs idle sleep removed
    D  eventual    pinned    spin-apply    — + UC2_JOURNAL_DURABILITY=eventual (1 ms interval): fsync off the path
    A' consistent  unpinned  sleep-apply   — drift bracket (same as A, run last)

Subtractions read off these arms:  A−B = placement,  B−C = the apply idle
sleep,  C−D = the quorum fdatasync.  What is left in D is the wire round trip
plus every agent's duty-cycle latency — the brief's WIRE(P) instrument
(docs/superpowers/specs/2026-08-02-uc2-net-decomp-brief.md) is what splits
THOSE two; this driver deliberately does not claim to.

The client's own poll thread is run with `--poll-idle spin` in every arm, so
the harness's 20 µs empty-poll sleep never lands in the number (a harness
sleeping on the response path measures itself, the M14a lesson).

A raw UDP ping-pong (hi-perf-cmp's `network-rtt-udp`, built on the hosts)
between two cluster hosts is measured first and last, so the report can
state the wire's own round trip on the SAME fleet, same day.

MEASUREMENT ROW — no bar. Exit 0 unless the harness itself fails.

Usage (from bench-infra/, fleet up via `make up-uc`, 4 hosts; the 4th host
is idle — it exists because the tfvars topology is M14-shaped):

    unset SSH_AUTH_SOCK
    python3 scripts/service_time_gate.py --fleet [--nodes 4] [--secs 20] \
        [--ladder 1,2,4,8,16,64,256] [--reps1 3] [--arms A,B,C,D,A2] \
        [--rtt-src /home/claude/scratch/rtt-udp-mini]
"""

import argparse
import json
import shlex
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
from m12_fleet_gate import (  # noqa: E402
    APP, PORT, PIN_MAP_C6ID_2XL, LEADER_WAIT_SECS, CLIENT_SLACK_SECS,
    BOOT_SETTLE_SECS, UNIT_PREFIX,
    ssh, kill_unit, wipe_dirs, members_str, truncate_log, unit_log,
    parse_result, echo, detect_iface, run_client_sampled, steady_rates,
    require_pin_layout, prepare_host, stop_cluster,
)
from m6_fleet_gate import wait_leader  # noqa: E402

DEFAULT_LADDER = "1,2,4,8,16,64,256"
RTT_PORT = 9101
RTT_REMOTE_SRC = "/opt/bench/rtt-udp-mini"
RTT_REMOTE_BIN = f"{RTT_REMOTE_SRC}/target/release/network-rtt-udp"

# Arm table: (label, durability env, pinned, apply idle env). The order is
# the pre-registered one; `--arms` may subset it but never reorders it.
ARMS = {
    "A":  ("consistent / unpinned / sleep-apply", {}, False, {}),
    "B":  ("consistent / pinned / sleep-apply", {}, True, {}),
    "C":  ("consistent / pinned / spin-apply", {}, True, {"UC2_APPLY_IDLE": "spin"}),
    "D":  ("eventual(1ms) / pinned / spin-apply",
           {"UC2_JOURNAL_DURABILITY": "eventual", "UC2_JOURNAL_EVENTUAL_FSYNC_MS": "1"},
           True, {"UC2_APPLY_IDLE": "spin"}),
    "A2": ("consistent / unpinned / sleep-apply (drift bracket)", {}, False, {}),
}
ARM_ORDER = ["A", "B", "C", "D", "A2"]


# ------------------------------------------------------------ unit start w/ env

def unit_start_cmd_env(host, unit, args, nofile=False, cpus=None, env=None):
    """`m12.unit_start_cmd` plus `--setenv=K=V` for each env entry — the node
    role reads UC2_JOURNAL_* and the service role reads UC2_APPLY_IDLE from
    the unit's environment (eventual_arm_gate.py's EnvSshHost did the same
    with a subclass; this keeps m12's command shape byte-identical when `env`
    is empty)."""
    base = m12.unit_start_cmd(host, unit, args, nofile=nofile, cpus=cpus)
    if not env:
        return base
    setenv = " ".join(f"--setenv={k}={v}" for k, v in env.items())
    marker = "--collect "
    assert marker in base
    return base.replace(marker, f"{marker}{setenv} ", 1)


def start_unit_env(host, unit, args, nofile=False, cpus=None, env=None):
    kill_unit(host, unit)
    cmd = unit_start_cmd_env(host, unit, args, nofile=nofile, cpus=cpus, env=env)
    r = ssh(host, cmd, label="systemd-run")
    if r.returncode != 0:
        raise RuntimeError(
            f"start {UNIT_PREFIX}-{unit} on {host.public_ip}: {r.stderr or r.stdout}"
        )


def start_cluster_arm(node_hosts, a, node_env, pins, service_env):
    """m12.start_cluster's shape (typed CountSm, envelope off) with per-role
    env and optional pins."""
    pins = pins or {}
    ms = members_str(node_hosts)
    for i, h in enumerate(node_hosts):
        start_unit_env(h, "node", [
            "node", "--id", str(i), "--bind", f"{h.private_ip}:{PORT}",
            "--instance-dir", h.dir, "--members", ms, "--app-id", APP,
            "--admission-kib", str(a.admission_kib),
            "--services", "count",
        ], nofile=True, cpus=pins.get("node"), env=node_env)
    time.sleep(BOOT_SETTLE_SECS)
    for h in node_hosts:
        truncate_log(h, "service")
        start_unit_env(h, "service", [
            "service", "--instance-dir", h.dir, "--app-id", APP,
            "--envelope", "off", "--fsm", "count",
        ], cpus=pins.get("service0"), env=service_env)
    time.sleep(BOOT_SETTLE_SECS)


# ------------------------------------------------------------------ one point

def client_point(lh, a, iface, inflight, secs, cpus):
    """One closed-loop point on the leader host. `--poll-idle spin` always
    (see the module doc); `taskset` when the arm is pinned."""
    argv = ["client-direct", "--instance-dir", lh.dir, "--app-id", APP,
            "--secs", str(secs), "--payload", str(a.payload),
            "--inflight", str(inflight), "--envelope", "off",
            "--poll-idle", "spin",
            "--warmup-secs", "2", "--measure-secs", str(max(1, secs - 4))]
    if cpus:
        # run_client_sampled builds `sudo {gate} …`; a pinned client needs the
        # taskset in front of the binary, so wrap the gate path for this call.
        gate = lh.gate
        lh.gate = f"taskset -c {cpus} {gate}"
        try:
            rc, out, samples = run_client_sampled(lh, argv, secs + CLIENT_SLACK_SECS, iface)
        finally:
            lh.gate = gate
    else:
        rc, out, samples = run_client_sampled(lh, argv, secs + CLIENT_SLACK_SECS, iface)
    echo(f"svc inflight={inflight}", out, lines=30)
    d = parse_result(out, "direct")
    if d is None:
        print(f"INFO inflight={inflight}: no RESULT line (rc={rc}) — not measured, "
              f"not zero", flush=True)
        return None
    rates = steady_rates(samples) or {}
    rps = d.get("responses_per_sec") or 0.0
    txb = rates.get("tx_bytes_per_sec")
    txp = rates.get("tx_pkts_per_sec")
    return {
        "inflight": inflight,
        "resp_per_sec": d.get("responses_per_sec"),
        "window_rps": d.get("window_rps"),
        "responses": d.get("responses"),
        "p50_ms": d.get("p50_ms"), "p90_ms": d.get("p90_ms"),
        "p95_ms": d.get("p95_ms"), "p99_ms": d.get("p99_ms"),
        "max_ms": d.get("max_ms"), "lost": d.get("lost"),
        "retried": d.get("retried"), "not_leader": d.get("not_leader"),
        "bytes_per_command": (txb / rps) if (txb and rps > 0) else None,
        "pkts_per_command": (txp / rps) if (txp and rps > 0) else None,
    }


# -------------------------------------------------------------------- raw RTT

def rtt_prepare(hosts, src):
    """rsync the two-crate copy of hi-perf-cmp's network-rtt-udp to two hosts
    and build it THERE (the control box's glibc is newer than the fleet's, so
    a locally built binary is not portable). Idempotent."""
    src = str(Path(src).resolve())
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    for h in hosts:
        print(f"INFO [rsync {h.public_ip}] {src}/ -> {RTT_REMOTE_SRC}/", flush=True)
        subprocess.run(["rsync", "-a", "--delete", "--exclude", "target",
                        "--exclude", "network-rtt-udp",
                        "-e", " ".join(shlex.quote(x) for x in h.ssh),
                        "--rsync-path", "sudo rsync",
                        f"{src}/", f"{h.target}:{RTT_REMOTE_SRC}/"], check=True)
        r = ssh(h, f"{env} {m6.SshHost.CARGO} build --release "
                   f"--manifest-path {RTT_REMOTE_SRC}/Cargo.toml && test -x {RTT_REMOTE_BIN} "
                   f"&& echo RTT-BUILT", label="build")
        if "RTT-BUILT" not in (r.stdout or ""):
            raise RuntimeError(f"rtt build on {h.public_ip}: {r.stderr or r.stdout}")


def rtt_measure(server, client, iterations=200_000, label="rtt"):
    """One UDP ping-pong run: responder on `server`, client on `client`, both
    unpinned (the reference is the kernel path an unpinned socket sees).
    Returns {p50_ns, p99_ns, mean_ns, samples} or None."""
    kill_unit(server, "rttsrv")
    cmd = (f"sudo systemd-run --unit={UNIT_PREFIX}-rttsrv --collect -p TimeoutStopSec=1 "
           f"--setenv=RTT_MODE=server --setenv=RTT_UDP_PORT={RTT_PORT} "
           f"-p StandardOutput=append:{unit_log(server, 'rttsrv')} "
           f"-p StandardError=append:{unit_log(server, 'rttsrv')} {RTT_REMOTE_BIN}")
    r = ssh(server, cmd, label="systemd-run")
    if r.returncode != 0:
        raise RuntimeError(f"rtt server on {server.public_ip}: {r.stderr or r.stdout}")
    time.sleep(1.0)
    try:
        ccmd = (f"sudo env RTT_MODE=client RTT_HOST={server.private_ip} "
                f"RTT_UDP_PORT={RTT_PORT} RTT_ITERATIONS={iterations} RTT_WARMUP=20000 "
                f"RTT_PAYLOAD_BYTES=64 {RTT_REMOTE_BIN}")
        r = ssh(client, ccmd, timeout=300, label="rtt")
        out = (r.stdout or "") + (r.stderr or "")
        echo(label, out, lines=6)
        res = {}
        for line in out.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                continue
            if j.get("metric") in ("rtt_p50", "rtt_p99", "rtt_mean"):
                res[j["metric"].replace("rtt_", "") + "_ns"] = j["value"]
                res["samples"] = j.get("samples")
        return res or None
    finally:
        kill_unit(server, "rttsrv")


# ----------------------------------------------------------------------- arms

def run_arm(key, node_hosts, a, ladder, out_rows):
    label, node_env, pinned, service_env = ARMS[key]
    pins = PIN_MAP_C6ID_2XL if pinned else None
    print(f"\n=== ARM {key}: {label} ===", flush=True)
    wipe_dirs(node_hosts)
    start_cluster_arm(node_hosts, a, node_env, pins, service_env)
    try:
        leader = wait_leader(node_hosts, list(range(len(node_hosts))), LEADER_WAIT_SECS)
        if leader is None:
            print(f"INFO arm {key}: no single serving leader within {LEADER_WAIT_SECS}s",
                  flush=True)
            return
        lh = node_hosts[leader]
        iface = detect_iface(lh)
        print(f"INFO arm {key}: leader n{leader} ({lh.public_ip}), NIC {iface}", flush=True)
        client_cpus = pins.get("client") if pins else None
        for k in ladder:
            reps = a.reps1 if k <= 2 else 1
            for rep in range(1, reps + 1):
                l2 = wait_leader(node_hosts, list(range(len(node_hosts))), LEADER_WAIT_SECS)
                if l2 is None:
                    print(f"INFO arm {key}: leader lost before inflight={k}; skipping",
                          flush=True)
                    continue
                if l2 != leader:
                    print(f"INFO arm {key}: leadership moved n{leader} -> n{l2}", flush=True)
                    leader, lh = l2, node_hosts[l2]
                    iface = detect_iface(lh)
                print(f"\nINFO --- arm {key} inflight={k} rep={rep}/{reps} ---", flush=True)
                p = client_point(lh, a, iface, k, a.secs, client_cpus)
                if p is None:
                    continue
                p.update({"arm": key, "arm_label": label, "rep": rep,
                          "leader": leader, "pinned": pinned,
                          "node_env": node_env, "service_env": service_env})
                print("SVCTIME-JSON " + json.dumps(p), flush=True)
                out_rows.append(p)
    finally:
        stop_cluster(node_hosts)


# --------------------------------------------------------------------- report

def fmt(x, w, d=3):
    return f"{x:>{w}.{d}f}" if isinstance(x, (int, float)) else f"{'—':>{w}}"


def report(rows, rtt_before, rtt_after, outdir):
    print("\n\nSERVICE-TIME REPORT — closed-loop round trip through commit + apply + response")
    print("(p50/p90/p99 in ms; inflight 1 = service time, no queueing)\n")
    print(f"  {'arm':>3} {'inflight':>8} {'rep':>3} {'resp/s':>10} {'p50':>8} {'p90':>8} "
          f"{'p99':>8} {'max':>8} {'lost':>5} {'B/cmd':>7} {'pkt/cmd':>8}")
    for p in rows:
        print(f"  {p['arm']:>3} {p['inflight']:>8} {p['rep']:>3} "
              f"{fmt(p['resp_per_sec'],10,0)} {fmt(p['p50_ms'],8)} {fmt(p['p90_ms'],8)} "
              f"{fmt(p['p99_ms'],8)} {fmt(p['max_ms'],8)} {str(p.get('lost','—')):>5} "
              f"{fmt(p['bytes_per_command'],7,1)} {fmt(p['pkts_per_command'],8,3)}")

    print("\nSERVICE TIME AT INFLIGHT 1 (median of reps, µs):")
    summary = {}
    for key in ARM_ORDER:
        pts = [p for p in rows if p["arm"] == key and p["inflight"] == 1
               and p.get("p50_ms") is not None]
        if not pts:
            continue
        med = {q: statistics.median(p[q] for p in pts) * 1000
               for q in ("p50_ms", "p90_ms", "p99_ms")}
        spread = (max(p["p50_ms"] for p in pts) - min(p["p50_ms"] for p in pts)) * 1000
        summary[key] = {"reps": len(pts), "p50_us": med["p50_ms"],
                        "p90_us": med["p90_ms"], "p99_us": med["p99_ms"],
                        "p50_spread_us": spread}
        print(f"  {key:>3} {ARMS[key][0]:<52} p50 {med['p50_ms']:8.1f}  "
              f"p90 {med['p90_ms']:8.1f}  p99 {med['p99_ms']:8.1f}  "
              f"(n={len(pts)}, p50 spread {spread:.1f})")

    def delta(x, y, what):
        if x in summary and y in summary:
            d = summary[x]["p50_us"] - summary[y]["p50_us"]
            print(f"  {x}−{y} = {d:8.1f} µs   ({what})")

    print("\nSUBTRACTIONS (p50, µs):")
    delta("A", "B", "placement / SMT-sibling noise")
    delta("B", "C", "the service apply agent's 50 µs idle sleep")
    delta("C", "D", "the quorum fdatasync on the commit path")
    delta("A", "A2", "drift bracket — same arm, first vs last")

    print("\nRAW UDP ROUND TRIP, same fleet (network-rtt-udp, 64 B, one in flight, unpinned):")
    for lab, r in (("before", rtt_before), ("after", rtt_after)):
        if r:
            print(f"  {lab:>6}: p50 {r.get('p50_ns',0)/1000:7.1f} µs  "
                  f"p99 {r.get('p99_ns',0)/1000:7.1f} µs  "
                  f"mean {r.get('mean_ns',0)/1000:7.1f} µs  (n={r.get('samples')})")
        else:
            print(f"  {lab:>6}: not measured")
    if "D" in summary and rtt_before and rtt_before.get("p50_ns"):
        wire = rtt_before["p50_ns"] / 1000
        d = summary["D"]["p50_us"]
        print(f"\n  Wire share, upper bound: one raw round trip is {wire:.1f} µs of arm D's "
              f"{d:.1f} µs p50 = {100*wire/d:.0f} %. The commit path crosses the wire once "
              f"per position (DATA out, APPEND_POSITION back), so a faster transport can "
              f"recover at most that share; what splits the rest is the net-decomp "
              f"brief's WIRE(P) instrument, not this run.")

    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "points.jsonl").write_text("".join(json.dumps(p) + "\n" for p in rows))
    (outdir / "summary.json").write_text(json.dumps(
        {"inflight1": summary, "rtt_before": rtt_before, "rtt_after": rtt_after}, indent=2))
    print(f"\nINFO wrote {outdir}/points.jsonl and summary.json", flush=True)


# ----------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fleet", action="store_true", required=True)
    ap.add_argument("--hosts", default="", help="pub/priv,... (else terraform output)")
    ap.add_argument("--nodes", type=int, default=4, help="TOTAL hosts; the first 3 are voters")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--secs", type=int, default=20, help="per point (2 s warmup inside)")
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--admission-kib", type=int, default=256)
    ap.add_argument("--ladder", default=DEFAULT_LADDER)
    ap.add_argument("--reps1", type=int, default=3, help="reps at inflight 1 and 2")
    ap.add_argument("--arms", default=",".join(ARM_ORDER))
    ap.add_argument("--rtt-src", default="/home/claude/scratch/rtt-udp-mini",
                    help="two-crate copy of hi-perf-cmp's network-rtt-udp; '' skips the RTT")
    ap.add_argument("--rtt-iterations", type=int, default=200_000)
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    ladder = [int(x) for x in a.ladder.split(",") if x.strip()]
    arms = [k for k in ARM_ORDER if k in {x.strip() for x in a.arms.split(",")}]
    outdir = Path(a.out) if a.out else (
        Path(__file__).resolve().parent.parent.parent / "bench-out"
        / f"service-time-{time.strftime('%Y-%m-%d')}")

    hosts = m6.build_fleet_hosts(
        m12.BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=a.nodes,
        unit_prefix=UNIT_PREFIX, remote_root=m12.REMOTE_ROOT, probe_bin=m12.BUILT_PROBE,
    )
    node_hosts = hosts[:3]
    print(f"INFO voters {[h.public_ip for h in node_hosts]}; arms {arms}; ladder {ladder}; "
          f"secs {a.secs}; reps at 1,2 = {a.reps1}", flush=True)
    for h in node_hosts:
        prepare_host(h, apply_profile=False)
        for u in ("node", "service", "edge", "rttsrv"):
            kill_unit(h, u)
    if any(ARMS[k][2] for k in arms):
        require_pin_layout(node_hosts)

    rtt_before = rtt_after = None
    if a.rtt_src:
        rtt_prepare(node_hosts[:2], a.rtt_src)
        rtt_before = rtt_measure(node_hosts[1], node_hosts[0], a.rtt_iterations, "rtt-before")
        print("RTT-JSON " + json.dumps({"when": "before", **(rtt_before or {})}), flush=True)

    rows = []
    try:
        for key in arms:
            run_arm(key, node_hosts, a, ladder, rows)
    finally:
        stop_cluster(node_hosts)

    if a.rtt_src:
        rtt_after = rtt_measure(node_hosts[1], node_hosts[0], a.rtt_iterations, "rtt-after")
        print("RTT-JSON " + json.dumps({"when": "after", **(rtt_after or {})}), flush=True)

    report(rows, rtt_before, rtt_after, outdir)
    return 0 if rows else 1


if __name__ == "__main__":
    sys.exit(main())
