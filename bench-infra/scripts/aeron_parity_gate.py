#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
UC v2 vs Aeron Cluster same-conditions scorecard driver — PRE-REGISTERED in
docs/benchmarks/uc2-aeron-parity-2026-08-15.md (committed before this ran).

Arms, in order (bracketing anchors detect intra-session fleet drift):
  1. UC anchor: m5_gate engine client, 256/1024 + 128/1024 (reuses
     m5_fleet_gate machinery)
  2. Aeron SHARED:    rate {200k..1400k} x batch {64,256}, fresh cluster
  3. Aeron DEDICATED: same grid
  4. UC anchor repeat

Aeron client edge: aeron:ipc ingress+egress, node0-ONLY IPC ingress render
(followers keep UDP ingress channel+endpoints — avoids task13 §11's crash
class by construction). A time-boxed smoke validates the IPC edge first;
on failure the run falls back to the UDP client edge and says so.

Every rung's rig stdout is saved verbatim (parse best-effort; the .hdr files
are scp'd at the end regardless). A rung is VALID iff no .FAIL marker for
its output file and the rig completed within timeout.
"""

import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from m6_fleet_gate import SshHost  # noqa: E402
from m5_fleet_gate import tf_hosts, run_point, GATE_BIN, INST  # noqa: E402

BENCH = "/opt/bench"
CFG = f"{BENCH}/aeron-cfg"
SCRIPTS = f"{BENCH}/aeron-deploy/scripts/aeron"
RESULTS = f"{BENCH}/results"

RATES = [200_000, 400_000, 600_000, 800_000, 1_000_000, 1_200_000, 1_400_000]
BATCHES = [64, 256]
WARMUP_S = 10
MEASURE_S = 15
MODES = ["shared", "dedicated"]

JAVA_ENV = ('export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"; ')


def sh(host, cmd, timeout=60):
    r = host._ssh(cmd, capture_output=True, timeout=timeout)
    return r.returncode, (r.stdout or "") + (r.stderr or "")


def aeron_clean(host):
    sh(host, "sudo bash -c \"pkill -9 -f '[i]o.aeron' || true; "
             f"rm -rf /dev/shm/*-driver {BENCH}/cluster {RESULTS}; "
             f"mkdir -p {RESULTS}\"")


def prep_props(host, role, mode, ipc):
    """Render per-mode node props + per-run cluster/client props on the host."""
    # node-<mode>.properties: base + threading mode (uppercase).
    cmds = [
        f"cp {CFG}/node.properties {CFG}/node-run.properties",
        f"echo 'aeron.threading.mode={mode.upper()}' >> {CFG}/node-run.properties",
        f"cp {CFG}/cluster.properties {CFG}/cluster-run.properties",
    ]
    if ipc and role == "node0":
        # node0-ONLY IPC ingress: strip the UDP ingress lines, add IPC channel
        # WITHOUT endpoints (the §11 crash was endpoint-append on IPC media).
        cmds += [
            f"sed -i '/aeron.cluster.ingress.channel=/d;/aeron.cluster.ingress.endpoints=/d' {CFG}/cluster-run.properties",
            f"echo 'aeron.cluster.ingress.channel=aeron:ipc' >> {CFG}/cluster-run.properties",
        ]
    if role == "node0":
        cmds += [f"cp {CFG}/client.properties {CFG}/client-run.properties"]
        if ipc:
            cmds += [
                f"sed -i '/aeron.cluster.egress.channel=/d' {CFG}/client-run.properties",
                f"echo 'aeron.cluster.egress.channel=aeron:ipc' >> {CFG}/client-run.properties",
            ]
    rc, out = sh(host, "sudo bash -c \"" + " && ".join(cmds) + "\"")
    if rc != 0:
        raise RuntimeError(f"prep_props {host.public_ip}: {out}")


def start_aeron(hosts, mode, ipc):
    roles = [f"node{i}" for i in range(len(hosts))]
    for h, role in zip(hosts, roles):
        aeron_clean(h)
        prep_props(h, role, mode, ipc)
    for h in hosts:
        sh(h, "sudo bash -c '" + JAVA_ENV +
           "export JVM_OPTS=\"-Xms16M\"; "
           f"setsid nohup {SCRIPTS}/media-driver {CFG}/cluster-run.properties "
           f"{CFG}/node-run.properties > {BENCH}/md.out 2>&1 < /dev/null &'")
    time.sleep(3)
    for h in hosts:
        sh(h, "sudo bash -c '" + JAVA_ENV +
           f"export JVM_OPTS=\"-Xms16M -Dio.aeron.benchmarks.output.directory={RESULTS}\"; "
           f"setsid nohup {SCRIPTS}/cluster-node {CFG}/cluster-run.properties "
           f"{CFG}/node-run.properties > {BENCH}/node.out 2>&1 < /dev/null &'")
    time.sleep(20)  # election settle (v1 protocol)


def stop_aeron(hosts):
    for h in hosts:
        sh(h, "sudo bash -c \"pkill -9 -f '[i]o.aeron' || true\"")


def run_rung(host0, rate, batch, tag, warmup_s=WARMUP_S, measure_s=MEASURE_S, outdir=None):
    jvm = (f"-Xms64M -Dio.aeron.benchmarks.output.directory={RESULTS} "
           f"-Dio.aeron.benchmarks.message.rate={rate} "
           f"-Dio.aeron.benchmarks.message.length=64 "
           f"-Dio.aeron.benchmarks.batch.size={batch} "
           f"-Dio.aeron.benchmarks.warmup.iterations={warmup_s} "
           f"-Dio.aeron.benchmarks.warmup.message.rate={rate} "
           f"-Dio.aeron.benchmarks.iterations={measure_s} "
           f"-Dio.aeron.benchmarks.output.file={tag}")
    cmd = ("sudo bash -c '" + JAVA_ENV +
           f"export JVM_OPTS=\"{jvm}\"; "
           f"timeout {warmup_s + measure_s + 90} {SCRIPTS}/cluster-client "
           f"{CFG}/cluster-run.properties {CFG}/client-run.properties "
           f"> {BENCH}/rung-{tag}.out 2>&1; echo rc=$?; tail -c 4000 {BENCH}/rung-{tag}.out'")
    rc, out = sh(host0, cmd, timeout=warmup_s + measure_s + 120)
    if outdir:
        (outdir / f"{tag}.stdout.txt").write_text(out)
    # Validity is ARTIFACT-based (the rig console is unreliable over ssh): a
    # plain .hdr for the tag = sustained; .FAIL = not sustained; neither = broken.
    _, ls_out = sh(host0, f"ls {RESULTS} | grep '^{tag}_' || true")
    files = ls_out.split()
    has_ok = any(f.endswith(".hdr") for f in files)
    has_fail = any(f.endswith(".FAIL") for f in files)
    row = {"tag": tag, "rate": rate, "batch": batch, "rc": rc,
           "artifact_ok": has_ok, "failed_marker": has_fail,
           "p50_us": None, "p90_us": None, "p99_us": None}
    return row


def fill_percentiles(row, mode_dir):
    """Parse the aggregator's -report.hgrm for this rung (value column is µs)."""
    reports = list(mode_dir.glob(f"{row['tag']}_*-report.hgrm"))
    if not reports:
        return
    want = {"p50_us": "0.500000000000", "p90_us": "0.900000000000",
            "p99_us": "0.990000000000"}
    for line in reports[0].read_text().splitlines():
        parts = line.split()
        if len(parts) >= 3:
            for k, pct in want.items():
                if parts[1] == pct and row[k] is None:
                    row[k] = float(parts[0])


def main():
    outdir = Path(__file__).parent.parent.parent / "bench-out" / "aeron-parity-2026-08-15"
    outdir.mkdir(parents=True, exist_ok=True)
    hosts, user, key = tf_hosts()
    h0 = hosts[0]
    print(f"hosts: {[(h.public_ip, h.private_ip) for h in hosts]}", flush=True)

    results = {"uc_anchor_pre": [], "uc_anchor_post": [], "aeron": [],
               "client_edge": None}

    # ---- 1. UC anchor (pre) --------------------------------------------
    import os
    stop_aeron(hosts)
    pre_pts = () if os.environ.get("SKIP_PRE_ANCHOR") else ((256, 1024), (128, 1024))
    for adm, w in pre_pts:
        print(f"== UC anchor pre adm={adm} W={w} ==", flush=True)
        row = run_point(hosts, adm, w, outdir, 1)
        print(f"   rps={row['rps']} p50={row['p50_ms']}ms"
              + (f" INVALID:{row['invalid']}" if row.get("invalid") else ""), flush=True)
        results["uc_anchor_pre"].append(row)
    for h in hosts:
        h.kill_unit("node"); h.kill_unit("service")

    # ---- 2. IPC-edge validation smoke ----------------------------------
    print("== IPC edge validation smoke (shared mode, 3s) ==", flush=True)
    start_aeron(hosts, "shared", ipc=True)
    smoke = run_rung(h0, 1000, 1, "ipc_smoke", warmup_s=2, measure_s=3, outdir=outdir)
    ipc_ok = smoke["artifact_ok"] and not smoke["failed_marker"]
    if not ipc_ok:
        _, node_out = sh(h0, f"tail -20 {BENCH}/node.out")
        print(f"   IPC smoke FAILED (rc={smoke['rc']}); node.out tail:\n{node_out}", flush=True)
        print("   FALLING BACK to UDP client edge (pre-registered fallback)", flush=True)
    else:
        print(f"   IPC smoke OK: p50={smoke['p50_us']}us", flush=True)
    results["client_edge"] = "ipc" if ipc_ok else "udp"
    stop_aeron(hosts)

    # ---- 3+4. Aeron sweeps, both modes ---------------------------------
    for mode in MODES:
        print(f"== Aeron {mode.upper()} sweep (client edge: {results['client_edge']}) ==", flush=True)
        start_aeron(hosts, mode, ipc=ipc_ok)
        for batch in BATCHES:
            for rate in RATES:
                tag = f"aeron_{mode}_b{batch}_r{rate}"
                row = run_rung(h0, rate, batch, tag, outdir=outdir)
                row["mode"] = mode
                ok = row["rc"] == 0 and not row["failed_marker"]
                print(f"   {tag}: p50={row['p50_us']}us rc={row['rc']}"
                      f"{' FAIL-marker' if row['failed_marker'] else ''}", flush=True)
                row["valid"] = ok
                results["aeron"].append(row)
        # Aggregate + pull this mode's artifacts BEFORE the next mode cleans RESULTS.
        sh(h0, "sudo bash -c '" + JAVA_ENV +
           f"{SCRIPTS}/../aggregate-results {RESULTS} > /dev/null 2>&1 || true'", timeout=180)
        mode_dir = outdir / f"rig-{mode}"
        mode_dir.mkdir(exist_ok=True)
        subprocess.run(["rsync", "-az", "-e",
                        f"ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes -i {key} -l {user}",
                        "--rsync-path", "sudo rsync",
                        f"{h0.public_ip}:{RESULTS}/", str(mode_dir) + "/"], check=False)
        for row in results["aeron"]:
            if row.get("mode") == mode and row["valid"]:
                fill_percentiles(row, mode_dir)
        stop_aeron(hosts)

    # ---- 5. UC anchor (post) -------------------------------------------
    for adm, w in ((256, 1024), (128, 1024)):
        print(f"== UC anchor post adm={adm} W={w} ==", flush=True)
        row = run_point(hosts, adm, w, outdir, 2)
        print(f"   rps={row['rps']} p50={row['p50_ms']}ms", flush=True)
        results["uc_anchor_post"].append(row)
    for h in hosts:
        h.kill_unit("node"); h.kill_unit("service")

    # ---- collect + summarize -------------------------------------------
    subprocess.run(["rsync", "-az", "-e",
                    f"ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes -i {key} -l {user}",
                    "--rsync-path", "sudo rsync",
                    f"{h0.public_ip}:{RESULTS}/", str(outdir / "rig-results") + "/"],
                   check=False)
    (outdir / "results.json").write_text(json.dumps(results, indent=1))

    print("\n=== AERON GRID (valid rungs; p50 in us) ===")
    print("| mode | batch | rate | p50 | p90 | p99 | valid |")
    print("|---|---|---|---|---|---|---|")
    for r in results["aeron"]:
        print(f"| {r['mode']} | {r['batch']} | {r['rate']:,} | {r['p50_us']} "
              f"| {r['p90_us']} | {r['p99_us']} | {r['valid']} |")
    for label in ("uc_anchor_pre", "uc_anchor_post"):
        for r in results[label]:
            print(f"{label}: adm={r['adm_kib']} W={r['inflight']} "
                  f"rps={r['rps']:,} p50={r['p50_ms']}ms")
    print(f"client edge used: {results['client_edge']}")


if __name__ == "__main__":
    main()
