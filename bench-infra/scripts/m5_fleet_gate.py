#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M5 fleet-gate sweep driver — the 2026-08-15 ENGINE RE-RUN of the original
2026-07-12 M5 gate, per the pre-committed decide rule in
docs/benchmarks/uc2-m5-engine-gate-2026-08-15.md (written & committed BEFORE
this ran). Reuses m6_fleet_gate.SshHost for all ssh/systemd-run plumbing.

Per sweep point (admission KiB, inflight W), on a 3-host fleet:
  1. kill any leftover units, wipe + recreate /opt/bench/m5/inst on all hosts
  2. start node0 FIRST (deliberate leader bias, original protocol), sleep,
     start node1/node2, start all three services
  3. run the client on host0 FOREGROUND (blocking ssh): --secs 15
     --payload 64 --inflight W  (the binary itself blocks up to 30 s for
     CAN_SERVE)
  4. parse the verbatim console; apply the doc's invalidation rules
     (not_leader>0 / lost>0 / inflight>0 / overwritten>0 -> re-run point ONCE)
  5. append the verbatim console + a JSON line to bench-out/

Exit 0 iff >=1 point prints RESULT: PASS (the binary's own bar). The PARITY
verdict (rule 2) is computed and printed but PASS/FAIL exit is rule 1 —
parity is adjudicated in the gate doc.
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

SWEEP = [  # (admission_kib, inflight) — the original 7 points, same order
    (64, 4096), (128, 4096), (256, 4096),
    (64, 1024), (128, 1024), (256, 1024),
    (64, 512),
]
SECS = 15
PAYLOAD = 64
PORT = 19100
GATE_BIN = "/opt/bench/uc/target/release/examples/m5_gate"
INST = "/opt/bench/m5/inst"
BASELINE_BEST = 1_639_187  # 256KiB/1024 from uc2-m5-gate-2026-07-12.md
PARITY_FLOOR = int(BASELINE_BEST * 0.9)  # pre-committed: 1,475,268

FIELDS = {
    "sends": r"sends\s*:\s*(\d+)",
    "responses": r"responses\s*:\s*(\d+)",
    "not_leader": r"not_leader redirects\s*:\s*(\d+)",
    "retries": r"retries\s*:\s*(\d+)",
    "dups": r"dup responses dropped\s*:\s*(\d+)",
    "overwritten": r"broadcast overwritten\s*:\s*(\d+)",
    "lost": r"lost \(timeout/restart\)\s*:\s*(\d+)",
    "inflight_end": r"in-flight at end\s*:\s*(\d+)",
    "rps": r"responses/s\s*:\s*(\d+)",
}
LAT = {
    "p50_ms": r"p50\s*:\s*([\d.]+) ms",
    "p90_ms": r"p90\s*:\s*([\d.]+) ms",
    "p99_ms": r"p99\s*:\s*([\d.]+) ms",
    "max_ms": r"max\s*:\s*([\d.]+) ms",
}


def tf_hosts():
    tf_dir = Path(__file__).parent.parent / "terraform"
    nodes = json.loads(subprocess.run(
        ["terraform", f"-chdir={tf_dir}", "output", "-json", "nodes"],
        capture_output=True, text=True, check=True).stdout)
    user = subprocess.run(
        ["terraform", f"-chdir={tf_dir}", "output", "-raw", "ssh_user"],
        capture_output=True, text=True, check=True).stdout.strip()
    key = None
    for line in (Path(__file__).parent.parent / "terraform.tfvars").read_text().splitlines():
        if "ssh_private_key_file" in line:
            key = line.split('"')[1]
    assert key, "ssh_private_key_file not found in terraform.tfvars"
    nodes = sorted(nodes, key=lambda n: n["name"])
    return [
        SshHost(GATE_BIN, INST, n["public_ip"], n["private_ip"], user, key,
                unit_prefix="m5", remote_root="/opt/bench/m5")
        for n in nodes
    ], user, key


def parse_console(out: str):
    row = {}
    for k, rx in FIELDS.items():
        m = re.search(rx, out)
        row[k] = int(m.group(1)) if m else None
    for k, rx in LAT.items():
        m = re.search(rx, out)
        row[k] = float(m.group(1)) if m else None
    row["result"] = "PASS" if "RESULT: PASS" in out else (
        "FAIL" if "RESULT: FAIL" in out else "UNPARSED")
    return row


def run_point(hosts, adm_kib, inflight, outdir, attempt):
    members = ",".join(f"{i}@{h.private_ip}:{PORT}" for i, h in enumerate(hosts))
    # fresh dirs + dead units everywhere
    def reset(h):
        h.kill_unit("node"); h.kill_unit("service")
        h._ssh(f"sudo rm -rf {INST} && sudo mkdir -p {INST}", capture_output=True)
    with ThreadPoolExecutor(3) as ex:
        list(ex.map(reset, hosts))
    # node0 first — deliberate leader bias (original protocol)
    for i, h in enumerate(hosts):
        h.start_unit("node", [
            "node", "--id", str(i), "--bind", f"{h.private_ip}:{PORT}",
            "--members", members, "--instance-dir", INST,
            "--admission-kib", str(adm_kib),
        ])
        if i == 0:
            time.sleep(0.7)
    for h in hosts:
        h.start_unit("service", ["service", "--instance-dir", INST])
    time.sleep(1.5)  # let the biased election settle before the client attaches
    code, out = hosts[0].run_foreground(
        ["client", "--instance-dir", INST, "--secs", str(SECS),
         "--payload", str(PAYLOAD), "--inflight", str(inflight)],
        timeout=SECS + 75,
    )
    tag = f"adm{adm_kib}_w{inflight}_try{attempt}"
    (outdir / f"m5-{tag}.console.txt").write_text(out)
    row = parse_console(out)
    row.update(adm_kib=adm_kib, inflight=inflight, attempt=attempt, exit=code)
    return row


def invalid(row):
    reasons = []
    for k in ("not_leader", "lost", "inflight_end", "overwritten"):
        if (row.get(k) or 0) > 0:
            reasons.append(f"{k}={row[k]}")
    if row["result"] == "UNPARSED":
        reasons.append("console unparsed")
    return reasons


def main():
    outdir = Path(__file__).parent.parent.parent / "bench-out" / "m5-engine-2026-08-15"
    outdir.mkdir(parents=True, exist_ok=True)
    hosts, user, key = tf_hosts()
    print(f"hosts: {[(h.public_ip, h.private_ip) for h in hosts]}")
    print("prepare (warm build check) ...")
    with ThreadPoolExecutor(3) as ex:
        list(ex.map(lambda h: h.prepare(examples=("m5_gate",)), hosts))

    results = []
    for adm, w in SWEEP:
        for attempt in (1, 2):  # invalidation rule: one re-run per point
            print(f"== point adm={adm}KiB W={w} (attempt {attempt}) ==", flush=True)
            row = run_point(hosts, adm, w, outdir, attempt)
            bad = invalid(row)
            print(f"   rps={row['rps']} p50={row['p50_ms']}ms result={row['result']}"
                  + (f" INVALID: {bad}" if bad else ""), flush=True)
            if not bad or attempt == 2:
                row["invalid"] = bad
                results.append(row)
                break

    for h in hosts:
        h.teardown() if hasattr(h, "teardown") else None
        h.kill_unit("node"); h.kill_unit("service")

    (outdir / "sweep.jsonl").write_text(
        "\n".join(json.dumps(r) for r in results) + "\n")

    print("\n| admission | inflight W | responses/s | p50 | p90 | p99 | GATE |")
    print("|---|---|---|---|---|---|---|")
    any_pass = False
    best_point = None
    for r in results:
        gate = r["result"] + (" (INVALID)" if r["invalid"] else "")
        any_pass |= (r["result"] == "PASS" and not r["invalid"])
        if r["adm_kib"] == 256 and r["inflight"] == 1024:
            best_point = r
        print(f"| {r['adm_kib']} KiB | {r['inflight']} | {r['rps']:,} "
              f"| {r['p50_ms']} ms | {r['p90_ms']} ms | {r['p99_ms']} ms | {gate} |")

    print(f"\nRULE 1 (gate bar): {'PASS' if any_pass else 'FAIL'}")
    if best_point and best_point["rps"] is not None:
        parity = best_point["rps"] >= PARITY_FLOOR and (best_point["p50_ms"] or 99) <= 1.0
        print(f"RULE 2 (parity @256/1024): rps={best_point['rps']:,} vs floor "
              f"{PARITY_FLOOR:,} (90% of {BASELINE_BEST:,}) -> "
              f"{'PARITY' if parity else 'REGRESSION'}")
    sys.exit(0 if any_pass else 1)


if __name__ == "__main__":
    main()
