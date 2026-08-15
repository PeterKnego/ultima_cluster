#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M5 engine A/B — PRE-REGISTERED follow-up to the 2026-08-15 engine re-run
(docs/benchmarks/uc2-m5-engine-gate-2026-08-15.md, "A/B follow-up" section,
committed before this ran).

Arms differ ONLY in the client binary on host0:
  A = /opt/bench/uc-old/target/release/examples/m5_gate  (raw pump, 35daaae)
  B = /opt/bench/uc/target/release/examples/m5_gate      (engine, main)
Node + service processes: the NEW binary on all hosts, all runs (server-side
source is byte-identical between the two trees — verified in the doc).

Primary: 6 interleaved pairs (A,B) @ 256 KiB / W=1024, 15 s, fresh dirs per
run. Secondary (context): 3 pairs @ 128 KiB / W=1024.
Decide (pre-committed): median(B_i/A_i) < 0.95 -> ENGINE COST CONFIRMED;
>= 0.95 -> PARITY. Per-run invalidation (redirects/lost/inflight > 0) ->
re-run that run once. Exit 0 always unless the harness itself fails.
"""

import json
import re
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from m6_fleet_gate import SshHost  # noqa: E402
from m5_fleet_gate import (  # noqa: E402
    FIELDS, LAT, GATE_BIN, INST, PORT, SECS, PAYLOAD, tf_hosts, parse_console,
)

OLD_GATE_BIN = "/opt/bench/uc-old/target/release/examples/m5_gate"
PRIMARY = (256, 1024, 6)   # (admission KiB, W, pairs)
SECONDARY = (128, 1024, 3)


def start_cluster(hosts, adm_kib):
    members = ",".join(f"{i}@{h.private_ip}:{PORT}" for i, h in enumerate(hosts))

    def reset(h):
        h.kill_unit("node"); h.kill_unit("service")
        h._ssh(f"sudo rm -rf {INST} && sudo mkdir -p {INST}", capture_output=True)

    with ThreadPoolExecutor(3) as ex:
        list(ex.map(reset, hosts))
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
    time.sleep(1.5)


def one_run(hosts, client_host, adm_kib, inflight, tag, outdir):
    start_cluster(hosts, adm_kib)
    code, out = client_host.run_foreground(
        ["client", "--instance-dir", INST, "--secs", str(SECS),
         "--payload", str(PAYLOAD), "--inflight", str(inflight)],
        timeout=SECS + 75,
    )
    (outdir / f"ab-{tag}.console.txt").write_text(out)
    row = parse_console(out)
    row.update(tag=tag, exit=code)
    # Old binary has no `lost` line: treat missing as 0 for invalidation.
    bad = [f"{k}={row[k]}" for k in ("not_leader", "inflight_end")
           if (row.get(k) or 0) > 0]
    if (row.get("lost") or 0) > 0:
        bad.append(f"lost={row['lost']}")
    if row["result"] == "UNPARSED":
        bad.append("unparsed")
    row["invalid"] = bad
    return row


def run_block(hosts, old_client, new_client, adm, w, pairs, outdir, results):
    for p in range(1, pairs + 1):
        for arm, ch in (("A", old_client), ("B", new_client)):
            for attempt in (1, 2):
                tag = f"adm{adm}_w{w}_p{p}{arm}_try{attempt}"
                print(f"== {tag} ==", flush=True)
                row = one_run(hosts, ch, adm, w, tag, outdir)
                print(f"   rps={row['rps']} p50={row['p50_ms']}ms"
                      + (f" INVALID:{row['invalid']}" if row["invalid"] else ""),
                      flush=True)
                if not row["invalid"] or attempt == 2:
                    row.update(arm=arm, pair=p, adm_kib=adm, inflight=w)
                    results.append(row)
                    break


def summarize(results, adm, w, pairs, label):
    block = [r for r in results if r["adm_kib"] == adm and r["inflight"] == w]
    ratios, lines = [], []
    for p in range(1, pairs + 1):
        a = next((r for r in block if r["pair"] == p and r["arm"] == "A"), None)
        b = next((r for r in block if r["pair"] == p and r["arm"] == "B"), None)
        if a and b and a["rps"] and b["rps"]:
            r = b["rps"] / a["rps"]
            ratios.append(r)
            lines.append(f"| {p} | {a['rps']:,} @ {a['p50_ms']} | "
                         f"{b['rps']:,} @ {b['p50_ms']} | {r:.4f} |")
    med = statistics.median(ratios) if ratios else float("nan")
    a_med = statistics.median([r["rps"] for r in block if r["arm"] == "A" and r["rps"]])
    b_med = statistics.median([r["rps"] for r in block if r["arm"] == "B" and r["rps"]])
    print(f"\n### {label} ({adm} KiB / W={w}, {pairs} pairs)")
    print("| pair | A (old pump) rps @ p50ms | B (engine) rps @ p50ms | B/A |")
    print("|---|---|---|---|")
    for l in lines:
        print(l)
    print(f"medians: A={a_med:,.0f}  B={b_med:,.0f}  ratio(pairwise med)={med:.4f} "
          f"min={min(ratios):.4f} max={max(ratios):.4f}")
    return med


def main():
    outdir = Path(__file__).parent.parent.parent / "bench-out" / "m5-ab-2026-08-15"
    outdir.mkdir(parents=True, exist_ok=True)
    hosts, user, key = tf_hosts()
    print(f"hosts: {[(h.public_ip, h.private_ip) for h in hosts]}")
    # Two client handles for host0 — same host, different gate binary.
    h0 = hosts[0]
    old_client = SshHost(OLD_GATE_BIN, INST, h0.public_ip, h0.private_ip, user, key,
                         unit_prefix="m5", remote_root="/opt/bench/m5")
    new_client = SshHost(GATE_BIN, INST, h0.public_ip, h0.private_ip, user, key,
                         unit_prefix="m5", remote_root="/opt/bench/m5")

    results = []
    adm, w, pairs = PRIMARY
    run_block(hosts, old_client, new_client, adm, w, pairs, outdir, results)
    s_adm, s_w, s_pairs = SECONDARY
    run_block(hosts, old_client, new_client, s_adm, s_w, s_pairs, outdir, results)

    for h in hosts:
        h.kill_unit("node"); h.kill_unit("service")

    (outdir / "ab.jsonl").write_text("\n".join(json.dumps(r) for r in results) + "\n")

    med = summarize(results, adm, w, pairs, "PRIMARY")
    summarize(results, s_adm, s_w, s_pairs, "SECONDARY (context only)")
    verdict = "ENGINE COST CONFIRMED" if med < 0.95 else "PARITY"
    print(f"\nPRE-COMMITTED VERDICT (primary median ratio {med:.4f} vs 0.95): {verdict}")


if __name__ == "__main__":
    main()
