#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
Eventual-interval retest — PRE-REGISTERED 2026-08-17 in
docs/benchmarks/uc2-aeron-parity-2026-08-15.md ("Eventual-interval retest").

Four arms INTERLEAVED x3 rounds at 256KiB/W=1024, 15s each:
  fsync   : default Consistent
  ev50    : UC2_JOURNAL_DURABILITY=eventual (interval default 50ms)
  ev5     : + UC2_JOURNAL_EVENTUAL_FSYNC_MS=5
  ev1     : + UC2_JOURNAL_EVENTUAL_FSYNC_MS=1
Decide rule lives in the doc; this driver only reports medians.
"""

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from m5_fleet_gate import tf_hosts, run_point  # noqa: E402
from eventual_arm_gate import EnvSshHost  # noqa: E402

ARMS = [
    ("fsync", None),
    ("ev50", {"UC2_JOURNAL_DURABILITY": "eventual"}),
    ("ev5", {"UC2_JOURNAL_DURABILITY": "eventual",
             "UC2_JOURNAL_EVENTUAL_FSYNC_MS": "5"}),
    ("ev1", {"UC2_JOURNAL_DURABILITY": "eventual",
             "UC2_JOURNAL_EVENTUAL_FSYNC_MS": "1"}),
]
ROUNDS = 3
ADM, W = 256, 1024


def main():
    outdir = Path(__file__).parent.parent.parent / "bench-out" / "interval-retest-2026-08-17"
    outdir.mkdir(parents=True, exist_ok=True)
    hosts, user, key = tf_hosts()
    print(f"hosts: {[(h.public_ip, h.private_ip) for h in hosts]}", flush=True)
    armed = {name: ([EnvSshHost(h, env) for h in hosts] if env else hosts)
             for name, env in ARMS}

    rows = []
    for rnd in range(1, ROUNDS + 1):
        for name, _env in ARMS:
            tag = f"{name}_r{rnd}"
            print(f"== {tag} ==", flush=True)
            row = run_point(armed[name], ADM, W, outdir, tag)
            print(f"   rps={row['rps']} p50={row['p50_ms']}ms p99={row['p99_ms']}ms"
                  + (f" INVALID:{row['invalid']}" if row.get("invalid") else ""), flush=True)
            row.update(arm=name, round=rnd)
            rows.append(row)
            for h in armed[name]:
                h.kill_unit("node"); h.kill_unit("service")

    (outdir / "rows.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    print("\n| arm | rps (3 runs) | p50 ms | p99 ms (3 runs) | median p99 |")
    print("|---|---|---|---|---|")
    med = {}
    for name, _ in ARMS:
        sub = [r for r in rows if r["arm"] == name and r["rps"]]
        p99s = [r["p99_ms"] for r in sub]
        med[name] = statistics.median(p99s)
        rps_cell = "/".join(f"{r['rps']:,}" for r in sub)
        print(f"| {name} | {rps_cell} "
              f"| {'/'.join(str(r['p50_ms']) for r in sub)} "
              f"| {'/'.join(str(x) for x in p99s)} | {med[name]:.3f} |")

    base = med["fsync"]
    print(f"\nDECIDE (doc rule): ev50 {med['ev50']/base:.2f}x fsync (need >2x), "
          f"ev5 {med['ev5']/base:.2f}x (need <=1.5x) -> "
          f"{'MECHANISM CONFIRMED' if med['ev50'] > 2*base and med['ev5'] <= 1.5*base else 'NOT CONFIRMED'}")


if __name__ == "__main__":
    main()
