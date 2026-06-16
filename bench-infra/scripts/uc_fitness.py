#!/usr/bin/env python3
"""Extract UC distributed-throughput fitness from a commit-path-load sweep CSV.

Prints ONE JSON line:
  {"uc_throughput_msgs": <max achieved_rate>,
   "knee_rate": <highest target_rate sustained (achieved >= 0.95*target)>,
   "p99_at_knee_ms": <p99_ns at the knee rung / 1e6>}

Fitness = uc_throughput_msgs (maximize) = UC's sustained 3-node throughput ceiling.
"""
import csv
import json
import sys


def main(path: str) -> int:
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print(json.dumps({"error": "empty csv"}))
        return 1
    achieved = [(float(r["target_rate"]), float(r["achieved_rate"]),
                 float(r["p99_ns"])) for r in rows]
    ceiling = max(a for _, a, _ in achieved)
    sustained = [(t, p99) for t, a, p99 in achieved if a >= 0.95 * t]
    if sustained:
        knee, knee_p99_ns = max(sustained, key=lambda x: x[0])
    else:
        knee, knee_p99_ns = achieved[0][0], achieved[0][2]
    print(json.dumps({
        "uc_throughput_msgs": round(ceiling, 3),
        "knee_rate": round(knee, 1) if knee % 1 else int(knee),
        "p99_at_knee_ms": round(knee_p99_ns / 1e6, 6),
    }))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: uc_fitness.py <uc_sweep.csv>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
