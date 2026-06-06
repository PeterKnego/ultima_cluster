#!/usr/bin/env python3
"""Parse cping's HdrHistogram CLASSIC percentile table from stdin and emit one
shared-schema CSV row. Values in the table are microseconds (cping prints with
scale 1000.0); convert to ns. Usage:
  cping ... | aeron_hdr_to_csv.py --payload 64 --inflight 1 --achieved <rate>
"""
import argparse, re, sys

P = {"p50_ns": 50.0, "p99_ns": 99.0, "p99_9_ns": 99.9, "p99_99_ns": 99.99}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", type=int, required=True)
    ap.add_argument("--inflight", type=int, default=1)
    ap.add_argument("--achieved", type=float, default=0.0)
    a = ap.parse_args()
    vals, count, mx = {}, 0, 0.0
    for line in sys.stdin:
        m = re.match(r"\s*([\d.]+)\s+([\d.]+)\s+(\d+)\s+([\d.]+)", line)
        if not m:
            continue
        value_us, pct = float(m.group(1)), float(m.group(2)) * 100.0
        count = int(m.group(3))
        mx = max(mx, value_us)
        for key, target in P.items():
            if key not in vals and pct >= target:
                vals[key] = value_us
    ns = lambda us: int(us * 1000)
    row = ["aeron", "ipc", "bytes", a.payload, a.inflight, f"{a.achieved:.0f}",
           f"{a.achieved:.1f}",
           ns(vals.get("p50_ns", 0)), ns(vals.get("p99_ns", 0)),
           ns(vals.get("p99_9_ns", 0)), ns(vals.get("p99_99_ns", 0)),
           ns(mx), count]
    print(",".join(str(x) for x in row))

if __name__ == "__main__":
    main()
