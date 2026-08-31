#!/usr/bin/env python3
"""Core-count sweep: how many physical cores does a UC NODE actually need?

The question this answers is the one `uc2-m14c2-fleet-pinning-2026-08-31`
raised and could not settle. There, constraining the node to 2 physical cores
cost 9.4 % of mean throughput versus letting the scheduler roam — one point on
a curve nobody had drawn.

METHOD. Hold the hardware, the binary, the workload and the service/client
placement fixed; vary ONLY the number of physical cores the node's four
polling agents may run on. That is what isolates core count from every other
variable — an instance-size sweep would confound cores with cache, memory
bandwidth, NIC and CPU generation all at once.

The node runs four agents (`uc2-{consensus,sender,receiver,archive}`, all
`IdleStrategy::Yield` — they hand the core back when a duty cycle finds no
work, but under saturation they never idle and are CPU-bound). So the naive
prediction is a plateau at 4 cores. This sweep is whether that holds.

Each width gets BOTH SMT threads of each core it is given, so a "3-core" node
means 3 whole physical cores, never 6 half-shared ones.

    python3 bench-infra/scripts/m14_core_sweep.py --fleet --hosts pub/priv,... \
        --widths 1,2,3,4,5,6 --reps 3
"""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
from m12_fleet_gate import kill_unit, ssh  # noqa: E402
from m13_hop_bench import sync_tree  # noqa: E402
from m14_ab_27_vs_28 import one_arm  # noqa: E402

# c8id.4xlarge: 16 logical / 8 physical, siblings (i, i+8). VERIFIED on all
# three voters 2026-08-31 (`lscpu -e=CPU,CORE` -> CORE 0..7 0..7), Intel Xeon
# 6975P-C, 1 socket. `verify_layout` re-checks it at run time rather than
# trusting this comment.
PHYS_CORES = 8
SMT_OFFSET = 8
SERVICE_CORE = 6          # service gets core 6 (CPUs 6,14) for every arm
CLIENT_CORE = 7           # client  gets core 7 (CPUs 7,15) for every arm
MAX_NODE_WIDTH = 6        # cores 0..5 are available to the node


def cpus_of(cores):
    """Both SMT threads of each physical core in `cores`."""
    out = []
    for c in cores:
        out += [c, c + SMT_OFFSET]
    return ",".join(str(x) for x in sorted(out))


def pin_map(width):
    """Node gets `width` whole physical cores; service and client are FIXED
    across every arm so the only thing that moves is the node's allocation."""
    if not 1 <= width <= MAX_NODE_WIDTH:
        raise SystemExit(f"width {width} outside 1..{MAX_NODE_WIDTH}")
    return {
        "node": cpus_of(range(width)),
        "service0": cpus_of([SERVICE_CORE]),
        "service1": cpus_of([SERVICE_CORE]),
        "client": cpus_of([CLIENT_CORE]),
        "edge": cpus_of([CLIENT_CORE]),
    }


def verify_layout(hosts):
    """Refuse to run if the real sibling layout is not (i, i+8) over 8 cores —
    the same fail-closed posture as m12.verify_pin_layout, because a wrong map
    silently measures something other than what it claims."""
    want = {(i, i + SMT_OFFSET) for i in range(PHYS_CORES)}
    for h in hosts:
        r = ssh(h, "lscpu -e=CPU,CORE | tail -n +2", label="lscpu")
        pairs = {}
        for line in (r.stdout or "").split("\n"):
            f = line.split()
            if len(f) >= 2 and f[0].isdigit():
                pairs.setdefault(int(f[1]), []).append(int(f[0]))
        got = {tuple(sorted(v)) for v in pairs.values() if len(v) == 2}
        if got != want:
            raise SystemExit(
                f"{h.public_ip}: sibling layout {sorted(got)} != expected {sorted(want)} "
                f"— the pin map is built on (i, i+{SMT_OFFSET}) over {PHYS_CORES} cores "
                f"and would pin the wrong CPUs. Redraw it before measuring.")
        print(f"INFO [layout {h.public_ip}] siblings verified: (i, i+{SMT_OFFSET}) x {PHYS_CORES}",
              flush=True)


def main():
    ap = argparse.ArgumentParser(description="node core-count sweep (pin width)")
    ap.add_argument("--fleet", action="store_true", required=True)
    ap.add_argument("--hosts", default="", help="pub/priv,... (3 voters)")
    ap.add_argument("--local-tree", default=str(Path(__file__).resolve().parent.parent.parent))
    ap.add_argument("--no-sync", action="store_true")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--widths", default="1,2,3,4,5,6")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--secs", type=int, default=12)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--inflight", type=int, default=4096)
    ap.add_argument("--admission-kib", type=int, default=256)
    ap.add_argument("--timeline", action="store_true",
                    help="ask client-direct for one TL line per elapsed second; the "
                         "per-second series is what shows a REGIME FLIP happening "
                         "rather than leaving it inferred from percentiles")
    ap.add_argument("--unpinned", action="store_true", default=True,
                    help="also run an unpinned control arm (default on)")
    a = ap.parse_args()

    widths = [int(w) for w in a.widths.split(",") if w.strip()]
    hosts = m6.build_fleet_hosts(m12.BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=3,
                                 unit_prefix=m12.UNIT_PREFIX, remote_root=m12.REMOTE_ROOT,
                                 probe_bin=m12.BUILT_PROBE)
    voters = hosts[:3]
    verify_layout(voters)
    if not a.no_sync:
        sync_tree(voters, a.local_tree)
    for h in voters:
        m12.prepare_host(h, apply_profile=False)
        for u in ("client", "service", "edge", "node"):
            kill_unit(h, u)

    # Interleave reps so any session-long drift hits every width equally,
    # rather than loading itself onto whichever width ran last.
    arms = []
    for rep in range(1, a.reps + 1):
        for w in widths:
            arms.append((f"w{w}", pin_map(w), rep))
        if a.unpinned:
            arms.append(("unpinned", None, rep))

    points = []
    try:
        for label, pins, rep in arms:
            p = one_arm(f"{label} rep{rep}", voters, a, pins=pins)
            p["width"] = label
            points.append(p)
            print(f"POINT {json.dumps(p)}", flush=True)
            time.sleep(2.0)
    finally:
        m12.stop_cluster(voters)

    print(f"\nNODE CORE-COUNT SWEEP — {a.reps} reps interleaved, {a.secs} s/arm, "
          f"payload {a.payload}, inflight {a.inflight}")
    print(f"  service pinned to core {SERVICE_CORE}, client to core {CLIENT_CORE}, both fixed")
    keys = [f"w{w}" for w in widths] + (["unpinned"] if a.unpinned else [])
    base = None
    for k in keys:
        xs = [p["rps"] for p in points if p["width"] == k]
        if not xs:
            continue
        mean = statistics.mean(xs)
        spread = 100.0 * (max(xs) - min(xs)) / mean
        if base is None:
            base = mean
        cores = "all 8" if k == "unpinned" else k[1:]
        print(f"  {k:9} ({cores:>5} cores)  mean {mean:12,.0f} resp/s  "
              f"[{min(xs):,.0f} .. {max(xs):,.0f}]  spread {spread:5.1f}%  "
              f"vs w{widths[0]} {mean / base:5.2f}x")
    print("\nSUMMARY-JSON " + json.dumps({
        "widths": keys, "reps": a.reps,
        "by_width": {k: [p["rps"] for p in points if p["width"] == k] for k in keys},
    }))


if __name__ == "__main__":
    main()
