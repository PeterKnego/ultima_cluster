#!/usr/bin/env python3
"""A/B on the fleet: `v2.7.0`'s `m12_gate` vs `main`'s (2.8.0), same hosts,
same driver, same arm — the M12 row-1 direct arm (`client-direct`, session
envelope on, one shmem client on the leader host).

    A = v2.7.0   tree rsynced to /opt/bench/uc27, built there
    B = main     tree rsynced to /opt/bench/uc,   built there (m12.prepare_host)

Interleaved reps (A B A B …) on FRESH clusters: every arm wipes the instance
dirs and boots all three voters on ONE version, so no mixed-wire cluster ever
exists (2.7.0 is wire 0.5.0 / cnc 2.x; 2.8.0 is 0.6.0 / cnc 3.0). Each
version's cluster is probed by its own `m6_gate` (the cnc page layout differs).

The rate is the RESULT line's whole-run `responses_per_sec` — the one number
both harness versions print (2.7.0 has no steady-window flags). This is a
MEASUREMENT: no bar. Resolution is the observed rep-to-rep spread, printed
beside the means; a delta inside it is "not detectable", not "none".

    python3 bench-infra/scripts/m14_ab_27_vs_28.py --fleet --hosts pub/priv,... \
        --tree27 /home/claude/ultima/uc-v270 --reps 3
"""

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
from m12_fleet_gate import ssh, kill_unit  # noqa: E402
from m13_hop_bench import sync_tree  # noqa: E402

SRC27 = "/opt/bench/uc27"
GATE27 = f"{SRC27}/target/release/examples/m12_gate"
PROBE27 = f"{SRC27}/target/release/examples/m6_gate"


def sync_tree_to(hosts, local_tree, remote_dir):
    for h in hosts:
        cmd = ["rsync", "-az", "--delete",
               "--exclude=target", "--exclude=.git", "--exclude=bench-out",
               "--exclude=bench-infra", "--exclude=.claude", "--exclude=.superpowers",
               "-e", " ".join(h.ssh), "--rsync-path", "sudo rsync",
               f"{local_tree.rstrip('/')}/", f"{h.target}:{remote_dir}/"]
        print(f"INFO [rsync {h.public_ip}] {' '.join(cmd)}", flush=True)
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"rsync to {h.public_ip} failed: {r.stderr}")


def prepare_host_27(host):
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    cargo = m6.SshHost.CARGO
    cmd = (f"sudo mkdir -p {SRC27} && "
           f"{env} {cargo} build --release --manifest-path {SRC27}/Cargo.toml "
           f"-p uc2_node --example m6_gate && "
           f"{env} {cargo} build --release --manifest-path {SRC27}/Cargo.toml "
           f"-p uc2_gateway --example m12_gate && test -x {GATE27} && echo PREPARED27")
    r = ssh(host, cmd, label="build-2.7.0")
    if "PREPARED27" not in (r.stdout or ""):
        raise RuntimeError(f"2.7.0 build on {host.public_ip}: {(r.stderr or r.stdout)[-2000:]}")


def one_arm(label, hosts, a):
    voters = hosts[:3]
    m12.stop_cluster(voters)
    m12.wipe_dirs(voters)
    m12.start_cluster(voters, a, "on")
    leader = m6.wait_leader(voters, [0, 1, 2], m12.LEADER_WAIT_SECS)
    if leader is None:
        raise RuntimeError(f"{label}: no single serving leader")
    d = m12.run_direct_arm(voters, leader, a, "on", payload=a.payload, secs=a.secs)
    m12.stop_cluster(voters)
    if d is None:
        raise RuntimeError(f"{label}: no RESULT line")
    return {"label": label, "leader": leader, "rps": float(d["responses_per_sec"]),
            "p50": d["p50_ms"], "p99": d["p99_ms"], "lost": d["lost"],
            "responses": d["responses"]}


def summarize(points):
    out = {}
    for ver in ("A-2.7.0", "B-2.8.0"):
        xs = [p["rps"] for p in points if p["label"].startswith(ver)]
        if xs:
            out[ver] = {"n": len(xs), "mean": statistics.mean(xs), "min": min(xs), "max": max(xs),
                        "spread_pct": 100.0 * (max(xs) - min(xs)) / statistics.mean(xs),
                        "p50_ms": statistics.mean(p["p50"] for p in points if p["label"].startswith(ver)),
                        "p99_ms": statistics.mean(p["p99"] for p in points if p["label"].startswith(ver))}
    if "A-2.7.0" in out and "B-2.8.0" in out:
        out["ratio_B_over_A"] = out["B-2.8.0"]["mean"] / out["A-2.7.0"]["mean"]
        # Overlap test: do the two rep ranges intersect? If they do, the delta is
        # inside the harness's own resolution (CLAUDE.md, M14c lesson).
        a, b = out["A-2.7.0"], out["B-2.8.0"]
        out["ranges_overlap"] = not (a["max"] < b["min"] or b["max"] < a["min"])
    return out


def main():
    ap = argparse.ArgumentParser(description="fleet A/B: m12_gate direct arm, v2.7.0 vs main")
    ap.add_argument("--fleet", action="store_true", required=True)
    ap.add_argument("--hosts", default="", help="pub/priv,... (4 entries; the first 3 are voters)")
    ap.add_argument("--tree27", required=True, help="local checkout of v2.7.0 (git worktree)")
    ap.add_argument("--local-tree", default=str(Path(__file__).resolve().parent.parent.parent))
    ap.add_argument("--no-sync", action="store_true")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--secs", type=int, default=12)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--inflight", type=int, default=4096)
    ap.add_argument("--admission-kib", type=int, default=256)
    ap.add_argument("--order", choices=("AB", "BA"), default="AB", help="arm order inside each rep")
    a = ap.parse_args()

    hosts28 = m6.build_fleet_hosts(m12.BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=4,
                                   unit_prefix=m12.UNIT_PREFIX, remote_root=m12.REMOTE_ROOT,
                                   probe_bin=m12.BUILT_PROBE)
    hosts27 = m6.build_fleet_hosts(GATE27, a.ssh_user, a.ssh_key, a.hosts, count=4,
                                   unit_prefix=m12.UNIT_PREFIX, remote_root=m12.REMOTE_ROOT,
                                   probe_bin=PROBE27)
    voters = hosts28[:3]
    if not a.no_sync:
        sync_tree(voters, a.local_tree)
        sync_tree_to(voters, a.tree27, SRC27)
    for h in voters:
        m12.prepare_host(h, apply_profile=False)
        prepare_host_27(h)
        for u in ("client", "service", "edge", "node"):
            kill_unit(h, u)
    # provenance
    for h in voters[:1]:
        r = ssh(h, f"sha256sum {m12.BUILT_GATE} {GATE27} && git -C {m6.SshHost.UC_SRC} rev-parse --short HEAD 2>/dev/null; "
                   f"git -C {SRC27} rev-parse --short HEAD 2>/dev/null; true", label="provenance")
        print("INFO provenance:\n" + (r.stdout or ""), flush=True)

    points = []
    try:
        for rep in range(1, a.reps + 1):
            pairs = (("A-2.7.0", hosts27), ("B-2.8.0", hosts28))
            for ver, hs in (pairs if a.order == "AB" else pairs[::-1]):
                label = f"{ver} rep{rep}"
                p = one_arm(label, hs, a)
                points.append(p)
                print(f"POINT {json.dumps(p)}", flush=True)
                time.sleep(2.0)
    finally:
        m12.stop_cluster(voters)

    s = summarize(points)
    print("\nA/B — m12_gate client-direct, envelope on, inflight "
          f"{a.inflight}, payload {a.payload}, {a.secs} s, {a.reps} reps interleaved")
    for ver in ("A-2.7.0", "B-2.8.0"):
        if ver in s:
            v = s[ver]
            print(f"  {ver}: mean {v['mean']:12.0f} resp/s  [{v['min']:.0f} .. {v['max']:.0f}] "
                  f"spread {v['spread_pct']:.1f}%  p50 {v['p50_ms']:.3f} ms  p99 {v['p99_ms']:.3f} ms")
    if "ratio_B_over_A" in s:
        print(f"  B/A = {s['ratio_B_over_A']:.4f}  ({100*(s['ratio_B_over_A']-1):+.2f}%)  "
              f"ranges overlap: {s['ranges_overlap']} → "
              + ("delta INSIDE the rep spread (not detectable)" if s["ranges_overlap"]
                 else "ranges disjoint (delta outside the rep spread)"))
    print("SUMMARY-JSON " + json.dumps(s), flush=True)
    sys.exit(0 if len(points) == 2 * a.reps else 1)


if __name__ == "__main__":
    main()
