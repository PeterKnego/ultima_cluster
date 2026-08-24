#!/usr/bin/env python3
"""M13 per-hop isolation bench — fleet driver.

Measures every hop of the remote request path ALONE (a dummy sink behind it,
a minimal driver in front of it) and then in composition, so the bottleneck
is located by subtraction rather than inferred from the end-to-end number:

    [RemoteClient] ─TCP─▶ [Edge] ─shmem Engine─▶ [node] ─▶ consensus ─▶ [service]
         hop 3             hop 2         hop 1

Roles come from `uc2_gateway/examples/hop_bench` (dummy-node, dummy-edge,
blaster, engine-load, remote-load, edge); the real-cluster arms reuse
`m12_gate`'s node/service roles through `m12_fleet_gate`'s helpers.

Topology (4 hosts, the M12 shape): hosts[0] is the SERVER host (dummy node +
edge, or a voter of the real cluster), hosts[3] is the CLIENT host (every TCP
driver); hosts[0..3] form the real cluster for the full-stack arms.

Matrix (every point one `RESULT` line + a same-window CPU sample of the
server host, the edge process and the client host):

  A  hop1        engine-load(S) → dummy-node(S)             inflight ladder, N engines
  B  hop3-floor  blaster(C)     → dummy-edge(S)             N conns
  C  hop3        remote-load(C) → dummy-edge(S)             N conns
  D  hop2        blaster(C)     → edge(S) → dummy-node(S)   N conns, edge window unbounded
  D' hop2-w4096  blaster(C)     → edge(S, max_inflight 4096) → dummy-node(S)  N conns
  E  hop2+3      remote-load(C) → edge(S) → dummy-node(S)   N conns
  F  full        blaster(C)     → edge(leader) → REAL 3-node cluster (raw sm) N conns
  G  direct      engine-load(leader) → REAL cluster          1 point (reference)

Output: one `HOP-JSON {...}` line per point, a `HOP-TABLE` at the end. This
is a MEASUREMENT driver — it has no bar; the exit code is 0 unless the
infrastructure failed (no leader, no RESULT at all).
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
from m12_fleet_gate import (  # noqa: E402
    ssh, start_unit, kill_unit, tail_log, parse_result, echo,
    sample_cpu_concurrently, wait_units_done, unit_log,
)

HOP_APP = "hop-bench"
HOP_BIN = "/opt/bench/uc/target/release/examples/hop_bench"
HOP_ROOT = "/opt/bench/m13"
EDGE_PORT = 9310
DUMMY_EDGE_PORT = 9311
RAMP_SECS = 3.0
DRAIN_SECS = 8.0
SLACK_SECS = 60


# ----------------------------------------------------------------- plumbing

def sync_tree(hosts, local_tree):
    """Ship the LOCAL tree to every host (the ansible provision rsyncs at
    bring-up; the harness may have changed since)."""
    for h in hosts:
        cmd = ["rsync", "-az", "--delete",
               "--exclude=target", "--exclude=.git", "--exclude=bench-out",
               "--exclude=bench-infra", "--exclude=.claude",
               "-e", " ".join(h.ssh),
               "--rsync-path", "sudo rsync",
               f"{local_tree.rstrip('/')}/", f"{h.target}:{m6.SshHost.UC_SRC}/"]
        print(f"INFO [rsync {h.public_ip}] {' '.join(cmd)}", flush=True)
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"rsync to {h.public_ip} failed: {r.stderr}")


def prepare_host(host):
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    cargo = m6.SshHost.CARGO
    src = m6.SshHost.UC_SRC
    cmd = (
        f"{env} {cargo} build --release --manifest-path {src}/Cargo.toml "
        f"-p uc2_node --example m6_gate "
        f"&& {env} {cargo} build --release --manifest-path {src}/Cargo.toml "
        f"-p uc2_gateway --example m12_gate --example hop_bench "
        f"&& sudo mkdir -p {HOP_ROOT} {m12.REMOTE_ROOT} "
        f"&& echo FSTYPE=$(stat -f -c %T {HOP_ROOT}) && echo PREPARED"
    )
    r = ssh(host, cmd, label="build")
    out = r.stdout or ""
    if "PREPARED" not in out:
        raise RuntimeError(f"prepare {host.public_ip} failed: {r.stderr or out}")


def wait_ready(host, unit, secs=30):
    deadline = time.time() + secs
    while time.time() < deadline:
        out = tail_log(host, unit, lines=50)
        if "READY" in out:
            return True
        if "panicked" in out or "Error" in out:
            echo(unit, out, lines=20)
            raise RuntimeError(f"unit {unit} on {host.public_ip} failed to start")
        time.sleep(0.5)
    echo(unit, tail_log(host, unit, lines=30), lines=30)
    raise RuntimeError(f"unit {unit} on {host.public_ip} never printed READY")


def hop_dir(host):
    return f"{HOP_ROOT}/n0"


def start_dummy_node(host, a):
    ssh(host, f"sudo rm -rf {hop_dir(host)} && sudo mkdir -p {hop_dir(host)}", label="ssh")
    ssh(host, f"sudo rm -f {unit_log(host, 'hb-dnode')}", label="ssh")
    start_unit(host, "hb-dnode", [
        "dummy-node", "--instance-dir", hop_dir(host), "--app-id", HOP_APP,
        "--max-payload", "512",
    ])
    wait_ready(host, "hb-dnode")


def start_dummy_edge(host, a, credits):
    ssh(host, f"sudo rm -f {unit_log(host, 'hb-dedge')}", label="ssh")
    start_unit(host, "hb-dedge", [
        "dummy-edge", "--listen", f"{host.private_ip}:{DUMMY_EDGE_PORT}",
        "--app-id", HOP_APP, "--credits", str(credits),
    ])
    wait_ready(host, "hb-dedge")


def start_hop_edge(host, instance_dir, app_id, max_inflight, per_conn, members=None):
    ssh(host, f"sudo rm -f {unit_log(host, 'hb-edge')}", label="ssh")
    args = ["edge", "--instance-dir", instance_dir, "--app-id", app_id,
            "--listen", f"{host.private_ip}:{EDGE_PORT}",
            "--max-inflight", str(max_inflight),
            "--per-conn-inflight", str(per_conn)]
    if members:
        args += ["--members", members]
    start_unit(host, "hb-edge", args)
    wait_ready(host, "hb-edge")
    time.sleep(1.0)


# ------------------------------------------------------------------ a point

def run_point(label, server_host, client_host, client_args, arm, secs, a,
              edge_unit=None, extra=None):
    """Start ONE client unit on `client_host`, sample CPU on the server host
    (and its edge/sink process) + the client host inside the run window,
    collect the RESULT line. Returns a HOP-JSON dict (never None)."""
    unit = "hb-client"
    ssh(client_host, f"sudo rm -f {unit_log(client_host, unit)}", label="ssh")
    print(f"\nINFO --- {label} ---", flush=True)
    t_start = time.time()
    start_unit(client_host, unit, client_args)
    window = max(3, min(a.cpu_window_secs, int(secs - RAMP_SECS - 2)))
    time.sleep(RAMP_SECS)
    targets = [("server", server_host, edge_unit),
               ("client", client_host, unit)]
    samples = sample_cpu_concurrently(targets, window)
    deadline = t_start + secs + DRAIN_SECS + SLACK_SECS
    wait_units_done([(client_host, [unit])], deadline)
    out = tail_log(client_host, unit, lines=400)
    d = parse_result(out, arm)
    kill_unit(client_host, unit)
    if d is None:
        print(f"INFO {label}: NO RESULT line (the client died — echoing tail)", flush=True)
        echo(unit, out, lines=25)
    else:
        for l in out.splitlines():
            if l.startswith("   responses/s") or l.startswith("   sends=") or l.startswith("== "):
                print(f"  [{unit}] {l}", flush=True)
    s, c = samples.get("server", {}), samples.get("client", {})
    point = {
        "label": label, "arm": arm, "ok": d is not None,
        "rps": d.get("responses_per_sec") if d else None,
        "p50_ms": d.get("p50_ms") if d else None,
        "p95_ms": d.get("p95_ms") if d else None,
        "p99_ms": d.get("p99_ms") if d else None,
        "lost": d.get("lost") if d else None,
        "retried": d.get("retried") if d else None,
        "sends": d.get("sends") if d else None,
        "server_host_cpu_pct": s.get("host_cpu_pct"),
        "server_proc_cpu_pct": s.get("proc_cpu_pct"),
        "client_host_cpu_pct": c.get("host_cpu_pct"),
        "client_proc_cpu_pct": c.get("proc_cpu_pct"),
    }
    if extra:
        point.update(extra)
    print("HOP-JSON " + json.dumps(point), flush=True)
    return point


def ladder(s):
    return [int(x) for x in s.split(",") if x.strip()]


# -------------------------------------------------------------------- arms

def arm_hop1(S, a, points):
    start_dummy_node(S, a)
    try:
        for infl in ladder(a.hop1_inflights):
            points.append(run_point(
                f"A hop1 engine→dummy-node engines=1 inflight={infl}", S, S,
                ["engine-load", "--instance-dir", hop_dir(S), "--app-id", HOP_APP,
                 "--secs", str(a.secs), "--payload", str(a.payload),
                 "--inflight", str(infl), "--engines", "1"],
                "engine", a.secs, a, edge_unit="hb-dnode",
                extra={"hop": "1", "inflight": infl, "n": 1}))
        for n in ladder(a.hop1_engines):
            if n == 1:
                continue
            points.append(run_point(
                f"A hop1 engine→dummy-node engines={n} inflight={a.inflight}", S, S,
                ["engine-load", "--instance-dir", hop_dir(S), "--app-id", HOP_APP,
                 "--secs", str(a.secs), "--payload", str(a.payload),
                 "--inflight", str(a.inflight), "--engines", str(n)],
                "engine", a.secs, a, edge_unit="hb-dnode",
                extra={"hop": "1", "inflight": a.inflight, "n": n}))
    finally:
        kill_unit(S, "hb-dnode")


def tcp_ladder(S, C, a, points, arm_label, hop, sink_unit, gateway, driver):
    """N-connection ladder of `driver` (blaster | remote-load) into `gateway`."""
    for n in ladder(a.conns):
        role = "blaster" if driver == "blaster" else "remote-load"
        flag = "--gateway" if driver == "blaster" else "--gateways"
        arm = "blaster" if driver == "blaster" else "remote"
        points.append(run_point(
            f"{arm_label} conns={n} inflight={a.conn_inflight}", S, C,
            [role, flag, gateway, "--app-id", HOP_APP,
             "--secs", str(a.secs), "--payload", str(a.payload),
             "--inflight", str(a.conn_inflight), "--conns", str(n)],
            arm, a.secs, a, edge_unit=sink_unit,
            extra={"hop": hop, "inflight": a.conn_inflight, "n": n, "driver": driver}))
    # One deep single connection, the row-2 reference shape.
    role = "blaster" if driver == "blaster" else "remote-load"
    flag = "--gateway" if driver == "blaster" else "--gateways"
    arm = "blaster" if driver == "blaster" else "remote"
    points.append(run_point(
        f"{arm_label} conns=1 inflight={a.inflight} (deep ref)", S, C,
        [role, flag, gateway, "--app-id", HOP_APP,
         "--secs", str(a.secs), "--payload", str(a.payload),
         "--inflight", str(a.inflight), "--conns", "1"],
        arm, a.secs, a, edge_unit=sink_unit,
        extra={"hop": hop, "inflight": a.inflight, "n": 1, "driver": driver, "ref": True}))


def arm_hop3(S, C, a, points):
    start_dummy_edge(S, a, credits=max(a.inflight, a.conn_inflight))
    gw = f"{S.private_ip}:{DUMMY_EDGE_PORT}"
    try:
        tcp_ladder(S, C, a, points, "B hop3-floor blaster→dummy-edge", "3f", "hb-dedge", gw, "blaster")
        # Batching variant: one frame per write (the pre-batching-fix shape) vs
        # the default 64 — quantifies the syscall-batching lever on the TCP
        # floor alone.
        for batch in (1, 8):
            points.append(run_point(
                f"B' hop3-floor blaster→dummy-edge conns=1 inflight={a.conn_inflight} batch={batch}", S, C,
                ["blaster", "--gateway", gw, "--app-id", HOP_APP,
                 "--secs", str(a.secs), "--payload", str(a.payload),
                 "--inflight", str(a.conn_inflight), "--conns", "1", "--batch", str(batch)],
                "blaster", a.secs, a, edge_unit="hb-dedge",
                extra={"hop": "3f", "inflight": a.conn_inflight, "n": 1, "driver": "blaster", "batch": batch}))
        tcp_ladder(S, C, a, points, "C hop3 remote→dummy-edge", "3", "hb-dedge", gw, "remote")
    finally:
        kill_unit(S, "hb-dedge")


def arm_hop2(S, C, a, points):
    start_dummy_node(S, a)
    gw = f"{S.private_ip}:{EDGE_PORT}"
    try:
        start_hop_edge(S, hop_dir(S), HOP_APP, a.edge_unbounded, a.edge_per_conn)
        tcp_ladder(S, C, a, points, "D hop2 blaster→edge(unbounded)→dummy-node", "2", "hb-edge", gw, "blaster")
        tcp_ladder(S, C, a, points, "E hop2+3 remote→edge(unbounded)→dummy-node", "23", "hb-edge", gw, "remote")
        kill_unit(S, "hb-edge")
        start_hop_edge(S, hop_dir(S), HOP_APP, 4096, a.edge_per_conn)
        tcp_ladder(S, C, a, points, "D' hop2 blaster→edge(window 4096)→dummy-node", "2w", "hb-edge", gw, "blaster")
    finally:
        kill_unit(S, "hb-edge")
        kill_unit(S, "hb-dnode")


def arm_full(m12hosts, hophosts, a, points):
    """The real thing: a 3-voter cluster (raw state machine, envelope off,
    admission window as configured), the hop_bench edge on the leader, drivers
    on the client host."""
    node_hosts = m12hosts[:3]
    C = hophosts[3]
    m12.wipe_dirs(node_hosts)
    m12.start_cluster(node_hosts, a, "off", raw_sm=True)
    try:
        leader = m6.wait_leader(node_hosts, list(range(3)), m12.LEADER_WAIT_SECS)
        if leader is None:
            raise RuntimeError("no serving leader in the real cluster")
        L = node_hosts[leader]
        LH = hophosts[leader]
        print(f"INFO real cluster leader = n{leader} ({L.public_ip})", flush=True)
        # G: hop 1 against the REAL backend (direct-arm reference).
        points.append(run_point(
            f"G direct engine→REAL cluster engines=1 inflight={a.inflight}", LH, LH,
            ["engine-load", "--instance-dir", L.dir, "--app-id", m12.APP,
             "--secs", str(a.secs), "--payload", str(a.payload),
             "--inflight", str(a.inflight), "--engines", "1"],
            "engine", a.secs, a, edge_unit="node",
            extra={"hop": "1real", "inflight": a.inflight, "n": 1}))
        # F: blaster → edge → real cluster, N conns.
        members = ",".join(f"{i}@{h.private_ip}:{EDGE_PORT}" for i, h in enumerate(node_hosts))
        start_hop_edge(LH, L.dir, m12.APP, a.edge_unbounded, a.edge_per_conn, members=members)
        gw = f"{L.private_ip}:{EDGE_PORT}"
        for infl in ladder(a.full_inflights):
            for n in ladder(a.full_conns):
                points.append(run_point(
                    f"F full blaster→edge→REAL cluster conns={n} inflight={infl}", LH, C,
                    ["blaster", "--gateway", gw, "--app-id", m12.APP,
                     "--secs", str(a.secs), "--payload", str(a.payload),
                     "--inflight", str(infl), "--conns", str(n)],
                    "blaster", a.secs, a, edge_unit="hb-edge",
                    extra={"hop": "full", "inflight": infl, "n": n, "driver": "blaster"}))
        kill_unit(LH, "hb-edge")
    finally:
        kill_unit(hophosts[0], "hb-edge")
        for h in hophosts[:3]:
            kill_unit(h, "hb-edge")
        m12.stop_cluster(node_hosts)


def arm_diag(S, C, a, points):
    """Collapse diagnosis: blaster → edge → dummy-node at the rungs around the
    knee, with the EDGE's per-thread CPU + current syscall sampled mid-rung
    and its own per-second stats (backpressure / retries / responses)
    echoed. Separates "number of connections (reader threads)" from "number
    of outstanding requests" as the trigger."""
    sampler_local = str(Path(__file__).resolve().parent / "thread_sample.sh")
    r = subprocess.run(["scp", "-o", "StrictHostKeyChecking=accept-new", "-i", a.ssh_key,
                        sampler_local, f"{S.target}:/tmp/thread_sample.sh"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"scp sampler: {r.stderr}")
    rungs = [(int(n), int(i)) for n, i in
             (x.split("x") for x in a.diag_rungs.split(",") if x.strip())]
    start_dummy_node(S, a)
    gw = f"{S.private_ip}:{EDGE_PORT}"
    try:
        start_hop_edge(S, hop_dir(S), HOP_APP, a.edge_unbounded, a.edge_per_conn)
        for n, infl in rungs:
            label = f"X diag blaster→edge(unbounded)→dummy-node conns={n} inflight={infl}"
            unit = "hb-client"
            ssh(C, f"sudo rm -f {unit_log(C, unit)}", label="ssh")
            print(f"\nINFO --- {label} ---", flush=True)
            t_start = time.time()
            start_unit(C, unit, ["blaster", "--gateway", gw, "--app-id", HOP_APP,
                                 "--secs", str(a.secs), "--payload", str(a.payload),
                                 "--inflight", str(infl), "--conns", str(n)])
            time.sleep(RAMP_SECS)
            r = ssh(S, f"PID=$(systemctl show -p MainPID --value {m12.UNIT_PREFIX}-hb-edge); "
                       f"sudo bash /tmp/thread_sample.sh $PID 3", timeout=60, label="threads")
            print(f"THREADS {label}\n{r.stdout}", flush=True)
            deadline = t_start + a.secs + DRAIN_SECS + SLACK_SECS
            wait_units_done([(C, [unit])], deadline)
            out = tail_log(C, unit, lines=400)
            d = parse_result(out, "blaster")
            kill_unit(C, unit)
            edge_log = tail_log(S, "hb-edge", lines=a.secs + 8)
            print(f"EDGE-STATS {label}", flush=True)
            for l in edge_log.splitlines():
                if l.startswith("edge:"):
                    print(f"  {l}", flush=True)
            point = {"label": label, "arm": "blaster", "ok": d is not None,
                     "rps": d.get("responses_per_sec") if d else None,
                     "p50_ms": d.get("p50_ms") if d else None,
                     "p95_ms": d.get("p95_ms") if d else None,
                     "p99_ms": d.get("p99_ms") if d else None,
                     "lost": d.get("lost") if d else None,
                     "retried": d.get("retried") if d else None,
                     "sends": d.get("sends") if d else None,
                     "server_host_cpu_pct": None, "server_proc_cpu_pct": None,
                     "client_host_cpu_pct": None, "client_proc_cpu_pct": None,
                     "hop": "diag", "inflight": infl, "n": n, "driver": "blaster"}
            print("HOP-JSON " + json.dumps(point), flush=True)
            points.append(point)
            # Fresh edge per rung so a collapsed rung's residue cannot leak.
            kill_unit(S, "hb-edge")
            start_hop_edge(S, hop_dir(S), HOP_APP, a.edge_unbounded, a.edge_per_conn)
    finally:
        kill_unit(S, "hb-edge")
        kill_unit(S, "hb-dnode")


# ------------------------------------------------------------------- table

def table(points):
    print("\nHOP-TABLE")
    hdr = f"{'point':64} {'resp/s':>10} {'p50ms':>8} {'p95ms':>8} {'p99ms':>9} {'lost':>6} {'retry':>6} {'srvCPU%':>8} {'proc%':>7} {'cliCPU%':>8}"
    print(hdr)
    print("-" * len(hdr))
    for p in points:
        f = lambda x, d=1: ("-" if x is None else f"{x:.{d}f}")
        print(f"{p['label'][:64]:64} {f(p['rps'],0):>10} {f(p['p50_ms'],3):>8} {f(p['p95_ms'],3):>8} "
              f"{f(p['p99_ms'],3):>9} {f(p['lost'],0):>6} {f(p['retried'],0):>6} "
              f"{f(p['server_host_cpu_pct']):>8} {f(p['server_proc_cpu_pct']):>7} {f(p['client_host_cpu_pct']):>8}")
    print("HOP-POINTS-JSON " + json.dumps(points), flush=True)


# -------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="M13 per-hop isolation bench (fleet)")
    ap.add_argument("--fleet", action="store_true", required=True)
    ap.add_argument("--hosts", default="", help="pub/priv,... (else terraform output)")
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--local-tree", default=str(Path(__file__).resolve().parent.parent.parent),
                    help="tree to rsync to the hosts before building (default: this checkout)")
    ap.add_argument("--no-sync", action="store_true", help="skip the rsync + build")
    ap.add_argument("--prepare-hosts", default="",
                    help="comma-separated host indices to rsync+build (default all); "
                         "the diag arm only needs 0 and 3")
    ap.add_argument("--secs", type=int, default=10)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--inflight", type=int, default=4096, help="deep single-stream inflight")
    ap.add_argument("--conn-inflight", type=int, default=1024, help="per-connection inflight in the N ladders")
    ap.add_argument("--conns", default="1,2,4,8,16")
    ap.add_argument("--hop1-inflights", default="256,1024,4096")
    ap.add_argument("--hop1-engines", default="1,2,4")
    ap.add_argument("--edge-unbounded", type=int, default=65536, help="edge max_inflight for the 'unbounded' arms")
    ap.add_argument("--edge-per-conn", type=int, default=4096)
    ap.add_argument("--full-conns", default="1,2,4,8")
    ap.add_argument("--full-inflights", default="256,1024")
    ap.add_argument("--admission-kib", type=int, default=256)
    ap.add_argument("--cpu-window-secs", type=int, default=5)
    ap.add_argument("--arms", default="1,3,2,full", help="subset of: 1,3,2,full,diag")
    ap.add_argument("--diag-rungs", default="4x1024,6x1024,8x128,8x1024",
                    help="diag arm: comma-separated <conns>x<inflight> rungs")
    a = ap.parse_args()

    hop_hosts = m6.build_fleet_hosts(HOP_BIN, a.ssh_user, a.ssh_key, a.hosts, count=a.nodes,
                                     unit_prefix=m12.UNIT_PREFIX, remote_root=HOP_ROOT,
                                     probe_bin=m12.BUILT_PROBE)
    m12_hosts = m6.build_fleet_hosts(m12.BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=a.nodes,
                                     unit_prefix=m12.UNIT_PREFIX, remote_root=m12.REMOTE_ROOT,
                                     probe_bin=m12.BUILT_PROBE)
    prep_idx = [int(x) for x in a.prepare_hosts.split(",")] if a.prepare_hosts else list(range(len(hop_hosts)))
    prep = [hop_hosts[i] for i in prep_idx]
    if not a.no_sync:
        sync_tree(prep, a.local_tree)
    for h in prep:
        prepare_host(h)
    for h in hop_hosts:
        for u in ("hb-dnode", "hb-dedge", "hb-edge", "hb-client", "node", "service", "edge"):
            kill_unit(h, u)
    S, C = hop_hosts[0], hop_hosts[3]
    print(f"INFO topology: server {S.public_ip} client {C.public_ip}; real-cluster voters "
          f"{[h.public_ip for h in hop_hosts[:3]]}", flush=True)
    arms = [x.strip() for x in a.arms.split(",") if x.strip()]
    points = []
    try:
        if "1" in arms:
            arm_hop1(S, a, points)
        if "3" in arms:
            arm_hop3(S, C, a, points)
        if "2" in arms:
            arm_hop2(S, C, a, points)
        if "full" in arms:
            arm_full(m12_hosts, hop_hosts, a, points)
        if "diag" in arms:
            arm_diag(S, C, a, points)
    finally:
        table(points)
        for h in hop_hosts:
            for u in ("hb-dnode", "hb-dedge", "hb-edge", "hb-client"):
                kill_unit(h, u)
    missing = [p["label"] for p in points if not p["ok"]]
    if not points:
        print("RESULT: FAIL (infrastructure) — no points measured")
        sys.exit(1)
    print(f"RESULT: MEASURED {len(points)} points, {len(missing)} without a RESULT line"
          + (f": {missing}" if missing else ""))
    sys.exit(0)


if __name__ == "__main__":
    main()
