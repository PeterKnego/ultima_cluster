#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M6 fleet-gate orchestrator — the cross-host driver the m6_gate binary cannot be
on its own (its metrics are node-internal; cnc is same-host shared memory).

It drives a 3-voter + 1-learner cluster of REAL SEPARATE PROCESSES over real UDP
and runs the two M6 scenarios, observing each node through the `m6_gate probe`
subcommand (reads that host's local cnc) and driving write load + the
committed-value-divergence guard through `m6_gate loadclient` on the leader host.

Two modes, SAME scenario logic:
  --local  : all four nodes are local processes on 127.0.0.1 (loopback UDP, real
             separate processes — validates the orchestrator + is itself a
             stronger proof than the in-process `all`).
  --fleet  : nodes run on remote hosts (one role per host) over their private
             IPs; every start/stop/probe goes over ssh. Host list from
             `terraform output -json nodes` (or --hosts).

Scenarios (spec §9 M6):
  1. learner-join : baseline the leader's commit rate, start the learner under
     load, PASS iff it catches up to commit-at-join within JOIN_BUDGET and the
     commit-rate dip stays < DIP_MAX (no quorum stall).
  2. purge-cycle  : N cycles of (ensure purge fired) -> crash a follower's
     service -> reconstruct; PASS iff every reconstruction converges within
     CONVERGE_BUDGET and the loadclient's monotonic read guard never trips.

Exit 0 = PASS, exit 1 = honest FAIL (composes in CI / the fleet driver).
"""

import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

APP = "m6-gate"
JOIN_BUDGET = 60.0        # s — learner must catch up within this
DIP_MAX = 10.0            # % — fleet gate: commit-rate dip during join
CONVERGE_BUDGET = 10.0    # s — follower reconstruction must converge within this
PURGE_WAIT = 20.0         # s — wait for the purge floor to advance before a cycle
BASELINE_SECS = 8.0       # s — commit-rate baseline window before the join

# Journal durability guard. Each node's instance dir CONTAINS its journal
# (uc2_node InstanceDir::journal_dir() lives under it), so an instance dir on a
# RAM-backed filesystem makes fsync a no-op and every durability number this
# gate produces fiction. Deny-list volatile fs types rather than allow-listing
# ext4 (xfs & friends must still pass). `stat -f -c %T` reports e.g.
# 'ext2/ext3' for ext4 and 'tmpfs' for tmpfs.
VOLATILE_FS = {"tmpfs", "ramfs", "devtmpfs", "shm"}


def assert_durable_fs(fstype, where, host):
    fstype = (fstype or "").strip()
    if not fstype or fstype in VOLATILE_FS:
        raise SystemExit(
            f"[m6-gate] FATAL: {where} on {host} is on '{fstype or 'unknown'}' — a "
            f"RAM-backed filesystem defeats journal fsync durability; refusing to "
            f"run the gate. Put the instance dirs on a real disk (fleet: os_tune "
            f"mounts the instance-store NVMe at /opt/bench — check it ran)."
        )


# ----------------------------------------------------------------- host models

class LocalHost:
    """A node that runs as local subprocesses (loopback UDP)."""

    def __init__(self, gate_bin, node_dir, log_dir):
        self.gate = gate_bin
        self.dir = str(node_dir)
        self.logs = Path(log_dir)
        self.procs = {}  # unit -> Popen

    def bind_addr(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        _, port = s.getsockname()
        s.close()
        return f"127.0.0.1:{port}"

    def start_unit(self, unit, args):
        log = open(self.logs / f"{unit}.log", "w")
        p = subprocess.Popen([self.gate] + args, stdout=log, stderr=subprocess.STDOUT)
        self.procs[unit] = p

    def kill_unit(self, unit):
        p = self.procs.pop(unit, None)
        if p and p.poll() is None:
            p.kill()
            p.wait(timeout=10)

    def unit_exit(self, unit):
        p = self.procs.get(unit)
        return None if p is None else p.poll()

    def probe(self):
        out = subprocess.check_output(
            [self.gate, "probe", "--instance-dir", self.dir, "--app-id", APP],
            text=True, timeout=15,
        )
        return json.loads(out.strip().splitlines()[-1])

    def teardown(self):
        for u in list(self.procs):
            self.kill_unit(u)


class SshHost:
    """A node that runs on a remote host; every action goes over ssh + systemd-run.

    Fleet layout (bench-infra ansible, memory): the UC tree is rsync'd to
    /opt/bench/uc and built AS ROOT (CARGO_HOME=/opt/bench/.cargo), so the gate
    binary, instance dirs, and cnc files are all root-owned — every gate
    invocation runs under `sudo`. `systemd-run` already runs the unit as root.
    """

    CARGO = "/opt/bench/.cargo/bin/cargo"
    UC_SRC = "/opt/bench/uc"

    def __init__(self, gate_bin, node_dir, public_ip, private_ip, ssh_user, ssh_key):
        self.gate = gate_bin           # path to m6_gate ON the remote host
        self.dir = str(node_dir)       # instance dir ON the remote host
        self.public_ip = public_ip
        self.private_ip = private_ip
        self.target = f"{ssh_user}@{public_ip}"
        self.ssh = ["ssh", "-o", "StrictHostKeyChecking=accept-new",
                    "-o", "BatchMode=yes", "-i", ssh_key]

    def _ssh(self, cmd, **kw):
        # SSH_AUTH_SOCK is left to the caller's env (unset it before running the
        # orchestrator — a forwarded agent hangs ssh here, bench-infra gotcha).
        return subprocess.run(self.ssh + [self.target, cmd], text=True, **kw)

    def prepare(self):
        """Build the m6_gate example (release builds no examples by default),
        create the instance-dir parent, and report its filesystem type (the
        journal lives under the instance dir — a tmpfs here would silently void
        every durability claim, so the caller hard-fails on volatile fs types).
        Idempotent; ~9 s on a warm target."""
        r = self._ssh(
            f"sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup "
            f"{self.CARGO} build --release --example m6_gate "
            f"--manifest-path {self.UC_SRC}/Cargo.toml -p uc2_node "
            f"&& sudo mkdir -p /opt/bench/m6 "
            f"&& echo FSTYPE=$(stat -f -c %T /opt/bench/m6) && echo PREPARED",
            capture_output=True,
        )
        out = r.stdout or ""
        if "PREPARED" not in out:
            raise RuntimeError(f"prepare {self.public_ip} failed: {r.stderr or out}")
        fstype = next(
            (l.split("=", 1)[1] for l in out.splitlines() if l.startswith("FSTYPE=")), ""
        )
        assert_durable_fs(fstype, "/opt/bench/m6 (instance-dir parent)", self.public_ip)

    def bind_addr(self):
        # Fleet nodes bind their PRIVATE NIC IP (the cross-host route) on a fixed
        # port — one node per host, so no port contention.
        return f"{self.private_ip}:19100"

    def start_unit(self, unit, args):
        # systemd-run --collect (transient unit, cleaned up on stop); the gate role
        # parks, so it stays until we stop it. TimeoutStopSec=1 — parked gate bins
        # ignore SIGTERM (M5 finding). Args are single-quoted.
        quoted = " ".join(f"'{a}'" for a in args)
        cmd = (
            f"sudo systemd-run --unit=m6-{unit} --collect -p TimeoutStopSec=1 "
            f"-p StandardOutput=append:/opt/bench/m6-{unit}.log "
            f"-p StandardError=append:/opt/bench/m6-{unit}.log "
            f"{self.gate} {quoted}"
        )
        r = self._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"start m6-{unit} on {self.public_ip} failed: {r.stderr}")

    def kill_unit(self, unit):
        self._ssh(
            f"sudo systemctl kill --signal=SIGKILL m6-{unit} 2>/dev/null; "
            f"sudo systemctl stop m6-{unit} 2>/dev/null; true",
            capture_output=True,
        )

    def unit_exit(self, unit):
        r = self._ssh(f"systemctl is-active m6-{unit}", capture_output=True)
        return None if r.stdout.strip() == "active" else 1

    def probe(self):
        r = self._ssh(
            f"sudo {self.gate} probe --instance-dir {self.dir} --app-id {APP}",
            capture_output=True,
        )
        if r.returncode != 0:
            raise RuntimeError(f"probe {self.public_ip} failed: {r.stderr}")
        return json.loads(r.stdout.strip().splitlines()[-1])

    def teardown(self):
        for u in ("node", "service", "loadclient"):
            self.kill_unit(u)


# --------------------------------------------------------------- orchestration

def log(msg):
    print(f"[m6-gate] {msg}", flush=True)


def wait_leader(hosts, voters, secs):
    """Return the index of the single serving-leader voter, or None on timeout."""
    deadline = time.time() + secs
    while time.time() < deadline:
        serving = []
        for i in voters:
            try:
                p = hosts[i].probe()
                if p["leader"] and p["can_serve"]:
                    serving.append(i)
            except Exception:
                pass
        if len(serving) == 1:
            return serving[0]
        if len(serving) > 1:
            raise RuntimeError(f"split-brain: voters {serving} all serve")
        time.sleep(0.3)
    return None


def start_node(host, role, node_id, members, learners):
    args = [
        role, "--id", str(node_id), "--bind", host.bind_addr(),
        "--instance-dir", host.dir, "--members", members,
        "--learners", learners, "--app-id", APP,
    ]
    host.start_unit("node", args)


def start_service(host):
    host.start_unit("service", ["service", "--instance-dir", host.dir, "--app-id", APP])


def run_gate(hosts, voters, learner, members, learners, cycles, stop_file):
    verdicts = []

    # 1. Bring up the three voters (node, then service once the cnc exists).
    for i in voters:
        start_node(hosts[i], "node", i, members, learners)
    time.sleep(2.0)
    for i in voters:
        start_service(hosts[i])

    leader = wait_leader(hosts, voters, 40)
    if leader is None:
        log("FAIL: no leader elected")
        return False, verdicts
    log(f"leader elected: node{leader}")

    # 2. Start the load driver on the leader host (writes + monotonic read guard).
    hosts[leader].start_unit(
        "loadclient",
        ["loadclient", "--instance-dir", hosts[leader].dir, "--app-id", APP,
         "--stop-file", stop_file],
    )

    verdicts.append(scenario_learner_join(hosts, voters, learner, leader, members, learners))
    verdicts.append(scenario_purge_cycle(hosts, voters, leader, cycles))

    # Loadclient divergence guard: it exits nonzero on a read regression.
    ec = hosts[leader].unit_exit("loadclient")
    if ec not in (None, 0):
        log(f"FAIL: loadclient exited {ec} — committed-value DIVERGENCE detected")
        verdicts.append(("divergence-guard", False, f"loadclient exit {ec}"))

    ok = all(v[1] for v in verdicts)
    return ok, verdicts


def scenario_learner_join(hosts, voters, learner, leader, members, learners):
    # Baseline commit rate from the leader's commit counter.
    c0 = hosts[leader].probe()["commit"]
    t0 = time.time()
    time.sleep(BASELINE_SECS)
    c1 = hosts[leader].probe()["commit"]
    baseline_rate = (c1 - c0) / (time.time() - t0)
    commit_at_join = hosts[leader].probe()["commit"]

    # Start the learner (node + service).
    start_node(hosts[learner], "learner", learner, members, learners)
    time.sleep(2.0)
    start_service(hosts[learner])

    # Measure the leader's commit rate over a FIXED window that spans the join,
    # AND detect join completion within the budget. Decoupling the two is what
    # makes the dip gateable even when the join itself is near-instant (a trivial
    # register's snapshot installs immediately) — the real signal is "did commit
    # keep flowing at ~baseline while the learner caught up", i.e. no quorum stall.
    MEASURE = 5.0
    jt0 = time.time()
    jc0 = hosts[leader].probe()["commit"]
    joined, join_secs = False, None
    while True:
        el = time.time() - jt0
        if not joined:
            try:
                if hosts[learner].probe()["durable"] >= commit_at_join:
                    joined, join_secs = True, el
            except Exception:
                pass
        if joined and el >= MEASURE:
            break
        if el >= JOIN_BUDGET:
            break
        time.sleep(0.1)
    window = time.time() - jt0
    jc1 = hosts[leader].probe()["commit"]
    join_rate = (jc1 - jc0) / max(window, 1e-6)
    learner_led = hosts[learner].probe().get("leader", False)

    dip = max(0.0, (baseline_rate - join_rate) / baseline_rate * 100.0) if baseline_rate > 0 else 100.0
    dip_ok = dip < DIP_MAX
    ok = joined and not learner_led and join_rate > 0 and dip_ok
    js = f"{join_secs:.2f}" if join_secs is not None else "NEVER"
    detail = (f"joined={joined} in {js}s (budget {JOIN_BUDGET:.0f}s), "
              f"baseline={baseline_rate:.0f} commits/s, "
              f"during-join={join_rate:.0f} commits/s over {window:.1f}s, "
              f"dip={dip:.1f}% (gate <{DIP_MAX:.0f}%), learner_led={learner_led}")
    log(f"[{'PASS' if ok else 'FAIL'}] learner-join — {detail}")
    return ("learner-join", ok, detail)


def scenario_purge_cycle(hosts, voters, leader, cycles):
    last_worst = 0.0
    purged_any = False
    for cyc in range(cycles):
        # Ensure purge fired somewhere (floor advanced) so the follower rebuild is
        # a below-floor snapshot install, not a plain tail replay.
        dl = time.time() + PURGE_WAIT
        while time.time() < dl:
            if max(hosts[i].probe()["archive_first_base"] for i in voters) > 0:
                purged_any = True
                break
            time.sleep(0.5)

        follower = next(i for i in voters if i != leader)
        # Crash the follower's service (SIGKILL), then restart it empty.
        hosts[follower].kill_unit("service")
        start_service(hosts[follower])

        ct0 = time.time()
        converged = False
        while time.time() - ct0 < CONVERGE_BUDGET:
            p = hosts[follower].probe()
            if p["commit"] > 0 and p["service_applied"] >= p["commit"]:
                converged = True
                break
            time.sleep(0.1)
        worst = time.time() - ct0
        last_worst = max(last_worst, worst)
        if not converged:
            p = hosts[follower].probe()
            detail = (f"cycle {cyc}: follower node{follower} did not reconstruct within "
                      f"{CONVERGE_BUDGET:.0f}s (commit={p['commit']}, applied={p['service_applied']})")
            log(f"[FAIL] purge-cycle — {detail}")
            return ("purge-cycle", False, detail)

    detail = (f"{cycles} cycles: every follower reconstruction converged "
              f"(worst {last_worst:.2f}s / {CONVERGE_BUDGET:.0f}s), purge_fired={purged_any}")
    log(f"[PASS] purge-cycle — {detail}")
    return ("purge-cycle", True, detail)


# ---------------------------------------------------------------- entrypoints

def build_local_hosts(gate_bin, root):
    root = Path(root)
    if root.exists():
        subprocess.run(["rm", "-rf", str(root)], check=True)
    root.mkdir(parents=True)
    # Same durability guard as the fleet path: the node instance dirs (and thus
    # the journals) live under this root. Catches --root on /tmp (RAM tmpfs on
    # dev boxes) or any TMPDIR-style redirection onto a volatile fs.
    fstype = subprocess.check_output(
        ["stat", "-f", "-c", "%T", str(root)], text=True
    )
    assert_durable_fs(fstype, f"{root} (local gate root)", "localhost")
    hosts = []
    for i in range(4):
        node_dir = root / f"n{i}"
        node_dir.mkdir()
        # Per-node log dir (sibling to the instance dir) so the 4 nodes' unit logs
        # don't clobber each other.
        log_dir = root / f"log{i}"
        log_dir.mkdir()
        hosts.append(LocalHost(gate_bin, node_dir, log_dir))
    return hosts


def build_fleet_hosts(gate_bin, ssh_user, ssh_key, hosts_arg):
    if hosts_arg:
        # "pub1/priv1,pub2/priv2,..." — 4 entries, voters 0-2 + learner 3.
        entries = [tuple(h.split("/")) for h in hosts_arg.split(",")]
    else:
        out = subprocess.check_output(
            ["terraform", "output", "-json", "nodes"],
            cwd=str(Path(__file__).resolve().parent.parent / "terraform"), text=True,
        )
        nodes = json.loads(out)
        entries = [(n["public_ip"], n["private_ip"]) for n in nodes]
    if len(entries) < 4:
        raise SystemExit(f"need 4 hosts (3 voters + 1 learner), got {len(entries)}")
    hosts = []
    for i, (pub, priv) in enumerate(entries[:4]):
        hosts.append(SshHost(gate_bin, f"/opt/bench/m6/n{i}", pub, priv, ssh_user, ssh_key))
    return hosts


def main():
    ap = argparse.ArgumentParser(description="M6 fleet-gate orchestrator")
    ap.add_argument("--local", action="store_true", help="run 4 local processes (loopback UDP)")
    ap.add_argument("--fleet", action="store_true", help="run over ssh on remote hosts")
    ap.add_argument("--bin", default="", help="path to the m6_gate binary")
    ap.add_argument("--root", default="/home/claude/.cache/m6_fleet", help="local root dir")
    ap.add_argument("--hosts", default="", help="fleet: pub/priv,... (else terraform output)")
    ap.add_argument("--ssh-user", default="ubuntu", help="fleet ssh user")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519", help="fleet ssh key")
    ap.add_argument("--cycles", type=int, default=5, help="purge-cycle count")
    args = ap.parse_args()

    if args.local == args.fleet:
        raise SystemExit("choose exactly one of --local / --fleet")

    if args.local:
        gate = args.bin or "/home/claude/.cache/cargo-target/release/examples/m6_gate"
        hosts = build_local_hosts(gate, args.root)
        stop_file = str(Path(args.root) / "STOP")
    else:
        gate = args.bin or "/opt/bench/uc/target/release/examples/m6_gate"
        hosts = build_fleet_hosts(gate, args.ssh_user, args.ssh_key, args.hosts)
        # Fleet: build the example on each host (release builds no examples) +
        # mkdir the instance-dir parent. The loadclient stop-file lives on the
        # remote leader host and is never created; teardown kills the unit.
        stop_file = "/opt/bench/m6_STOP"
        log("preparing fleet hosts (build m6_gate example + mkdir)...")
        for h in hosts:
            h.prepare()

    voters, learner = [0, 1, 2], 3
    addr = {i: hosts[i].bind_addr() for i in range(4)}
    members = ",".join(f"{i}@{addr[i]}" for i in voters)
    learners = f"{learner}@{addr[learner]}"
    # Re-pin the addresses onto the hosts so start_node reuses the SAME addr the
    # member map advertises (local mode re-binds ephemeral ports otherwise).
    for i in range(4):
        hosts[i]._fixed_addr = addr[i]
        hosts[i].bind_addr = (lambda h=hosts[i]: h._fixed_addr)

    # Clear any stale stop-file.
    try:
        Path(stop_file).unlink()
    except FileNotFoundError:
        pass

    log(f"== M6 {'LOCAL (4 procs, loopback UDP)' if args.local else 'FLEET'} gate ==")
    ok = False
    verdicts = []
    try:
        ok, verdicts = run_gate(hosts, voters, learner, members, learners, args.cycles, stop_file)
    finally:
        # Local: signal the loadclient to finish cleanly via its stop-file. Fleet:
        # the stop-file is on the remote leader (not reachable from here) — the
        # per-host teardown kills the loadclient unit instead.
        if args.local:
            Path(stop_file).write_text("stop")
            time.sleep(1.0)
        for h in hosts:
            try:
                h.teardown()
            except Exception:
                pass

    log("== results ==")
    for name, v_ok, detail in verdicts:
        log(f"  [{'PASS' if v_ok else 'FAIL'}] {name} — {detail}")
    log(f"RESULT: {'PASS' if ok else 'FAIL (honest)'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
