#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M9 restart-cost gate driver — the cross-host driver for the pre-committed
decide rule in docs/benchmarks/uc2-m9-gate-2026-08-19.md (written and
committed BEFORE any run).

WHY THIS IS ITS OWN SCRIPT, not an `--m9` mode of m6_fleet_gate.py:

  m6_fleet_gate.py's `start_node` launches a GATE BINARY's `node` role via CLI
  flags. `m9_gate` deliberately defines no `node` role — M9's whole thesis is
  that a real `uc2-node` daemon exists, configured by a TOML FILE, and that a
  gate-specific node role would measure something the operator never runs. So
  there is nothing to share on that path. What IS shared — ssh, systemd-run,
  build/rsync, leader discovery, and the on-host dip measurement — is imported
  from m6_fleet_gate below, exactly as m5_fleet_gate/m5_ab_gate/
  aeron_parity_gate/eventual_arm_gate already do. m6_fleet_gate.py also carries
  four BANKED fleet PASSes (M5, M6, M7, protocol 0.5.0); a new file cannot
  regress them.

The four pre-committed bars (restated from the gate doc; changing a number
here without changing the doc, in a commit that says why, is exactly what the
honest-failure protocol exists to prevent):

  row 1  SIGTERM -> process exit, leader under load : < 1 s AND exit code 0
  row 2  snapshot install on restart               : incoming_snapshot_pos
                                                     UNCHANGED across the cycle
  row 3  cluster commit-rate dip across stop+start  : < 10 %   (FLEET-GATED)
  row 4  config refusals                            : every rule refuses with a
                                                     message naming the field

Row 3 is measured in both modes but ADJUDICATED ONLY ON THE FLEET, matching
m6_gate/m7_gate and the gate doc's Ruling 12: seven identical local runs spread
0.0-18.0 % against a 10-point bar, so a local verdict is a coin flip in either
direction. `--local` exists to debug THIS ORCHESTRATOR before it runs on billed
hosts, not to decide the gate.

Row 2 is anti-vacuity checked (Ruling 13): "incoming_snapshot_pos unchanged at
0" is also what a cluster that never snapshots looks like, and such a cluster
could not have installed one however badly the restart went. The row therefore
requires positive evidence that snapshots were being BUILT
(`service_snapshot_pos > 0`) and reports INCONCLUSIVE-and-FAIL otherwise.

Exit 0 iff every gated row passes. Exit 1 on FAIL — a green terminal is not a
PASS; the exit code is.
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402

# SshHost.probe()/rate_probe_start() read the module-global APP when they build
# the probe command line, so it must name THIS gate's app id before any host is
# constructed. m6_fleet_gate's own main() does the same for --m7.
m6.APP = "m9-gate"

from m6_fleet_gate import (  # noqa: E402
    LocalHost, SshHost, wait_leader,
)

APP = "m9-gate"
PORT = 19100
BAR_STOP_SECS = 1.0
BAR_DIP_PCT = 10.0              # run-1 bar; now printed UNGATED for comparability
BAR_RECOVER_SECS = 15.0         # re-specified row 3 (gate doc, pre-committed)
RECOVER_FRACTION = 0.8          # a window qualifies at >= 80 % of baseline
RECOVER_WINDOW_SECS = 2.0
RECOVER_HARD_DEADLINE = 40.0    # stop polling; a FAIL still prints the timeline
DRAIN_TIMEOUT_SECS = 5          # uc2-node --drain-timeout-secs
UNIT_STOP_TIMEOUT = 10          # systemd TimeoutStopSec; must exceed the drain
LEADER_WAIT_SECS = 45
LOAD_SECS = 60
SETTLE_SECS = 6                 # let snapshots fire before the stop cycle


# --------------------------------------------------------------- config file

def render_config(node_id, bind, instance_dir, members):
    """The operator-facing artifact: exactly the TOML a deployment ships.

    Deliberately NOT a flag list. The gate measures the real daemon reading a
    real config file, because that is the thing M9 claims to deliver.

    `purge`/snapshot settings are ON: without them a snapshot install is
    impossible and row 2 would be vacuously green (Ruling 13).
    """
    lines = [
        f"id = {node_id}",
        f'bind = "{bind}"',
        f'instance_dir = "{instance_dir}"',
        f'app_id = "{APP}"',
        "buffer_bytes = 4194304",
        "max_payload = 256",
        "journal_segment_bytes = 16384",
        "",
        "[purge]",
        "below_snapshot_slack_bytes = 0",
        "",
        # M12b: [crypto]/[admin] became required (explicit-choice config,
        # spec §3.3) — a node.toml missing either is now a startup refusal.
        # This gate exercises neither wire crypto nor admin auth, so both
        # choose the cleartext/filesystem posture.
        "[crypto]",
        "enabled = false",
        "",
        "[admin]",
        'auth = "none"',
        "",
    ]
    for mid, maddr in members:
        lines += ["[[members]]", f"id = {mid}", f'addr = "{maddr}"', ""]
    return "\n".join(lines)


# ----------------------------------------------------------------- M9 node

class M9Node:
    """One node: the real `uc2-node` daemon plus the gate's service role.

    Wraps a LocalHost/SshHost for everything that already exists (probe, rate
    probe, the service/loadclient roles) and owns only what is genuinely M9's:
    rendering a config file, starting the DAEMON rather than a gate role, and
    timing a SIGTERM-to-exit ON THE HOST.
    """

    def __init__(self, host, node_id, uc_node_bin, local):
        self.host = host
        self.id = node_id
        self.uc_node_bin = uc_node_bin
        self.local = local
        self.cfg_path = f"{Path(host.dir).parent}/n{node_id}.toml"
        self.daemon = None          # local: Popen
        self.unit = f"m9-node{node_id}"

    # -- config ------------------------------------------------------------
    def write_config(self, members):
        body = render_config(self.id, self.host.bind_addr_fixed, self.host.dir, members)
        if self.local:
            Path(self.cfg_path).write_text(body)
        else:
            # tee under sudo: /opt/bench is root-owned on the fleet.
            r = self.host._ssh(
                f"sudo mkdir -p {Path(self.cfg_path).parent} && "
                f"sudo tee {self.cfg_path} >/dev/null <<'M9CFG'\n{body}\nM9CFG",
                capture_output=True,
            )
            if r.returncode != 0:
                raise RuntimeError(f"write config on {self.host.public_ip}: {r.stderr}")

    # -- daemon lifecycle --------------------------------------------------
    def start_daemon(self):
        if self.local:
            log = open(Path(self.host.logs) / f"{self.unit}.log", "a")
            log.write(f"\n=== start {self.unit} ===\n")
            log.flush()
            self.daemon = subprocess.Popen(
                [self.uc_node_bin, "--config", self.cfg_path,
                 "--drain-timeout-secs", str(DRAIN_TIMEOUT_SECS)],
                stdout=log, stderr=subprocess.STDOUT,
            )
            return

        # NOT --collect: the transient unit must survive its own stop so
        # `systemctl show -p ExecMainStatus` can report the daemon's REAL exit
        # code afterwards. TimeoutStopSec must exceed the drain, or systemd
        # SIGKILLs a drain in progress and row 1 measures a kill, not a clean
        # stop (m6_fleet_gate's start_unit hard-codes 1 s, which is why the
        # daemon cannot use it).
        self.host._ssh(f"sudo systemctl reset-failed {self.unit} 2>/dev/null; true",
                       capture_output=True)
        cmd = (
            f"sudo systemd-run --unit={self.unit} "
            f"-p TimeoutStopSec={UNIT_STOP_TIMEOUT} -p KillSignal=SIGTERM "
            f"-p StandardOutput=append:/opt/bench/{self.unit}.log "
            f"-p StandardError=append:/opt/bench/{self.unit}.log "
            f"{self.uc_node_bin} --config {self.cfg_path} "
            f"--drain-timeout-secs {DRAIN_TIMEOUT_SECS}"
        )
        r = self.host._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"start {self.unit} on {self.host.public_ip}: {r.stderr}")

    def stop_daemon_timed(self):
        """SIGTERM the daemon and return (elapsed_secs, exit_code).

        Timed ON THE HOST in both modes. M7's first fleet run was a 100 %
        harness artifact largely because wall-clock was taken around ssh round
        trips; row 1's bar is 1 s and an ssh RTT is 0.4-0.8 s, so measuring
        across the link would be measuring the link.
        """
        if self.local:
            t0 = time.perf_counter()
            self.daemon.terminate()
            try:
                rc = self.daemon.wait(timeout=UNIT_STOP_TIMEOUT + 5)
            except subprocess.TimeoutExpired:
                self.daemon.kill()
                self.daemon.wait(timeout=5)
                return (float(UNIT_STOP_TIMEOUT + 5), None)
            return (time.perf_counter() - t0, rc)

        script = (
            f"S=$(date +%s%N); sudo systemctl stop {self.unit}; E=$(date +%s%N); "
            f"echo M9_ELAPSED_NS=$((E-S)); "
            f"echo M9_EXIT=$(sudo systemctl show {self.unit} -p ExecMainStatus --value)"
        )
        r = self.host._ssh(script, capture_output=True)
        out = r.stdout or ""
        ns = _grab(out, "M9_ELAPSED_NS=")
        code = _grab(out, "M9_EXIT=")
        if ns is None:
            raise RuntimeError(f"stop {self.unit}: no timing in {out!r} {r.stderr!r}")
        return (int(ns) / 1e9, int(code) if code not in (None, "") else None)

    # -- gate roles (unchanged, reused as-is) -------------------------------
    def start_service(self):
        self.host.start_unit("service", ["service", "--instance-dir", self.host.dir,
                                         "--app-id", APP])

    def start_load(self, secs):
        self.host.start_unit("load", ["loadclient", "--instance-dir", self.host.dir,
                                      "--app-id", APP, "--secs", str(secs)])

    def probe(self, rate_secs=0.0):
        return self.host.probe(rate_secs=rate_secs)


def leader_probe_rate(node, secs):
    """One on-host rate window on `node`; 0.0 on a transient probe failure
    (a failed window reads as not-qualified, which errs against PASS)."""
    try:
        return node.probe(rate_secs=secs).get("rate") or 0.0
    except Exception:
        return 0.0


def _grab(text, key):
    for line in text.splitlines():
        if line.startswith(key):
            return line[len(key):].strip()
    return None


# ------------------------------------------------------------- row 4

REFUSALS = [
    # (name, extra/replacement body, substring the refusal MUST contain)
    ("unknown-key",       "buffer_bytez = 4096",                 "buffer_bytez"),
    ("buffer-not-pow2",   "buffer_bytes = 4097",                 "buffer_bytes"),
    ("payload-over-mtu",  "max_payload = 65536",                 "max_payload"),
    ("election-window",   "election_timeout_min_ns = 400000000", "election_timeout"),
]


def check_refusals(node, members):
    """Every preflight rule must refuse BY NAME, from the real binary, on the
    real host. A refusal that does not name the offending field is a FAIL: the
    operator cannot act on 'invalid config'."""
    base_dir = f"{Path(node.host.dir).parent}/refusals"
    failures, checked = [], 0
    member_block = "".join(
        f'[[members]]\nid = {m}\naddr = "{a}"\n\n' for m, a in members
    )
    good = (f'id = {members[0][0]}\nbind = "{members[0][1]}"\n'
            f'instance_dir = "{base_dir}/inst"\napp_id = "{APP}"\n\n')

    cases = [(n, good + extra + "\n\n" + member_block, want) for n, extra, want in REFUSALS]
    # bind-mismatch and self-not-a-member replace a key line rather than adding one.
    cases.append((
        "bind-mismatch",
        f'id = {members[0][0]}\nbind = "0.0.0.0:{PORT}"\n'
        f'instance_dir = "{base_dir}/inst"\napp_id = "{APP}"\n\n' + member_block,
        "bind",
    ))
    cases.append((
        "self-not-a-member",
        f'id = 99\nbind = "{members[0][1]}"\n'
        f'instance_dir = "{base_dir}/inst"\napp_id = "{APP}"\n\n' + member_block,
        "99",
    ))

    for name, body, want in cases:
        checked += 1
        path = f"{base_dir}/{name}.toml"
        rc, err = _run_refusal(node, base_dir, path, body)
        if rc == 0:
            failures.append(f"{name}: node STARTED on a config that must be refused")
        elif want not in err:
            failures.append(f"{name}: refusal did not name {want!r}: {err.strip()[:200]}")
    return checked, failures


def _run_refusal(node, base_dir, path, body):
    if node.local:
        Path(base_dir).mkdir(parents=True, exist_ok=True)
        (Path(base_dir) / "inst").mkdir(parents=True, exist_ok=True)
        Path(path).write_text(body)
        p = subprocess.run([node.uc_node_bin, "--config", path],
                           capture_output=True, text=True, timeout=30)
        return p.returncode, (p.stderr or "")
    r = node.host._ssh(
        f"sudo mkdir -p {base_dir}/inst && "
        f"sudo tee {path} >/dev/null <<'M9CFG'\n{body}\nM9CFG\n"
        f"sudo {node.uc_node_bin} --config {path}; echo M9_RC=$?",
        capture_output=True,
    )
    out = (r.stdout or "") + (r.stderr or "")
    rc = _grab(out, "M9_RC=")
    return (int(rc) if rc is not None else 1), out


# ------------------------------------------------------------------ the run

class Verdict:
    def __init__(self, row, passed, detail, gated=True):
        self.row, self.passed, self.detail, self.gated = row, passed, detail, gated


def run(nodes, members, leader_idx, fleet):
    verdicts = []
    leader = nodes[leader_idx]
    # The rate is measured on a SURVIVOR, never on the node being restarted.
    # `rate_over`/`max_commit` in the in-process mode take the MAX commit across
    # all nodes — cluster progress, which survives one node going down. Probing
    # the restarted node instead measures its own outage by construction and
    # reports ~86 % where the in-process mode reports 0-18 %. A survivor's commit
    # counter tracks the cluster (followers learn commit from the leader), and
    # probing it keeps the on-host, zero-ssh-skew property of `dip_for_transition`.
    survivor = nodes[(leader_idx + 1) % len(nodes)]
    print(f"INFO measuring cluster rate on survivor n{survivor.id}", flush=True)

    before = leader.probe()
    print(f"INFO pre-stop leader n{leader.id}: {json.dumps(before)}", flush=True)

    # Row 3 as re-specified (gate doc, pre-committed before this run):
    # time-to-recover. Baseline = one 8 s on-host window on the survivor;
    # recovered = first 2 s window at >= 80 % of baseline whose END is within
    # 15 s of the SIGTERM, confirmed by the next window. t0 is taken BEFORE
    # the stop's ssh round trip, so every skew inflates the measured recovery
    # time — the measurement errs against PASS. The run-1-style 5 s dip is
    # still taken (background window spanning the action) and printed UNGATED.
    baseline = leader_probe_rate(survivor, 8.0)
    print(f"INFO baseline (survivor n{survivor.id}, 8s): {baseline:.0f} B/s", flush=True)

    dip_handle = survivor.host.rate_probe_start(5.0)
    time.sleep(0.5)

    timing = {}
    t0 = time.monotonic()
    elapsed, code = leader.stop_daemon_timed()
    timing["elapsed"], timing["code"] = elapsed, code
    print(f"INFO stop: {elapsed:.3f}s exit={code}", flush=True)
    leader.start_daemon()
    # Service restarts with the node (BindsTo semantics — see packaging).
    # Load clients are untouched: one per node, the new leader's just serves.
    leader.start_service()

    timeline, recovered, first_end = [], None, None
    while time.monotonic() - t0 < RECOVER_HARD_DEADLINE:
        rate = leader_probe_rate(survivor, RECOVER_WINDOW_SECS)
        w_end = time.monotonic() - t0
        timeline.append((w_end, rate))
        qualifies = baseline > 0 and rate >= RECOVER_FRACTION * baseline
        if qualifies and first_end is None:
            first_end = w_end          # candidate; next window must confirm
        elif first_end is not None:
            if qualifies:
                recovered = first_end
                break
            first_end = None           # one lucky window is not recovery
    tl = ", ".join(f"{t:.1f}s:{r:.0f}" for t, r in timeline)
    print(f"INFO recovery timeline (B/s): {tl}", flush=True)

    for n in nodes:
        try:
            pr = n.probe()
            print(f"INFO post-recovery n{n.id}: leader={pr.get('leader')} "
                  f"can_serve={pr.get('can_serve')} term={pr.get('term')} "
                  f"commit={pr.get('commit')}", flush=True)
        except Exception as e:
            print(f"INFO post-recovery n{n.id}: probe failed: {e}", flush=True)

    during = survivor.host.rate_probe_result(dip_handle, timeout=40.0) or 0.0
    dip = max(0.0, (baseline - during) / baseline * 100.0) if baseline > 0 else 100.0
    print(f"INFO run-1-style 5s dip {dip:.1f}% (baseline {baseline:.0f} -> "
          f"{during:.0f} B/s) — UNGATED, printed for comparability with run 1",
          flush=True)

    # -- row 1
    elapsed, code = timing.get("elapsed"), timing.get("code")
    if elapsed is None:
        verdicts.append(Verdict("1 SIGTERM->exit", False, "no timing captured"))
    else:
        ok = elapsed < BAR_STOP_SECS and code == 0
        verdicts.append(Verdict(
            "1 SIGTERM->exit", ok,
            f"{elapsed:.3f}s, exit {code} (bar: < {BAR_STOP_SECS}s and exit 0)"))

    # -- wait for the restarted node to rejoin before reading row 2
    deadline = time.time() + LEADER_WAIT_SECS
    after = {}
    while time.time() < deadline:
        try:
            after = leader.probe()
            if after.get("can_serve"):
                break
        except Exception:
            pass
        time.sleep(0.5)

    # -- row 2 (+ Ruling 13 anti-vacuity)
    built = []
    for n in nodes:
        try:
            built.append(n.probe().get("service_snapshot_pos", 0))
        except Exception:
            built.append(0)
    inc_before = before.get("incoming_snapshot_pos", 0)
    inc_after = after.get("incoming_snapshot_pos", 0)
    if not any(b > 0 for b in built):
        verdicts.append(Verdict(
            "2 no snapshot install", False,
            f"INCONCLUSIVE — no node built a snapshot (service_snapshot_pos {built}), "
            f"so an install was impossible however badly the restart went"))
    else:
        ok = inc_after == inc_before
        verdicts.append(Verdict(
            "2 no snapshot install", ok,
            f"incoming_snapshot_pos {inc_before} -> {inc_after} (bar: unchanged; "
            f"snapshots WERE being built: service_snapshot_pos {built})"))

    # -- row 3, re-specified: time-to-recover (fleet-gated only; Ruling 12)
    if recovered is not None:
        detail = (f"recovered at {recovered:.1f}s (first >= {RECOVER_FRACTION:.0%}-of-"
                  f"baseline window end; confirmed by next), bar <= {BAR_RECOVER_SECS}s")
        ok = recovered <= BAR_RECOVER_SECS
    else:
        detail = (f"NOT recovered within {RECOVER_HARD_DEADLINE:.0f}s "
                  f"(bar <= {BAR_RECOVER_SECS}s); timeline above")
        ok = False
    if fleet:
        verdicts.append(Verdict("3 time-to-recover", ok, detail))
    else:
        verdicts.append(Verdict(
            "3 time-to-recover", True,
            detail + " — NOT gated locally; the bar is fleet-only "
                     "(see m6_gate/m7_gate and Ruling 12)", gated=False))

    # -- row 4
    checked, failures = check_refusals(nodes[0], members)
    verdicts.append(Verdict(
        "4 refusals name the field", not failures,
        f"{checked - len(failures)}/{checked} rules refused by name"
        + ("" if not failures else " — " + "; ".join(failures))))
    return verdicts


# --------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="UC v2 M9 restart-cost gate driver")
    ap.add_argument("--local", action="store_true", help="local processes (loopback UDP)")
    ap.add_argument("--fleet", action="store_true", help="remote hosts over ssh")
    ap.add_argument("--bin", default="", help="path to the m9_gate binary")
    ap.add_argument("--uc2-node-bin", default="", help="path to the uc2-node binary")
    ap.add_argument("--root", default="/home/claude/.cache/m9_fleet",
                    help="local root dir (NEVER /tmp — RAM tmpfs, see CLAUDE.md)")
    ap.add_argument("--hosts", default="", help="fleet: pub/priv,... (else terraform output)")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--secs", type=int, default=LOAD_SECS)
    a = ap.parse_args()

    if a.local == a.fleet:
        ap.error("pass exactly one of --local / --fleet")
    if a.local and a.root.startswith("/tmp"):
        ap.error("--root must be on a real filesystem (never /tmp — RAM tmpfs)")

    if a.local:
        nodes, members = setup_local(a)
    else:
        nodes, members = setup_fleet(a)

    print("INFO waiting for a leader", flush=True)
    idx = wait_leader([n.host for n in nodes], list(range(len(nodes))), LEADER_WAIT_SECS)
    if idx is None:
        print("RESULT: FAIL (honest) — no leader within "
              f"{LEADER_WAIT_SECS}s; nothing was measured.")
        teardown(nodes)
        sys.exit(1)
    print(f"INFO leader is n{nodes[idx].id}", flush=True)

    # One client per node: the leader's does the work, the followers' redirect.
    # Lifetime must cover settle + 8 s baseline + the action + the full
    # recovery poll; a client that exits mid-poll reads as a dead cluster
    # (this exact bug: --secs 40 ended the clients ~24 s into the window).
    need = SETTLE_SECS + 10 + RECOVER_HARD_DEADLINE + 20
    load_secs = max(a.secs, int(need))
    if load_secs != a.secs:
        print(f"INFO load lifetime raised {a.secs} -> {load_secs}s to cover "
              f"the recovery poll", flush=True)
    for n in nodes:
        n.start_load(load_secs)
    time.sleep(SETTLE_SECS)

    try:
        verdicts = run(nodes, members, idx, fleet=a.fleet)
    finally:
        teardown(nodes)

    print()
    print(f"M9 restart-cost gate — {'FLEET' if a.fleet else 'LOCAL SMOKE'}")
    for v in verdicts:
        print(f"  [{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    gated = [v for v in verdicts if v.gated]
    if all(v.passed for v in gated):
        if a.fleet:
            print("RESULT: PASS — every pre-committed bar held on the fleet.")
        else:
            print("RESULT: PASS (smoke) — rows 1, 2 and 4 held locally. Row 3 is "
                  "fleet-gated, so this is NOT an M9 PASS.")
        sys.exit(0)
    print("RESULT: FAIL (honest) — a gated row did not hold.")
    sys.exit(1)


def setup_local(a):
    root = Path(a.root)
    subprocess.run(["rm", "-rf", str(root)], check=False)
    (root / "logs").mkdir(parents=True, exist_ok=True)
    gate = a.bin or str(Path.home() / ".cache/cargo-target/release/examples/m9_gate")
    ucnode = a.uc_node_bin or str(Path.home() / ".cache/cargo-target/release/uc2-node")
    for p in (gate, ucnode):
        if not Path(p).is_file():
            sys.exit(f"binary not found: {p} — build it first")

    hosts, nodes = [], []
    for i in range(3):
        d = root / f"n{i}"
        d.mkdir(parents=True, exist_ok=True)
        h = LocalHost(gate, d, root / "logs")
        # bind_addr() mints a FRESH ephemeral port on every call, so it is
        # called ONCE and pinned — calling it again would advertise a port
        # nothing is listening on (a real bug m7_gate's own run caught).
        h.bind_addr_fixed = h.bind_addr()
        hosts.append(h)
        nodes.append(M9Node(h, i, ucnode, local=True))

    members = [(n.id, n.host.bind_addr_fixed) for n in nodes]
    for n in nodes:
        n.write_config(members)
    for n in nodes:
        n.start_daemon()
    time.sleep(1.0)
    for n in nodes:
        n.start_service()
    return nodes, members


def setup_fleet(a):
    specs = m6.fleet_hosts(a.hosts) if hasattr(m6, "fleet_hosts") else _hosts_from_arg(a.hosts)
    gate = a.bin or "/opt/bench/uc/target/release/examples/m9_gate"
    ucnode = a.uc_node_bin or "/opt/bench/uc/target/release/uc2-node"
    nodes = []
    for i, (pub, priv) in enumerate(specs[:3]):
        h = SshHost(gate, f"/opt/bench/m9/n{i}", pub, priv, a.ssh_user, a.ssh_key,
                    unit_prefix="m9", remote_root="/opt/bench/m9")
        h.bind_addr_fixed = f"{priv}:{PORT}"
        h.prepare(examples=("m9_gate",), bins=("uc_node",))
        h._ssh(f"sudo rm -rf {h.dir} && sudo mkdir -p {h.dir}", capture_output=True)
        nodes.append(M9Node(h, i, ucnode, local=False))

    members = [(n.id, n.host.bind_addr_fixed) for n in nodes]
    for n in nodes:
        n.write_config(members)
    for n in nodes:
        n.start_daemon()
    time.sleep(2.0)
    for n in nodes:
        n.start_service()
    return nodes, members


def _hosts_from_arg(spec):
    if not spec:
        sys.exit("--fleet needs --hosts pub/priv,pub/priv,pub/priv")
    return [tuple(p.split("/")) for p in spec.split(",")]


def teardown(nodes):
    for n in nodes:
        try:
            n.host.kill_unit("load")
            n.host.kill_unit("service")
        except Exception:
            pass
        try:
            if n.local:
                if n.daemon and n.daemon.poll() is None:
                    n.daemon.kill()
                    n.daemon.wait(timeout=10)
            else:
                n.host._ssh(f"sudo systemctl stop {n.unit} 2>/dev/null; "
                            f"sudo systemctl reset-failed {n.unit} 2>/dev/null; true",
                            capture_output=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
