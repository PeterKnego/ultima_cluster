#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M11 flag-day gate driver — row 5 of the pre-committed decide rule in
docs/benchmarks/uc2-m11-gate-2026-08-20.md (written and committed BEFORE any
run; rows 1/2/3a/4 are local/CI rows already adjudicated in that document).

    row 5  full stop -> verify-equal-durable -> upgrade -> start ->
           verify-serving on a real fleet: DOWNTIME (last node stopped ->
           cluster serving a single leader) <= 60 s, binaries pre-staged
           (transfer excluded from the timer), zero acked loss across the
           flag day.                                        FLEET ONLY

WHY THIS IS ITS OWN SCRIPT, and why it cannot reuse m9_fleet_gate's node:

  Every prior fleet gate starts its nodes with `systemd-run --unit=X`, i.e. a
  TRANSIENT unit. `scripts/uc2_flag_day.sh` — the operator artifact this row
  measures — stops each node with `systemctl stop $UNIT` and later starts it
  again with `systemctl start $UNIT`. A transient unit cannot serve the second
  half of that: once stopped it is gone, and `systemctl start` on it fails with
  "Unit uc2-node.service not found". So M11's fleet nodes are installed the way
  a deployment installs them — the SHIPPED unit file
  (packaging/systemd/uc2-node.service, ExecStart=/usr/local/bin/uc2-node
  --config /etc/uc2/node.toml) in /etc/systemd/system, the binary in
  /usr/local/bin, the config in /etc/uc2 — which is also the only honest way to
  measure an upgrade procedure whose whole claim is "this is what the operator
  runs".

  What IS shared is imported from m6_fleet_gate/m9_fleet_gate exactly as
  m5_fleet_gate/m9_fleet_gate/aeron_parity_gate already do: SshHost (ssh,
  build/rsync prepare, probe, rate probes, transient units for the SERVICE and
  LOAD roles, which are gate scaffolding rather than the thing under test),
  wait_leader, and m9's `render_config` shape. m9_gate's binary supplies the
  `service`/`loadclient`/`probe` roles — it deliberately defines no `node` role
  (M9's thesis: the real daemon, configured by a real TOML file), which is
  precisely what M11 needs too. Those files carry banked fleet PASSes (M5, M6,
  M7, protocol 0.5.0, M9, M10); a new file cannot regress them.

WHAT "ZERO ACKED LOSS" MEANS HERE, concretely, and why it is not vacuous:

  Two independent checks, both required:
    (a) uc2_flag_day.sh's own step 3 refuses to upgrade unless EVERY stopped
        node reports the SAME durable position ("verify OK: all N node(s)
        durable=D"). That line is parsed out of its output and required — a
        flag day that never printed it either aborted or never got that far.
    (b) the cluster's committed high-water, read from every node's cnc page
        BEFORE traffic stops and again AFTER the cluster is serving, must not
        go backwards. This is the same committed-high-water idiom M6/M7 use.
  Plus a positive liveness check: load is restarted afterwards and the leader
  must show a NON-ZERO commit rate. "Nothing was lost" is trivially true of a
  cluster that also does nothing; the rate probe is what distinguishes them.

ANTI-VACUITY ON THE UPGRADE ITSELF (recorded honestly, not papered over):
  This run has no second binary version to install — the fleet builds one tree.
  `--upgrade-cmd` therefore installs a binary that is byte-identical to the one
  that was running. What that does and does not prove:
    proven      — the real install path runs on every host, as root, under the
                  script's own parallel-ssh fan-out, and the resulting file is
                  a NEW inode on every host (asserted below: the pre-flag-day
                  inode of /usr/local/bin/uc2-node must differ from the post
                  one on every host, so `install` genuinely replaced the file
                  rather than no-opping);
    NOT proven  — that a genuinely different binary version boots and
                  interoperates. That is a wire-compatibility claim, not a
                  downtime claim, and it is what protocol 0.5.0's flag-day
                  posture (upgrade all nodes together) exists to make moot.
  The staging copy happens BEFORE the script is invoked, per the bar's
  "binaries pre-staged (transfer excluded from the timer)".

Exit 0 iff row 5 holds. Exit 1 on FAIL — a green terminal is not a PASS; the
exit code is.
"""

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402

# SshHost.probe()/rate_probe_start() read the module-global APP when they build
# the probe command line, so it must name THIS gate's app id before any host is
# constructed (m9_fleet_gate does the same).
m6.APP = "m11-gate"

from m6_fleet_gate import wait_leader  # noqa: E402

APP = "m11-gate"
PORT = 19100
REPO_ROOT = Path(__file__).resolve().parents[2]

# The bar (pre-committed; changing a number here without changing the gate doc,
# in a commit that says why, is exactly what the honest-failure protocol exists
# to prevent).
BAR_DOWNTIME_SECS = 60.0

# Fleet layout. /opt/bench is the instance-store NVMe mount (durable, non-tmpfs
# — SshHost.prepare asserts the filesystem type). The unit/binary/config paths
# are the SHIPPED ones from packaging/, not gate-invented paths.
REMOTE_ROOT = "/opt/bench/m11"
NODE_BIN = "/usr/local/bin/uc2-node"
CTL_BIN = "/usr/local/bin/uc2ctl"
CONFIG_PATH = "/etc/uc2/node.toml"
UNIT = "uc2-node"
STAGED_BIN = f"{REMOTE_ROOT}/uc2-node.new"
BUILT_NODE = "/opt/bench/uc/target/release/uc2-node"
BUILT_CTL = "/opt/bench/uc/target/release/uc2ctl"
BUILT_GATE = "/opt/bench/uc/target/release/examples/m9_gate"
PACKAGED_UNIT = "/opt/bench/uc/packaging/systemd/uc2-node.service"

LEADER_WAIT_SECS = 60
PRE_LOAD_SECS = 45          # traffic BEFORE the flag day (must end before it)
POST_LOAD_SECS = 30         # traffic AFTER, to prove the cluster really serves
QUIESCE_SECS = 4            # let the last in-flight writes commit + drain
DRAIN_WAIT_SECS = 30        # bound on waiting for every node's durable to agree
FLAGDAY_TIMEOUT_SECS = 420  # generous: 5 stops + 5 upgrades + convergence poll
RATE_WINDOW_SECS = 5.0


# --------------------------------------------------------------- config file

def render_config(node_id, bind, instance_dir, members):
    """The operator-facing artifact: exactly the TOML a deployment ships — the
    shape m9_fleet_gate.render_config established, unchanged. No [metrics]
    section: the endpoint is M10 surface this row does not measure, and every
    extra moving part in a billed fleet run is another way to lose the run to
    something that is not the bar."""
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


# ------------------------------------------------------------------ M11 node

class M11Node:
    """One fleet node: the SHIPPED uc2-node unit, plus the gate's own service
    and load roles (transient units — they are scaffolding, not the artifact
    under test, and uc2_flag_day.sh never touches them)."""

    def __init__(self, host, node_id):
        self.host = host
        self.id = node_id
        self.unit = UNIT

    # -- install (the operator path) ---------------------------------------
    def install(self, members):
        body = render_config(self.id, self.host.bind_addr_fixed, self.host.dir, members)
        script = (
            f"set -e; "
            f"sudo install -m755 {BUILT_NODE} {NODE_BIN}; "
            f"sudo install -m755 {BUILT_CTL} {CTL_BIN}; "
            f"sudo mkdir -p /etc/uc2 {REMOTE_ROOT}; "
            f"sudo rm -rf {self.host.dir}; sudo mkdir -p {self.host.dir}; "
            f"sudo tee {CONFIG_PATH} >/dev/null <<'M11CFG'\n{body}\nM11CFG\n"
            f"sudo cp {PACKAGED_UNIT} /etc/systemd/system/{UNIT}.service; "
            f"sudo systemctl daemon-reload; "
            f"echo INSTALLED"
        )
        r = self.host._ssh(script, capture_output=True)
        if "INSTALLED" not in (r.stdout or ""):
            raise RuntimeError(f"install on {self.host.public_ip}: {r.stderr or r.stdout}")

    def start_daemon(self):
        r = self.host._ssh(
            f"sudo systemctl reset-failed {UNIT} 2>/dev/null; "
            f"sudo systemctl start {UNIT} && echo STARTED",
            capture_output=True,
        )
        if "STARTED" not in (r.stdout or ""):
            raise RuntimeError(f"start {UNIT} on {self.host.public_ip}: {r.stderr or r.stdout}")

    def stop_daemon(self):
        self.host._ssh(f"sudo systemctl stop {UNIT} 2>/dev/null; "
                       f"sudo systemctl reset-failed {UNIT} 2>/dev/null; true",
                       capture_output=True)

    def node_bin_stamp(self):
        """The install-really-happened witness (see the module doc's
        anti-vacuity note): inode + mtime + ctime of the live binary path.

        NOT the inode alone. Run 1 of this gate FAILED on exactly that: GNU
        `install` opens the destination `O_WRONLY|O_CREAT|O_TRUNC` and copies
        into it — it does not unlink and recreate — so a successful install
        leaves the inode NUMBER untouched and only the timestamps move. The
        inode-only witness therefore reported "the binary was never replaced"
        for four installs that had each exited 0. mtime (contents written) and
        ctime (metadata/permissions rewritten) both move under `install`, so
        the triple changes iff the file was really rewritten."""
        r = self.host._ssh(f"stat -c %i:%Y:%Z {NODE_BIN} 2>/dev/null", capture_output=True)
        out = (r.stdout or "").strip().splitlines()
        return out[-1] if out else ""

    def stage_new_binary(self):
        """Pre-stage the 'new' binary. Deliberately BEFORE the timer starts —
        the bar excludes transfer, because an operator stages binaries during
        business hours and takes the outage only for the swap."""
        r = self.host._ssh(
            f"sudo mkdir -p {REMOTE_ROOT} && sudo cp {BUILT_NODE} {STAGED_BIN} "
            f"&& sudo chmod 755 {STAGED_BIN} && echo STAGED",
            capture_output=True,
        )
        if "STAGED" not in (r.stdout or ""):
            raise RuntimeError(f"stage on {self.host.public_ip}: {r.stderr or r.stdout}")

    # -- gate roles (transient units; unchanged from m9's shape) -----------
    def start_service(self):
        self.host.start_unit("service", ["service", "--instance-dir", self.host.dir,
                                         "--app-id", APP])

    def start_load(self, secs):
        self.host.start_unit("load", ["loadclient", "--instance-dir", self.host.dir,
                                      "--app-id", APP, "--secs", str(secs)])

    def stop_roles(self):
        for u in ("load", "service"):
            try:
                self.host.kill_unit(u)
            except Exception:
                pass

    def probe(self, rate_secs=0.0):
        return self.host.probe(rate_secs=rate_secs)


def safe_probe(node, rate_secs=0.0):
    try:
        return node.probe(rate_secs=rate_secs)
    except Exception as e:
        print(f"INFO probe n{node.id} failed: {e}", flush=True)
        return {}


# ------------------------------------------------------------- the flag day

def run_flag_day(nodes, ssh_user, ssh_key):
    """Invoke the OPERATOR ARTIFACT, unmodified, from this box — the same
    command line docs/how-to/upgrade-a-cluster.md tells an operator to run.
    Returns (returncode, combined output)."""
    hosts = ",".join(f"{ssh_user}@{n.host.public_ip}" for n in nodes)
    cmd = [
        str(REPO_ROOT / "scripts" / "uc2_flag_day.sh"),
        "--hosts", hosts,
        "--ssh-key", ssh_key,
        "--unit", UNIT,
        # The instance dirs are root-owned (the daemon runs as root under
        # systemd), so `uc2ctl status` needs sudo — exactly as an operator's
        # own status check on a root-owned /srv/uc2 would. The script embeds
        # this string verbatim in its remote command, which is why a prefixed
        # command works where a bare path would hit EACCES on cnc2.dat.
        "--uc2ctl", f"sudo {CTL_BIN}",
        "--instance-dir", f"{REMOTE_ROOT}/nX",
        "--app-id", APP,
        "--upgrade-cmd", f"sudo install -m755 {STAGED_BIN} {NODE_BIN}",
        "--yes-traffic-stopped",
    ]
    print("INFO flag day: " + " ".join(cmd), flush=True)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=FLAGDAY_TIMEOUT_SECS)
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    except subprocess.TimeoutExpired as e:
        out = e.stdout if isinstance(e.stdout, str) else (e.stdout or b"").decode(errors="replace")
        err = e.stderr if isinstance(e.stderr, str) else (e.stderr or b"").decode(errors="replace")
        return 124, (out or "") + (err or "") + \
            f"\n[orchestrator] TIMEOUT after {FLAGDAY_TIMEOUT_SECS}s\n"


DOWNTIME_RE = re.compile(r"^DOWNTIME:\s*([0-9.]+)s\s*$", re.M)
VERIFY_RE = re.compile(r"verify OK: all (\d+) node\(s\) durable=(\d+)")


# ------------------------------------------------------------------ verdicts

class Verdict:
    def __init__(self, row, passed, detail):
        self.row, self.passed, self.detail = row, passed, detail


def run(nodes, ssh_user, ssh_key):
    checks = []

    print("INFO waiting for a leader", flush=True)
    idx = wait_leader([n.host for n in nodes], list(range(len(nodes))), LEADER_WAIT_SECS)
    if idx is None:
        return [Verdict("5 flag-day downtime", False,
                        f"no leader within {LEADER_WAIT_SECS}s BEFORE the flag day; "
                        f"nothing was measured")]
    print(f"INFO leader is n{nodes[idx].id}", flush=True)

    # -- traffic, then a clean stop of traffic (the script's own precondition)
    for n in nodes:
        n.start_load(PRE_LOAD_SECS)
    print(f"INFO load running {PRE_LOAD_SECS}s on all {len(nodes)} nodes", flush=True)
    time.sleep(PRE_LOAD_SECS * 0.5)
    pre_rate = safe_probe(nodes[idx], rate_secs=RATE_WINDOW_SECS).get("rate") or 0.0
    print(f"INFO under-load commit rate on the leader: {pre_rate:.0f} B/s", flush=True)
    time.sleep(max(0.0, PRE_LOAD_SECS * 0.5 - RATE_WINDOW_SECS))
    for n in nodes:
        n.host.kill_unit("load")
    time.sleep(QUIESCE_SECS)

    # Wait for the drain the operator is told to wait for. uc2_flag_day.sh's
    # step 3 ABORTS (exit 1, un-upgrade path) if the stopped nodes disagree on
    # durable — correctly: upgrading a cluster whose followers are still behind
    # risks acked writes. Polling for convergence here is the same thing
    # docs/how-to/upgrade-a-cluster.md tells an operator to do after stopping
    # traffic; NOT doing it would let an ordinary lagging follower read as a
    # row-5 FAIL, which would be a harness artifact, not a real one. Bounded:
    # if the cluster genuinely cannot converge with no traffic, the flag day
    # runs anyway and its own abort is the honest answer.
    conv_deadline = time.time() + DRAIN_WAIT_SECS
    while time.time() < conv_deadline:
        ds = [safe_probe(n).get("durable", -1) for n in nodes]
        if ds and min(ds) == max(ds) and min(ds) > 0:
            print(f"INFO durable converged across all nodes at {ds[0]} "
                  f"({DRAIN_WAIT_SECS - int(conv_deadline - time.time())}s after "
                  f"traffic stopped)", flush=True)
            break
        time.sleep(1.0)
    else:
        print(f"INFO durable did NOT converge within {DRAIN_WAIT_SECS}s "
              f"(last read {ds}); running the flag day anyway — its own step-3 "
              f"verify is the honest adjudicator", flush=True)

    pre = [safe_probe(n) for n in nodes]
    pre_commit = max((p.get("commit", 0) for p in pre), default=0)
    pre_durable = [p.get("durable", 0) for p in pre]
    print(f"INFO pre-flag-day: commit high-water {pre_commit}, durable {pre_durable}",
          flush=True)
    if pre_commit == 0:
        return [Verdict("5 flag-day downtime", False,
                        "INCONCLUSIVE — the cluster committed nothing before the flag "
                        "day, so 'no acked loss' would be vacuously true")]

    # -- stage binaries (excluded from the timer, per the bar)
    stamps_before = {}
    for n in nodes:
        n.stage_new_binary()
        stamps_before[n.id] = n.node_bin_stamp()
    print(f"INFO staged {STAGED_BIN} on every host; "
          f"pre-upgrade {NODE_BIN} inode:mtime:ctime {stamps_before}", flush=True)

    # -- THE MEASURED THING
    rc, out = run_flag_day(nodes, ssh_user, ssh_key)
    for line in out.splitlines():
        print(f"  [flagday] {line}", flush=True)

    m = DOWNTIME_RE.search(out)
    downtime = float(m.group(1)) if m else None
    v = VERIFY_RE.search(out)

    # -- post state
    idx2 = wait_leader([n.host for n in nodes], list(range(len(nodes))), LEADER_WAIT_SECS)
    post = [safe_probe(n) for n in nodes]
    post_commit = max((p.get("commit", 0) for p in post), default=0)
    print(f"INFO post-flag-day: commit high-water {post_commit}, "
          f"leader n{nodes[idx2].id if idx2 is not None else '?'}", flush=True)
    stamps_after = {n.id: n.node_bin_stamp() for n in nodes}
    print(f"INFO post-upgrade {NODE_BIN} inode:mtime:ctime {stamps_after}", flush=True)

    # -- liveness: the cluster must actually serve NEW writes afterwards
    for n in nodes:
        n.start_service()
    time.sleep(2.0)
    for n in nodes:
        n.start_load(POST_LOAD_SECS)
    time.sleep(5.0)
    post_rate = 0.0
    if idx2 is not None:
        post_rate = safe_probe(nodes[idx2], rate_secs=RATE_WINDOW_SECS).get("rate") or 0.0
    print(f"INFO post-flag-day commit rate on the leader: {post_rate:.0f} B/s", flush=True)
    final = [safe_probe(n) for n in nodes]
    final_commit = max((p.get("commit", 0) for p in final), default=0)

    # -- the sub-checks; row 5 passes iff every one holds
    checks.append(("script exit 0", rc == 0, f"uc2_flag_day.sh exited {rc}"))
    checks.append((
        f"downtime <= {BAR_DOWNTIME_SECS:.0f}s",
        downtime is not None and downtime <= BAR_DOWNTIME_SECS,
        f"DOWNTIME: {downtime}s" if downtime is not None
        else "no DOWNTIME line in the script's output"))
    checks.append((
        "equal durable across every stopped node",
        v is not None and int(v.group(1)) == len(nodes),
        f"verify OK: all {v.group(1)} node(s) durable={v.group(2)}" if v
        else "the script never printed its step-3 verify line"))
    checks.append((
        "no committed-high-water loss",
        post_commit >= pre_commit,
        f"commit high-water {pre_commit} -> {post_commit} (bar: never backwards)"))
    checks.append((
        "one serving leader afterwards",
        idx2 is not None,
        f"leader n{nodes[idx2].id}" if idx2 is not None
        else f"no single serving leader within {LEADER_WAIT_SECS}s after the flag day"))
    checks.append((
        "the binary was really rewritten on every host",
        all(stamps_before.get(k) and stamps_after.get(k) and
            stamps_before[k] != stamps_after[k] for k in stamps_before),
        f"inode:mtime:ctime {stamps_before} -> {stamps_after}"))
    checks.append((
        "the cluster serves new writes afterwards",
        post_rate > 0.0 and final_commit > post_commit,
        f"post-upgrade rate {post_rate:.0f} B/s, commit {post_commit} -> {final_commit}"))

    for name, ok, detail in checks:
        print(f"  [{'ok ' if ok else 'BAD'}] {name} — {detail}", flush=True)

    passed = all(ok for _, ok, _ in checks)
    summary = "; ".join(f"{name}: {detail}" for name, _, detail in checks)
    return [Verdict("5 flag-day downtime", passed, summary)]


# --------------------------------------------------------------------- setup

def setup_fleet(a):
    hosts = m6.build_fleet_hosts(
        BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=a.nodes,
        ctl_bin=CTL_BIN, unit_prefix="m11", remote_root=REMOTE_ROOT,
    )
    nodes = []
    for i, h in enumerate(hosts):
        h.bind_addr_fixed = f"{h.private_ip}:{PORT}"
        h.prepare(examples=("m9_gate",), bins=("uc_node", "uc2ctl"))
        nodes.append(M11Node(h, i))

    members = [(n.id, n.host.bind_addr_fixed) for n in nodes]
    for n in nodes:
        n.stop_daemon()          # idempotent: a re-run must not inherit a live node
        n.install(members)
    for n in nodes:
        n.start_daemon()
    time.sleep(2.0)
    for n in nodes:
        n.start_service()
    return nodes


def teardown(nodes):
    for n in nodes:
        try:
            n.stop_roles()
        except Exception:
            pass
        try:
            n.stop_daemon()
        except Exception:
            pass


def print_bar(count):
    print("M11 gate row 5 (survivable cluster) — the pre-committed bar:")
    print("  full stop -> verify-equal-durable -> upgrade -> start -> verify-serving")
    print(f"  on a real fleet ({count} hosts): DOWNTIME (last node stopped -> cluster")
    print(f"  serving a single leader) <= {BAR_DOWNTIME_SECS:.0f} s, binaries pre-staged")
    print("  (transfer excluded from the timer), zero acked loss across the flag day.")
    print("  Source: docs/benchmarks/uc2-m11-gate-2026-08-20.md, plan commit 7ff6b4b.")
    print()


def main():
    ap = argparse.ArgumentParser(description="UC v2 M11 flag-day gate driver (row 5)")
    ap.add_argument("--fleet", action="store_true", required=True,
                    help="remote hosts over ssh (there is no local mode: the bar is "
                         "fleet-only, and the local smoke lives in m11_gate flagday-smoke)")
    ap.add_argument("--hosts", default="", help="pub/priv,... (else terraform output)")
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    a = ap.parse_args()

    print_bar(a.nodes)
    nodes = setup_fleet(a)
    try:
        verdicts = run(nodes, a.ssh_user, a.ssh_key)
    finally:
        teardown(nodes)

    print()
    print("M11 flag-day gate — FLEET")
    for v in verdicts:
        print(f"  [{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    if all(v.passed for v in verdicts):
        print("RESULT: PASS — the pre-committed row-5 bar held on the fleet.")
        sys.exit(0)
    print("RESULT: FAIL (honest) — row 5 did not hold.")
    sys.exit(1)


if __name__ == "__main__":
    main()
