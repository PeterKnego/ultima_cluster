#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M12 fleet-gate driver — rows 1, 2 and 3 of
docs/benchmarks/uc2-m12-gate-2026-08-22.md.

    row 1  NETWORK-BUDGET: how much of a fleet box's NIC the consensus
           (inter-node UDP replication) plane taxes while the DIRECT shmem
           client drives the leader up an inflight ladder — with full latency
           percentiles and the NIC bytes/pkts spent PER COMMAND. Reports two
           operating points: the highest inflight that still holds p99 < 1 ms,
           and the peak responses/s point. MEASUREMENT ROW — no bar.  FLEET ONLY
    row 2  gateway (Edge + RemoteClient) throughput vs direct `Engine`
           throughput at EQUAL inflight.  BAR: ratio >= 0.8.       FLEET ONLY
    row 3  codec share on the apply thread at the M5 ladder, typed vs raw
           state-machine tier.  MEASUREMENT ROW — no bar; it states two
           numbers.                                               FLEET ONLY
    edgesat  N-CLIENT EDGE SATURATION: aggregate gateway throughput as
           CONCURRENT client connections scale (1,2,4,8,16 by default), all
           into the leader's edge, with CPU attribution per rung — the edge
           process (in percent of ONE core), the leader host and every client
           host — so the flattening can be blamed on the right stage. Reports
           the knee, the attributed ceiling, and aggregate-at-ceiling over the
           measured direct-arm peak ("edge saturation ratio"). Grounds a
           re-spec of row 2, whose ratio is per-CONNECTION rather than
           aggregate. MEASUREMENT ROW — no bar.                   FLEET ONLY

ROW 1 (path 1 of the network-budget investigation) answers: at ~1.5M/s, can a
network fluke break p99 < 1 ms, and is there NIC headroom for a co-located TCP
client? It reuses row 2's cluster bring-up verbatim (three hosts = node +
service, typed CountSm, NO edge for the baseline sweep), drives
`m12_gate client-direct --envelope off` on the LEADER host over shmem at each
inflight in a ladder (default 1,16,64,256,1024,4096), and while each point
runs it samples the leader's primary-ENI `/proc/net/dev` counters (~1 Hz) to
derive steady-state rx/tx bytes/s and pkts/s. Dividing the tx rate by
responses/s gives the replication cost PER COMMAND — the headline number. The
optional `--with-remote-load` tail (default on) then adds a concurrent TCP
`client-remote` on the fourth host and re-measures the p99<1ms point to see
whether the extra client pushes the box's NIC over.

WHY A FLEET RUN AT ALL (i.e. what the local smoke could not do):

  `uc2_gateway/examples/m12_gate.rs`'s in-process arms build BOTH three-node
  clusters inside ONE process on loopback, one arm after the other. On the
  4-vCPU dev box each arm already oversubscribes the box on its own, and the
  gateway arm adds three edges, a reader thread and a waiter pool on top of
  that — so the 0.073 ratio in the gate doc's "Local smoke numbers" is two
  separate oversubscription stories, not a measurement of the edge. The gate
  doc says so explicitly and defers the real number to a fleet run.

  This driver removes that confound the only way it can be removed: ONE
  PROCESS PER ROLE PER HOST. Three hosts each run exactly one node + one
  service (+ one edge, only while the gateway arm is running); the direct
  client runs on the leader's host (it MUST — it attaches over shmem); the
  remote client runs on a fourth host that carries no cluster role at all.

WHAT MAKES THE RATIO APPLES-TO-APPLES (each of these is deliberate):

  * ONE CLUSTER GENERATION PER CYCLE. Both arms of a cycle are measured
    against the same live cluster, same hosts, same leader, direct first and
    then remote, without restarting anything in between. The in-process smoke
    booted a SEPARATE cluster per arm; this holds hardware and leadership
    constant, which is the difference between measuring the edge and measuring
    two boot-to-boot samples.
  * IDENTICAL FRAMES. With `--envelope on` (the default) the service is
    `Sessioned<CountSm>`, the edge prepends the 16-byte `client_id ++ seq`
    header for the remote arm, and `client-direct` prepends the SAME header
    itself. Neither arm pays for an envelope the other skipped.
  * EQUAL INFLIGHT. `--inflight` is the client's cap, the edge's
    `max_inflight` AND its `per_conn_inflight`, exactly as the in-process arm
    wires them.
  * EDGES ONLY EXIST DURING THE GATEWAY ARM. They are started after the
    direct arm's measurement and stopped after the remote arm's, so the direct
    arm never shares a core with an idle acceptor/driver thread pair.

WHAT THE RATIO STILL INCLUDES, and must (gate doc, "Facts the gate must
state" (d)): the single driver thread per edge serialises outbound writes, and
`RemoteClient` funnels every submit/response through one `Mutex<State>`. Both
are documented single-writer constraints of the shipped design. They are part
of the cost this row measures, not artifacts to be tuned away before
reporting.

ONE ASYMMETRY THAT CANNOT BE REMOVED, stated rather than hidden: the direct
client is CO-LOCATED with the leader's node and service (shmem attachment is
not optional), while the remote client has a host to itself. That favours the
GATEWAY arm if anything — the direct arm is the one sharing cores — so it
cannot manufacture a passing ratio.

ROW 3 is a different measurement and gets its own cluster: the whole tree is
rebuilt with `--features uc2_service/apply-profile`, the M5-ladder payload
(509 B — the largest raw payload whose bincode encoding lands exactly on the
node's 512 B `max_payload` door) is driven by `client-direct`, and the
service's own `apply-profile[...]` line is read back off its unit log for the
typed tier and again for the raw tier (`--raw-sm`). Row 3 runs with
`--envelope off`: 509 B encodes to 512 B and the 16-byte session envelope
would not fit under `max_payload`. The apply-profile counters print every
1,000,000 applied frames, so a run that never reaches a million frames
reports no line — that is a "not measured", never a zero.

Exit 0 iff row 2's bar holds (row 3 states numbers and never fails the run).
A green terminal is not a PASS; the exit code is.
"""

import argparse
import json
import re
import shlex
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402

# SshHost.probe() reads the module-global APP when it builds the probe command
# line, so it must name THIS gate's app id before any host is constructed
# (m9/m11_fleet_gate do the same).
m6.APP = "m12-gate"

from m6_fleet_gate import wait_leader  # noqa: E402

APP = "m12-gate"
PORT = 19100          # node-to-node UDP (private NIC)
EDGE_PORT = 9300      # gateway edge TCP (private NIC)
REMOTE_ROOT = "/opt/bench/m12"
UNIT_PREFIX = "m12"

# Built on the fleet hosts by prepare_host(). m12_gate carries every role this
# gate drives; m6_gate is used ONLY for its `probe` role (the cnc reader
# wait_leader() calls), which m12_gate deliberately does not duplicate.
BUILT_GATE = "/opt/bench/uc/target/release/examples/m12_gate"
BUILT_PROBE = "/opt/bench/uc/target/release/examples/m6_gate"

# The bar (pre-committed in the gate doc's row 2; changing this number without
# changing that document, in a commit that says why, is exactly what the
# honest-failure protocol exists to prevent).
BAR_RATIO = 0.8

# Row 3's payload: see the module doc. `m5_gate`'s own constant comment spells
# out the arithmetic (509 raw -> 512 encoded == max_payload).
ROW3_PAYLOAD = 509

LEADER_WAIT_SECS = 90
CLIENT_SLACK_SECS = 150   # ssh + attach + leader wait + drain, on top of --secs
EDGE_SETTLE_SECS = 2.0
BOOT_SETTLE_SECS = 3.0

RESULT_RE = re.compile(r"^RESULT\s+(\{.*\})\s*$", re.M)
PROFILE_RE = re.compile(
    r"apply-profile\[[^\]]*\].*?sm_apply=([0-9.]+)ns.*?"
    r"sm_apply/apply_cycle_total=([0-9.]+)%"
)
PROFILE_FRAMES_RE = re.compile(r"apply-profile\[[^\]]*\]\s+frames=(\d+)")


# ------------------------------------------------------------- ssh plumbing
#
# Every remote command this driver issues is printed before it runs (the m6
# convention): a fleet run that goes wrong must be reproducible by hand from
# the transcript alone.

def ssh(host, cmd, timeout=None, label="ssh"):
    print(f"INFO [{label} {host.public_ip}] {cmd}", flush=True)
    kw = {"capture_output": True}
    if timeout is not None:
        kw["timeout"] = timeout
    return host._ssh(cmd, **kw)


def unit_log(host, unit):
    return f"/opt/bench/{UNIT_PREFIX}-{unit}.log"


def unit_start_cmd(host, unit, args, nofile=False, cpus=None):
    """The `systemd-run` command line for one unit (no ssh — see `start_unit`
    for the single-unit path and `start_units_batch` for the N-at-once one).

    `cpus` (e.g. "0,1,4,5"), when set, adds `-p CPUAffinity={cpus} ` right
    after the `LimitNOFILE` flag position — whether or not `nofile` is set —
    pinning the unit to those logical CPUs (see `PIN_MAP_C6ID_2XL` and
    `--pin` in m14_fleet_gate.py / m14_ab_27_vs_28.py). `cpus=None` (the
    default) leaves the line byte-identical to before this parameter
    existed."""
    quoted = " ".join(shlex.quote(a) for a in args)
    limit = "-p LimitNOFILE=65536 " if nofile else ""
    affinity = f"-p CPUAffinity={cpus} " if cpus else ""
    return (
        f"sudo systemd-run --unit={UNIT_PREFIX}-{unit} --collect -p TimeoutStopSec=1 "
        f"{limit}"
        f"{affinity}"
        f"-p StandardOutput=append:{unit_log(host, unit)} "
        f"-p StandardError=append:{unit_log(host, unit)} "
        f"{host.gate} {quoted}"
    )


def start_unit(host, unit, args, nofile=False, cpus=None):
    """A transient `systemd-run --collect` unit running the m12_gate binary.

    `nofile` mirrors packaging/systemd/uc2-node.service's LimitNOFILE=65536 and
    is set for NODE units: the journal holds an fd per segment, and systemd's
    default soft limit of 1024 is what turned an earlier fleet run's small
    segments into EMFILE fail-stops (m10_fleet_gate's comment on the same
    line).

    `cpus` is forwarded to `unit_start_cmd` (see its docstring); `None` (the
    default) pins nothing, unchanged from before `--pin` existed."""
    kill_unit(host, unit)
    cmd = unit_start_cmd(host, unit, args, nofile=nofile, cpus=cpus)
    r = ssh(host, cmd, label="systemd-run")
    if r.returncode != 0:
        raise RuntimeError(
            f"start {UNIT_PREFIX}-{unit} on {host.public_ip}: {r.stderr or r.stdout}"
        )


def start_units_batch(host, specs, cpus=None):
    """Start several units on ONE host in ONE ssh round trip.

    The edge-saturation row starts N client units that must all be loading the
    same edge at the same time; N separate ssh invocations would stagger their
    starts by N x (ssh handshake + systemd-run), which at N=16 is seconds of
    skew against a 15 s window. `systemd-run` returns as soon as the transient
    unit is queued, so issuing all N from a single remote shell puts the skew
    in the tens of milliseconds instead.

    Each unit's log file is removed first: the units write with
    `StandardOutput=append:`, and a leftover log from an earlier ladder point
    would let that point's RESULT line be re-read as this one's.

    `cpus`, when set, pins every unit in this batch to the same CPUs (all
    edgesat client units share one role); `None` (the default) pins nothing.

    Returns the list of units that reported STARTED."""
    parts = []
    for unit, args in specs:
        parts.append(f"sudo systemctl reset-failed {UNIT_PREFIX}-{unit} 2>/dev/null; true")
        parts.append(f"sudo rm -f {unit_log(host, unit)}")
        parts.append(
            f"{unit_start_cmd(host, unit, args, cpus=cpus)} "
            f"&& echo STARTED:{unit} || echo FAILED:{unit}"
        )
    r = ssh(host, "; ".join(parts), label="systemd-run")
    out = (r.stdout or "") + (r.stderr or "")
    started = [u for u, _ in specs if f"STARTED:{u}" in out]
    failed = [u for u, _ in specs if u not in started]
    if failed:
        print(f"INFO [{host.public_ip}] units failed to start: {failed}\n{out}",
              flush=True)
    return started


def kill_unit(host, unit):
    ssh(
        host,
        f"sudo systemctl kill --signal=SIGKILL {UNIT_PREFIX}-{unit} 2>/dev/null; "
        f"sudo systemctl stop {UNIT_PREFIX}-{unit} 2>/dev/null; "
        f"sudo systemctl reset-failed {UNIT_PREFIX}-{unit} 2>/dev/null; true",
        label="systemctl",
    )


def kill_units_batch(host, units):
    """`kill_unit` for several units on one host in ONE ssh round trip — the
    edge-saturation row tears down up to 16 client units per rung."""
    parts = []
    for u in units:
        parts.append(
            f"sudo systemctl kill --signal=SIGKILL {UNIT_PREFIX}-{u} 2>/dev/null; "
            f"sudo systemctl stop {UNIT_PREFIX}-{u} 2>/dev/null; "
            f"sudo systemctl reset-failed {UNIT_PREFIX}-{u} 2>/dev/null; true"
        )
    if parts:
        ssh(host, "; ".join(parts), label="systemctl")


def truncate_log(host, unit):
    """Row 3 reads a counter line out of a unit log; an append-log carried over
    from an earlier phase would let the TYPED number be re-read as the RAW
    one."""
    ssh(host, f"sudo rm -f {unit_log(host, unit)}", label="ssh")


def tail_log(host, unit, lines=200):
    r = ssh(host, f"sudo tail -n {lines} {unit_log(host, unit)} 2>/dev/null",
            label="ssh")
    return r.stdout or ""


def run_foreground(host, args, timeout, cpus=None):
    """A BLOCKING ssh command (not systemd-run): the client roles run for
    `--secs` and exit, and their stdout IS the measurement.

    `cpus` (e.g. "3,7"), when set, pins the process via `taskset -c` — there
    is no systemd transient unit here to carry `-p CPUAffinity=`, since this
    path runs the client in the foreground and blocks on its exit. `None`
    (the default) pins nothing."""
    quoted = " ".join(shlex.quote(a) for a in args)
    prefix = f"taskset -c {cpus} " if cpus else ""
    cmd = f"sudo {prefix}{host.gate} {quoted}"
    print(f"INFO [ssh {host.public_ip}] {cmd}", flush=True)
    try:
        r = host._ssh(cmd, capture_output=True, timeout=timeout)
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    except Exception as e:  # subprocess.TimeoutExpired and friends
        out = getattr(e, "stdout", "") or ""
        err = getattr(e, "stderr", "") or ""
        if isinstance(out, bytes):
            out = out.decode(errors="replace")
        if isinstance(err, bytes):
            err = err.decode(errors="replace")
        return 124, out + err + f"\n[orchestrator] client failed/timed out: {e}\n"


def prepare_host(host, apply_profile=False):
    """Build BOTH gate binaries on the host and assert the instance-dir parent
    is on a durable filesystem.

    m6's own `SshHost.prepare` cannot serve here: it hardcodes `-p uc2_node`,
    and `m12_gate` is an example of `uc2_gateway`. The rest (root cargo env,
    the FSTYPE assertion) is the same shape, deliberately.

    Order matters. `m6_gate` is built FIRST and `m12_gate` last, because the
    two builds resolve `uc2_service`'s features differently when
    `apply_profile` is set — building m6_gate afterwards would rebuild
    uc2_service without the feature. Only m12_gate's linked binary needs the
    feature, and it is the one written last."""
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    cargo = m6.SshHost.CARGO
    src = m6.SshHost.UC_SRC
    feat = " --features uc2_service/apply-profile" if apply_profile else ""
    cmd = (
        f"{env} {cargo} build --release --manifest-path {src}/Cargo.toml "
        f"-p uc2_node --example m6_gate "
        f"&& {env} {cargo} build --release --manifest-path {src}/Cargo.toml "
        f"-p uc2_gateway --example m12_gate{feat} "
        f"&& sudo mkdir -p {REMOTE_ROOT} "
        f"&& echo FSTYPE=$(stat -f -c %T {REMOTE_ROOT}) && echo PREPARED"
    )
    r = ssh(host, cmd, label="build")
    out = r.stdout or ""
    if "PREPARED" not in out:
        raise RuntimeError(f"prepare {host.public_ip} failed: {r.stderr or out}")
    fstype = next(
        (l.split("=", 1)[1] for l in out.splitlines() if l.startswith("FSTYPE=")), ""
    )
    m6.assert_durable_fs(fstype, f"{REMOTE_ROOT} (instance-dir parent)", host.public_ip)


# --------------------------------------------------------------- CPU pinning
#
# The 2026-08-30 A/B (docs/benchmarks/uc2-m14d-ab-2.7.0-vs-2.8.0-2026-08-30.md)
# found the SAME binary landing in per-generation rate modes 25% apart on
# this rig's 8-vCPU `c6id.2xlarge` hosts; the leading hypothesis is thread
# placement drifting onto a hyperthread sibling of one of the node's four
# busy-spin polling agents (consensus/sender/receiver/archive). `--pin` (in
# m14_fleet_gate.py and m14_ab_27_vs_28.py) is the mitigation under test: pin
# every role to disjoint physical cores so a generation's placement can't
# collide with itself.
#
# ASSUMED sibling layout for `c6id.2xlarge` (8 vCPU = 4 physical cores x 2
# SMT threads): logical CPU `i` and `i+4` are the two threads of one
# physical core, i.e. siblings are (0,4) (1,5) (2,6) (3,7). This has NOT
# been verified on a real host yet — Task 9 Step 2's validation run must run
# `lscpu -p=CPU,CORE` on a host FIRST and record the actual layout;
# `verify_pin_layout`/`require_pin_layout` below refuse to proceed if the
# assumption doesn't hold, so a wrong-layout host family fails closed
# instead of pinning onto siblings silently.
#
# The map: the node's four busy-spin agents get cores 0 and 1, BOTH threads
# of each (0,1,4,5) — two whole physical cores, so no agent shares a
# physical core with another agent or with anything else. Each service gets
# one thread of a third core (service0 on cpu 2, service1 on cpu 6 — its
# sibling, left idle on purpose so a second FSM never shares a physical core
# with the first); M14 allows up to 8 FSMs, and a service id >= 2 has no
# dedicated pin here — callers share it onto service1's thread (cpu 6; see
# m14_fleet_gate.py's `service_cpu`). client/edge share the fourth core's
# two threads (3,7) — on the rows this rig drives they are never both live
# at once (an edge unit only exists during a gateway arm, started after the
# direct client's own measurement has already stopped), so sharing costs
# nothing.
PIN_MAP_C6ID_2XL = {
    "node": "0,1,4,5",
    "service0": "2",
    "service1": "6",
    "client": "3,7",
    "edge": "3,7",
}

EXPECTED_SIBLING_PAIRS = {(0, 4), (1, 5), (2, 6), (3, 7)}


def sibling_pairs(lscpu_text):
    """Parse `lscpu -p=CPU,CORE` output into the set of hyperthread sibling
    pairs {(lo, hi), ...} — two logical CPUs sharing one physical CORE id.
    Pure (no ssh), so `--selftest` can pin it against canned text. Comment
    lines (`lscpu -p` prefixes them with '#') and blank lines are skipped."""
    by_core = {}
    for line in lscpu_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) < 2:
            continue
        cpu, core = int(fields[0]), int(fields[1])
        by_core.setdefault(core, []).append(cpu)
    pairs = set()
    for cpus in by_core.values():
        cpus = sorted(cpus)
        if len(cpus) == 2:
            pairs.add((cpus[0], cpus[1]))
    return pairs


def verify_pin_layout(host):
    """ssh `lscpu -p=CPU,CORE` on `host` and return True iff its hyperthread
    sibling pairs are EXACTLY `EXPECTED_SIBLING_PAIRS` — the assumption
    `PIN_MAP_C6ID_2XL` is built on (see the comment above it). Prints the
    actual layout on a mismatch so the refusal is diagnosable without a
    second ssh."""
    r = ssh(host, "lscpu -p=CPU,CORE", label="lscpu")
    pairs = sibling_pairs(r.stdout or "")
    if pairs != EXPECTED_SIBLING_PAIRS:
        print(f"WARN [{host.public_ip}] sibling layout {sorted(pairs)} != "
              f"expected {sorted(EXPECTED_SIBLING_PAIRS)} — PIN_MAP_C6ID_2XL's "
              f"assumption does not hold on this host", flush=True)
        return False
    return True


def require_pin_layout(hosts):
    """`verify_pin_layout` on every host in `hosts`; SystemExit (naming the
    offending hosts — the WARN lines above already printed the actual
    layout) if any mismatches. This is `--pin`'s gate in both m14 drivers,
    called on every voter before the first arm, so a wrong-layout host
    family never runs pinned silently."""
    bad = [h for h in hosts if not verify_pin_layout(h)]
    if bad:
        raise SystemExit(
            f"--pin refused: sibling layout mismatch on "
            f"{[h.public_ip for h in bad]} (see the WARN line(s) above for "
            f"the actual layout); PIN_MAP_C6ID_2XL assumes "
            f"{sorted(EXPECTED_SIBLING_PAIRS)}"
        )


# ------------------------------------------------------------------ cluster

def members_str(node_hosts):
    return ",".join(f"{i}@{h.private_ip}:{PORT}" for i, h in enumerate(node_hosts))


def edge_members_str(node_hosts):
    """The node-id -> EDGE address map (what a client dials), which is a
    different address family from the UDP `bind` map above — hence the
    separate flag on the edge role."""
    return ",".join(f"{i}@{h.private_ip}:{EDGE_PORT}" for i, h in enumerate(node_hosts))


def wipe_dirs(node_hosts):
    for h in node_hosts:
        ssh(h, f"sudo rm -rf {h.dir} && sudo mkdir -p {h.dir}", label="ssh")


def start_cluster(node_hosts, a, envelope, raw_sm=False, pins=None):
    """`pins`, when given, is a role -> CPU-list dict (see
    `PIN_MAP_C6ID_2XL`); this single-FSM cluster has one service unit, pinned
    to the map's `service0` entry. `None` (the default) pins nothing —
    byte-identical to before `pins` existed."""
    pins = pins or {}
    ms = members_str(node_hosts)
    for i, h in enumerate(node_hosts):
        start_unit(h, "node", [
            "node", "--id", str(i), "--bind", f"{h.private_ip}:{PORT}",
            "--instance-dir", h.dir, "--members", ms, "--app-id", APP,
            "--admission-kib", str(a.admission_kib),
        ], nofile=True, cpus=pins.get("node"))
    time.sleep(BOOT_SETTLE_SECS)
    for h in node_hosts:
        args = ["service", "--instance-dir", h.dir, "--app-id", APP,
                "--envelope", envelope]
        if raw_sm:
            args.append("--raw-sm")
        truncate_log(h, "service")
        start_unit(h, "service", args, cpus=pins.get("service0"))
    time.sleep(BOOT_SETTLE_SECS)


def stop_cluster(node_hosts):
    for h in node_hosts:
        for u in ("edge", "service", "node"):
            kill_unit(h, u)


def start_edges(node_hosts, a, envelope, inflight=None, pins=None):
    """`inflight` overrides `a.inflight` for the edge's `max_inflight` /
    `per_conn_inflight` pair. Rows 1-3 leave it None (the whole point of row 2
    is EQUAL inflight on both arms); the edge-saturation row overrides it, for
    the reason spelled out on `--edgesat-edge-inflight`.

    `pins` (role -> CPU-list dict, e.g. `PIN_MAP_C6ID_2XL`), when given, pins
    the edge unit to its `edge` entry; `None` (the default) pins nothing."""
    em = edge_members_str(node_hosts)
    infl = a.inflight if inflight is None else inflight
    edge_cpus = (pins or {}).get("edge")
    for i, h in enumerate(node_hosts):
        start_unit(h, "edge", [
            "edge", "--instance-dir", h.dir, "--app-id", APP,
            "--listen", f"{h.private_ip}:{EDGE_PORT}",
            "--members", em, "--envelope", envelope,
            "--inflight", str(infl),
        ], cpus=edge_cpus)
    time.sleep(EDGE_SETTLE_SECS)


def stop_edges(node_hosts):
    for h in node_hosts:
        kill_unit(h, "edge")


# ------------------------------------------------------------------- arms

def parse_result(out, arm):
    """Pull the single machine-readable RESULT line out of a client role's
    output. Returns the dict, or None (with the output already echoed by the
    caller) if the role never printed one — which means it died, not that it
    measured zero."""
    hits = RESULT_RE.findall(out)
    for h in hits:
        try:
            d = json.loads(h)
        except json.JSONDecodeError:
            continue
        if d.get("arm") == arm:
            return d
    return None


def echo(prefix, out, lines=40):
    tail = out.strip().splitlines()[-lines:]
    for l in tail:
        print(f"  [{prefix}] {l}", flush=True)


def run_direct_arm(node_hosts, leader, a, envelope, payload=None, secs=None, cpus=None):
    """`cpus` pins the direct client via `run_foreground`'s `taskset`; `None`
    (the default) pins nothing."""
    payload = a.payload if payload is None else payload
    secs = a.secs if secs is None else secs
    h = node_hosts[leader]
    rc, out = run_foreground(h, [
        "client-direct", "--instance-dir", h.dir, "--app-id", APP,
        "--secs", str(secs), "--payload", str(payload),
        "--inflight", str(a.inflight), "--envelope", envelope,
    ], timeout=secs + CLIENT_SLACK_SECS, cpus=cpus)
    echo("direct", out)
    d = parse_result(out, "direct")
    if d is None:
        print(f"INFO direct arm produced no RESULT line (rc={rc})", flush=True)
    return d


def run_gateway_arm(node_hosts, client_host, leader, a, cpus=None):
    """`cpus` pins the remote client via `run_foreground`'s `taskset`; `None`
    (the default) pins nothing."""
    rc, out = run_foreground(client_host, [
        "client-remote",
        "--gateways", f"{node_hosts[leader].private_ip}:{EDGE_PORT}",
        "--app-id", APP, "--secs", str(a.secs), "--payload", str(a.payload),
        "--inflight", str(a.inflight),
    ], timeout=a.secs + CLIENT_SLACK_SECS, cpus=cpus)
    echo("gateway", out)
    d = parse_result(out, "gateway")
    if d is None:
        print(f"INFO gateway arm produced no RESULT line (rc={rc})", flush=True)
    return d


# -------------------------------------------------------------------- row 2

class Verdict:
    def __init__(self, row, passed, detail):
        self.row, self.passed, self.detail = row, passed, detail


def row2(node_hosts, client_host, a):
    envelope = a.envelope
    print(f"INFO ROW 2: {a.cycles} cycle(s), envelope={envelope}, "
          f"secs={a.secs}, payload={a.payload}, inflight={a.inflight}", flush=True)
    wipe_dirs(node_hosts)
    start_cluster(node_hosts, a, envelope)

    pairs = []
    try:
        for cycle in range(1, a.cycles + 1):
            print(f"\nINFO ===== row 2 cycle {cycle}/{a.cycles} =====", flush=True)
            leader = wait_leader(node_hosts, list(range(len(node_hosts))),
                                 LEADER_WAIT_SECS)
            if leader is None:
                print(f"INFO no single serving leader within {LEADER_WAIT_SECS}s; "
                      f"skipping cycle {cycle}", flush=True)
                continue
            print(f"INFO leader is n{leader} ({node_hosts[leader].public_ip})",
                  flush=True)

            # --- arm A: direct (no edge process exists yet this cycle)
            d = run_direct_arm(node_hosts, leader, a, envelope)

            # --- arm B: gateway, SAME cluster generation, same hosts
            start_edges(node_hosts, a, envelope)
            leader2 = wait_leader(node_hosts, list(range(len(node_hosts))),
                                  LEADER_WAIT_SECS)
            if leader2 is None:
                print("INFO lost the leader between the two arms; "
                      "discarding this cycle", flush=True)
                stop_edges(node_hosts)
                continue
            if leader2 != leader:
                print(f"INFO leadership moved n{leader} -> n{leader2} between the "
                      f"arms; the pair is no longer same-leader, discarding",
                      flush=True)
                stop_edges(node_hosts)
                continue
            g = run_gateway_arm(node_hosts, client_host, leader2, a)
            stop_edges(node_hosts)

            if not d or not g or not d.get("responses_per_sec"):
                print(f"INFO cycle {cycle} produced no usable pair; discarding",
                      flush=True)
                continue
            ratio = g["responses_per_sec"] / d["responses_per_sec"]
            pairs.append({
                "cycle": cycle, "leader": leader,
                "direct_rps": d["responses_per_sec"],
                "gateway_rps": g["responses_per_sec"],
                "ratio": ratio,
                "direct_p50_ms": d["p50_ms"], "gateway_p50_ms": g["p50_ms"],
                "direct_p99_ms": d["p99_ms"], "gateway_p99_ms": g["p99_ms"],
                "direct_lost": d["lost"], "gateway_lost": g["lost"],
            })
            print(f"INFO cycle {cycle}: direct {d['responses_per_sec']:.0f}/s, "
                  f"gateway {g['responses_per_sec']:.0f}/s, ratio {ratio:.3f}",
                  flush=True)
    finally:
        stop_cluster(node_hosts)

    print("\nROW 2 — gateway vs direct at equal inflight")
    for p in pairs:
        print(f"  cycle {p['cycle']} (leader n{p['leader']}): "
              f"direct {p['direct_rps']:.0f}/s p50 {p['direct_p50_ms']:.3f} ms | "
              f"gateway {p['gateway_rps']:.0f}/s p50 {p['gateway_p50_ms']:.3f} ms | "
              f"ratio {p['ratio']:.3f} | lost {p['direct_lost']}/{p['gateway_lost']}")
    if not pairs:
        return Verdict("2 gateway throughput cost", False,
                       "no usable arm pair was measured — nothing to adjudicate")
    med = statistics.median(p["ratio"] for p in pairs)
    lost = sum(p["direct_lost"] + p["gateway_lost"] for p in pairs)
    print(f"  median ratio over {len(pairs)} cycle(s): {med:.3f} (bar >= {BAR_RATIO})")
    print("  MEASURED-PAIRS-JSON " + json.dumps(pairs))
    passed = med >= BAR_RATIO and lost == 0
    return Verdict(
        "2 gateway throughput cost",
        passed,
        f"median gateway/direct responses/s ratio {med:.3f} over {len(pairs)} "
        f"cycle(s) (bar >= {BAR_RATIO}); lost responses across both arms {lost} "
        f"(bar 0)",
    )


# -------------------------------------------------------------------- row 3

def row3_phase(node_hosts, a, raw_sm):
    """One tier's measurement: fresh cluster, M5-ladder load from the leader's
    own host, then read the service's apply-profile line back off its log."""
    tier = "raw (RawCountSm)" if raw_sm else "typed (CountSm)"
    print(f"\nINFO ROW 3 phase: {tier}, payload {ROW3_PAYLOAD}, "
          f"secs {a.row3_secs}, envelope off", flush=True)
    wipe_dirs(node_hosts)
    start_cluster(node_hosts, a, "off", raw_sm=raw_sm)
    try:
        leader = wait_leader(node_hosts, list(range(len(node_hosts))),
                             LEADER_WAIT_SECS)
        if leader is None:
            return {"tier": tier, "error": f"no leader within {LEADER_WAIT_SECS}s"}
        print(f"INFO leader is n{leader}", flush=True)
        d = run_direct_arm(node_hosts, leader, a, "off",
                           payload=ROW3_PAYLOAD, secs=a.row3_secs)
        log = tail_log(node_hosts[leader], "service", lines=400)
        echo("service-log", log, lines=12)
        m = None
        for m in PROFILE_RE.finditer(log):
            pass  # keep the LAST (largest-sample) periodic report
        frames = PROFILE_FRAMES_RE.findall(log)
        if m is None:
            return {
                "tier": tier,
                "responses_per_sec": (d or {}).get("responses_per_sec"),
                "error": "no apply-profile line in the leader's service log — the "
                         "counters print every 1,000,000 applied frames, so either "
                         "the run never reached a million frames or the binary was "
                         "not built with --features uc2_service/apply-profile",
            }
        return {
            "tier": tier,
            "sm_apply_ns": float(m.group(1)),
            "sm_apply_pct_of_apply_cycle": float(m.group(2)),
            "frames": int(frames[-1]) if frames else None,
            "responses_per_sec": (d or {}).get("responses_per_sec"),
        }
    finally:
        stop_cluster(node_hosts)


def row3(node_hosts, client_host, a):
    print("\nINFO ROW 3: rebuilding with --features uc2_service/apply-profile",
          flush=True)
    for h in list(node_hosts) + [client_host]:
        prepare_host(h, apply_profile=True)
    typed = row3_phase(node_hosts, a, raw_sm=False)
    raw = row3_phase(node_hosts, a, raw_sm=True)

    print("\nROW 3 — codec share on the apply thread (M5 ladder, payload "
          f"{ROW3_PAYLOAD})")
    for r in (typed, raw):
        if "error" in r:
            print(f"  {r['tier']}: NOT MEASURED — {r['error']}")
        else:
            print(f"  {r['tier']}: sm_apply={r['sm_apply_ns']:.0f} ns/frame "
                  f"({r['sm_apply_pct_of_apply_cycle']:.1f}% of apply_cycle_total) "
                  f"over {r['frames']} frames, "
                  f"{(r['responses_per_sec'] or 0):.0f} responses/s")
    print("  ROW3-JSON " + json.dumps({"typed": typed, "raw": raw}))
    ok = "error" not in typed and "error" not in raw
    if ok:
        detail = (f"typed sm_apply={typed['sm_apply_ns']:.0f} ns "
                  f"({typed['sm_apply_pct_of_apply_cycle']:.1f}%) vs "
                  f"raw sm_apply={raw['sm_apply_ns']:.0f} ns "
                  f"({raw['sm_apply_pct_of_apply_cycle']:.1f}%)")
    else:
        detail = ("one or both tiers produced no apply-profile line: "
                  f"typed={typed.get('error', 'ok')}; raw={raw.get('error', 'ok')}")
    # Row 3 is a MEASUREMENT row: it states numbers and has no bar, so it never
    # decides the run's exit code (see `main`).
    return Verdict("3 codec share on the apply thread (measurement, no bar)",
                   ok, detail)


# --------------------------------------------------------------- row 1 (netbudget)
#
# Everything below is pure where it can be (parse_proc_net_dev, nic_rate,
# steady_rates, select_operating_points) so the parsing and the p99<1ms
# selection are unit-testable off-fleet; only the sampler and the row driver
# touch ssh.

DEFAULT_NETBUDGET_INFLIGHTS = "1,16,64,256,1024,4096"
P99_MS_BUDGET = 1.0   # the "network-budget" operating point: highest inflight with p99 below this


def detect_iface(host):
    """The host's primary ENI: the interface the default route uses (what
    node<->node UDP replication egresses on). Falls back to the first non-lo
    interface in operstate `up`. Never hardcoded — a fleet box may be ens5,
    eth0, enp… ."""
    cmd = (
        "IFACE=$(ip -o route get 8.8.8.8 2>/dev/null | "
        "sed -n 's/.* dev \\([^ ]*\\).*/\\1/p' | head -n1); "
        "if [ -z \"$IFACE\" ]; then "
        "for i in $(ls /sys/class/net 2>/dev/null | grep -v '^lo$'); do "
        "st=$(cat /sys/class/net/$i/operstate 2>/dev/null); "
        "if [ \"$st\" = up ]; then IFACE=$i; break; fi; done; fi; "
        "echo IFACE=$IFACE"
    )
    r = ssh(host, cmd, label="iface")
    out = r.stdout or ""
    for l in out.splitlines():
        if l.startswith("IFACE=") and len(l.strip()) > len("IFACE="):
            return l.strip()[len("IFACE="):]
    raise RuntimeError(f"could not detect a primary NIC on {host.public_ip}: {out!r}")


def parse_proc_net_dev(text, iface):
    """Pull (rx_bytes, rx_pkts, tx_bytes, tx_pkts) for `iface` out of a raw
    /proc/net/dev dump. Field order (kernel-stable): after the `iface:` label
    come rx_bytes rx_packets rx_errs rx_drop rx_fifo rx_frame rx_compressed
    rx_multicast tx_bytes tx_packets … so rx_bytes=f[0], rx_packets=f[1],
    tx_bytes=f[8], tx_packets=f[9]."""
    for line in text.splitlines():
        line = line.strip()
        if ":" not in line:
            continue
        name, rest = line.split(":", 1)
        if name.strip() != iface:
            continue
        f = rest.split()
        if len(f) < 10:
            raise ValueError(f"short /proc/net/dev line for {iface}: {line!r}")
        return {
            "rx_bytes": int(f[0]), "rx_pkts": int(f[1]),
            "tx_bytes": int(f[8]), "tx_pkts": int(f[9]),
        }
    raise ValueError(f"interface {iface!r} not found in /proc/net/dev")


def nic_rate(sample_a, sample_b):
    """Per-second deltas between two (timestamp, counters) samples. None if the
    clock did not advance (or went backwards) between them."""
    ta, ca = sample_a
    tb, cb = sample_b
    dt = tb - ta
    if dt <= 0:
        return None
    return {
        "rx_bytes_per_sec": (cb["rx_bytes"] - ca["rx_bytes"]) / dt,
        "tx_bytes_per_sec": (cb["tx_bytes"] - ca["tx_bytes"]) / dt,
        "rx_pkts_per_sec": (cb["rx_pkts"] - ca["rx_pkts"]) / dt,
        "tx_pkts_per_sec": (cb["tx_pkts"] - ca["tx_pkts"]) / dt,
    }


def steady_rates(samples):
    """Median per-second rate across the consecutive-sample intervals. When
    there are >= 4 intervals the first and last are trimmed to drop the client
    ramp-up and drain edges (the sends do not start the instant the ssh lands).
    Returns None if fewer than two usable samples."""
    if len(samples) < 2:
        return None
    intervals = [nic_rate(samples[i], samples[i + 1]) for i in range(len(samples) - 1)]
    intervals = [r for r in intervals if r is not None]
    if not intervals:
        return None
    if len(intervals) >= 4:
        intervals = intervals[1:-1]
    keys = ("rx_bytes_per_sec", "tx_bytes_per_sec", "rx_pkts_per_sec", "tx_pkts_per_sec")
    return {k: statistics.median(r[k] for r in intervals) for k in keys}


def select_operating_points(points):
    """(a) the highest-inflight point that still holds p99 < 1 ms, and
    (b) the peak responses/s point. Either may be None if no point qualifies."""
    valid = [p for p in points
             if p.get("resp_per_sec") and p.get("p99_ms") is not None]
    p99ok = [p for p in valid if p["p99_ms"] < P99_MS_BUDGET]
    budget = max(p99ok, key=lambda p: p["inflight"]) if p99ok else None
    peak = max(valid, key=lambda p: p["resp_per_sec"]) if valid else None
    return budget, peak


def run_client_sampled(host, argv, timeout, iface):
    """Run `m12_gate <argv>` on `host` (blocking, its stdout is the RESULT) while
    sampling that host's NIC counters ~1 Hz over a SECOND ssh channel. Returns
    (rc, combined_output, samples) where each sample is (timestamp, counters)."""
    quoted = " ".join(shlex.quote(a) for a in argv)
    cmd = f"sudo {host.gate} {quoted}"
    print(f"INFO [ssh {host.public_ip}] {cmd}", flush=True)
    snap = f"cat /proc/net/dev  # NIC sample for {iface}, ~1 Hz while the client runs"
    print(f"INFO [ssh {host.public_ip}] {snap}", flush=True)
    proc = subprocess.Popen(host.ssh + [host.target, cmd], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    samples = []
    deadline = time.time() + timeout
    try:
        while True:
            if proc.poll() is not None:
                break
            if time.time() > deadline:
                print(f"INFO NIC-sampled client exceeded {timeout}s; killing", flush=True)
                proc.kill()
                break
            try:
                r = subprocess.run(host.ssh + [host.target, "cat /proc/net/dev"],
                                   text=True, capture_output=True, timeout=15)
                samples.append((time.time(), parse_proc_net_dev(r.stdout or "", iface)))
            except Exception as e:
                print(f"INFO NIC sample skipped (continuing): {e}", flush=True)
            time.sleep(1.0)
        out, _ = proc.communicate(timeout=60)
    except Exception:
        proc.kill()
        out, _ = proc.communicate()
    return proc.returncode, out or "", samples


def netbudget_point(host, a, iface, inflight, secs):
    """One ladder point: the direct client at `inflight`, envelope off, with the
    NIC sampled underneath it. Emits and returns the NETBUDGET-JSON dict, or
    None if the client printed no RESULT (it died — not a zero)."""
    argv = ["client-direct", "--instance-dir", host.dir, "--app-id", APP,
            "--secs", str(secs), "--payload", str(a.payload),
            "--inflight", str(inflight), "--envelope", "off"]
    rc, out, samples = run_client_sampled(host, argv, secs + CLIENT_SLACK_SECS, iface)
    echo(f"netbudget if={inflight}", out)
    d = parse_result(out, "direct")
    if d is None:
        print(f"INFO netbudget inflight={inflight} produced no RESULT line "
              f"(rc={rc}) — treated as not-measured, not zero", flush=True)
        return None
    rates = steady_rates(samples)
    if rates is None:
        print(f"INFO netbudget inflight={inflight}: only {len(samples)} NIC "
              f"sample(s), cannot form a rate", flush=True)
        rates = {"rx_bytes_per_sec": None, "tx_bytes_per_sec": None,
                 "rx_pkts_per_sec": None, "tx_pkts_per_sec": None}
    rps = d.get("responses_per_sec") or 0.0

    def per_cmd(x):
        # NIC tx delta per second / commands committed per second == tx delta
        # per command committed (the replication cost of one command).
        return (x / rps) if (x is not None and rps > 0) else None

    point = {
        "inflight": inflight,
        "resp_per_sec": d.get("responses_per_sec"),
        "responses": d.get("responses"),
        "p50_ms": d.get("p50_ms"), "p90_ms": d.get("p90_ms"),
        "p95_ms": d.get("p95_ms"), "p99_ms": d.get("p99_ms"),
        "max_ms": d.get("max_ms"), "lost": d.get("lost"),
        "nic_tx_bytes_per_sec": rates["tx_bytes_per_sec"],
        "nic_rx_bytes_per_sec": rates["rx_bytes_per_sec"],
        "nic_tx_pkts_per_sec": rates["tx_pkts_per_sec"],
        "nic_rx_pkts_per_sec": rates["rx_pkts_per_sec"],
        "bytes_per_command": per_cmd(rates["tx_bytes_per_sec"]),
        "pkts_per_command": per_cmd(rates["tx_pkts_per_sec"]),
        "nic_samples": len(samples),
    }
    print("NETBUDGET-JSON " + json.dumps(point), flush=True)
    return point


def run_with_remote_load(node_hosts, client_host, leader, lh, iface, a, budget):
    """Optional tail: with the cluster still up, start edges + a concurrent TCP
    client-remote on the measurement host, then re-run the direct client at the
    p99<1ms inflight and report whether p99 degrades / NIC pkts/s climbs."""
    infl = budget["inflight"]
    print(f"\nINFO ROW 1 optional (--with-remote-load): concurrent TCP load while "
          f"re-measuring the p99<1ms point (inflight={infl})", flush=True)
    start_edges(node_hosts, a, "off")
    l2 = wait_leader(node_hosts, list(range(len(node_hosts))), LEADER_WAIT_SECS)
    if l2 is None or l2 != leader:
        print("INFO leader moved/lost after starting edges; skipping remote-load step",
              flush=True)
        stop_edges(node_hosts)
        return None
    dur = a.netbudget_secs
    rargv = ["client-remote", "--gateways", f"{lh.private_ip}:{EDGE_PORT}",
             "--app-id", APP, "--secs", str(dur + 30), "--payload", str(a.payload),
             "--inflight", str(a.inflight)]
    rquoted = " ".join(shlex.quote(x) for x in rargv)
    rcmd = f"sudo {client_host.gate} {rquoted}"
    print(f"INFO [ssh {client_host.public_ip}] {rcmd}   (background TCP load)", flush=True)
    rproc = subprocess.Popen(client_host.ssh + [client_host.target, rcmd], text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = None
    try:
        time.sleep(EDGE_SETTLE_SECS + 2.0)  # let the TCP client ramp before we sample
        p = netbudget_point(lh, a, iface, infl, dur)
        if p is not None:
            result = {
                "inflight": infl,
                "baseline_p99_ms": budget["p99_ms"],
                "under_tcp_load_p99_ms": p["p99_ms"],
                "baseline_resp_per_sec": budget["resp_per_sec"],
                "under_tcp_load_resp_per_sec": p["resp_per_sec"],
                "baseline_nic_tx_pkts_per_sec": budget["nic_tx_pkts_per_sec"],
                "under_tcp_load_nic_tx_pkts_per_sec": p["nic_tx_pkts_per_sec"],
                "baseline_nic_tx_bytes_per_sec": budget["nic_tx_bytes_per_sec"],
                "under_tcp_load_nic_tx_bytes_per_sec": p["nic_tx_bytes_per_sec"],
                "p99_still_under_1ms": (p["p99_ms"] is not None
                                        and p["p99_ms"] < P99_MS_BUDGET),
            }
            print("NETBUDGET-REMOTELOAD-JSON " + json.dumps(result), flush=True)
        else:
            print("INFO remote-load re-measure produced no RESULT", flush=True)
    finally:
        for fn in (rproc.terminate, rproc.kill):
            try:
                fn()
                rproc.communicate(timeout=15)
                break
            except Exception:
                continue
        stop_edges(node_hosts)
    return result


def row1(node_hosts, client_host, a):
    ladder = [int(x) for x in a.netbudget_inflights.split(",") if x.strip()]
    secs = a.netbudget_secs
    print(f"INFO ROW 1: NETWORK-BUDGET, ladder={ladder}, secs={secs}, "
          f"payload={a.payload}, envelope=off, with_remote_load={a.with_remote_load}",
          flush=True)
    wipe_dirs(node_hosts)
    start_cluster(node_hosts, a, "off")

    points = []
    remote = None
    try:
        leader = wait_leader(node_hosts, list(range(len(node_hosts))), LEADER_WAIT_SECS)
        if leader is None:
            print(f"INFO no single serving leader within {LEADER_WAIT_SECS}s", flush=True)
            return Verdict("1 network-budget (measurement, no bar)", False,
                           "no leader — nothing measured")
        lh = node_hosts[leader]
        iface = detect_iface(lh)
        print(f"INFO leader is n{leader} ({lh.public_ip}), primary NIC {iface}",
              flush=True)

        for k in ladder:
            print(f"\nINFO --- netbudget ladder point inflight={k} ---", flush=True)
            l2 = wait_leader(node_hosts, list(range(len(node_hosts))), LEADER_WAIT_SECS)
            if l2 is None:
                print(f"INFO leader lost before inflight={k}; skipping this point",
                      flush=True)
                continue
            if l2 != leader:
                print(f"INFO leadership moved n{leader} -> n{l2}; re-targeting the "
                      f"direct client and NIC sampler at the new leader host",
                      flush=True)
                leader, lh = l2, node_hosts[l2]
                iface = detect_iface(lh)
            p = netbudget_point(lh, a, iface, k, secs)
            if p is not None:
                points.append(p)

        budget, peak = select_operating_points(points)

        if a.with_remote_load and budget is not None:
            remote = run_with_remote_load(node_hosts, client_host, leader, lh,
                                          iface, a, budget)
        elif a.with_remote_load:
            print("INFO --with-remote-load set but no p99<1ms point to re-measure; "
                  "skipping the remote-load step", flush=True)
    finally:
        stop_edges(node_hosts)
        stop_cluster(node_hosts)

    # ----- report
    budget, peak = select_operating_points(points)
    print("\nROW 1 — network budget: direct shmem client vs consensus NIC cost")
    print(f"  {'inflight':>8} {'resp/s':>12} {'p50':>7} {'p90':>7} {'p95':>7} "
          f"{'p99':>7} {'lost':>6} {'tx MB/s':>9} {'tx kpps':>8} "
          f"{'B/cmd':>8} {'pkt/cmd':>8}")
    for p in points:
        def f(x, w, d=3):
            return f"{x:>{w}.{d}f}" if isinstance(x, (int, float)) else f"{'—':>{w}}"
        txmb = (p["nic_tx_bytes_per_sec"] / 1e6) if p["nic_tx_bytes_per_sec"] else None
        txk = (p["nic_tx_pkts_per_sec"] / 1e3) if p["nic_tx_pkts_per_sec"] else None
        print(f"  {p['inflight']:>8} {f(p['resp_per_sec'],12,0)} "
              f"{f(p['p50_ms'],7)} {f(p['p90_ms'],7)} {f(p['p95_ms'],7)} "
              f"{f(p['p99_ms'],7)} {p.get('lost','—'):>6} "
              f"{f(txmb,9,2)} {f(txk,8,1)} "
              f"{f(p['bytes_per_command'],8,1)} {f(p['pkts_per_command'],8,3)}")
    print("  NETBUDGET-POINTS-JSON " + json.dumps(points))

    print("\nNETBUDGET-SUMMARY")
    if budget is not None:
        print(f"  (a) p99<1ms budget: inflight={budget['inflight']}, "
              f"resp/s={budget['resp_per_sec']:.0f}, p99={budget['p99_ms']:.3f} ms, "
              f"NIC tx={_fmt(budget['nic_tx_bytes_per_sec'])} B/s "
              f"({_fmt(budget['nic_tx_pkts_per_sec'])} pkt/s), "
              f"{_fmt(budget['bytes_per_command'])} B/cmd "
              f"{_fmt(budget['pkts_per_command'])} pkt/cmd")
    else:
        print("  (a) p99<1ms budget: NONE — no ladder point held p99 below 1 ms")
    if peak is not None:
        print(f"  (b) peak throughput: inflight={peak['inflight']}, "
              f"resp/s={peak['resp_per_sec']:.0f}, p99={_fmt(peak['p99_ms'])} ms, "
              f"NIC tx={_fmt(peak['nic_tx_bytes_per_sec'])} B/s "
              f"({_fmt(peak['nic_tx_pkts_per_sec'])} pkt/s), "
              f"{_fmt(peak['bytes_per_command'])} B/cmd "
              f"{_fmt(peak['pkts_per_command'])} pkt/cmd")
    else:
        print("  (b) peak throughput: NONE — no usable ladder point")
    print("  NETBUDGET-SUMMARY-JSON " + json.dumps({"budget": budget, "peak": peak}))

    if remote is not None:
        verdict = ("p99 held" if remote["p99_still_under_1ms"] else "p99 BROKE")
        print("\nNETBUDGET-REMOTE-LOAD (concurrent TCP client on the measurement host)")
        print(f"  inflight={remote['inflight']}: p99 "
              f"{_fmt(remote['baseline_p99_ms'])} -> "
              f"{_fmt(remote['under_tcp_load_p99_ms'])} ms ({verdict} the 1 ms bar); "
              f"NIC tx pkts/s {_fmt(remote['baseline_nic_tx_pkts_per_sec'])} -> "
              f"{_fmt(remote['under_tcp_load_nic_tx_pkts_per_sec'])}")
    elif a.with_remote_load:
        print("\nNETBUDGET-REMOTE-LOAD: SKIPPED (no p99<1ms point, or the step could "
              "not run) — the ladder above is the measurement")

    ok = len(points) > 0
    detail = (f"measured {len(points)} ladder point(s); "
              + (f"p99<1ms at inflight={budget['inflight']} "
                 f"({budget['resp_per_sec']:.0f}/s); " if budget else "no p99<1ms point; ")
              + (f"peak {peak['resp_per_sec']:.0f}/s at inflight={peak['inflight']}"
                 if peak else "no peak point"))
    return Verdict("1 network-budget (measurement, no bar)", ok, detail)


def _fmt(x, d=1):
    return f"{x:.{d}f}" if isinstance(x, (int, float)) else "—"


# -------------------------------------------- row `edgesat` (N-client edge saturation)
#
# WHAT THIS ROW ANSWERS, and why it is not row 2.
#
#   Row 2 drives ONE `RemoteClient` over ONE TCP connection into ONE edge and
#   divides by the direct shmem client: on the fleet that is ~140k resp/s
#   against ~1.42M resp/s. That is a PER-CONNECTION number, and per-connection
#   is not the question an operator asks of a gateway. The operator's question
#   is AGGREGATE: with K client connections open at once, what does the edge
#   deliver in total, and WHICH stage stops the total climbing?
#
#   This row answers it by walking a ladder of concurrent client connections
#   (default 1,2,4,8,16) into the LEADER's edge, all clients started as close
#   to simultaneously as ssh allows, and reporting the summed responses/s at
#   each rung together with the CPU of every stage that could be the limiter.
#
# THE THREE CANDIDATE CEILINGS, and how each is made visible:
#
#   (a) THE EDGE ITSELF. `Edge` serialises every outbound write through a
#       single driver thread (a documented single-writer constraint of the
#       shipped design, not an artifact). If that thread is the ceiling, the
#       edge PROCESS's CPU pins near one core's worth — so this row samples
#       /proc/<edge pid>/stat and reports `edge_proc_cpu_pct`, where ~100%
#       means "one core, saturated" and 250% would mean "two and a half cores
#       busy, the driver thread is not alone in the picture".
#   (b) THE CLIENT HOSTS. Every `client-remote` process spends a sender thread
#       plus eight waiter threads, so 16 of them on one 8-vCPU box is heavily
#       oversubscribed. If the client host runs out of CPU the aggregate stops
#       climbing for a reason that has nothing to do with the gateway — this
#       is THE way this measurement can lie, so `EDGESAT-SUMMARY` refuses to
#       call a knee once the client hosts are saturated and the edge process
#       is not (see `classify_edgesat`).
#   (c) THE BACKEND. The node + service under the edge. Visible as the leader
#       host's overall CPU with the edge process's share already broken out.
#
# WHY ENVELOPE OFF. The session table in `Sessioned<S>` grows one entry per
# distinct `client_id`, and every rung of this ladder introduces N fresh ones.
# Row 2 needs the envelope for frame-identical arms; this row wants a clean
# throughput curve, so the services, the edges and therefore the clients all
# run raw pass-through.
#
# Everything below the ssh helpers is pure, so the aggregation, the CPU deltas,
# the knee and the client-bound labelling are unit-testable off-fleet.

DEFAULT_EDGESAT_CLIENTS = "1,2,4,8,16"

# The direct-arm peak measured by row 1 on the fleet (responses/s, one direct
# shmem client on the leader's own host). The saturation ratio below divides
# the aggregate at the ceiling by this, which is the candidate re-specced
# row-2 metric: "what fraction of the box's own commit rate can the gateway
# hand to remote clients". Update this only alongside a new row-1 measurement.
DIRECT_ARM_PEAK_RPS = 1_424_941.0

# Knee: the first rung whose aggregate gains less than this over the previous.
EDGESAT_KNEE_GAIN = 0.15
# "This host has no CPU left" and "this process is holding down ~a core".
HOST_SAT_PCT = 85.0
EDGE_CORE_SAT_PCT = 90.0

EDGESAT_RAMP_SECS = 3.0     # let every client connect + reach steady state
EDGESAT_DRAIN_SECS = 5.0    # slack after --edgesat-secs before we start polling


def cpu_window_cmd(secs, edge_unit=None):
    """A ONE-ROUND-TRIP CPU sample over a `secs` window on one host.

    Takes /proc/stat (and, when `edge_unit` is given, /proc/<MainPID>/stat)
    before and after the window, and runs `mpstat 1 <secs>` inside the window
    when sysstat is installed — sysstat is NOT assumed, and the /proc/stat
    delta is what the number falls back to. Both are parsed by
    `parse_cpu_window`; `getconf CLK_TCK` comes back in the same payload so the
    process-CPU maths never hardcodes 100 Hz.

    One round trip matters: the leader and every client host must be sampled
    over the SAME window while the clients are running, so the caller issues
    these concurrently and cannot afford a multi-step conversation with each
    host."""
    pid = (
        f"PID=$(systemctl show -p MainPID --value {UNIT_PREFIX}-{edge_unit} "
        f"2>/dev/null || true); [ \"$PID\" = 0 ] && PID=; "
    ) if edge_unit else "PID=; "
    return (
        pid
        + "HZ=$(getconf CLK_TCK); "
        "T0=$(date +%s.%N); S0=$(grep '^cpu ' /proc/stat); "
        "P0=; [ -n \"$PID\" ] && P0=$(cat /proc/$PID/stat 2>/dev/null); "
        f"if command -v mpstat >/dev/null 2>&1; then MP=$(mpstat 1 {secs} 2>/dev/null); "
        f"else sleep {secs}; MP=; fi; "
        "T1=$(date +%s.%N); S1=$(grep '^cpu ' /proc/stat); "
        "P1=; [ -n \"$PID\" ] && P1=$(cat /proc/$PID/stat 2>/dev/null); "
        "printf '===HZ\\n%s\\n===T0\\n%s\\n===S0\\n%s\\n===P0\\n%s\\n"
        "===T1\\n%s\\n===S1\\n%s\\n===P1\\n%s\\n===MP\\n%s\\n===END\\n' "
        "\"$HZ\" \"$T0\" \"$S0\" \"$P0\" \"$T1\" \"$S1\" \"$P1\" \"$MP\""
    )


def _split_sections(text):
    """Split a `===KEY` / value payload. No content this driver asks for (a
    /proc line, an mpstat table, a timestamp) can begin with `===`."""
    out, key = {}, None
    for line in text.splitlines():
        if line.startswith("==="):
            key = line[3:].strip()
            out[key] = []
        elif key is not None:
            out[key].append(line)
    return {k: "\n".join(v).strip() for k, v in out.items()}


def proc_stat_busy_idle(line):
    """(busy_ticks, idle_ticks) from a `cpu ` aggregate line of /proc/stat.

    Fields after the label: user nice system idle iowait irq softirq steal
    [guest guest_nice]. `guest`/`guest_nice` are ALREADY counted inside
    user/nice, so summing them again would double-count — only the first eight
    are used. iowait counts as idle: the CPU was available."""
    f = line.split()
    if not f or not f[0].startswith("cpu"):
        raise ValueError(f"not a /proc/stat cpu line: {line!r}")
    v = [int(x) for x in f[1:9]]
    if len(v) < 8:
        raise ValueError(f"short /proc/stat cpu line: {line!r}")
    idle = v[3] + v[4]
    return sum(v) - idle, idle


def cpu_pct_from_proc_stat(s0, s1):
    """Overall utilisation pct across the whole box between two `cpu ` lines."""
    b0, i0 = proc_stat_busy_idle(s0)
    b1, i1 = proc_stat_busy_idle(s1)
    db, di = b1 - b0, i1 - i0
    if db + di <= 0:
        return None
    return 100.0 * db / (db + di)


def parse_mpstat_idle(text):
    """%idle off `mpstat 1 N`'s Average row (the `all` line). None if absent."""
    for line in text.splitlines():
        if not line.lower().startswith("average"):
            continue
        f = line.split()
        if len(f) < 3 or f[1] != "all":
            continue
        try:
            return float(f[-1].replace(",", "."))
        except ValueError:
            return None
    return None


def pid_stat_ticks(line):
    """utime+stime (ticks) out of a raw /proc/<pid>/stat line.

    The comm field is parenthesised and may itself contain spaces AND close
    parens, so the split is on the LAST ')': everything after it starts at
    field 3 (state), which puts utime at offset 11 and stime at 12."""
    if ")" not in line:
        raise ValueError(f"not a /proc/pid/stat line: {line!r}")
    rest = line.rsplit(")", 1)[1].split()
    if len(rest) < 13:
        raise ValueError(f"short /proc/pid/stat line: {line!r}")
    return int(rest[11]) + int(rest[12])


def parse_cpu_window(text):
    """Turn one `cpu_window_cmd` payload into
    {host_cpu_pct, source, proc_cpu_pct, window_secs, hz}.

    `proc_cpu_pct` is in "percent of ONE core" units — 100.0 means the process
    burned a full core over the window, 300.0 means three. That is the unit in
    which "the edge's single driver thread is saturated" is directly readable."""
    s = _split_sections(text)
    out = {"host_cpu_pct": None, "source": None, "proc_cpu_pct": None,
           "window_secs": None, "hz": None}
    if "END" not in s:
        return out
    try:
        out["hz"] = int(s.get("HZ", "") or 0) or None
    except ValueError:
        pass
    try:
        t0, t1 = float(s.get("T0", "")), float(s.get("T1", ""))
        if t1 > t0:
            out["window_secs"] = t1 - t0
    except ValueError:
        pass
    idle = parse_mpstat_idle(s.get("MP", ""))
    if idle is not None:
        out["host_cpu_pct"], out["source"] = 100.0 - idle, "mpstat"
    else:
        try:
            pct = cpu_pct_from_proc_stat(s.get("S0", ""), s.get("S1", ""))
            if pct is not None:
                out["host_cpu_pct"], out["source"] = pct, "proc_stat"
        except ValueError:
            pass
    if s.get("P0") and s.get("P1") and out["window_secs"] and out["hz"]:
        try:
            dticks = pid_stat_ticks(s["P1"]) - pid_stat_ticks(s["P0"])
            out["proc_cpu_pct"] = 100.0 * (dticks / out["hz"]) / out["window_secs"]
        except ValueError:
            pass
    return out


def sample_cpu_concurrently(targets, secs):
    """Sample every (label, host, edge_unit) target over the SAME window.

    Sequential sampling would need len(targets) x secs and would fall outside
    the clients' run window; these are issued from a thread each (the work is
    an ssh subprocess, so the GIL is irrelevant)."""
    import concurrent.futures as cf

    def one(t):
        label, host, edge_unit = t
        cmd = cpu_window_cmd(secs, edge_unit=edge_unit)
        try:
            r = ssh(host, cmd, timeout=secs + 60, label=f"cpu {label}")
            return label, parse_cpu_window((r.stdout or "") + (r.stderr or ""))
        except Exception as e:  # a lost CPU sample must not lose the rung
            print(f"INFO CPU sample on {label} failed (continuing): {e}", flush=True)
            return label, parse_cpu_window("")

    with cf.ThreadPoolExecutor(max_workers=max(1, len(targets))) as ex:
        return dict(ex.map(one, targets))


def units_still_active(host, units):
    """The subset of `units` systemd still calls active, in ONE ssh call."""
    names = " ".join(f"{UNIT_PREFIX}-{u}" for u in units)
    r = ssh(host, f"systemctl is-active {names} 2>/dev/null; true", label="systemctl")
    states = (r.stdout or "").split()
    return [u for u, st in zip(units, states) if st == "active"]


def wait_units_done(by_host, deadline_ts, poll_secs=3.0):
    """Poll until no client unit is active, or the deadline passes. Bounded by
    construction — `deadline_ts` is an absolute wall-clock time."""
    while time.time() < deadline_ts:
        active = 0
        for host, units in by_host:
            active += len(units_still_active(host, units))
        if active == 0:
            return True
        time.sleep(poll_secs)
    print("INFO client units still active at the deadline; reading what they "
          "have written and killing the rest", flush=True)
    return False


def _median_client(results, key):
    """The MIDDLE client by responses/s, and one of its latency percentiles.

    A percentile cannot be averaged across clients, so "the median client's
    p99" has to name an actual client: rank the clients by responses/s and take
    index (n-1)//2 (the lower middle for an even count)."""
    if not results:
        return None
    ordered = sorted(results, key=lambda d: d.get("responses_per_sec") or 0.0)
    return ordered[(len(ordered) - 1) // 2].get(key)


def aggregate_edgesat(n, inflight, results, cpu, clients_started, reference=False):
    """One ladder rung's EDGESAT-JSON dict from the clients' RESULT dicts plus
    the CPU samples. Pure — `cpu` is {"leader": {...}, "clients": [{...}, ...]}."""
    rps = [r.get("responses_per_sec") or 0.0 for r in results]
    p99s = [r.get("p99_ms") for r in results if r.get("p99_ms") is not None]
    leader = cpu.get("leader") or {}
    return {
        "n": n,
        "inflight_per_client": inflight,
        "reference": reference,
        "agg_resp_per_sec": sum(rps),
        "per_client_min": min(rps) if rps else None,
        "per_client_med": statistics.median(rps) if rps else None,
        "per_client_max": max(rps) if rps else None,
        "p50_med_ms": _median_client(results, "p50_ms"),
        "p95_med_ms": _median_client(results, "p95_ms"),
        "p99_med_ms": _median_client(results, "p99_ms"),
        "p99_worst_ms": max(p99s) if p99s else None,
        "lost": sum(r.get("lost") or 0 for r in results),
        "leader_cpu_pct": leader.get("host_cpu_pct"),
        "edge_proc_cpu_pct": leader.get("proc_cpu_pct"),
        "client_hosts_cpu_pct": [c.get("host_cpu_pct") for c in cpu.get("clients", [])],
        "cpu_source": leader.get("source"),
        "clients_started": clients_started,
        "clients_reported": len(results),
        "per_client_rps": rps,
    }


def _client_saturated(p):
    vals = [c for c in (p.get("client_hosts_cpu_pct") or []) if c is not None]
    return bool(vals) and max(vals) >= HOST_SAT_PCT


def _edge_saturated(p):
    v = p.get("edge_proc_cpu_pct")
    return v is not None and v >= EDGE_CORE_SAT_PCT


def select_knee(points, gain=EDGESAT_KNEE_GAIN):
    """The first rung whose aggregate gains less than `gain` over the previous
    one. Returns (knee_point, prev_point, measured_gain) or (None, None, None)
    when every rung kept climbing."""
    usable = [p for p in points if p.get("agg_resp_per_sec")]
    for prev, cur in zip(usable, usable[1:]):
        g = (cur["agg_resp_per_sec"] - prev["agg_resp_per_sec"]) / prev["agg_resp_per_sec"]
        if g < gain:
            return cur, prev, g
    return None, None, None


def classify_edgesat(points):
    """Decide what the ladder actually showed, and REFUSE to call a knee that
    the client hosts manufactured.

    The failure mode this exists to prevent: at the top rungs the client host
    runs out of CPU (16 `client-remote` processes x ~9 threads on one box), the
    aggregate flattens, and a naive reader records "the edge saturates at
    N=8" when the edge process was sitting at 60% of one core. So: any rung
    where a client host is >= HOST_SAT_PCT while the edge process is NOT
    >= EDGE_CORE_SAT_PCT is CLIENT-TAINTED. If the tainted rungs form the tail
    of the ladder, the verdict is `client_bound` — the honest statement is a
    LOWER BOUND on the edge ("edge ceiling >= the highest clean aggregate"),
    not a knee.

    Returns a dict describing the verdict, the clean prefix, the knee (computed
    over clean rungs only) and the attributed ceiling."""
    ladder = [p for p in points if not p.get("reference")]
    tainted = [p["n"] for p in ladder if _client_saturated(p) and not _edge_saturated(p)]
    client_bound_from = min(tainted) if tainted else None
    clean = [p for p in ladder if client_bound_from is None or p["n"] < client_bound_from]
    knee, prev, gain = select_knee(clean)
    best_clean = max((p for p in clean if p.get("agg_resp_per_sec")),
                     key=lambda p: p["agg_resp_per_sec"], default=None)
    best_any = max((p for p in ladder if p.get("agg_resp_per_sec")),
                   key=lambda p: p["agg_resp_per_sec"], default=None)

    if client_bound_from is not None:
        verdict = "client_bound"
        # The RATIO is quoted off the highest CLEAN rung — a lower bound on the
        # edge. The ATTRIBUTION is quoted off the first tainted rung, because
        # that is where the ladder actually stopped and the honest sentence is
        # "the load generator ran out", not "nothing was saturated at N=4".
        ceiling_point = best_clean
        attribution_point = next(p for p in ladder if p["n"] == client_bound_from)
    elif knee is not None:
        verdict = "knee"
        ceiling_point = attribution_point = knee
    else:
        verdict = "no_knee"
        ceiling_point = attribution_point = best_any
    return {
        "verdict": verdict,
        "client_bound_from": client_bound_from,
        "knee": knee, "knee_prev": prev, "knee_gain": gain,
        "clean_points": [p["n"] for p in clean],
        "highest_clean_agg": (best_clean or {}).get("agg_resp_per_sec"),
        "highest_agg": (best_any or {}).get("agg_resp_per_sec"),
        "ceiling_point": ceiling_point,
        "attribution": attribute_ceiling(attribution_point),
        "ratio_is_lower_bound": verdict != "knee",
        "saturation_ratio": ((ceiling_point or {}).get("agg_resp_per_sec") or 0.0)
        / DIRECT_ARM_PEAK_RPS if ceiling_point else None,
    }


def attribute_ceiling(p):
    """Which stage is saturated at this rung — the sentence that makes the
    curve interpretable. Ordered most-specific first."""
    if p is None:
        return "no usable rung — nothing to attribute"
    edge = p.get("edge_proc_cpu_pct")
    lead = p.get("leader_cpu_pct")
    cli = [c for c in (p.get("client_hosts_cpu_pct") or []) if c is not None]
    worst_cli = max(cli) if cli else None
    if _edge_saturated(p):
        return (f"EDGE PROCESS — the edge burned {edge:.0f}% of one core "
                f"(~{edge / 100.0:.1f} cores); its single driver thread is the "
                f"binding constraint")
    if worst_cli is not None and worst_cli >= HOST_SAT_PCT:
        return (f"CLIENT HOSTS — worst client host at {worst_cli:.0f}% CPU while "
                f"the edge process held only {_fmt(edge)}% of one core: the "
                f"load generator ran out, not the gateway")
    if lead is not None and lead >= HOST_SAT_PCT:
        return (f"LEADER HOST (backend) — leader box at {lead:.0f}% CPU with the "
                f"edge process at {_fmt(edge)}% of one core: node + service, not "
                f"the edge, own the ceiling")
    return (f"UNSATURATED — edge {_fmt(edge)}% of one core, leader host "
            f"{_fmt(lead)}%, worst client host {_fmt(worst_cli)}%: no sampled "
            f"stage is at its limit, so the ceiling is a serialisation "
            f"(per-connection credit, the client's Mutex<State>) rather than CPU")


def edgesat_point(node_hosts, leader, client_hosts, a, n, inflight, reference=False):
    """One rung: N concurrent `client-remote` units into the LEADER's edge,
    round-robined over the client hosts, with every stage's CPU sampled inside
    the run window. Returns the EDGESAT-JSON dict (never None — a rung where no
    client reported is reported as such, not silently dropped)."""
    secs = a.edgesat_secs
    edge_addr = f"{node_hosts[leader].private_ip}:{EDGE_PORT}"
    print(f"\nINFO --- edgesat rung N={n}, inflight/client={inflight}, "
          f"secs={secs}{' (REFERENCE point)' if reference else ''} ---", flush=True)

    # Round-robin the N clients over the client hosts, then group by host so
    # each host's units go out in ONE ssh call (see `start_units_batch`).
    grouped, order = {}, []
    for i in range(n):
        h = client_hosts[i % len(client_hosts)]
        if id(h) not in grouped:
            grouped[id(h)] = (h, [])
            order.append(id(h))
        grouped[id(h)][1].append(f"esat-c{n}-{i}")
    by_host = [grouped[k] for k in order]   # [(host, [unit, ...])]

    started = []
    try:
        t_start = time.time()
        for h, units in by_host:
            specs = [(u, ["client-remote", "--gateways", edge_addr,
                          "--app-id", APP, "--secs", str(secs),
                          "--payload", str(a.payload),
                          "--inflight", str(inflight)]) for u in units]
            started += start_units_batch(h, specs)
        print(f"INFO started {len(started)}/{n} client unit(s) in "
              f"{time.time() - t_start:.2f}s of wall skew", flush=True)

        # CPU is sampled INSIDE the run: ramp first, then a window that must fit
        # under --edgesat-secs with room for the clients' own drain.
        window = max(3, min(a.edgesat_cpu_window_secs,
                            int(secs - EDGESAT_RAMP_SECS - 2)))
        time.sleep(EDGESAT_RAMP_SECS)
        targets = [("leader", node_hosts[leader], "edge")]
        targets += [(f"client{i}", h, None) for i, h in enumerate(client_hosts)]
        print(f"INFO sampling CPU on {len(targets)} host(s) over a {window}s "
              f"window while the rung runs", flush=True)
        samples = sample_cpu_concurrently(targets, window)
        cpu = {"leader": samples.get("leader", {}),
               "clients": [samples.get(f"client{i}", {})
                           for i in range(len(client_hosts))]}

        deadline = t_start + secs + EDGESAT_DRAIN_SECS + CLIENT_SLACK_SECS
        wait_units_done(by_host, deadline)

        results = []
        for h, units in by_host:
            for u in units:
                out = tail_log(h, u, lines=200)
                d = parse_result(out, "gateway")
                if d is None:
                    print(f"INFO unit {UNIT_PREFIX}-{u} on {h.public_ip} produced "
                          f"no RESULT line — not counted (a missing client is "
                          f"missing, not a zero)", flush=True)
                    echo(u, out, lines=8)
                else:
                    results.append(d)
    finally:
        for h, units in by_host:
            kill_units_batch(h, units)

    point = aggregate_edgesat(n, inflight, results, cpu, len(started),
                              reference=reference)
    print("EDGESAT-JSON " + json.dumps(point), flush=True)
    return point


def edgesat(node_hosts, client_hosts, a):
    ladder = [int(x) for x in a.edgesat_clients.split(",") if x.strip()]
    print(f"INFO ROW EDGESAT: ladder N={ladder}, inflight/client="
          f"{a.edgesat_inflight}, secs={a.edgesat_secs}, payload={a.payload}, "
          f"envelope=off, edge inflight={a.edgesat_edge_inflight}, "
          f"{len(node_hosts)} node host(s), {len(client_hosts)} client host(s)",
          flush=True)
    wipe_dirs(node_hosts)
    start_cluster(node_hosts, a, "off")

    points = []
    try:
        leader = wait_leader(node_hosts, list(range(len(node_hosts))),
                             LEADER_WAIT_SECS)
        if leader is None:
            return Verdict("edgesat N-client edge saturation (measurement, no bar)",
                           False,
                           f"no single serving leader within {LEADER_WAIT_SECS}s — "
                           f"nothing measured (infrastructure failure)")
        print(f"INFO leader is n{leader} ({node_hosts[leader].public_ip}); edges "
              f"on all {len(node_hosts)} node hosts so REDIRECTs resolve, every "
              f"client dials the LEADER's edge", flush=True)
        start_edges(node_hosts, a, "off", inflight=a.edgesat_edge_inflight)

        for n in ladder:
            l2 = wait_leader(node_hosts, list(range(len(node_hosts))),
                             LEADER_WAIT_SECS)
            if l2 is None:
                print(f"INFO leader lost before N={n}; skipping this rung",
                      flush=True)
                continue
            if l2 != leader:
                print(f"INFO leadership moved n{leader} -> n{l2}; re-targeting the "
                      f"clients and the CPU sampler at the new leader", flush=True)
                leader = l2
            points.append(edgesat_point(node_hosts, leader, client_hosts, a, n,
                                        a.edgesat_inflight))

        # The reference rung, LAST and deliberately outside the ladder: one
        # client at the row-2 inflight, so this row's curve can be tied back to
        # the ~140k/s single-connection number row 2 reports.
        if a.edgesat_ref:
            l2 = wait_leader(node_hosts, list(range(len(node_hosts))),
                             LEADER_WAIT_SECS)
            if l2 is not None:
                leader = l2
                points.append(edgesat_point(node_hosts, leader, client_hosts, a,
                                            1, a.edgesat_ref_inflight,
                                            reference=True))
            else:
                print("INFO leader lost before the reference rung; skipping it",
                      flush=True)
    finally:
        stop_edges(node_hosts)
        stop_cluster(node_hosts)

    return edgesat_report(points, a)


def edgesat_report(points, a):
    """Pure: the table, the EDGESAT-SUMMARY block and the Verdict."""
    print("\nEDGESAT — aggregate gateway throughput vs concurrent client connections")
    print(f"  {'N':>4} {'if/cl':>6} {'agg resp/s':>12} {'min':>10} {'med':>10} "
          f"{'max':>10} {'p50':>7} {'p95':>7} {'p99':>7} {'p99w':>7} {'lost':>5} "
          f"{'edge%core':>10} {'lead%':>6} {'client%':>9}")
    for p in points:
        def f(x, w, d=3):
            return f"{x:>{w}.{d}f}" if isinstance(x, (int, float)) else f"{'—':>{w}}"
        cli = [c for c in (p["client_hosts_cpu_pct"] or []) if c is not None]
        tag = "1*" if p["reference"] else str(p["n"])
        print(f"  {tag:>4} {p['inflight_per_client']:>6} "
              f"{f(p['agg_resp_per_sec'],12,0)} {f(p['per_client_min'],10,0)} "
              f"{f(p['per_client_med'],10,0)} {f(p['per_client_max'],10,0)} "
              f"{f(p['p50_med_ms'],7)} {f(p['p95_med_ms'],7)} {f(p['p99_med_ms'],7)} "
              f"{f(p['p99_worst_ms'],7)} {p['lost']:>5} "
              f"{f(p['edge_proc_cpu_pct'],10,0)} {f(p['leader_cpu_pct'],6,0)} "
              f"{f(max(cli) if cli else None,9,0)}")
    print("  (1* = the reference rung: one client at the row-2 inflight)")
    print("  EDGESAT-POINTS-JSON " + json.dumps(points))

    c = classify_edgesat(points)
    print("\nEDGESAT-SUMMARY")
    if not points:
        print("  no rung produced a measurement")
    if c["verdict"] == "client_bound":
        hi = c["highest_clean_agg"]
        print(f"  CLIENT-BOUND above N={c['client_bound_from']} — at and above that "
              f"rung a client host sat at >= {HOST_SAT_PCT:.0f}% CPU while the "
              f"edge process stayed under {EDGE_CORE_SAT_PCT:.0f}% of one core, so "
              f"the flattening is the LOAD GENERATOR, not the gateway.")
        if hi is None:
            print("  No knee is reported, and NO LOWER BOUND EITHER: not one rung "
                  "was free of client-host saturation, so this ladder measured "
                  "the load generator end to end. Re-run with more client hosts "
                  "(hosts[3..]) or a smaller ladder before quoting any number "
                  "from it.")
        else:
            print(f"  No knee is reported. The honest statement is a lower bound: "
                  f"edge ceiling >= {_fmt(hi, 0)} resp/s "
                  f"(highest clean aggregate, at N in {c['clean_points']}).")
    elif c["verdict"] == "knee":
        k, prev, g = c["knee"], c["knee_prev"], c["knee_gain"]
        print(f"  KNEE at N={k['n']}: aggregate {k['agg_resp_per_sec']:.0f} resp/s, "
              f"only {g * 100:.1f}% over N={prev['n']}'s "
              f"{prev['agg_resp_per_sec']:.0f} resp/s "
              f"(< {EDGESAT_KNEE_GAIN * 100:.0f}% = the knee rule).")
    else:
        print("  NO KNEE — the aggregate was still gaining >= "
              f"{EDGESAT_KNEE_GAIN * 100:.0f}% at the top of the ladder; the edge "
              f"ceiling is above {_fmt(c['highest_agg'], 0)} resp/s and this "
              f"ladder did not reach it.")
    print(f"  ATTRIBUTED CEILING: {c['attribution']}")
    if c["saturation_ratio"] is not None:
        cp = c["ceiling_point"]
        bound = ">= " if c["ratio_is_lower_bound"] else ""
        print(f"  EDGE SATURATION RATIO: {bound}{cp['agg_resp_per_sec']:.0f} / "
              f"{DIRECT_ARM_PEAK_RPS:.0f} = {bound}{c['saturation_ratio']:.3f} "
              f"(aggregate at the ceiling rung N={cp['n']} over the measured "
              f"direct-arm peak — the candidate re-specced row-2 metric"
              + ("; a LOWER BOUND, since the ladder did not reach a clean "
                 "ceiling)" if c["ratio_is_lower_bound"] else ")"))
    lost = sum(p["lost"] for p in points)
    missing = [(p["n"], p["clients_started"], p["clients_reported"]) for p in points
               if p["clients_reported"] != p["clients_started"]]
    if lost:
        print(f"  WARNING: {lost} lost response(s) across the ladder (bar 0 for a "
              f"clean curve)")
    if missing:
        print(f"  WARNING: rungs where not every started client reported "
              f"(N, started, reported): {missing}")
    # The rung dicts themselves are already in EDGESAT-POINTS-JSON; the summary
    # names them by N so it stays one readable line.
    print("  EDGESAT-SUMMARY-JSON " + json.dumps(
        {k: v for k, v in c.items()
         if k not in ("ceiling_point", "knee", "knee_prev")}
        | {"ceiling_n": (c["ceiling_point"] or {}).get("n"),
           "knee_n": (c["knee"] or {}).get("n"),
           "knee_prev_n": (c["knee_prev"] or {}).get("n"),
           "direct_arm_peak_rps": DIRECT_ARM_PEAK_RPS,
           "lost_total": lost}))

    detail = (f"{len(points)} rung(s); verdict={c['verdict']}; "
              + ((f"client-bound above N={c['client_bound_from']}, edge ceiling >= "
                  f"{_fmt(c['highest_clean_agg'], 0)} resp/s; "
                  if c["highest_clean_agg"] is not None else
                  f"client-bound from the first rung (N={c['client_bound_from']}) "
                  f"— no clean rung, so no bound on the edge; ")
                 if c["verdict"] == "client_bound" else
                 (f"knee at N={c['knee']['n']} ({c['knee']['agg_resp_per_sec']:.0f} "
                  f"resp/s); " if c["verdict"] == "knee" else
                  f"no knee, top aggregate {_fmt(c['highest_agg'], 0)} resp/s; "))
              + (f"saturation ratio {c['saturation_ratio']:.3f}"
                 if c["saturation_ratio"] is not None else "no saturation ratio"))
    # MEASUREMENT row: it states numbers. It only goes red on an infrastructure
    # failure (no leader, no rung at all), never on the shape of the curve.
    return Verdict("edgesat N-client edge saturation (measurement, no bar)",
                   len(points) > 0, detail)


# --------------------------------------------------------------------- setup

def setup_fleet(a):
    hosts = m6.build_fleet_hosts(
        BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=a.nodes,
        unit_prefix=UNIT_PREFIX, remote_root=REMOTE_ROOT, probe_bin=BUILT_PROBE,
    )
    for h in hosts:
        prepare_host(h, apply_profile=False)
        for u in ("node", "service", "edge"):
            kill_unit(h, u)
    if a.row == "edgesat":
        # The edge-saturation topology is FIXED at three cluster hosts, so that
        # every remaining host is a client host: the row scales CLIENTS, and a
        # fourth voter would only move hardware from the load generator into
        # the consensus plane. hosts[3..] stays a list — the proven fleet shape
        # is 4 hosts (one client host), but 5+ works unchanged if a vCPU quota
        # ever allows it.
        node_hosts = hosts[:3]
        client_hosts = hosts[3:]
    else:
        node_hosts = hosts[: a.nodes - 1]
        client_hosts = [hosts[a.nodes - 1]]
    print(f"INFO topology: {len(node_hosts)} cluster host(s) "
          f"{[h.public_ip for h in node_hosts]}, "
          f"{len(client_hosts)} client host(s) "
          f"{[h.public_ip for h in client_hosts]}", flush=True)
    return node_hosts, client_hosts


def print_bar(a):
    if a.row == "edgesat":
        print("M12 EDGESAT — N-client edge saturation (MEASUREMENT, no bar):")
        print("  Aggregate gateway throughput as concurrent client connections")
        print("  scale, with the CPU of every candidate ceiling sampled inside")
        print("  each rung's window: the edge process (percent of ONE core, so")
        print("  a saturated single driver thread is directly readable), the")
        print("  leader host, and every client host. The summary refuses to")
        print("  call a knee that the client hosts manufactured.")
        print("  Grounds a re-spec of gate row 2: today's row-2 ratio is a")
        print("  PER-CONNECTION number; the deployment-relevant one is the")
        print("  aggregate at the edge's ceiling over the direct-arm peak.")
        print()
        return
    print("M12 gate rows 2-3 — the pre-committed bars:")
    print(f"  row 2  gateway (Edge + RemoteClient) responses/s >= {BAR_RATIO} x direct")
    print("         `Engine` responses/s at EQUAL inflight, on a real fleet,")
    print("         one process per role per host, both arms measured against")
    print("         the SAME cluster generation and the SAME leader.")
    print("  row 3  codec share on the apply thread at the M5 ladder, typed vs")
    print("         raw state-machine tier — a MEASUREMENT row: it states two")
    print("         numbers and has no pass/fail bar.")
    print("  Source: docs/benchmarks/uc2-m12-gate-2026-08-22.md, rows 2 and 3.")
    print()


def main():
    ap = argparse.ArgumentParser(
        description="UC v2 M12 fleet-gate driver (rows 1, 2 and 3)")
    ap.add_argument("--fleet", action="store_true", required=True,
                    help="remote hosts over ssh (there is no local mode: both "
                         "rows are fleet-only, and the local smoke lives in "
                         "m12_gate's own in-process arms)")
    ap.add_argument("--hosts", default="",
                    help="pub/priv,... (else `terraform output -json nodes`)")
    ap.add_argument("--nodes", type=int, default=4,
                    help="TOTAL hosts. Rows 1-3: the first N-1 run one node + one "
                         "service (+ an edge during the gateway arm), the last "
                         "runs the remote client and nothing else (default 4 = a "
                         "3-node cluster + 1 measurement host). `--row edgesat` "
                         "fixes the cluster at THREE hosts and turns every "
                         "remaining host into a client host, so 4 gives the "
                         "proven shape (3 + 1) and 5 would give 3 + 2")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--secs", type=int, default=10)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--inflight", type=int, default=4096)
    ap.add_argument("--cycles", type=int, default=5)
    ap.add_argument("--admission-kib", type=int, default=256)
    ap.add_argument("--envelope", choices=("on", "off"), default="on",
                    help="row 2 only: `on` runs Sessioned<CountSm> and BOTH arms "
                         "carry the 16-byte session envelope (default); `off` is "
                         "the raw pass-through control. Row 3 always runs `off` "
                         "(509 B + a 16 B envelope exceeds max_payload).")
    ap.add_argument("--row3-secs", type=int, default=30,
                    help="row 3 load duration; the apply-profile counters print "
                         "every 1,000,000 applied frames, so this must be long "
                         "enough to reach one million")
    ap.add_argument("--row", choices=("1", "2", "3", "both", "edgesat"),
                    default="both",
                    help="`both` = rows 2 and 3 (the pass/measurement pair); "
                         "`1` = the network-budget measurement (run on its own); "
                         "`edgesat` = the N-client edge-saturation ladder (run on "
                         "its own — `both` deliberately does NOT include it)")
    ap.add_argument("--netbudget-inflights", default=DEFAULT_NETBUDGET_INFLIGHTS,
                    help="row 1 only: comma-separated inflight ladder driven on "
                         "the leader (default %(default)s)")
    ap.add_argument("--netbudget-secs", type=int, default=20,
                    help="row 1 only: per-ladder-point client-direct duration "
                         "(default 20)")
    ap.add_argument("--with-remote-load", action=argparse.BooleanOptionalAction,
                    default=True,
                    help="row 1 only (default on): after the ladder, add a "
                         "concurrent TCP client-remote on the measurement host and "
                         "re-measure the p99<1ms point to see if the box tips over")
    ap.add_argument("--edgesat-clients", default=DEFAULT_EDGESAT_CLIENTS,
                    help="edgesat only: comma-separated ladder of CONCURRENT "
                         "client connections, one systemd unit each, all into "
                         "the leader's edge (default %(default)s). The proven "
                         "fleet shape has ONE client host, and each "
                         "`client-remote` costs a sender thread plus eight "
                         "waiters — past 16 the rungs measure the load "
                         "generator, which is why the default stops there")
    ap.add_argument("--edgesat-inflight", type=int, default=1024,
                    help="edgesat only: per-client inflight window (default "
                         "%(default)s)")
    ap.add_argument("--edgesat-secs", type=int, default=15,
                    help="edgesat only: per-rung load duration (default "
                         "%(default)s)")
    ap.add_argument("--edgesat-edge-inflight", type=int, default=65536,
                    help="edgesat only: the EDGE's max_inflight/per_conn_inflight "
                         "(default %(default)s). Deliberately oversized: row 2 "
                         "keeps the edge window equal to the client's because it "
                         "measures a ratio at equal inflight, but here a shared "
                         "engine window smaller than N x --edgesat-inflight would "
                         "make the ladder measure the edge's ADMISSION WINDOW "
                         "instead of its service rate. Each client's own "
                         "--edgesat-inflight stays the binding per-connection cap")
    ap.add_argument("--edgesat-cpu-window-secs", type=int, default=6,
                    help="edgesat only: CPU sampling window inside each rung "
                         "(default %(default)s; clamped to fit --edgesat-secs "
                         "after the ramp)")
    ap.add_argument("--edgesat-ref", action=argparse.BooleanOptionalAction,
                    default=True,
                    help="edgesat only (default on): one extra rung at the END, "
                         "N=1 at --edgesat-ref-inflight, tying this curve back to "
                         "the single-connection number row 2 reports")
    ap.add_argument("--edgesat-ref-inflight", type=int, default=4096,
                    help="edgesat only: the reference rung's inflight (default "
                         "%(default)s = row 2's --inflight)")
    a = ap.parse_args()

    if a.nodes < 4:
        raise SystemExit("--nodes must be at least 4 (3 cluster hosts + 1 "
                         "measurement host); a 2-node cluster has no quorum "
                         "story worth measuring")
    if a.row == "edgesat":
        try:
            ladder = [int(x) for x in a.edgesat_clients.split(",") if x.strip()]
        except ValueError:
            raise SystemExit("--edgesat-clients must be a comma-separated list of "
                             "integers")
        if not ladder or any(n < 1 for n in ladder):
            raise SystemExit("--edgesat-clients must name at least one rung and "
                             "every rung must be >= 1")
        if a.edgesat_cpu_window_secs + EDGESAT_RAMP_SECS >= a.edgesat_secs:
            print(f"INFO --edgesat-cpu-window-secs {a.edgesat_cpu_window_secs} does "
                  f"not fit inside --edgesat-secs {a.edgesat_secs} after the "
                  f"{EDGESAT_RAMP_SECS:.0f}s ramp; it will be clamped per rung",
                  flush=True)

    print_bar(a)
    node_hosts, client_hosts = setup_fleet(a)
    client_host = client_hosts[0]
    verdicts = []
    try:
        if a.row == "1":
            verdicts.append(row1(node_hosts, client_host, a))
        if a.row == "edgesat":
            verdicts.append(edgesat(node_hosts, client_hosts, a))
        if a.row in ("2", "both"):
            verdicts.append(row2(node_hosts, client_host, a))
        if a.row in ("3", "both"):
            verdicts.append(row3(node_hosts, client_host, a))
    finally:
        stop_cluster(node_hosts)

    print()
    print("M12 fleet gate — FLEET")
    for v in verdicts:
        print(f"  [{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    # Only row 2 carries a bar. Row 3 states numbers; a missing measurement is
    # reported honestly above but does not turn the run red.
    row2_v = next((v for v in verdicts if v.row.startswith("2 ")), None)
    if row2_v is None:
        # The edge-saturation row is a measurement too, but "no leader" or "no
        # rung at all" is an INFRASTRUCTURE failure, not a measurement — that
        # must not exit green.
        esat_v = next((v for v in verdicts if v.row.startswith("edgesat")), None)
        if esat_v is not None and not esat_v.passed:
            print(f"RESULT: FAIL (infrastructure) — {esat_v.detail}")
            sys.exit(1)
        print("RESULT: PASS (measurement rows only — no bar was adjudicated)")
        sys.exit(0)
    if row2_v.passed:
        print(f"RESULT: PASS bar={BAR_RATIO} — {row2_v.detail}")
        sys.exit(0)
    print(f"RESULT: FAIL (honest) bar={BAR_RATIO} — {row2_v.detail}")
    sys.exit(1)


if __name__ == "__main__":
    main()
