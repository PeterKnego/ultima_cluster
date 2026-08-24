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


def start_unit(host, unit, args, nofile=False):
    """A transient `systemd-run --collect` unit running the m12_gate binary.

    `nofile` mirrors packaging/systemd/uc2-node.service's LimitNOFILE=65536 and
    is set for NODE units: the journal holds an fd per segment, and systemd's
    default soft limit of 1024 is what turned an earlier fleet run's small
    segments into EMFILE fail-stops (m10_fleet_gate's comment on the same
    line)."""
    kill_unit(host, unit)
    quoted = " ".join(shlex.quote(a) for a in args)
    limit = "-p LimitNOFILE=65536 " if nofile else ""
    cmd = (
        f"sudo systemd-run --unit={UNIT_PREFIX}-{unit} --collect -p TimeoutStopSec=1 "
        f"{limit}"
        f"-p StandardOutput=append:{unit_log(host, unit)} "
        f"-p StandardError=append:{unit_log(host, unit)} "
        f"{host.gate} {quoted}"
    )
    r = ssh(host, cmd, label="systemd-run")
    if r.returncode != 0:
        raise RuntimeError(
            f"start {UNIT_PREFIX}-{unit} on {host.public_ip}: {r.stderr or r.stdout}"
        )


def kill_unit(host, unit):
    ssh(
        host,
        f"sudo systemctl kill --signal=SIGKILL {UNIT_PREFIX}-{unit} 2>/dev/null; "
        f"sudo systemctl stop {UNIT_PREFIX}-{unit} 2>/dev/null; "
        f"sudo systemctl reset-failed {UNIT_PREFIX}-{unit} 2>/dev/null; true",
        label="systemctl",
    )


def truncate_log(host, unit):
    """Row 3 reads a counter line out of a unit log; an append-log carried over
    from an earlier phase would let the TYPED number be re-read as the RAW
    one."""
    ssh(host, f"sudo rm -f {unit_log(host, unit)}", label="ssh")


def tail_log(host, unit, lines=200):
    r = ssh(host, f"sudo tail -n {lines} {unit_log(host, unit)} 2>/dev/null",
            label="ssh")
    return r.stdout or ""


def run_foreground(host, args, timeout):
    """A BLOCKING ssh command (not systemd-run): the client roles run for
    `--secs` and exit, and their stdout IS the measurement."""
    quoted = " ".join(shlex.quote(a) for a in args)
    cmd = f"sudo {host.gate} {quoted}"
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


def start_cluster(node_hosts, a, envelope, raw_sm=False):
    ms = members_str(node_hosts)
    for i, h in enumerate(node_hosts):
        start_unit(h, "node", [
            "node", "--id", str(i), "--bind", f"{h.private_ip}:{PORT}",
            "--instance-dir", h.dir, "--members", ms, "--app-id", APP,
            "--admission-kib", str(a.admission_kib),
        ], nofile=True)
    time.sleep(BOOT_SETTLE_SECS)
    for h in node_hosts:
        args = ["service", "--instance-dir", h.dir, "--app-id", APP,
                "--envelope", envelope]
        if raw_sm:
            args.append("--raw-sm")
        truncate_log(h, "service")
        start_unit(h, "service", args)
    time.sleep(BOOT_SETTLE_SECS)


def stop_cluster(node_hosts):
    for h in node_hosts:
        for u in ("edge", "service", "node"):
            kill_unit(h, u)


def start_edges(node_hosts, a, envelope):
    em = edge_members_str(node_hosts)
    for i, h in enumerate(node_hosts):
        start_unit(h, "edge", [
            "edge", "--instance-dir", h.dir, "--app-id", APP,
            "--listen", f"{h.private_ip}:{EDGE_PORT}",
            "--members", em, "--envelope", envelope,
            "--inflight", str(a.inflight),
        ])
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


def run_direct_arm(node_hosts, leader, a, envelope, payload=None, secs=None):
    payload = a.payload if payload is None else payload
    secs = a.secs if secs is None else secs
    h = node_hosts[leader]
    rc, out = run_foreground(h, [
        "client-direct", "--instance-dir", h.dir, "--app-id", APP,
        "--secs", str(secs), "--payload", str(payload),
        "--inflight", str(a.inflight), "--envelope", envelope,
    ], timeout=secs + CLIENT_SLACK_SECS)
    echo("direct", out)
    d = parse_result(out, "direct")
    if d is None:
        print(f"INFO direct arm produced no RESULT line (rc={rc})", flush=True)
    return d


def run_gateway_arm(node_hosts, client_host, leader, a):
    rc, out = run_foreground(client_host, [
        "client-remote",
        "--gateways", f"{node_hosts[leader].private_ip}:{EDGE_PORT}",
        "--app-id", APP, "--secs", str(a.secs), "--payload", str(a.payload),
        "--inflight", str(a.inflight),
    ], timeout=a.secs + CLIENT_SLACK_SECS)
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
    node_hosts = hosts[: a.nodes - 1]
    client_host = hosts[a.nodes - 1]
    print(f"INFO topology: {len(node_hosts)} cluster host(s) "
          f"{[h.public_ip for h in node_hosts]}, measurement host "
          f"{client_host.public_ip}", flush=True)
    return node_hosts, client_host


def print_bar(a):
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
                    help="TOTAL hosts: the first N-1 run one node + one service "
                         "(+ an edge during the gateway arm), the last runs the "
                         "remote client and nothing else (default 4 = a 3-node "
                         "cluster + 1 measurement host)")
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
    ap.add_argument("--row", choices=("1", "2", "3", "both"), default="both",
                    help="`both` = rows 2 and 3 (the pass/measurement pair); "
                         "`1` = the network-budget measurement (run on its own)")
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
    a = ap.parse_args()

    if a.nodes < 4:
        raise SystemExit("--nodes must be at least 4 (3 cluster hosts + 1 "
                         "measurement host); a 2-node cluster has no quorum "
                         "story worth measuring")

    print_bar(a)
    node_hosts, client_host = setup_fleet(a)
    verdicts = []
    try:
        if a.row == "1":
            verdicts.append(row1(node_hosts, client_host, a))
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
        print("RESULT: PASS (measurement rows only — no bar was adjudicated)")
        sys.exit(0)
    if row2_v.passed:
        print(f"RESULT: PASS bar={BAR_RATIO} — {row2_v.detail}")
        sys.exit(0)
    print(f"RESULT: FAIL (honest) bar={BAR_RATIO} — {row2_v.detail}")
    sys.exit(1)


if __name__ == "__main__":
    main()
