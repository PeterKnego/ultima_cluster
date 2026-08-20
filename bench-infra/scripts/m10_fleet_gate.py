#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M10 observable-cluster fleet gate driver — the cross-host driver for the two
fleet-only rows in docs/benchmarks/uc2-m10-gate-2026-08-20.md ("Fleet rows"),
plus a new quiet-on-healthy row added by the 2026-08-20 post-review amendment.
Brief: .superpowers/sdd/2026-08-19-uc2-m10-observable-cluster/fleet-orchestrator-brief.md

WHY THIS IS ITS OWN SCRIPT, not an `--m10` mode of m6_fleet_gate.py: same
reasoning as m9_fleet_gate.py — M10 measures the real `uc2-node` DAEMON
(TOML config, `[metrics]` section) plus a real Prometheus, not a gate
binary's CLI-flag `node` role. What IS shared (ssh, systemd-run, host
classes, leader-wait) is imported from m6_fleet_gate, same as
m5_ab_gate.py/m9_fleet_gate.py.

Design decisions worth disclosing up front (also repeated in the report):

  1. NO RUST `probe` ROLE. Neither `m10_gate` (an in-process-only harness —
     it has no node/service/client CLI roles at all) nor `m5_gate` (which the
     brief names as the service/client role binary) ships a `probe` role the
     way `m6_gate`/`m7_gate`/`m9_gate` do. Rather than add one (a repo
     change, disallowed), this script reads `cnc2.dat` directly with a small
     embedded, dependency-free Python script (`CNC_TOOL_SRC`, shipped to
     each host via the sudo-tee heredoc pattern and run with the system
     `python3`) — exactly the technique the brief already prescribes for row
     2's dual sampler, extended to also serve leader-detection and
     commit-rate windows (rows Q/2/3 all need one or both). Offsets are the
     pinned ones in docs/reference/cnc-page.md: `commit`@448, `node_flags`@768.
     `M10Node.probe()`/`.run_sampler()` present the SAME shape m6_fleet_gate's
     `wait_leader()` expects (`{"leader":..,"can_serve":..}`), so `wait_leader`
     is imported and reused UNCHANGED against `M10Node` instances.
  2. `CONTRACT_SERIES` SOURCE: parsed from `uc2_node/src/obs/metrics.rs` at
     orchestrator start (regex over the `pub const CONTRACT_SERIES: &[&str] =
     &[...]` literal) rather than scraped-and-boundary-matched from a live
     node. This is the ground truth the encoder itself is compiled from, it
     needs no running cluster, and it can't drift from what row Q ends up
     checking within the same run. Disclosed per the brief's "pick one".
  3. PROMETHEUS NEVER REACHABLE FROM THE ORCHESTRATOR DIRECTLY. Prometheus
     listens on `127.0.0.1:9090` on node0 only (never a public bind); the
     security group does not open 9090 (see the SG finding below), and this
     script does not add such a rule — it queries Prometheus's HTTP API by
     shelling `curl` on node0 itself over the existing ssh channel
     (`prom_http`), so no new network exposure is needed at all.
  4. SECURITY GROUP: `bench-infra/terraform/modules/aws/main.tf`'s
     `aws_security_group.bench` already has an ingress rule
     `{from_port=0,to_port=0,protocol="-1",self=true}` — ALL ports/protocols
     between members of the same security group. TCP 9600 (metrics) and UDP
     9100/19100 (peers) are both already covered; no SG change was needed or
     made.
  5. `--local` SMOKE never adjudicates (per the constraints): every row is
     printed with `gated=False` in `--local` mode, matching m9/m10's own
     honest-failure convention that a dev box cannot decide a percentage-point
     bar. Exit code still distinguishes plumbing failure (1) from a clean
     smoke run (0).

Bars (restated from the gate doc's "Fleet rows"; the doc records the actual
run, not this file):
  row Q  quiet-on-healthy   : healthy 3-node cluster >=10 min, Prometheus
                              scraping all 3 nodes @1s w/ uc2-alerts.yml
                              verbatim -> zero firing alerts AND every
                              CONTRACT_SERIES family ingested from all 3
                              instances.                        (FLEET-GATED)
  row 2  probe regime        : K=3 leader kills; on-host dual sampler on every
                              survivor -> zero (readyz==200 && flags==0x01)
                              samples; some node 200 within 5s per kill;
                              union(windowed_samples) > 0 (non-vacuity).
                                                                 (FLEET-GATED)
  row 3  scrape A/B          : >=5 interleaved pairs, fresh daemon cluster per
                              run, arm A = Prometheus stopped, arm B =
                              Prometheus scraping @1s -> median(B/A) >= 0.95.
                              Also: reports_unattested == 0 everywhere.
                                                                 (FLEET-GATED)

Exit 0 iff every gated row passes (only in --fleet mode); exit 1 on FAIL;
exit 2 on usage/environment errors (argparse's own `ap.error` already does
this via SystemExit(2)).
"""

import argparse
import json
import re
import shlex
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402

# SshHost/LocalHost don't reference a module-global APP themselves (unlike
# m6_fleet_gate's own probe()/rate_probe_start(), which this script does NOT
# use — see design decision #1 above), but setting it keeps this module
# consistent with the m9/m5-ab precedent of stamping the app id early.
m6.APP = "m10-gate"

from m6_fleet_gate import LocalHost, SshHost, wait_leader  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

APP = "m10-gate"
PORT = 19100                    # peer UDP port (fleet: private_ip:PORT) — m9's convention
METRICS_PORT = 9600             # fleet: private_ip:METRICS_PORT (never public)
LOCAL_METRICS_BASE = 19600      # local smoke: 127.0.0.1:BASE+i (loopback, distinct per node)
DRAIN_TIMEOUT_SECS = 5
LEADER_WAIT_SECS = 45
SETTLE_SECS = 6

QUIET_SECS_FLEET = 600          # row Q: >= 10 minutes, per the bar
QUIET_SECS_LOCAL = 60           # --local: shortened, SMOKE only

K_KILLS_FLEET = 3               # row 2: repeated kills, union windowed_samples
K_KILLS_LOCAL = 1
SAMPLE_WINDOW = 5.0              # row 2: per-kill sampler window (s)

AB_PAIRS_FLEET = 5              # row 3: interleaved A,B pairs
AB_PAIRS_LOCAL = 1
RATE_WINDOW = 8.0                # row 3: on-host rate-window length (s), x2 -> mean
AB_RATIO_BAR = 0.95
AB_CLIENT_MARGIN_SECS = 30       # load-client lifetime margin over the measured window

PROM_LISTEN = "127.0.0.1:9090"
PROM_VERSION = "3.5.0"
PROM_URL = (f"https://github.com/prometheus/prometheus/releases/download/"
            f"v{PROM_VERSION}/prometheus-{PROM_VERSION}.linux-amd64.tar.gz")

REMOTE_TOOL_PATH = "/opt/bench/m10/cnc_tool.py"

LOCAL_RELEASE = str(Path.home() / ".cache/cargo-target/release")
M5_GATE_LOCAL = f"{LOCAL_RELEASE}/examples/m5_gate"
UC2_NODE_LOCAL = f"{LOCAL_RELEASE}/uc2-node"

REMOTE_RELEASE = "/opt/bench/uc/target/release"
M5_GATE_REMOTE = f"{REMOTE_RELEASE}/examples/m5_gate"
UC2_NODE_REMOTE = f"{REMOTE_RELEASE}/uc2-node"


# ----------------------------------------------------------- the cnc reader
#
# Shipped verbatim to every host (sudo-tee heredoc, m9's pattern) and run
# with the system `python3` — no repo/product-code change, no new Rust, and
# it never touches anything but a read-only mmap of `cnc2.dat` plus a plain
# HTTP GET against the node's OWN `/readyz`. See design decision #1 above.

CNC_TOOL_SRC = r'''#!/usr/bin/env python3
# Embedded by bench-infra/scripts/m10_fleet_gate.py — DO NOT EDIT ON HOST.
# Read-only cnc2.dat mmap reader + a readyz/flags dual sampler.
# Offsets per docs/reference/cnc-page.md.
import argparse
import json
import mmap
import struct
import sys
import time
import urllib.error
import urllib.request

OFF_DURABLE = 320
OFF_COMMIT = 448
OFF_TERM = 704
OFF_FLAGS = 768

NODE_FLAG_LEADER = 1
NODE_FLAG_CAN_SERVE = 2


def read_u64(mm, off):
    return struct.unpack_from("<Q", mm, off)[0]


def snapshot(path):
    with open(path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        try:
            flags = read_u64(mm, OFF_FLAGS)
            return {
                "term": read_u64(mm, OFF_TERM),
                "flags": flags,
                "leader": bool(flags & NODE_FLAG_LEADER),
                "can_serve": bool(flags & NODE_FLAG_CAN_SERVE),
                "commit": read_u64(mm, OFF_COMMIT),
                "durable": read_u64(mm, OFF_DURABLE),
            }
        finally:
            mm.close()


def cmd_probe(args):
    print(json.dumps(snapshot(args.cnc)))


def cmd_rate(args):
    s0 = snapshot(args.cnc)
    t0 = time.monotonic()
    time.sleep(args.secs)
    s1 = snapshot(args.cnc)
    dt = time.monotonic() - t0
    rate = (s1["commit"] - s0["commit"]) / dt if dt > 0 else 0.0
    print(json.dumps({"rate": rate, "commit0": s0["commit"], "commit1": s1["commit"],
                      "secs": dt}))


def cmd_sampler(args):
    # Row 2's dual sampler: GET http://<own bind>/readyz + read own flags in
    # the SAME sample, ~100Hz best-effort for `window` seconds. The endpoint
    # only ever binds a specific address (never 0.0.0.0/loopback-wildcard),
    # so `--http-addr` must be the node's OWN metrics bind (private IP:port
    # on the fleet, 127.0.0.1:port for the local smoke) — reachable from the
    # node's own host either way.
    url = f"http://{args.http_addr}/readyz"
    deadline = time.monotonic() + args.window
    t_start = time.monotonic()
    samples = violations = windowed_samples = 0
    first_200_ms = None
    while time.monotonic() < deadline:
        ts = time.monotonic()
        try:
            with urllib.request.urlopen(url, timeout=0.3) as resp:
                status = resp.getcode()
        except Exception:
            status = 0
        snap = snapshot(args.cnc)
        samples += 1
        is_0x01 = (snap["flags"] & (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE)) == NODE_FLAG_LEADER
        if is_0x01:
            windowed_samples += 1
            if status == 200:
                violations += 1
        if status == 200 and first_200_ms is None:
            first_200_ms = int((time.monotonic() - t_start) * 1000)
        remain = 0.01 - (time.monotonic() - ts)
        if remain > 0:
            time.sleep(remain)
    print(json.dumps({
        "samples": samples,
        "violations": violations,
        "windowed_samples": windowed_samples,
        "first_200_ms": first_200_ms,
    }))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnc", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("probe")
    p.set_defaults(func=cmd_probe)

    p = sub.add_parser("rate")
    p.add_argument("--secs", type=float, required=True)
    p.set_defaults(func=cmd_rate)

    p = sub.add_parser("sampler")
    p.add_argument("--http-addr", required=True)
    p.add_argument("--window", type=float, default=5.0)
    p.set_defaults(func=cmd_sampler)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
'''


# --------------------------------------------------------------- config file

def render_config(node_id, bind, metrics_bind, instance_dir, members):
    """The operator-facing artifact: the real daemon TOML, `[metrics]` added.

    No `[purge]` (M10 doesn't test snapshots/purge — that's M6/M9's job) and
    no `[log]` (constraint: default level).  `[metrics].bind` is the PRIVATE
    IP on the fleet, never a public one — cross-host scraping happens over
    the private network only.
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
        "[metrics]",
        f'bind = "{metrics_bind}"',
        "",
    ]
    for mid, maddr in members:
        lines += ["[[members]]", f"id = {mid}", f'addr = "{maddr}"', ""]
    return "\n".join(lines)


# --------------------------------------------------------------------- node

class M10Node:
    """One node: the real `uc2-node` daemon + m5_gate's service/client roles.

    See the module docstring's design decision #1 for why probing goes
    through the embedded `CNC_TOOL_SRC` rather than a Rust `probe` role.
    """

    def __init__(self, host, node_id, uc2_node_bin, metrics_addr, local):
        self.host = host
        self.id = node_id
        self.uc2_node_bin = uc2_node_bin
        self.metrics_addr = metrics_addr
        self.local = local
        self.cfg_path = f"{Path(host.dir).parent}/n{node_id}.toml"
        self.cnc_path = f"{host.dir}/cnc2.dat"
        self.daemon = None            # local: Popen
        self.unit = f"m10-node{node_id}"
        self.tool_path = (
            str(Path(host.dir).parent / "cnc_tool.py") if local else REMOTE_TOOL_PATH
        )

    # -- config / tool shipping ---------------------------------------------
    def write_config(self, members):
        body = render_config(self.id, self.host.bind_addr_fixed, self.metrics_addr,
                             self.host.dir, members)
        if self.local:
            Path(self.cfg_path).write_text(body)
        else:
            r = self.host._ssh(
                f"sudo mkdir -p {Path(self.cfg_path).parent} && "
                f"sudo tee {self.cfg_path} >/dev/null <<'M10CFG'\n{body}\nM10CFG",
                capture_output=True,
            )
            if r.returncode != 0:
                raise RuntimeError(f"write config on {self.host.public_ip}: {r.stderr}")

    def write_cnc_tool(self):
        if self.local:
            Path(self.tool_path).write_text(CNC_TOOL_SRC)
        else:
            r = self.host._ssh(
                f"sudo mkdir -p {Path(self.tool_path).parent} && "
                f"sudo tee {self.tool_path} >/dev/null <<'M10PY'\n{CNC_TOOL_SRC}\nM10PY",
                capture_output=True,
            )
            if r.returncode != 0:
                raise RuntimeError(f"ship cnc_tool to {self.host.public_ip}: {r.stderr}")

    # -- daemon lifecycle ----------------------------------------------------
    def start_daemon(self):
        if self.local:
            log = open(Path(self.host.logs) / f"{self.unit}.log", "a")
            log.write(f"\n=== start {self.unit} ===\n")
            log.flush()
            self.daemon = subprocess.Popen(
                [self.uc2_node_bin, "--config", self.cfg_path,
                 "--drain-timeout-secs", str(DRAIN_TIMEOUT_SECS)],
                stdout=log, stderr=subprocess.STDOUT,
            )
            return
        self.host._ssh(f"sudo systemctl reset-failed {self.unit} 2>/dev/null; true",
                       capture_output=True)
        cmd = (
            f"sudo systemd-run --unit={self.unit} --collect "
            f"-p StandardOutput=append:/opt/bench/{self.unit}.log "
            f"-p StandardError=append:/opt/bench/{self.unit}.log "
            f"{self.uc2_node_bin} --config {self.cfg_path} "
            f"--drain-timeout-secs {DRAIN_TIMEOUT_SECS}"
        )
        r = self.host._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"start {self.unit} on {self.host.public_ip}: {r.stderr}")

    def kill_daemon(self):
        """SIGKILL — row 2's stimulus (the leader kill)."""
        if self.local:
            if self.daemon and self.daemon.poll() is None:
                self.daemon.kill()
                try:
                    self.daemon.wait(timeout=10)
                except Exception:
                    pass
            return
        self.host._ssh(
            f"sudo systemctl kill --signal=SIGKILL {self.unit} 2>/dev/null; "
            f"sudo systemctl stop {self.unit} 2>/dev/null; "
            f"sudo systemctl reset-failed {self.unit} 2>/dev/null; true",
            capture_output=True,
        )

    def stop_daemon(self):
        """Graceful stop between runs — SIGTERM, falling back to kill."""
        if self.local:
            if self.daemon and self.daemon.poll() is None:
                self.daemon.terminate()
                try:
                    self.daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.daemon.kill()
                    self.daemon.wait(timeout=5)
            return
        self.host._ssh(
            f"sudo systemctl stop {self.unit} 2>/dev/null; "
            f"sudo systemctl reset-failed {self.unit} 2>/dev/null; true",
            capture_output=True,
        )

    # -- gate roles (m5_gate binary: service + client) ------------------------
    def start_service(self):
        self.host.start_unit("service", ["service", "--instance-dir", self.host.dir,
                                         "--app-id", APP])

    def start_load(self, secs):
        self.host.start_unit("load", ["client", "--instance-dir", self.host.dir,
                                      "--app-id", APP, "--secs", str(secs),
                                      "--payload", "64", "--inflight", "256"])

    # -- cnc probing / row-2 sampler (Python on-host tool) --------------------
    def _run_tool(self, args, timeout):
        if self.local:
            out = subprocess.check_output(
                [sys.executable, self.tool_path, "--cnc", self.cnc_path] + args,
                text=True, timeout=timeout,
            )
        else:
            quoted = " ".join(shlex.quote(a) for a in ["--cnc", self.cnc_path] + args)
            r = self.host._ssh(f"python3 {self.tool_path} {quoted}",
                               capture_output=True, timeout=timeout)
            if r.returncode != 0:
                raise RuntimeError(f"cnc_tool on {self.host.public_ip}: {r.stderr}")
            out = r.stdout
        return json.loads(out.strip().splitlines()[-1])

    def probe(self, rate_secs=0.0):
        """Shape-compatible with `SshHost.probe`/`LocalHost.probe` — used
        directly by the imported `wait_leader`."""
        if rate_secs > 0:
            return self._run_tool(["rate", "--secs", str(rate_secs)], timeout=15 + rate_secs)
        return self._run_tool(["probe"], timeout=10)

    def run_sampler(self, window):
        """Row 2: the on-host dual sampler, run via `systemd-run --wait` on
        the fleet (blocks until the transient unit finishes, output captured
        to a file) or a direct blocking subprocess locally."""
        if self.local:
            out = subprocess.check_output(
                [sys.executable, self.tool_path, "--cnc", self.cnc_path,
                 "sampler", "--http-addr", self.metrics_addr, "--window", str(window)],
                text=True, timeout=window + 15,
            )
            return json.loads(out.strip().splitlines()[-1])
        unit = f"m10-sampler-{self.id}-{int(time.time() * 1000)}"
        path = f"/opt/bench/{unit}.json"
        cmd = (
            f"sudo systemd-run --unit={unit} --collect --wait "
            f"-p StandardOutput=file:{path} "
            f"python3 {self.tool_path} --cnc {self.cnc_path} sampler "
            f"--http-addr {self.metrics_addr} --window {window}"
        )
        r = self.host._ssh(cmd, capture_output=True, timeout=window + 30)
        if r.returncode != 0:
            raise RuntimeError(f"sampler on {self.host.public_ip}: {r.stderr}")
        r2 = self.host._ssh(f"sudo cat {path} 2>/dev/null", capture_output=True)
        lines = [line for line in (r2.stdout or "").splitlines() if line.strip()]
        if not lines:
            return {"samples": 0, "violations": 0, "windowed_samples": 0, "first_200_ms": None}
        return json.loads(lines[-1])


# ------------------------------------------------------------- metrics scrape

def scrape_metrics_text(node):
    url = f"http://{node.metrics_addr}/metrics"
    if node.local:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.read().decode(errors="replace")
    r = node.host._ssh(f"curl -sS {url}", capture_output=True, timeout=15)
    if r.returncode != 0:
        raise RuntimeError(f"scrape {url} on {node.host.public_ip} failed: {r.stderr}")
    return r.stdout


def parse_counter(text, name):
    prefix = name + " "
    for line in text.splitlines():
        if line.startswith(prefix):
            try:
                return float(line[len(prefix):].strip())
            except ValueError:
                return None
    return None


def read_reports_unattested(node):
    try:
        text = scrape_metrics_text(node)
    except Exception:
        return 0.0
    return parse_counter(text, "uc2_reports_unattested_total") or 0.0


def load_contract_series(repo_root):
    """See design decision #2: parsed from the Rust source, not scraped."""
    path = repo_root / "uc2_node" / "src" / "obs" / "metrics.rs"
    text = path.read_text()
    m = re.search(r"pub const CONTRACT_SERIES:\s*&\[&str\]\s*=\s*&\[(.*?)\];", text, re.S)
    if not m:
        raise RuntimeError(f"could not locate CONTRACT_SERIES in {path}")
    names = re.findall(r'"([^"]+)"', m.group(1))
    if not names:
        raise RuntimeError(f"CONTRACT_SERIES parsed empty from {path}")
    return names


# --------------------------------------------------------------- prometheus

def prom_paths(a):
    if a.fleet:
        d = "/opt/bench/m10/prom"
        return {"dir": d, "cfg": f"{d}/prometheus.yml", "alerts": f"{d}/uc2-alerts.yml",
                "data": f"{d}/data", "bin": "/opt/bench/prometheus/prometheus"}
    d = str(Path(a.root) / "prom")
    return {"dir": d, "cfg": f"{d}/prometheus.yml", "alerts": f"{d}/uc2-alerts.yml",
            "data": f"{d}/data", "bin": a.prometheus_bin}


def ensure_prometheus_binary(node0, a, pp):
    if a.fleet:
        cmd = (
            f"sudo mkdir -p /opt/bench/prometheus && cd /opt/bench/prometheus && "
            f"if [ ! -x ./prometheus ]; then "
            f"sudo curl -sL -o p.tar.gz {PROM_URL} && "
            f"sudo tar xzf p.tar.gz --strip-components=1 && sudo rm -f p.tar.gz; fi && "
            f"./prometheus --version | head -1"
        )
        r = node0.host._ssh(cmd, capture_output=True, timeout=180)
        if r.returncode != 0 or "prometheus, version" not in (r.stdout or ""):
            raise RuntimeError(f"prometheus binary not ready on node0: {r.stderr or r.stdout}")
        print(f"INFO node0 prometheus: {(r.stdout or '').strip().splitlines()[-1]}", flush=True)
    else:
        p = Path(pp["bin"])
        if not p.is_file():
            raise RuntimeError(
                f"local prometheus binary not found at {p} — download it first: "
                f"curl -sL -o p.tar.gz {PROM_URL} && tar xzf p.tar.gz && "
                f"cp prometheus-{PROM_VERSION}.linux-amd64/prometheus {p}"
            )


def prom_setup_files(nodes, a, pp):
    node0 = nodes[0]
    targets = "\n".join(f"          - {n.metrics_addr}" for n in nodes)
    cfg = (
        "global:\n"
        "  scrape_interval: 1s\n"
        "  evaluation_interval: 1s\n"
        f"rule_files:\n  - {pp['alerts']}\n"
        "scrape_configs:\n"
        "  - job_name: uc2\n"
        "    static_configs:\n"
        "      - targets:\n"
        f"{targets}\n"
    )
    alerts_text = (REPO_ROOT / "packaging" / "prometheus" / "uc2-alerts.yml").read_text()
    if a.fleet:
        node0.host._ssh(f"sudo mkdir -p {pp['dir']} {pp['data']}", capture_output=True)
        r1 = node0.host._ssh(
            f"sudo tee {pp['cfg']} >/dev/null <<'M10PCFG'\n{cfg}\nM10PCFG", capture_output=True)
        r2 = node0.host._ssh(
            f"sudo tee {pp['alerts']} >/dev/null <<'M10PALERT'\n{alerts_text}\nM10PALERT",
            capture_output=True)
        if r1.returncode != 0 or r2.returncode != 0:
            raise RuntimeError(f"write prometheus config on node0 failed: "
                               f"{r1.stderr}{r2.stderr}")
    else:
        Path(pp["dir"]).mkdir(parents=True, exist_ok=True)
        Path(pp["data"]).mkdir(parents=True, exist_ok=True)
        Path(pp["cfg"]).write_text(cfg)
        Path(pp["alerts"]).write_text(alerts_text)


def start_prometheus(node0, a, pp):
    """Returns an opaque handle: `None` on the fleet (systemd owns it), a
    `Popen` locally."""
    if a.fleet:
        node0.host._ssh("sudo systemctl reset-failed m10-prometheus 2>/dev/null; true",
                        capture_output=True)
        cmd = (
            f"sudo systemd-run --unit=m10-prometheus --collect "
            f"-p StandardOutput=append:/opt/bench/m10-prometheus.log "
            f"-p StandardError=append:/opt/bench/m10-prometheus.log "
            f"{pp['bin']} --config.file={pp['cfg']} --storage.tsdb.path={pp['data']} "
            f"--web.listen-address={PROM_LISTEN}"
        )
        r = node0.host._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"start prometheus on node0 failed: {r.stderr}")
        return None
    log = open(Path(pp["dir"]) / "prometheus.log", "a")
    return subprocess.Popen(
        [pp["bin"], f"--config.file={pp['cfg']}", f"--storage.tsdb.path={pp['data']}",
         f"--web.listen-address={PROM_LISTEN}"],
        stdout=log, stderr=subprocess.STDOUT,
    )


def stop_prometheus(node0, a, handle):
    if a.fleet:
        node0.host._ssh(
            "sudo systemctl stop m10-prometheus 2>/dev/null; "
            "sudo systemctl reset-failed m10-prometheus 2>/dev/null; true",
            capture_output=True,
        )
        return
    if handle and handle.poll() is None:
        handle.terminate()
        try:
            handle.wait(timeout=10)
        except subprocess.TimeoutExpired:
            handle.kill()
            handle.wait(timeout=5)


def prom_http(node0, a, path):
    """See design decision #3: always via curl-on-node0-over-ssh on the
    fleet, direct urllib locally — Prometheus is never reachable from the
    orchestrator's own network path."""
    url = f"http://{PROM_LISTEN}{path}"
    if a.fleet:
        r = node0.host._ssh(f"curl -sS '{url}'", capture_output=True, timeout=30)
        if r.returncode != 0:
            raise RuntimeError(f"prometheus query {path} on node0 failed: {r.stderr}")
        return json.loads(r.stdout)
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode())


def prom_query_instant(node0, a, expr):
    data = prom_http(node0, a, f"/api/v1/query?query={urllib.parse.quote(expr)}")
    return data.get("data", {}).get("result", [])


def prom_alerts(node0, a):
    data = prom_http(node0, a, "/api/v1/alerts")
    return data.get("data", {}).get("alerts", [])


def check_series_presence(node0, a, nodes, series_names):
    expected = {n.metrics_addr for n in nodes}
    missing = {}
    for name in series_names:
        try:
            result = prom_query_instant(node0, a, name)
        except Exception as e:
            missing[name] = f"query failed: {e}"
            continue
        got = {r["metric"].get("instance") for r in result}
        gap = expected - got
        if gap:
            missing[name] = sorted(gap)
    return missing


# ----------------------------------------------------------- cluster lifecycle

def start_cluster(nodes, a):
    for n in nodes:
        n.host.reset_dir()
    for n in nodes:
        n.start_daemon()
    time.sleep(2.0 if a.fleet else 1.0)
    for n in nodes:
        n.start_service()


def start_load_all(nodes, secs):
    for n in nodes:
        n.start_load(secs)


def stop_cluster(nodes):
    for n in nodes:
        try:
            n.host.kill_unit("load")
        except Exception:
            pass
    for n in nodes:
        try:
            n.host.kill_unit("service")
        except Exception:
            pass
    for n in nodes:
        try:
            n.stop_daemon()
        except Exception:
            pass


def teardown(nodes, a):
    stop_cluster(nodes)
    if a.fleet:
        nodes[0].host._ssh(
            "sudo systemctl stop m10-prometheus 2>/dev/null; "
            "sudo systemctl reset-failed m10-prometheus 2>/dev/null; true",
            capture_output=True,
        )


# ------------------------------------------------------------------ verdicts

class Verdict:
    def __init__(self, row, passed, detail, gated=True):
        self.row, self.passed, self.detail, self.gated = row, passed, detail, gated


# ------------------------------------------------------------- row Q (quiet)

def row_quiet(nodes, a, secs):
    start_cluster(nodes, a)
    start_load_all(nodes, secs + AB_CLIENT_MARGIN_SECS)
    time.sleep(SETTLE_SECS)

    idx = wait_leader(nodes, list(range(len(nodes))), LEADER_WAIT_SECS)
    if idx is None:
        stop_cluster(nodes)
        return Verdict("Q quiet-on-healthy", False, "no leader elected before the quiet window")

    node0 = nodes[0]
    pp = prom_paths(a)
    ensure_prometheus_binary(node0, a, pp)
    prom_setup_files(nodes, a, pp)
    handle = start_prometheus(node0, a, pp)
    print(f"INFO row Q: leader n{idx} elected; Prometheus scraping all "
          f"{len(nodes)} nodes @1s for {secs}s (uc2-alerts.yml loaded verbatim)", flush=True)
    time.sleep(secs)

    try:
        alerts = prom_alerts(node0, a)
        firing = [al for al in alerts if al.get("state") == "firing"]
        series = load_contract_series(REPO_ROOT)
        missing = check_series_presence(node0, a, nodes, series)
    finally:
        stop_prometheus(node0, a, handle)
        stop_cluster(nodes)

    ok = not firing and not missing
    firing_names = sorted({al.get("labels", {}).get("alertname", "?") for al in firing})
    detail = (
        f"{secs}s @1s scrape, {len(nodes)} nodes; firing alerts={len(firing)} {firing_names}; "
        f"CONTRACT_SERIES coverage: {len(series) - len(missing)}/{len(series)} families present "
        f"on all {len(nodes)} instances"
        + ("" if not missing else f"; MISSING: {missing}")
    )
    return Verdict("Q quiet-on-healthy", ok, detail)


# ------------------------------------------------------------ row 2 (probes)

def row_probes(nodes, a, k, window):
    start_cluster(nodes, a)
    load_secs = SETTLE_SECS + k * (window + 30) + 30
    start_load_all(nodes, load_secs)
    time.sleep(SETTLE_SECS)

    kill_results = []
    total_violations = 0
    total_windowed = 0
    for ki in range(k):
        idx = wait_leader(nodes, list(range(len(nodes))), LEADER_WAIT_SECS)
        if idx is None:
            stop_cluster(nodes)
            return Verdict("2 probe regime", False, f"kill {ki}: no leader elected")
        leader = nodes[idx]
        survivors = [n for j, n in enumerate(nodes) if j != idx]
        print(f"INFO row 2 kill {ki}: SIGKILL leader n{leader.id}", flush=True)
        leader.kill_daemon()

        with ThreadPoolExecutor(max_workers=len(survivors)) as ex:
            futs = {n.id: ex.submit(n.run_sampler, window) for n in survivors}
            results = {nid: f.result() for nid, f in futs.items()}

        viol = sum(r.get("violations", 0) for r in results.values())
        windowed = sum(r.get("windowed_samples", 0) for r in results.values())
        some_200 = any(
            r.get("first_200_ms") is not None and r["first_200_ms"] <= window * 1000
            for r in results.values()
        )
        total_violations += viol
        total_windowed += windowed
        kill_results.append((ki, leader.id, viol, windowed, some_200, results))
        print(f"INFO row 2 kill {ki}: violations={viol} windowed_samples={windowed} "
              f"some_200_within_{window:.0f}s={some_200} detail={results}", flush=True)

        leader.start_daemon()
        leader.start_service()
        time.sleep(2.0)

    stop_cluster(nodes)

    zero_violations = total_violations == 0
    every_kill_had_200 = all(r[4] for r in kill_results)
    nonvacuous = total_windowed > 0
    ok = zero_violations and every_kill_had_200 and nonvacuous
    detail = (
        f"{k} kill(s): total violations={total_violations} (bar 0), "
        f"union windowed_samples={total_windowed} (bar >0, non-vacuity), "
        f"every kill served a 200 within {window:.0f}s={every_kill_had_200}; "
        + "; ".join(
            f"kill{ki}(leader n{lid}): viol={v} windowed={w} some200={s2}"
            for ki, lid, v, w, s2, _ in kill_results
        )
    )
    return Verdict("2 probe regime", ok, detail)


# --------------------------------------------------------------- row 3 (A/B)

def leader_rate_mean(node, window=RATE_WINDOW, reps=2):
    rates = []
    for _ in range(reps):
        try:
            r = node.probe(rate_secs=window)
            rates.append(r.get("rate") or 0.0)
        except Exception:
            rates.append(0.0)
    return sum(rates) / len(rates) if rates else 0.0


def one_ab_run(nodes, a, arm, node0, pp):
    start_cluster(nodes, a)
    client_secs = SETTLE_SECS + int(2 * RATE_WINDOW) + AB_CLIENT_MARGIN_SECS
    start_load_all(nodes, client_secs)
    time.sleep(SETTLE_SECS)

    idx = wait_leader(nodes, list(range(len(nodes))), LEADER_WAIT_SECS)
    if idx is None:
        stop_cluster(nodes)
        return None, "no leader elected"
    leader = nodes[idx]

    handle = None
    try:
        if arm == "B":
            handle = start_prometheus(node0, a, pp)
            time.sleep(2.0)  # let a scrape or two land before measuring
        rate = leader_rate_mean(leader)
        unattested = max(read_reports_unattested(n) for n in nodes)
    finally:
        if arm == "B":
            stop_prometheus(node0, a, handle)
        stop_cluster(nodes)
    return rate, unattested


def row_ab(nodes, a, pairs):
    node0 = nodes[0]
    pp = prom_paths(a)
    ensure_prometheus_binary(node0, a, pp)
    prom_setup_files(nodes, a, pp)

    results = []
    max_unattested = 0.0
    for p in range(1, pairs + 1):
        for arm in ("A", "B"):
            print(f"== row 3 pair {p} arm {arm} ==", flush=True)
            rate, extra = one_ab_run(nodes, a, arm, node0, pp)
            if rate is None:
                return Verdict("3 scrape A/B", False, f"pair {p} arm {arm}: {extra}")
            max_unattested = max(max_unattested, extra)
            results.append((p, arm, rate))
            print(f"   rate={rate:.0f} B/s  reports_unattested={extra:.0f}", flush=True)

    ratios = []
    for p in range(1, pairs + 1):
        a_rate = next(r for pp2, arm, r in results if pp2 == p and arm == "A")
        b_rate = next(r for pp2, arm, r in results if pp2 == p and arm == "B")
        ratio = (b_rate / a_rate) if a_rate else 0.0
        ratios.append(ratio)
        print(f"pair {p}: A={a_rate:.0f} B/s  B={b_rate:.0f} B/s  ratio={ratio:.4f}", flush=True)

    med = statistics.median(ratios) if ratios else 0.0
    ok = med >= AB_RATIO_BAR and max_unattested == 0
    detail = (
        f"{pairs} pair(s), median(B/A)={med:.4f} (bar >= {AB_RATIO_BAR}); "
        f"reports_unattested max across all runs/nodes={max_unattested:.0f} (bar 0); "
        f"per-pair ratios={[round(r, 4) for r in ratios]}"
    )
    return Verdict("3 scrape A/B", ok, detail)


# --------------------------------------------------------------------- setup

def setup_fleet(a):
    hosts = m6.build_fleet_hosts(
        M5_GATE_REMOTE, a.ssh_user, a.ssh_key, a.hosts, count=3,
        unit_prefix="m10", remote_root="/opt/bench/m10",
    )
    for h in hosts:
        h.bind_addr_fixed = f"{h.private_ip}:{PORT}"
        h.prepare(examples=("m10_gate", "m5_gate"), bins=("uc2_node",))
        h._ssh(f"sudo rm -rf {h.dir} && sudo mkdir -p {h.dir}", capture_output=True)

    ucnode = a.uc2_node_bin or UC2_NODE_REMOTE
    nodes = [
        M10Node(h, i, ucnode, f"{h.private_ip}:{METRICS_PORT}", local=False)
        for i, h in enumerate(hosts)
    ]
    members = [(n.id, n.host.bind_addr_fixed) for n in nodes]
    for n in nodes:
        n.write_config(members)
        n.write_cnc_tool()
    return nodes, members


def setup_local(a):
    root = Path(a.root)
    subprocess.run(["rm", "-rf", str(root)], check=False)
    (root / "logs").mkdir(parents=True, exist_ok=True)
    gate = a.m5_gate_bin or M5_GATE_LOCAL
    ucnode = a.uc2_node_bin or UC2_NODE_LOCAL
    for p in (gate, ucnode):
        if not Path(p).is_file():
            sys.exit(f"binary not found: {p} — build it first (cargo build --release "
                     f"--example m5_gate -p uc2_node && cargo build --release -p uc2_node)")

    nodes = []
    for i in range(3):
        d = root / f"n{i}"
        d.mkdir(parents=True, exist_ok=True)
        h = LocalHost(gate, d, root / "logs")
        h.bind_addr_fixed = h.bind_addr()
        metrics_addr = f"127.0.0.1:{LOCAL_METRICS_BASE + i}"
        nodes.append(M10Node(h, i, ucnode, metrics_addr, local=True))

    members = [(n.id, n.host.bind_addr_fixed) for n in nodes]
    for n in nodes:
        n.write_config(members)
        n.write_cnc_tool()
    return nodes, members


# --------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="UC v2 M10 observable-cluster fleet gate driver")
    ap.add_argument("--local", action="store_true", help="local processes (loopback UDP), SMOKE only")
    ap.add_argument("--fleet", action="store_true", help="remote hosts over ssh (adjudicates the gate)")
    ap.add_argument("--row", choices=["q", "probes", "ab", "all"], default="all",
                    help="run one row independently, or all (default)")
    ap.add_argument("--root", default="/home/claude/.cache/m10_fleet",
                    help="local root dir (NEVER /tmp — RAM tmpfs, see CLAUDE.md)")
    ap.add_argument("--hosts", default="", help="fleet: pub/priv,... (else terraform output)")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--uc2-node-bin", default="", help="override the uc2-node daemon binary path")
    ap.add_argument("--m5-gate-bin", default="", help="local only: override the m5_gate binary path")
    ap.add_argument("--prometheus-bin", default=str(Path.home() / ".local/bin/prometheus"),
                    help="local only: path to a prometheus binary (persisted, downloaded once)")
    ap.add_argument("--quiet-secs", type=int, default=0,
                    help="override row-Q duration (0 = default: 600s fleet / 60s local)")
    ap.add_argument("--kills", type=int, default=0,
                    help="override row-2 kill count K (0 = default: 3 fleet / 1 local)")
    ap.add_argument("--pairs", type=int, default=0,
                    help="override row-3 A/B pair count (0 = default: 5 fleet / 1 local)")
    a = ap.parse_args()
    a.prometheus_bin = a.prometheus_bin  # referenced via prom_paths(a) for --local

    if a.local == a.fleet:
        ap.error("pass exactly one of --local / --fleet")
    if a.local and a.root.startswith("/tmp"):
        ap.error("--root must be on a real filesystem (never /tmp — RAM tmpfs)")

    quiet_secs = a.quiet_secs or (QUIET_SECS_FLEET if a.fleet else QUIET_SECS_LOCAL)
    kills = a.kills or (K_KILLS_FLEET if a.fleet else K_KILLS_LOCAL)
    pairs = a.pairs or (AB_PAIRS_FLEET if a.fleet else AB_PAIRS_LOCAL)

    if a.local:
        nodes, members = setup_local(a)
    else:
        nodes, members = setup_fleet(a)

    verdicts = []
    try:
        if a.row in ("q", "all"):
            verdicts.append(row_quiet(nodes, a, quiet_secs))
        if a.row in ("probes", "all"):
            verdicts.append(row_probes(nodes, a, kills, SAMPLE_WINDOW))
        if a.row in ("ab", "all"):
            verdicts.append(row_ab(nodes, a, pairs))
    finally:
        teardown(nodes, a)

    # --local never adjudicates (constraint): every row prints as SMOKE/ungated.
    if a.local:
        for v in verdicts:
            v.gated = False

    print()
    print(f"M10 observable-cluster fleet gate — {'FLEET' if a.fleet else 'LOCAL SMOKE'} "
          f"— row={a.row}")
    for v in verdicts:
        tag = "PASS" if v.passed else "FAIL"
        note = "" if v.gated else " (UNGATED — SMOKE)"
        print(f"  [{tag}]{note} {v.row} — {v.detail}")

    gated = [v for v in verdicts if v.gated]
    if all(v.passed for v in gated):
        if a.fleet:
            print("RESULT: PASS — every pre-committed bar held on the fleet.")
        else:
            print("RESULT: PASS (smoke) — plumbing green; --local never adjudicates the gate "
                 "(docs/benchmarks/uc2-m10-gate-2026-08-20.md, honest-failure protocol).")
        sys.exit(0)
    print("RESULT: FAIL (honest) — a gated row did not hold.")
    sys.exit(1)


if __name__ == "__main__":
    main()
