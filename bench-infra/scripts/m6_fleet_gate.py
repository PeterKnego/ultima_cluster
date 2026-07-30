#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
M6/M7 fleet-gate orchestrator — the cross-host driver the m6_gate/m7_gate
binaries cannot be on their own (their metrics are node-internal; cnc is
same-host shared memory).

Two milestones' worth of scenarios share this one orchestrator (host classes,
the durable-fs guard, and the loadclient/probe plumbing are IDENTICAL between
the two gate binaries — spec §9.6's M7 fleet-gate row explicitly reuses them):

  (default) M6 : a 3-voter + 1-learner cluster; learner-join under load +
                 snapshot-backed purge-cycle reconstruction (unchanged from
                 the M6 doc history — `docs/benchmarks/uc2-m6-gate-*.md`).
  --m7         : a 3-voter + 2-spare-host (5 total) cluster; live single-server
                 reconfiguration under load — spec
                 `docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md` §9.6:
                   1. replace-a-box  : add-learner (5th host) -> catch up ->
                      promote -> remove a (crashed) original voter. Gate:
                      commit-rate dip < DIP_MAX across EVERY transition window
                      (each measured over a fixed MEASURE_WINDOW spanning the
                      transition, same technique as M6's learner-join), zero
                      loadclient divergence.
                   2. resize-3-5-3   : two add+promote pairs (3->5), then two
                      demote+remove-learner pairs (5->3). Same bars.
                   3. leader-self-removal : the serving leader removes itself.
                      Gate: zero committed-high-water loss; new leader serving
                      within SELF_REMOVAL_BUDGET (the measured gap is reported
                      either way — it is the ~200ms failover class, not a dip).
                 Admin ops are driven via the separate `uc2ctl` binary (the
                 same admin cnc-slot protocol `m7_gate`'s in-process scenarios
                 speak directly) — `Host.ctl(...)` shells out to it
                 (subprocess for `--local`, ssh for `--fleet`), mirroring how
                 `Host.probe()` already reads a remote node's cnc.

  --read-profile : NOT a gate — an INSTRUMENT. A 3-host read_profile ladder
                 (`uc2_node/examples/read_profile.rs`; design spec
                 `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`,
                 plan Task 7): one `node` + one `service` per host, the
                 measuring `client` on the leader's host (the query ring is
                 same-host shared memory), sweeping
                 `--readers x {lin,snap} x {0, W}` with a FRESH cluster per
                 rung. The pre-committed decision rule is NEVER re-implemented
                 here: the rung JSON lines the clients print are collected to a
                 file and `read_profile decide --rungs FILE` returns the
                 verdict. Exit 0 even for an unfavourable verdict; exit 1 only
                 on harness failure. See the "read-profile mode" section.

Two host-connectivity modes, SAME scenario logic (both milestones):
  --local  : all nodes are local processes on 127.0.0.1 (loopback UDP, real
             separate processes — validates the orchestrator + is itself a
             stronger proof than the in-process `all`).
  --fleet  : nodes run on remote hosts (one role per host) over their private
             IPs; every start/stop/probe/ctl goes over ssh. Host list from
             `terraform output -json nodes` (or --hosts).

Exit 0 = PASS, exit 1 = honest FAIL (composes in CI / the fleet driver).
"""

import argparse
import json
import shutil
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

APP = "m6-gate"           # M7 mode reassigns this to "m7-gate" in main()
# Extra args appended to every M7 loadclient unit. EMPTY on the fleet — the
# whole point of Fix 4 is that the M7 fleet gate drives M6-level UNTHROTTLED
# load (m7_gate's `--rate` defaults to 0 = no pacing, matching m6_gate's
# loadclient exactly), so the fleet orchestrator passes nothing. main() sets
# a modest pace for `--m7 --local` ONLY: even with the M6 pairing (snapshots +
# purge, `start_node_m7`/`start_service(m7=True)` below) giving a late spare a
# snapshot-install + tail-replay path instead of pure journal replay, on one
# core-starved dev box 5 busy-spinning node processes still leave the
# catch-up path measurably slower than an unthrottled local writer (the
# original finding here, before the M6 pairing landed, was pure replay at
# ~700 records/s vs live ~1000/s — the spare fell monotonically further
# behind). That is a genuine local capacity limit, the same class as the
# fleet-only dip bar — not something a longer budget or promote-retry fixes —
# while the fleet's replay (dedicated 16-core hosts, M5-class pipeline)
# outruns a single synchronous submit client by orders of magnitude.
M7_LOAD_ARGS = []
JOIN_BUDGET = 60.0        # s — learner/spare must catch up within this
DIP_MAX = 10.0            # % — fleet gate: commit-rate dip during a transition
CONVERGE_BUDGET = 10.0    # s — follower reconstruction must converge within this
PURGE_WAIT = 20.0         # s — wait for the purge floor to advance before a cycle
BASELINE_SECS = 8.0       # s — commit-rate baseline window before a transition
MEASURE_WINDOW = 5.0      # s — M7: fixed window a transition's dip is measured over
CONFIG_CONVERGE_BUDGET = 30.0  # s — M7: cluster-wide config-version convergence
SELF_REMOVAL_BUDGET = 10.0     # s — M7: new-leader-serving gap after self-removal

# M6 pairing (fleet-load joiner convergence fix): M7's `resize-3-5-3` learner
# joins after several prior scenarios' worth of unthrottled write history and
# can be outrun by pure journal replay (the documented runbook caveat — a real
# repro during gate authoring left a learner stuck at config v1 while the
# cluster was at v4). `m7_gate`'s node/service roles now carry the SAME
# snapshot/purge machinery `m6_gate` has always run unconditionally, gated
# behind explicit flags so the M7 gate opts in deliberately. Sizes mirror
# `m6_gate.rs`'s own hardcoded `SEGMENT_BYTES`/`SNAPSHOT_INTERVAL_BYTES`
# constants exactly (the M6 fleet gate has never exposed these as CLI flags —
# they are baked into every m6_gate node/service unconditionally).
M7_JOURNAL_SEGMENT_BYTES = 16 * 1024
M7_SNAPSHOT_INTERVAL_BYTES = 32 * 1024

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

    def __init__(self, gate_bin, node_dir, log_dir, ctl_bin=None, probe_bin=None):
        self.gate = gate_bin
        self.dir = str(node_dir)
        self.logs = Path(log_dir)
        self.procs = {}  # unit -> Popen
        self.ctl_bin = ctl_bin  # M7 only: path to the uc2ctl binary
        # Binary that serves the `probe` role. Defaults to the gate binary
        # (M6/M7: same binary owns node/service/probe). --read-profile splits
        # them: `read_profile` has no `probe` role, so the read-profile mode
        # points this at `m7_gate` — its `probe` only opens `cnc2.dat` with an
        # `--app-id` and prints the counters, which is app-agnostic.
        self.probe_bin = probe_bin or gate_bin

    def bind_addr(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        _, port = s.getsockname()
        s.close()
        return f"127.0.0.1:{port}"

    def start_unit(self, unit, args):
        # Kill any process still tracked under this SAME unit key first — M7
        # reuses a host's physical slot across scenarios (a fresh node id
        # each time), and starting a NEW node process for that dir while an
        # OLD one from an earlier, failed/aborted attempt is still holding
        # `instance.lock` would fail with `AlreadyRunning` (a real ordering
        # bug this gate's own local run caught: a failed scenario left its
        # spare's node process orphaned, and the next scenario's reuse of the
        # same host slot collided with it).
        self.kill_unit(unit)
        # Append, not truncate: the same host index (and thus the same log
        # file) is reused across scenarios for successive, DIFFERENT node ids
        # as physical capacity frees up — truncating would silently destroy
        # an earlier generation's diagnostics on every restart of a unit name.
        log = open(self.logs / f"{unit}.log", "a")
        log.write(f"\n=== start_unit {unit} args={args} ===\n")
        log.flush()
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

    def unit_pid(self, unit):
        """PID of a running unit, or None. Read-profile mode only: the client's
        `--node-pid` / `--service-pid` feed a DIAGNOSTIC /proc occupancy sample
        and nothing else — a None here costs the run nothing (the decision rule
        does not use the sample). `start_unit` execs the binary directly, so the
        Popen pid IS the node/service process."""
        p = self.procs.get(unit)
        return None if p is None or p.poll() is not None else p.pid

    def run_foreground(self, args, timeout):
        """Run a NON-daemon role to completion and return `(rc, combined_out)`.
        Distinct from `start_unit`, which fires off a parked daemon and never
        looks at its output: the read-profile `client` role exits on its own and
        its stdout carries the rung JSON line, while a NONZERO exit means the
        rung failed its validity checks and deliberately emitted no such line
        (spec §6 threat 6) — so both halves have to come back to the caller."""
        try:
            r = subprocess.run([self.gate] + args, capture_output=True, text=True,
                               timeout=timeout)
            return r.returncode, (r.stdout or "") + (r.stderr or "")
        except subprocess.TimeoutExpired as e:
            out = (e.stdout or b"").decode(errors="replace") if isinstance(e.stdout, bytes) \
                else (e.stdout or "")
            err = (e.stderr or b"").decode(errors="replace") if isinstance(e.stderr, bytes) \
                else (e.stderr or "")
            return 124, out + err + f"\n[orchestrator] TIMEOUT after {timeout}s\n"

    def divergence_detected(self, unit):
        """Fix 1: the loadclient divergence signal comes from its LOG, not its
        exit code — both m6_gate/m7_gate's loadclient print an unambiguous
        "DIVERGENCE:" line to stderr before exiting 1 on a committed-value
        regression, and grepping that is unaffected by whether the process is
        still alive, was reaped normally (secs elapsed / stop-file), or (fleet)
        already garbage-collected by systemd `--collect` — see the identical
        SshHost method and `SshHost.unit_exit`'s docstring for why exit-code/
        `is-active` checking is the wrong tool here."""
        path = self.logs / f"{unit}.log"
        try:
            return "DIVERGENCE:" in path.read_text(errors="replace")
        except FileNotFoundError:
            return False

    def probe(self, rate_secs=0.0):
        """`rate_secs > 0`: block for that many seconds while the probe itself
        measures an on-host commit rate (see `m7_gate probe --rate-secs`) —
        the returned dict then carries a `"rate"` field. `rate_secs == 0`
        (default): unchanged, instant single-sample read."""
        args = [self.probe_bin, "probe", "--instance-dir", self.dir, "--app-id", APP]
        if rate_secs > 0:
            args += ["--rate-secs", str(rate_secs)]
        out = subprocess.check_output(args, text=True, timeout=15 + rate_secs)
        return json.loads(out.strip().splitlines()[-1])

    def rate_probe_start(self, secs):
        """Fix 2: start an on-host `probe --rate-secs` window in the
        BACKGROUND, writing its one JSON line to a file. The caller runs the
        transition action while this window is open, so the rate this
        measures SPANS the action with zero extra ssh/subprocess round-trip
        skew in the measurement itself (see `dip_for_transition`). Returns an
        opaque handle for `rate_probe_result`."""
        path = self.logs / f"rateprobe-{time.time_ns()}.json"
        f = open(path, "w")
        p = subprocess.Popen(
            [self.probe_bin, "probe", "--instance-dir", self.dir, "--app-id", APP,
             "--rate-secs", str(secs)],
            stdout=f, stderr=subprocess.DEVNULL,
        )
        return (p, f, path)

    def rate_probe_result(self, handle, timeout=60.0):
        """Wait for the background rate-probe window to finish and return its
        measured rate (`None` if it never produced a reading)."""
        p, f, path = handle
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)
        finally:
            f.close()
        try:
            text = path.read_text().strip()
            data = json.loads(text.splitlines()[-1]) if text else {}
            return data.get("rate")
        except (FileNotFoundError, json.JSONDecodeError, IndexError):
            return None
        finally:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def ctl(self, op, node_id=None, addr=None):
        """M7: drive one admin op against THIS host's node via the `uc2ctl`
        binary (same admin cnc-slot protocol `m7_gate`'s in-process scenarios
        speak directly). Returns `(returncode, combined_output)` — 0 means
        accepted; the caller decides how to react to a refusal/timeout."""
        args = [self.ctl_bin, op, "--instance-dir", self.dir, "--app-id", APP]
        if node_id is not None:
            args += ["--id", str(node_id)]
        if addr is not None:
            args += ["--addr", addr]
        r = subprocess.run(args, capture_output=True, text=True, timeout=20)
        return r.returncode, (r.stdout + r.stderr)

    def reset_dir(self):
        """M7: wipe this host's instance dir before it takes on a BRAND-NEW
        node id — a real, previously-hit bug: a freed host's dir still holds
        the PRIOR id's durable state (journal, `state/config.state` with that
        old id's ConfigRecord/tombstones/vote history), and a new id must
        never inherit it (the runbook's fresh-id rule, generalized: fresh id
        implies fresh instance dir too, even when the physical host — or,
        here, the local dir slot — is reused). No-op-safe on a dir that was
        never used yet (`build_local_hosts` already created it empty)."""
        shutil.rmtree(self.dir, ignore_errors=True)
        Path(self.dir).mkdir(parents=True, exist_ok=True)

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

    def __init__(self, gate_bin, node_dir, public_ip, private_ip, ssh_user, ssh_key,
                 ctl_bin=None, unit_prefix="m6", remote_root="/opt/bench/m6",
                 probe_bin=None):
        self.gate = gate_bin           # path to the gate binary ON the remote host
        self.dir = str(node_dir)       # instance dir ON the remote host
        self.public_ip = public_ip
        self.private_ip = private_ip
        self.target = f"{ssh_user}@{public_ip}"
        self.ssh = ["ssh", "-o", "StrictHostKeyChecking=accept-new",
                    "-o", "BatchMode=yes", "-i", ssh_key]
        self.ctl_bin = ctl_bin          # M7 only: path to uc2ctl on the remote host
        self.unit_prefix = unit_prefix  # systemd unit / log-file name prefix
        self.remote_root = remote_root  # instance-dir parent on the remote host
        # See `LocalHost.__init__` for why this is separable from `gate`.
        self.probe_bin = probe_bin or gate_bin

    def _ssh(self, cmd, **kw):
        # SSH_AUTH_SOCK is left to the caller's env (unset it before running the
        # orchestrator — a forwarded agent hangs ssh here, bench-infra gotcha).
        return subprocess.run(self.ssh + [self.target, cmd], text=True, **kw)

    def prepare(self, examples=("m6_gate",), bins=()):
        """Build the given examples (release builds no examples by default),
        create the instance-dir parent, and report its filesystem type (the
        journal lives under the instance dir — a tmpfs here would silently void
        every durability claim, so the caller hard-fails on volatile fs types).
        Idempotent; ~9 s on a warm target per example."""
        example_flags = " ".join(f"--example {e}" for e in examples)
        env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
        # `uc2ctl` is its own workspace crate (a real bin, not a uc2_node
        # example), so it needs a second `-p` build; its artifact lands in
        # target/release/ rather than target/release/examples/.
        bin_build = (
            f" && {env} {self.CARGO} build --release "
            f"--manifest-path {self.UC_SRC}/Cargo.toml "
            + " ".join(f"-p {b}" for b in bins)
        ) if bins else ""
        r = self._ssh(
            f"{env} "
            f"{self.CARGO} build --release {example_flags} "
            f"--manifest-path {self.UC_SRC}/Cargo.toml -p uc2_node "
            f"{bin_build} "
            f"&& sudo mkdir -p {self.remote_root} "
            f"&& echo FSTYPE=$(stat -f -c %T {self.remote_root}) && echo PREPARED",
            capture_output=True,
        )
        out = r.stdout or ""
        if "PREPARED" not in out:
            raise RuntimeError(f"prepare {self.public_ip} failed: {r.stderr or out}")
        fstype = next(
            (l.split("=", 1)[1] for l in out.splitlines() if l.startswith("FSTYPE=")), ""
        )
        assert_durable_fs(fstype, f"{self.remote_root} (instance-dir parent)", self.public_ip)

    def bind_addr(self):
        # Fleet nodes bind their PRIVATE NIC IP (the cross-host route) on a fixed
        # port — one node per host, so no port contention.
        return f"{self.private_ip}:19100"

    def start_unit(self, unit, args):
        # Kill any unit still active under this SAME name first — M7 reuses a
        # host's physical slot across scenarios for successive, different node
        # ids, and `systemd-run --unit=X` on an already-active `X` fails. See
        # the identical comment/fix on `LocalHost.start_unit`.
        self.kill_unit(unit)
        # systemd-run --collect (transient unit, cleaned up on stop); the gate role
        # parks, so it stays until we stop it. TimeoutStopSec=1 — parked gate bins
        # ignore SIGTERM (M5 finding). Args are single-quoted.
        quoted = " ".join(f"'{a}'" for a in args)
        cmd = (
            f"sudo systemd-run --unit={self.unit_prefix}-{unit} --collect -p TimeoutStopSec=1 "
            f"-p StandardOutput=append:/opt/bench/{self.unit_prefix}-{unit}.log "
            f"-p StandardError=append:/opt/bench/{self.unit_prefix}-{unit}.log "
            f"{self.gate} {quoted}"
        )
        r = self._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"start {self.unit_prefix}-{unit} on {self.public_ip} failed: {r.stderr}")

    def kill_unit(self, unit):
        self._ssh(
            f"sudo systemctl kill --signal=SIGKILL {self.unit_prefix}-{unit} 2>/dev/null; "
            f"sudo systemctl stop {self.unit_prefix}-{unit} 2>/dev/null; true",
            capture_output=True,
        )

    def unit_exit(self, unit):
        """Best-effort ACTUAL exit status, distinct from `systemctl is-active`
        (which reports "not active" — i.e. this used to return `1` — for ANY
        stopped unit, including one this harness itself stopped cleanly via
        `stop-file`/teardown; that false-positive was 4 of the fleet's FAILs).
        `None` = still running, OR the transient `--collect` unit was already
        garbage-collected and its exit status is simply unknowable — callers
        that need a real signal after a unit may have exited should NOT rely
        on this; the loadclient divergence guard uses `divergence_detected`
        (log-based) instead precisely because of this ambiguity."""
        r = self._ssh(
            f"systemctl show -p ActiveState,ExecMainStatus,Result --value "
            f"{self.unit_prefix}-{unit} 2>/dev/null",
            capture_output=True,
        )
        lines = (r.stdout or "").splitlines()
        if len(lines) < 3:
            return None
        active_state, exec_main_status, result = lines[0], lines[1], lines[2]
        if active_state == "active":
            return None
        if result == "success":
            return 0
        try:
            code = int(exec_main_status)
        except ValueError:
            return None
        return code

    def unit_pid(self, unit):
        """See `LocalHost.unit_pid` (diagnostic-only PIDs). systemd's MainPID is
        the `ExecStart` process — i.e. the gate binary itself — and reads 0 for
        a unit that is not running."""
        r = self._ssh(
            f"systemctl show -p MainPID --value {self.unit_prefix}-{unit} 2>/dev/null",
            capture_output=True,
        )
        try:
            pid = int((r.stdout or "").strip().splitlines()[-1])
        except (ValueError, IndexError):
            return None
        return pid or None

    def run_foreground(self, args, timeout):
        """See `LocalHost.run_foreground`. Fleet equivalent: a plain BLOCKING
        ssh command, not `systemd-run` — the read-profile `client` role runs for
        `--secs` and exits, so it is the same shape as `probe --rate-secs`
        (which already blocks for its whole window over ssh). systemd-run is
        only needed for the roles that PARK forever, where a bare `ssh ... &`
        hangs the connection."""
        quoted = " ".join(f"'{a}'" for a in args)
        try:
            r = self._ssh(f"sudo {self.gate} {quoted}", capture_output=True, timeout=timeout)
            return r.returncode, (r.stdout or "") + (r.stderr or "")
        except subprocess.TimeoutExpired as e:
            out = e.stdout if isinstance(e.stdout, str) else (e.stdout or b"").decode(errors="replace")
            err = e.stderr if isinstance(e.stderr, str) else (e.stderr or b"").decode(errors="replace")
            return 124, (out or "") + (err or "") + f"\n[orchestrator] TIMEOUT after {timeout}s\n"

    def divergence_detected(self, unit):
        """See `LocalHost.divergence_detected` — same log-grep signal, over
        ssh, against the unit's append-log file (the same path `start_unit`
        wires as this unit's `StandardOutput`/`StandardError`)."""
        path = f"/opt/bench/{self.unit_prefix}-{unit}.log"
        r = self._ssh(f"grep -q 'DIVERGENCE:' {path} 2>/dev/null", capture_output=True)
        return r.returncode == 0

    def probe(self, rate_secs=0.0):
        cmd = f"sudo {self.probe_bin} probe --instance-dir {self.dir} --app-id {APP}"
        if rate_secs > 0:
            cmd += f" --rate-secs {rate_secs}"
        r = self._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"probe {self.public_ip} failed: {r.stderr}")
        return json.loads(r.stdout.strip().splitlines()[-1])

    def rate_probe_start(self, secs):
        """See `LocalHost.rate_probe_start`. Fleet equivalent: a transient
        `systemd-run` unit whose stdout is redirected straight to a file on
        the remote host (`StandardOutput=file:...`) — started, then left
        running in the background while the caller drives the transition."""
        unit = f"{self.unit_prefix}-rateprobe"
        path = f"/opt/bench/{unit}.json"
        cmd = (
            f"sudo rm -f {path}; "
            f"sudo systemd-run --unit={unit} --collect -p StandardOutput=file:{path} "
            f"{self.probe_bin} probe --instance-dir {self.dir} --app-id {APP} --rate-secs {secs}"
        )
        r = self._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"rate-probe start on {self.public_ip} failed: {r.stderr}")
        return (unit, path)

    def rate_probe_result(self, handle, timeout=60.0):
        unit, path = handle
        dl = time.time() + timeout
        while time.time() < dl:
            r = self._ssh(f"systemctl is-active {unit}", capture_output=True)
            if r.stdout.strip() != "active":
                break
            time.sleep(0.3)
        r = self._ssh(f"sudo cat {path} 2>/dev/null", capture_output=True)
        lines = [l for l in (r.stdout or "").strip().splitlines() if l.strip()]
        if not lines:
            return None
        try:
            return json.loads(lines[-1]).get("rate")
        except json.JSONDecodeError:
            return None

    def ctl(self, op, node_id=None, addr=None):
        """M7: drive one admin op against THIS host's node via the remote
        `uc2ctl` binary over ssh+sudo — same shape as `probe()`."""
        cmd = f"sudo {self.ctl_bin} {op} --instance-dir {self.dir} --app-id {APP}"
        if node_id is not None:
            cmd += f" --id {node_id}"
        if addr is not None:
            cmd += f" --addr {addr}"
        r = self._ssh(cmd, capture_output=True)
        return r.returncode, ((r.stdout or "") + (r.stderr or ""))

    def reset_dir(self):
        """M7: wipe this host's instance dir before it takes on a BRAND-NEW
        node id — see the identical `LocalHost.reset_dir` doc/comment for why
        this matters (a real bug this gate's own local run caught)."""
        self._ssh(f"sudo rm -rf {self.dir} && sudo mkdir -p {self.dir}", capture_output=True)

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


def start_node_m7(host, node_id, members, bind_addr):
    # m7_gate's `node` role has no `--learners` flag (M7 spares are never in a
    # static learners list — they are admitted live via `add-learner`; see the
    # m7_gate module doc). `bind_addr` MUST be the exact address already
    # registered with the cluster (the `uc2ctl add-learner --addr` argument),
    # computed ONCE by the caller — `LocalHost.bind_addr()` mints a FRESH
    # ephemeral port on every call, so calling it again here would silently
    # bind a different port than the one the voters were just told about (a
    # real bug this gate's own local run caught: the spare's node process
    # started and ran fine, but sat at config_version=0 forever because the
    # cluster was sending its replication data to a port nothing was
    # listening on).
    #
    # M6 pairing: always pass `--purge-below-snapshot` +
    # `--journal-segment-bytes` for the M7 gate (this function is M7-only —
    # M6 uses `start_node`) — see the M7_JOURNAL_SEGMENT_BYTES module comment.
    # Every M7 node runs with purge on, original 3 voters included, since the
    # journal a LATE joiner needs purged-below is the ORIGINAL voters', not
    # just a spare's.
    args = [
        "node", "--id", str(node_id), "--bind", bind_addr,
        "--instance-dir", host.dir, "--members", members, "--app-id", APP,
        "--purge-below-snapshot",
        "--journal-segment-bytes", str(M7_JOURNAL_SEGMENT_BYTES),
    ]
    host.start_unit("node", args)


def start_service(host, m7=False):
    args = ["service", "--instance-dir", host.dir, "--app-id", APP]
    if m7:
        # M6 pairing (M7 only — m6_gate's `service` role has no such flag; it
        # runs with snapshots unconditionally instead). See the
        # M7_SNAPSHOT_INTERVAL_BYTES module comment.
        args += ["--snapshot-interval-bytes", str(M7_SNAPSHOT_INTERVAL_BYTES)]
    host.start_unit("service", args)


def run_gate(hosts, voters, learner, members, learners, cycles, stop_file):
    verdicts = []

    # Clean room (same rationale as run_gate_m7): never inherit durable state
    # from a prior run on these hosts.
    for h in hosts:
        h.reset_dir()

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

    # Loadclient divergence guard (Fix 1): it prints an unambiguous
    # "DIVERGENCE:" line to its log before exiting 1 on a committed-value
    # regression — checking the LOG rather than the exit code, because
    # `is-active`-based exit-status checking false-positives on ANY normally-
    # stopped unit (including this one, once teardown's stop-file signal
    # lands and it exits 0).
    if hosts[leader].divergence_detected("loadclient"):
        log("FAIL: loadclient log shows DIVERGENCE — committed-value regression detected")
        verdicts.append(("divergence-guard", False, "loadclient log contains DIVERGENCE:"))

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


# ------------------------------------------------------------- M7 scenarios
#
# 5 hosts total: 3 fixed-address voters (indices 0-2, monkeypatched to a
# stable `bind_addr` in `main()` exactly like M6's 4) + 2 SPARE host slots
# (indices 3-4) that get reused across scenarios as physical capacity frees
# up — e.g. `replace-a-box` crashes an ORIGINAL voter, which frees that
# physical host for `resize-3-5-3`'s own growth. Ids are fresh-forever (spec
# §3: a removed id can never reappear), so `M7Cluster.fresh_id()` hands out a
# brand-new id for every add, never reusing 0/1/2 or any earlier spare id even
# when the underlying HOST is reused.

class M7Cluster:
    """Tracks the orchestrator's own view of "who is currently a voter, on
    which physical host, at which address" across the three scenarios — the
    thing `m7_gate`'s in-process `all` smoke gets for free from a single Rust
    process's own `Vec<NodeH>`, but the fleet orchestrator must track itself
    since each node is a genuinely separate OS process/host."""

    def __init__(self, addr0, addr1, addr2):
        self.voters = {0: 0, 1: 1, 2: 2}  # node_id -> host index
        self.addr = {0: addr0, 1: addr1, 2: addr2}  # node_id -> "ip:port"
        self.next_id = 10
        self.version = 0  # cluster-wide config version this orchestrator expects

    def members_str(self):
        return ",".join(f"{nid}@{self.addr[nid]}" for nid in sorted(self.voters))

    def fresh_id(self):
        i = self.next_id
        self.next_id += 1
        return i

    def free_host_index(self, n_hosts):
        used = set(self.voters.values())
        for i in range(n_hosts):
            if i not in used:
                return i
        raise RuntimeError("no free host slot")


def _safe_probe(host):
    try:
        return host.probe()
    except Exception:
        return {}


def dip_for_transition(leader_host, action_fn, baseline_secs=BASELINE_SECS, window=MEASURE_WINDOW):
    """The M6 learner-join dip technique, generalized to an arbitrary
    `action_fn`.

    Fix 2 (measurement-skew fix): both the baseline and the during-transition
    rate are now measured ENTIRELY ON THE HOST, via `m7_gate probe
    --rate-secs` — the probe process itself reads the cnc commit counter,
    sleeps locally, reads it again, and computes `delta/secs` before ever
    touching the network. The OLD technique bracketed two REMOTE commit reads
    with LOCAL wall-clock around each `probe()` ssh+sudo round trip
    (~0.4-0.8s on the fleet): the numerator (commit delta) was correct but the
    denominator (elapsed wall-clock) included that RTT twice, systematically
    UNDERcounting the rate by ~8-16% — worse for voter-set transitions, whose
    convergence-polling loop (`bump_and_await_config`) issues many more ssh
    calls per window. On-host 390Hz sampling proved the cluster itself never
    stalled >=200ms during any transition; the "dip" was this skew, not a
    real quorum stall.

    baseline: one blocking `probe(rate_secs=baseline_secs)` ssh exec — a
    single round trip regardless of `baseline_secs`, so its relative skew is
    negligible.
    during: a rate-probe window STARTED on-host in the BACKGROUND
    (`rate_probe_start`, `window` secs) ~0.5s before `action_fn` runs, so it
    is already sampling when the transition begins, then read after
    `action_fn` returns (`rate_probe_result` blocks until that background
    window finishes). Zero ssh RTT inside the measured interval itself.

    Returns `(dip_pct, during_rate, baseline_rate, error_or_None)` —
    `action_fn` raising is caught and reported as the error, not propagated
    (a scenario needs to still print a dip/rate reading even when the
    transition itself failed)."""
    p = leader_host.probe(rate_secs=baseline_secs)
    baseline_rate = p.get("rate") or 0.0

    handle = leader_host.rate_probe_start(window)
    time.sleep(0.5)
    err = None
    try:
        action_fn()
    except Exception as e:  # noqa: BLE001 - reported to the caller, not swallowed
        err = str(e)
    rate = leader_host.rate_probe_result(handle, timeout=window + 30.0)
    if rate is None:
        rate = 0.0
        err = err or "on-host rate-probe window produced no reading"

    dip = max(0.0, (baseline_rate - rate) / baseline_rate * 100.0) if baseline_rate > 0 else 100.0
    return dip, rate, baseline_rate, err


def bump_and_await_config(cluster, hosts, host_idxs, budget=CONFIG_CONVERGE_BUDGET):
    """Optimistically bump the orchestrator's own expected config version by 1
    (every successful admin op advances the wire version by exactly 1 — spec
    §3) and poll `host_idxs` until every one reports it via `probe()`."""
    cluster.version += 1
    v = cluster.version
    dl = time.time() + budget
    last = {}
    while time.time() < dl:
        try:
            last = {i: hosts[i].probe().get("config_version") for i in host_idxs}
            if all((last.get(i) or -1) >= v for i in host_idxs):
                return True
        except Exception as e:
            last = {"probe_error": str(e)}
        # 0.5s, not tighter: this loop spawns a `probe` SUBPROCESS per host
        # per iteration — on a core-starved local box, polling much faster
        # directly competes with the busy-spin consensus threads it is trying
        # to observe, artificially slowing convergence (a real effect this
        # gate's own local run hit: tighter polling here measurably worsened
        # both convergence time AND the dip numbers `dip_for_transition`
        # reports, since the poll subprocess overhead eats the same 4 cores).
        time.sleep(0.5)
    log(f"config v{v} never converged on host_idxs={host_idxs}; last observed: {last}")
    return False


PROMOTE_RETRY_BUDGET = 30.0  # s — bounded retry for a promote hitting NotCaughtUp


def ctl_retry_transient(host, op, node_id=None, addr=None, budget=PROMOTE_RETRY_BUDGET,
                         retry_substr="NotCaughtUp"):
    """Fix 4: retry `host.ctl(op, ...)` while it is refused specifically for
    `retry_substr` (default `NotCaughtUp`) — a live cluster's legitimate,
    transient precondition refusal, not a bug. Any OTHER refusal propagates
    immediately (same as a single-shot `ctl()` call), so a genuine bug still
    fails fast.

    Needed now that the loadclient runs unthrottled at M6's pace (Fix 4): the
    orchestrator's own "spare caught up" check reads the SPARE's own reported
    `durable` counter, which can be a beat ahead of what the LEADER itself has
    heard from that peer (the actual signal `NotCaughtUp` gates on) — under a
    fast writer, enough commits can land in that gap to make the very next
    `promote` call observe the peer as still-behind. The old 3ms-per-write
    throttle papered over this race by keeping the gap small; the real fix is
    this bounded retry (mirrors `m7_gate.rs`'s own in-process
    `admin_request_ok`, which already tolerates reasons 2/3/10)."""
    dl = time.time() + budget
    while True:
        rc, out = host.ctl(op, node_id=node_id, addr=addr)
        if rc == 0 or retry_substr not in out:
            return rc, out
        if time.time() >= dl:
            return rc, out
        time.sleep(0.5)


def scenario_replace_a_box(hosts, cluster):
    voter_idxs = list(cluster.voters.values())
    leader_hidx = wait_leader(hosts, voter_idxs, 20)
    if leader_hidx is None:
        return ("replace-a-box", False, "no leader elected at scenario start")
    leader_host = hosts[leader_hidx]
    leader_id = next(nid for nid, h in cluster.voters.items() if h == leader_hidx)

    spare_hidx = cluster.free_host_index(len(hosts))
    spare_host = hosts[spare_hidx]
    new_id = cluster.fresh_id()
    addr = spare_host.bind_addr()

    dips = []

    def add_and_catchup():
        rc, out = leader_host.ctl("add-learner", new_id, addr)
        if rc != 0:
            raise RuntimeError(f"add-learner refused: {out.strip()}")
        cluster.addr[new_id] = addr
        spare_host.reset_dir()  # this host slot may be a REUSED, previously-removed id's dir
        start_node_m7(spare_host, new_id, cluster.members_str(), addr)
        time.sleep(2.0)
        start_service(spare_host, m7=True)
        if not bump_and_await_config(cluster, hosts, voter_idxs + [spare_hidx], JOIN_BUDGET):
            raise RuntimeError("add-learner never converged cluster-wide")
        target = leader_host.probe()["commit"]
        dl = time.time() + JOIN_BUDGET
        while time.time() < dl:
            if _safe_probe(spare_host).get("durable", -1) >= target:
                return
            time.sleep(0.5)  # subprocess-spawn probe poll — see bump_and_await_config's comment
        raise RuntimeError("spare never caught up within JOIN_BUDGET")

    d, _, _, err = dip_for_transition(leader_host, add_and_catchup)
    dips.append(("add+catchup", d))
    if err:
        return ("replace-a-box", False, f"add-learner/catch-up failed: {err}")

    def promote():
        rc, out = ctl_retry_transient(leader_host, "promote", new_id)
        if rc != 0:
            raise RuntimeError(f"promote refused: {out.strip()}")
        cluster.voters[new_id] = spare_hidx
        if not bump_and_await_config(cluster, hosts, list(cluster.voters.values()), CONFIG_CONVERGE_BUDGET):
            raise RuntimeError("promote never converged cluster-wide")

    d, _, _, err = dip_for_transition(leader_host, promote)
    dips.append(("promote", d))
    if err:
        return ("replace-a-box", False, f"promote failed: {err}")

    # Remove one of the ORIGINAL voters (never the leader, never the just-added
    # spare) — the "replace a dead box" half of the recipe.
    victim_id = next(nid for nid in cluster.voters if nid not in (leader_id, new_id))
    victim_hidx = cluster.voters[victim_id]

    def crash_and_remove():
        hosts[victim_hidx].kill_unit("node")
        hosts[victim_hidx].kill_unit("service")
        rc, out = leader_host.ctl("remove-voter", victim_id)
        if rc != 0:
            raise RuntimeError(f"remove-voter refused: {out.strip()}")
        del cluster.voters[victim_id]
        if not bump_and_await_config(cluster, hosts, list(cluster.voters.values()), CONFIG_CONVERGE_BUDGET):
            raise RuntimeError("remove-voter never converged cluster-wide")

    d, _, _, err = dip_for_transition(leader_host, crash_and_remove)
    dips.append(("remove", d))
    if err:
        return ("replace-a-box", False, f"remove-voter failed: {err}")

    worst = max(d for _, d in dips)
    ok = worst < DIP_MAX
    detail = (
        f"add-learner {new_id} -> catch-up -> promote -> remove {victim_id} "
        f"(new voter set host-ids {sorted(cluster.voters)}); per-transition dip: "
        + ", ".join(f"{name}={d:.1f}%" for name, d in dips)
        + f" (gate <{DIP_MAX:.0f}%)"
    )
    log(f"[{'PASS' if ok else 'FAIL'}] replace-a-box — {detail}")
    return ("replace-a-box", ok, detail)


def scenario_resize_3_5_3(hosts, cluster):
    voter_idxs = list(cluster.voters.values())
    leader_hidx = wait_leader(hosts, voter_idxs, 20)
    if leader_hidx is None:
        return ("resize-3-5-3", False, "no leader elected at scenario start")
    leader_host = hosts[leader_hidx]
    base_voters = dict(cluster.voters)  # the set this scenario must return to

    dips = []
    grown_ids = []

    for _ in range(2):
        spare_hidx = cluster.free_host_index(len(hosts))
        spare_host = hosts[spare_hidx]
        new_id = cluster.fresh_id()
        addr = spare_host.bind_addr()
        grown_ids.append(new_id)

        def add_and_catchup(spare_host=spare_host, new_id=new_id, addr=addr, spare_hidx=spare_hidx):
            rc, out = leader_host.ctl("add-learner", new_id, addr)
            if rc != 0:
                raise RuntimeError(f"add-learner {new_id} refused: {out.strip()}")
            cluster.addr[new_id] = addr
            spare_host.reset_dir()  # this host slot may be a REUSED, previously-removed id's dir
            start_node_m7(spare_host, new_id, cluster.members_str(), addr)
            time.sleep(2.0)
            start_service(spare_host, m7=True)
            if not bump_and_await_config(cluster, hosts, list(cluster.voters.values()) + [spare_hidx], JOIN_BUDGET):
                raise RuntimeError(f"add-learner {new_id} never converged cluster-wide")
            target = leader_host.probe()["commit"]
            dl = time.time() + JOIN_BUDGET
            while time.time() < dl:
                if _safe_probe(spare_host).get("durable", -1) >= target:
                    return
                time.sleep(0.5)  # subprocess-spawn probe poll — see bump_and_await_config's comment
            raise RuntimeError(f"spare {new_id} never caught up within JOIN_BUDGET")

        d, _, _, err = dip_for_transition(leader_host, add_and_catchup)
        dips.append((f"add-{new_id}", d))
        if err:
            return ("resize-3-5-3", False, f"add-learner {new_id}/catch-up failed: {err}")

        def promote(new_id=new_id, spare_hidx=spare_hidx):
            rc, out = ctl_retry_transient(leader_host, "promote", new_id)
            if rc != 0:
                raise RuntimeError(f"promote {new_id} refused: {out.strip()}")
            cluster.voters[new_id] = spare_hidx
            if not bump_and_await_config(cluster, hosts, list(cluster.voters.values()), CONFIG_CONVERGE_BUDGET):
                raise RuntimeError(f"promote {new_id} never converged cluster-wide")

        d, _, _, err = dip_for_transition(leader_host, promote)
        dips.append((f"promote-{new_id}", d))
        if err:
            return ("resize-3-5-3", False, f"promote {new_id} failed: {err}")

    # Now 5 voters. Shrink back: demote+remove-learner for each grown id.
    for gid in grown_ids:
        def demote(gid=gid):
            rc, out = leader_host.ctl("demote", gid)
            if rc != 0:
                raise RuntimeError(f"demote {gid} refused: {out.strip()}")
            if not bump_and_await_config(cluster, hosts, list(cluster.voters.values()), CONFIG_CONVERGE_BUDGET):
                raise RuntimeError(f"demote {gid} never converged cluster-wide")

        d, _, _, err = dip_for_transition(leader_host, demote)
        dips.append((f"demote-{gid}", d))
        if err:
            return ("resize-3-5-3", False, f"demote {gid} failed: {err}")

        def remove(gid=gid):
            rc, out = leader_host.ctl("remove-learner", gid)
            if rc != 0:
                raise RuntimeError(f"remove-learner {gid} refused: {out.strip()}")
            del cluster.voters[gid]
            remaining = list(cluster.voters.values())
            if not bump_and_await_config(cluster, hosts, remaining, CONFIG_CONVERGE_BUDGET):
                raise RuntimeError(f"remove-learner {gid} never converged cluster-wide")

        d, _, _, err = dip_for_transition(leader_host, remove)
        dips.append((f"remove-{gid}", d))
        if err:
            return ("resize-3-5-3", False, f"remove-learner {gid} failed: {err}")

    if cluster.voters != base_voters:
        return (
            "resize-3-5-3", False,
            f"final voter set {cluster.voters} != the set this scenario started with {base_voters}",
        )

    worst = max(d for _, d in dips)
    ok = worst < DIP_MAX
    detail = (
        f"3->5 (add+promote {grown_ids[0]}, add+promote {grown_ids[1]}) -> "
        f"5->3 (demote+remove {grown_ids[0]}, demote+remove {grown_ids[1]}); "
        f"final voter set == the set this scenario started with; per-transition dip: "
        + ", ".join(f"{name}={d:.1f}%" for name, d in dips)
        + f" (gate <{DIP_MAX:.0f}%)"
    )
    log(f"[{'PASS' if ok else 'FAIL'}] resize-3-5-3 — {detail}")
    return ("resize-3-5-3", ok, detail)


def scenario_leader_self_removal(hosts, cluster, stop_file):
    voter_idxs = list(cluster.voters.values())
    leader_hidx = wait_leader(hosts, voter_idxs, 20)
    if leader_hidx is None:
        return ("leader-self-removal", False, "no leader elected at scenario start")
    leader_host = hosts[leader_hidx]
    leader_id = next(nid for nid, h in cluster.voters.items() if h == leader_hidx)

    high_water = max((_safe_probe(hosts[h]).get("commit", 0) for h in voter_idxs), default=0)
    t0 = time.time()
    rc, out = leader_host.ctl("remove-voter", leader_id)
    if rc != 0:
        return ("leader-self-removal", False, f"self-removal refused: {out.strip()}")

    survivor_hidxs = [h for nid, h in cluster.voters.items() if nid != leader_id]
    # Fix 3: probe ONLY the old leader + survivors (== voter_idxs; never the
    # unrelated free/spare host slots), and probe them CONCURRENTLY — the
    # previous version made 2-3 SEQUENTIAL ssh probes per "0.1s-intended"
    # iteration (~1-2s real per iteration on the fleet), so the measured
    # `gap` was mostly ssh-RTT observation granularity, not real failover
    # time. With a thread pool, one iteration costs ~1 ssh RTT (the slowest
    # probe), not the sum — the measurement ceiling is now ~1 ssh RTT
    # (~0.4-0.8s on the fleet), not several.
    probe_targets = [leader_hidx] + survivor_hidxs
    regressed = False
    new_leader_hidx = None
    dl = time.time() + SELF_REMOVAL_BUDGET + 5.0  # small slack past the bar itself
    with ThreadPoolExecutor(max_workers=max(1, len(probe_targets))) as pool:
        while time.time() < dl:
            probed = dict(zip(probe_targets, pool.map(lambda h: _safe_probe(hosts[h]), probe_targets)))

            cur = max((probed[h].get("commit", 0) for h in probe_targets), default=0)
            if cur < high_water:
                regressed = True
            high_water = max(high_water, cur)

            serving = [h for h in survivor_hidxs if probed[h].get("can_serve")]
            if len(serving) > 1:
                return ("leader-self-removal", False, f"split-brain among survivors: {serving}")
            old_stepped_down = not probed[leader_hidx].get("can_serve", True)
            if old_stepped_down and len(serving) == 1:
                new_leader_hidx = serving[0]
                break
            time.sleep(0.25)
    gap = time.time() - t0

    if regressed:
        return ("leader-self-removal", False, "committed high-water regressed across the handoff")
    if new_leader_hidx is None:
        return (
            "leader-self-removal", False,
            f"no new leader within {SELF_REMOVAL_BUDGET:.0f}s budget (gap {gap:.2f}s)",
        )

    del cluster.voters[leader_id]
    new_leader_id = next(nid for nid, h in cluster.voters.items() if h == new_leader_hidx)

    # Move the write-load driver onto the new leader host: a fleet loadclient
    # only ever submits to its OWN host's node (same-host shmem), so it must
    # follow leadership across this specific scenario (the only one of the
    # three that ever changes who leads).
    leader_host.kill_unit("loadclient")
    hosts[new_leader_hidx].start_unit(
        "loadclient",
        ["loadclient", "--instance-dir", hosts[new_leader_hidx].dir, "--app-id", APP,
         "--stop-file", stop_file] + M7_LOAD_ARGS,
    )
    time.sleep(2.0)
    before = _safe_probe(hosts[new_leader_hidx]).get("commit", 0)
    dl2 = time.time() + 10.0
    advanced = False
    while time.time() < dl2:
        if _safe_probe(hosts[new_leader_hidx]).get("commit", 0) > before:
            advanced = True
            break
        time.sleep(0.5)

    ok = gap < SELF_REMOVAL_BUDGET and advanced
    detail = (
        f"gap={gap:.2f}s (gate <{SELF_REMOVAL_BUDGET:.0f}s), zero committed-high-water loss, "
        f"new leader node{new_leader_id} serving_and_committing={advanced}"
    )
    log(f"[{'PASS' if ok else 'FAIL'}] leader-self-removal — {detail}")
    return ("leader-self-removal", ok, detail)


def run_gate_m7(hosts, members_seed, stop_file):
    """3 voters + 2 spare hosts (5 total). Brings up the 3 voters, runs all
    three M7 scenarios in sequence (each reusing whatever physical host slots
    the prior scenario freed up), and checks the loadclient divergence guard
    at the end — same composition contract as M6's `run_gate`."""
    verdicts = []

    # Clean room: a gate run must never inherit durable state from a prior run
    # (or any other experiment) on these hosts — stale `config.state` carries
    # old tombstones/versions, and the orchestrator's fresh-id counter would
    # collide with them ("add-learner refused: Tombstoned", hit on a real
    # rerun). Wipe every instance dir up front, exactly like a fresh provision.
    for h in hosts:
        h.reset_dir()

    for i in range(3):
        # `bind_addr()` is safe to call directly here — `main()` already
        # monkeypatched hosts 0-2 to a FIXED address before calling in
        # (the same address baked into `members_seed`).
        start_node_m7(hosts[i], i, members_seed, hosts[i].bind_addr())
    time.sleep(2.0)
    for i in range(3):
        start_service(hosts[i], m7=True)

    leader_hidx = wait_leader(hosts, [0, 1, 2], 40)
    if leader_hidx is None:
        log("FAIL: no leader elected")
        return False, verdicts
    log(f"leader elected: node{leader_hidx}")

    cluster = M7Cluster(hosts[0].bind_addr(), hosts[1].bind_addr(), hosts[2].bind_addr())

    hosts[leader_hidx].start_unit(
        "loadclient",
        ["loadclient", "--instance-dir", hosts[leader_hidx].dir, "--app-id", APP,
         "--stop-file", stop_file] + M7_LOAD_ARGS,
    )

    verdicts.append(scenario_replace_a_box(hosts, cluster))
    verdicts.append(scenario_resize_3_5_3(hosts, cluster))
    verdicts.append(scenario_leader_self_removal(hosts, cluster, stop_file))

    # Divergence guard (Fix 1, log-based — see run_gate's identical comment):
    # the loadclient unit may have moved host in `scenario_leader_self_removal`
    # (a fleet loadclient only ever talks to its own host's node), so check
    # every host's log — exactly one of them ever ran a loadclient by this
    # name, and a missing log file is simply "no divergence", not a FAIL.
    for h in hosts:
        if h.divergence_detected("loadclient"):
            log("FAIL: loadclient log on a host shows DIVERGENCE — committed-value regression detected")
            verdicts.append(("divergence-guard", False, "loadclient log contains DIVERGENCE:"))

    ok = all(v[1] for v in verdicts)
    return ok, verdicts


# --------------------------------------------------------- read-profile mode
#
# `--read-profile` runs the linearizable-read profile ladder of
# `uc2_node/examples/read_profile.rs` across 3 hosts (one `node` + one
# `service` per host, the measuring `client` on the leader's host because the
# query ring is same-host shared memory). Design:
# `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`; plan Task 7.
#
# THIS IS AN INSTRUMENT, NOT A GATE. The decision rule (spec §2) is a
# pre-commitment implemented and unit-tested in Rust — `evaluate_decision_rule`
# — and is NEVER re-implemented here: the orchestrator collects one rung JSON
# line per client run and shells out to `read_profile decide --rungs FILE` for
# the verdict. A rule re-derived in Python would sit outside the tests that pin
# it and would stop being a pre-commitment. Consequently an unfavourable (or
# INCONCLUSIVE) verdict still exits 0; only HARNESS failure exits 1.
#
# Three structural facts drive the shape below:
#   * Fresh cluster per rung (M6/M7's own teardown discipline): no rung inherits
#     the previous one's leader, warm buffers, or journal.
#   * BOTH write mixes are required. Clause (b) is evaluated on the READ-ONLY
#     arm, so a sweep missing `--write-rate 0` cannot produce a verdict at all.
#   * Exit codes are the validity signal, not stdout. A client that fails its
#     validity checks (`inflight_at_end != 0`, or a read regression) exits
#     nonzero and deliberately prints NO rung JSON line — spec §6 threat 6 calls
#     such a rung INVALID, not merely suspicious. Teeing stdout without checking
#     the exit code would silently drop rungs that must be reported as failures,
#     so `rp_run_rung` records the failure and never fabricates a substitute.

RP_LEADER_WAIT = 40.0     # s — same budget M6/M7 give a fresh cluster's election
RP_BOOT_SETTLE = 2.0      # s — node -> service start gap (M6/M7 use the same)
RP_CLIENT_SLACK = 180.0   # s — added to --secs for the client's own leader wait
                          #     (30 s), write ramp, and 5 s drain grace
RP_MODES = ("lin", "snap")


def rp_members(hosts):
    """`id@addr` seed for the 3 read-profile voters. Addresses are the ones
    `main()` already pinned onto hosts 0-2 (local `bind_addr()` mints a fresh
    ephemeral port on every call — the M7 bug re-learned here would be a node
    listening on a port no peer replicates to)."""
    return ",".join(f"{i}@{hosts[i].bind_addr()}" for i in range(3))


def rp_start_cluster(hosts, members, admission_kib):
    """Boot a FRESH 3-node read_profile cluster and return the leader's host
    index (or None if no single leader appeared). Mirrors `run_gate`'s bring-up
    order exactly: wipe every instance dir, start the nodes, settle, start the
    services, then `wait_leader` — which itself raises on split-brain (two hosts
    reporting leader+can_serve)."""
    for h in hosts:
        h.reset_dir()
    for i in range(3):
        hosts[i].start_unit("node", [
            "node", "--id", str(i), "--bind", hosts[i].bind_addr(),
            "--instance-dir", hosts[i].dir, "--members", members,
            "--app-id", APP, "--admission-kib", str(admission_kib),
        ])
    time.sleep(RP_BOOT_SETTLE)
    for i in range(3):
        hosts[i].start_unit("service", [
            "service", "--instance-dir", hosts[i].dir, "--app-id", APP,
        ])
    return wait_leader(hosts, [0, 1, 2], RP_LEADER_WAIT)


def rp_assert_single_serving(hosts, when):
    """Re-check, AFTER the measured window, that exactly one host still reports
    leader+can_serve. `wait_leader` only proves it at boot; the requirement is
    that two hosts never serve at any point in a rung. A host whose probe fails
    (node gone) is not 'serving' and does not trip this."""
    serving = []
    for i in range(3):
        try:
            p = hosts[i].probe()
            if p["leader"] and p["can_serve"]:
                serving.append(i)
        except Exception:
            pass
    if len(serving) > 1:
        raise RuntimeError(f"split-brain {when}: hosts {serving} all serve")
    return serving


def rp_extract_rung(out):
    """Return the client's machine-readable rung JSON line, or None. The client
    prints its human report first and the rung line last; anything that parses
    as an object carrying `"rung"` is it."""
    found = None
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("{") or '"rung"' not in line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and "rung" in obj:
            found = line
    return found


def rp_regression(out, rung_line):
    """Reads-went-backwards signal for a rung, from BOTH places it can appear:
    the report's `read regression : N` line (printed even on the failing path,
    before the client's `ensure` aborts) and the rung JSON's own field. A
    nonzero here is a HARNESS-level failure — the numbers describe nothing and
    the run must exit 1 — so it is read defensively rather than trusted to one
    source."""
    worst = 0
    for line in out.splitlines():
        if "read regression" in line and ":" in line:
            try:
                worst = max(worst, int(line.split(":", 1)[1].strip()))
            except ValueError:
                pass
    if "READ REGRESSED" in out:
        worst = max(worst, 1)
    if rung_line:
        try:
            worst = max(worst, int(json.loads(rung_line).get("regression", 0)))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return worst


def rp_run_rung(hosts, members, readers, mode, write_rate, secs, admission_kib, out_dir):
    """One rung: fresh cluster, one `client` run on the leader's host, teardown.

    Returns `(ok, rung_line_or_None, regression, detail)`. `ok` is False for any
    rung that did not produce a valid measurement — no leader, split-brain, or a
    nonzero client exit."""
    tag = f"w{write_rate}-{mode}-r{readers}"
    log(f"-- rung {tag}: booting a fresh 3-node cluster")
    try:
        leader = rp_start_cluster(hosts, members, admission_kib)
    except RuntimeError as e:          # split-brain out of wait_leader
        return False, None, 0, f"cluster bring-up failed: {e}"
    if leader is None:
        return False, None, 0, f"no leader elected within {RP_LEADER_WAIT:.0f}s"
    log(f"-- rung {tag}: leader is host {leader}; running client there")

    args = [
        "client", "--instance-dir", hosts[leader].dir, "--app-id", APP,
        "--secs", str(secs), "--readers", str(readers),
        "--mode", mode, "--write-rate", str(write_rate),
    ]
    # DIAGNOSTIC only (agent yield-rate sample). Absence does not impair the
    # run: clause (b) no longer uses the occupancy ranking, so a missing pid is
    # not worth failing — or even warning loudly — over.
    for flag, unit in (("--node-pid", "node"), ("--service-pid", "service")):
        try:
            pid = hosts[leader].unit_pid(unit)
        except Exception:
            pid = None
        if pid:
            args += [flag, str(pid)]

    rc, out = hosts[leader].run_foreground(args, timeout=secs + RP_CLIENT_SLACK)
    (out_dir / f"client-{tag}.log").write_text(out)

    rung_line = rp_extract_rung(out) if rc == 0 else None
    regression = rp_regression(out, rung_line)

    try:
        rp_assert_single_serving(hosts, f"after rung {tag}")
    except RuntimeError as e:
        return False, None, regression, str(e)

    if rc != 0:
        tail = " | ".join(l for l in out.strip().splitlines()[-4:])
        return False, None, regression, f"client exited {rc} (INVALID rung, no JSON emitted): {tail}"
    if rung_line is None:
        return False, None, regression, "client exited 0 but printed no rung JSON line"
    return True, rung_line, regression, "ok"


def rp_decide(decide_bin, rungs_path, write_rate, hosts, out_dir):
    """Shell out to `read_profile decide` for the verdict — never re-implement
    the rule here (see the section header). Prefers a LOCAL binary; on a fleet
    where the example was only ever built remotely (rsync ships the tree, ansible
    builds as root on the hosts) it falls back to running `decide` on host 0 with
    the rung file piped over ssh. `decide` is a pure function of the file, so
    where it runs is immaterial."""
    text = rungs_path.read_text()
    if decide_bin and Path(decide_bin).exists():
        r = subprocess.run([decide_bin, "decide", "--rungs", str(rungs_path),
                            "--write-rate", str(write_rate)],
                           capture_output=True, text=True, timeout=120)
        out, rc = (r.stdout or "") + (r.stderr or ""), r.returncode
    elif hosts and hasattr(hosts[0], "_ssh"):
        remote = f"{hosts[0].remote_root}/rungs.jsonl"
        hosts[0]._ssh(f"sudo tee {remote} >/dev/null", input=text, capture_output=True)
        r = hosts[0]._ssh(
            f"sudo {hosts[0].gate} decide --rungs {remote} --write-rate {write_rate}",
            capture_output=True,
        )
        out, rc = (r.stdout or "") + (r.stderr or ""), r.returncode
    else:
        return 1, f"no read_profile binary available to run `decide` (looked at {decide_bin})"
    (out_dir / f"verdict-write-rate-{write_rate}.txt").write_text(out)
    return rc, out


def run_read_profile(hosts, members, sweep, secs, write_rate, admission_kib,
                     decide_bin, out_dir):
    """The read-profile sweep: `readers` x {lin,snap} x {0, write_rate}, a fresh
    cluster per rung, then the Rust decision rule over the collected rungs.

    Returns `(harness_ok, verdicts)` — `harness_ok` False ONLY on harness
    failure (a nonzero read regression on any rung, or client failures on more
    than one rung). An unfavourable or INCONCLUSIVE verdict is a RESULT, not a
    failure: this mode is an instrument."""
    verdicts = []
    rung_lines, failures, regressions = [], [], []
    mixes = [0, write_rate] if write_rate else [0]
    if not write_rate:
        log("NOTE: --rp-write-rate 0 — running the read-only arm only. The mixed "
            "arm is then absent; clause (b) is still evaluable (it reads the "
            "read-only arm), but the mixed-mix verdict will not exist.")

    total = len(mixes) * len(sweep) * len(RP_MODES)
    n = 0
    for mix in mixes:
        for readers in sweep:
            for mode in RP_MODES:
                n += 1
                log(f"== rung {n}/{total}: readers={readers} mode={mode} writes/s={mix} ==")
                try:
                    ok, line, regression, detail = rp_run_rung(
                        hosts, members, readers, mode, mix, secs, admission_kib, out_dir)
                except Exception as e:   # bring-up/probe blew up: one failed rung
                    ok, line, regression, detail = False, None, 0, f"orchestrator error: {e!r}"
                finally:
                    for h in hosts:
                        try:
                            h.teardown()
                        except Exception:
                            pass
                if regression:
                    # Loud, and fatal to the RUN: a read returned a value lower
                    # than one already returned. Either the harness is mis-wired
                    # or the read path served stale state; every number in the
                    # sweep is suspect until that is explained.
                    log(f"  *** READ REGRESSION on rung {mode}/r{readers}/w{mix}: {regression} ***")
                    regressions.append(f"{mode}/r{readers}/w{mix}={regression}")
                if ok:
                    rung_lines.append(line)
                    log(f"  rung collected: {line}")
                else:
                    log(f"  *** RUNG FAILED ({mode}/r{readers}/w{mix}): {detail}")
                    failures.append(f"{mode}/r{readers}/w{mix}: {detail}")

    rungs_path = out_dir / "rungs.jsonl"
    rungs_path.write_text("".join(l + "\n" for l in rung_lines))
    log(f"collected {len(rung_lines)}/{total} rungs -> {rungs_path}")
    verdicts.append((
        "rung-collection",
        len(failures) <= 1,
        f"{len(rung_lines)}/{total} rungs valid, {len(failures)} client failure(s)"
        + ("; " + " | ".join(failures) if failures else ""),
    ))

    # The verdict, from the SAME Rust rule its unit tests pin — one evaluation
    # per write mix present (clause (b) always reads the read-only rungs out of
    # the same file, whichever mix is being reported).
    if rung_lines:
        for mix in mixes:
            log(f"== decision rule (write_rate={mix}) ==")
            rc, out = rp_decide(decide_bin, rungs_path, mix, hosts, out_dir)
            print(out, flush=True)
            verdict_line = next(
                (l.strip() for l in out.splitlines() if l.strip().startswith("VERDICT:")),
                "<no VERDICT line — see the decide output>",
            )
            verdicts.append((
                f"decide(write_rate={mix})",
                True,   # a verdict is a RESULT; only harness failure fails the run
                verdict_line if rc == 0 else f"decide exited {rc}: {verdict_line}",
            ))
    else:
        verdicts.append(("decide", False, "no valid rungs to evaluate"))

    harness_ok = not regressions and len(failures) <= 1
    if regressions:
        verdicts.append(("read-regression-guard", False, "; ".join(regressions)))
    log(f"artifacts: {out_dir}")
    return harness_ok, verdicts


# ---------------------------------------------------------------- entrypoints

def build_local_hosts(gate_bin, root, count=4, ctl_bin=None, probe_bin=None):
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
    for i in range(count):
        node_dir = root / f"n{i}"
        node_dir.mkdir()
        # Per-node log dir (sibling to the instance dir) so the nodes' unit
        # logs don't clobber each other.
        log_dir = root / f"log{i}"
        log_dir.mkdir()
        hosts.append(LocalHost(gate_bin, node_dir, log_dir, ctl_bin=ctl_bin,
                               probe_bin=probe_bin))
    return hosts


def build_fleet_hosts(gate_bin, ssh_user, ssh_key, hosts_arg, count=4,
                       ctl_bin=None, unit_prefix="m6", remote_root="/opt/bench/m6",
                       probe_bin=None):
    if hosts_arg:
        # "pub1/priv1,pub2/priv2,..." — `count` entries.
        entries = [tuple(h.split("/")) for h in hosts_arg.split(",")]
    else:
        out = subprocess.check_output(
            ["terraform", "output", "-json", "nodes"],
            cwd=str(Path(__file__).resolve().parent.parent / "terraform"), text=True,
        )
        nodes = json.loads(out)
        entries = [(n["public_ip"], n["private_ip"]) for n in nodes]
    if len(entries) < count:
        raise SystemExit(f"need {count} hosts, got {len(entries)}")
    hosts = []
    for i, (pub, priv) in enumerate(entries[:count]):
        hosts.append(SshHost(
            gate_bin, f"{remote_root}/n{i}", pub, priv, ssh_user, ssh_key,
            ctl_bin=ctl_bin, unit_prefix=unit_prefix, remote_root=remote_root,
            probe_bin=probe_bin,
        ))
    return hosts


def main():
    global APP, M7_LOAD_ARGS

    ap = argparse.ArgumentParser(description="M6/M7 fleet-gate orchestrator")
    ap.add_argument("--local", action="store_true", help="run local processes (loopback UDP)")
    ap.add_argument("--fleet", action="store_true", help="run over ssh on remote hosts")
    ap.add_argument("--m7", action="store_true", help="run the M7 reconfig scenarios instead of M6")
    ap.add_argument("--read-profile", action="store_true",
                    help="run the read_profile linearizable-read ladder (3 hosts) instead of a gate")
    ap.add_argument("--rp-readers", default="1,4,16,64,256,1024",
                    help="read-profile: comma-separated concurrency sweep")
    ap.add_argument("--rp-secs", type=int, default=20,
                    help="read-profile: measured seconds per rung")
    ap.add_argument("--rp-write-rate", type=int, default=20000,
                    help="read-profile: writes/s for the MIXED arm (the read-only arm, "
                         "write_rate=0, is always run — clause (b) needs it)")
    ap.add_argument("--rp-admission-kib", type=int, default=256,
                    help="read-profile: node admission window (the harness default)")
    ap.add_argument("--rp-out", default="",
                    help="read-profile: artifact dir (default /home/claude/.cache/"
                         "uc2-read-profile/<ts>; must NOT be under /tmp — RAM tmpfs)")
    ap.add_argument("--rp-decide-bin", default="",
                    help="read-profile: LOCAL read_profile binary used for `decide` "
                         "(fleet fallback: runs it on host 0 over ssh)")
    ap.add_argument("--bin", default="", help="path to the gate binary (m6_gate/m7_gate)")
    ap.add_argument("--ctl-bin", default="", help="M7: path to the uc2ctl binary")
    ap.add_argument("--root", default="/home/claude/.cache/m6_fleet", help="local root dir")
    ap.add_argument("--hosts", default="", help="fleet: pub/priv,... (else terraform output)")
    ap.add_argument("--ssh-user", default="ubuntu", help="fleet ssh user")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519", help="fleet ssh key")
    ap.add_argument("--cycles", type=int, default=5, help="M6: purge-cycle count")
    args = ap.parse_args()

    if args.local == args.fleet:
        raise SystemExit("choose exactly one of --local / --fleet")
    if args.m7 and args.read_profile:
        raise SystemExit("--m7 and --read-profile are different experiments; choose one")

    # read-profile: 3 hosts, the `read_profile` example as the role binary.
    # `read_profile` has NO `probe` role, so leader detection borrows m7_gate's
    # (it only opens `cnc2.dat` under an --app-id — app-agnostic); `probe_bin`
    # is the seam. APP is the read_profile roles' default app id.
    n_hosts = 3 if args.read_profile else (5 if args.m7 else 4)
    gate_name = "read_profile" if args.read_profile else ("m7_gate" if args.m7 else "m6_gate")
    probe_name = "m7_gate" if args.read_profile else gate_name
    if args.read_profile:
        APP = "uc2-read-profile"
    if args.m7:
        APP = "m7-gate"
        if args.local:
            # Local-ONLY loadclient pace (~ the previously-proven local level;
            # the fleet passes nothing = unthrottled M6-level load). See the
            # M7_LOAD_ARGS module comment for the measured local
            # replayer-slower-than-writer capacity limit this guards.
            M7_LOAD_ARGS = ["--rate", "300"]

    if args.local:
        local_release = "/home/claude/.cache/cargo-target/release"
        local_examples = f"{local_release}/examples"
        gate = args.bin or f"{local_examples}/{gate_name}"
        probe_bin = f"{local_examples}/{probe_name}"
        ctl_bin = (args.ctl_bin or f"{local_release}/uc2ctl") if args.m7 else None
        hosts = build_local_hosts(gate, args.root, count=n_hosts, ctl_bin=ctl_bin,
                                  probe_bin=probe_bin)
        stop_file = str(Path(args.root) / "STOP")
        # `decide` is a pure function of the rung file; locally it is the same
        # binary that ran the rungs.
        decide_bin = args.rp_decide_bin or gate
    else:
        remote_release = "/opt/bench/uc/target/release"
        remote_examples = f"{remote_release}/examples"
        gate = args.bin or f"{remote_examples}/{gate_name}"
        probe_bin = f"{remote_examples}/{probe_name}"
        ctl_bin = (args.ctl_bin or f"{remote_release}/uc2ctl") if args.m7 else None
        remote_root = ("/opt/bench/rp" if args.read_profile
                       else ("/opt/bench/m7" if args.m7 else "/opt/bench/m6"))
        unit_prefix = "rp" if args.read_profile else ("m7" if args.m7 else "m6")
        hosts = build_fleet_hosts(
            gate, args.ssh_user, args.ssh_key, args.hosts, count=n_hosts,
            ctl_bin=ctl_bin, unit_prefix=unit_prefix, remote_root=remote_root,
            probe_bin=probe_bin,
        )
        # Fleet: build the example(s) on each host (release builds no examples
        # by default) + mkdir the instance-dir parent. The loadclient
        # stop-file lives on whichever remote host currently runs it and is
        # never created locally; teardown kills the unit instead.
        stop_file = f"{remote_root}_STOP"
        if args.read_profile:
            # m7_gate too: it supplies the `probe` role read_profile lacks.
            examples = (gate_name, probe_name)
        else:
            examples = (gate_name,)
        bins = ("uc2ctl",) if args.m7 else ()
        log(f"preparing fleet hosts (build {', '.join(examples + bins)} + mkdir)...")
        for h in hosts:
            h.prepare(examples=examples, bins=bins)
        # No local build is implied by a fleet run (rsync ships the tree;
        # ansible builds ON the hosts), so `decide` may have to run remotely —
        # `rp_decide` falls back to host 0 when this path does not exist.
        decide_bin = args.rp_decide_bin

    addr = {i: hosts[i].bind_addr() for i in range(n_hosts)}
    # Re-pin the ORIGINAL voters' addresses onto their hosts so start_node
    # reuses the SAME addr the member map advertises (local mode re-binds
    # ephemeral ports on every `bind_addr()` call otherwise). M7's 2 spare
    # hosts (indices 3,4) deliberately do NOT get this treatment — each gets a
    # fresh address exactly once per scenario, when it is actually admitted.
    for i in range(3):  # the 3 original voters, in both M6 and M7 topologies
        hosts[i]._fixed_addr = addr[i]
        hosts[i].bind_addr = (lambda h=hosts[i]: h._fixed_addr)
    if not args.m7 and not args.read_profile:
        # M6: the 4th host is a single pre-provisioned learner, fixed too.
        # (read-profile has exactly 3 hosts and no learner at all.)
        hosts[3]._fixed_addr = addr[3]
        hosts[3].bind_addr = (lambda h=hosts[3]: h._fixed_addr)

    # Clear any stale stop-file.
    try:
        Path(stop_file).unlink()
    except FileNotFoundError:
        pass

    mode = "LOCAL" if args.local else "FLEET"
    conn = f"{n_hosts} procs, loopback UDP" if args.local else f"{n_hosts} hosts"
    what = "READ-PROFILE" if args.read_profile else ("M7" if args.m7 else "M6")
    log(f"== {what} {mode} ({conn}) {'ladder' if args.read_profile else 'gate'} ==")
    ok = False
    verdicts = []
    try:
        if args.read_profile:
            sweep = [int(x) for x in args.rp_readers.split(",") if x.strip()]
            if not sweep:
                raise SystemExit("--rp-readers is empty")
            out_dir = Path(args.rp_out or (
                "/home/claude/.cache/uc2-read-profile/" + time.strftime("%Y%m%d-%H%M%S")))
            # Artifacts are logs, not journals — but /tmp on the dev box is
            # RAM-backed with no swap (CLAUDE.md), and a sweep's client logs
            # are not what should be competing with the nodes for RAM.
            if str(out_dir.resolve()).startswith("/tmp"):
                raise SystemExit(f"--rp-out {out_dir} is under /tmp (RAM tmpfs, no swap) — "
                                 f"put run artifacts on real disk")
            out_dir.mkdir(parents=True, exist_ok=True)
            members = ",".join(f"{i}@{addr[i]}" for i in range(3))
            log(f"sweep: readers={sweep} x modes={list(RP_MODES)} x writes/s="
                f"{[0, args.rp_write_rate] if args.rp_write_rate else [0]}, "
                f"{args.rp_secs}s per rung, fresh cluster per rung")
            log("*** INSTRUMENT, not a gate: an unfavourable verdict still exits 0 ***")
            if args.local:
                log("*** LOCAL: orchestration proof only — these numbers are NOT data "
                    "(3 nodes + 3 services + client share one box) ***")
            ok, verdicts = run_read_profile(
                hosts, members, sweep, args.rp_secs, args.rp_write_rate,
                args.rp_admission_kib, decide_bin, out_dir)
        elif args.m7:
            members_seed = ",".join(f"{i}@{addr[i]}" for i in range(3))
            ok, verdicts = run_gate_m7(hosts, members_seed, stop_file)
        else:
            voters, learner = [0, 1, 2], 3
            members = ",".join(f"{i}@{addr[i]}" for i in voters)
            learners = f"{learner}@{addr[learner]}"
            ok, verdicts = run_gate(hosts, voters, learner, members, learners, args.cycles, stop_file)
    finally:
        # Local: signal the loadclient to finish cleanly via its stop-file. Fleet:
        # the stop-file is on whichever remote host runs it (not reachable from
        # here) — the per-host teardown kills the loadclient unit instead.
        if args.local and not args.read_profile:
            # read-profile has no loadclient: its `client` role is foreground
            # and already exited, so there is nothing to signal.
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
    if args.read_profile:
        # The VERDICT is printed above by `read_profile decide`; this line is
        # only about whether the instrument ran. Exit 1 means the measurement
        # is not trustworthy (a read regression, or client failures on more
        # than one rung) — never "the rule declined to justify Rung A".
        log(f"RESULT: run {'OK' if ok else 'FAILED (harness)'} — the verdict is the "
            f"`decide` block above, and does not affect this exit code")
    else:
        log(f"RESULT: {'PASS' if ok else 'FAIL (honest)'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
