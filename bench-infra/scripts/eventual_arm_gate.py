#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""
Eventual-durability arm — PRE-REGISTERED 2026-08-16 in
docs/benchmarks/uc2-aeron-parity-2026-08-15.md ("Eventual-durability arm"
section, committed before this ran).

Order:
  1. UC anchors, fsync-on (256/1024, 128/1024)     — same-fleet baseline
  2. UC anchors, EVENTUAL (UC2_JOURNAL_DURABILITY) — the new arm
  3. Aeron SHARED / batch 64, sync.level=0, rates 200k..1.4M, IPC edge
  4. UC anchor fsync-on repeat (drift bracket)

Same reading + invalidation rules as the parity run. Exit 0 unless the
harness itself fails.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from m5_fleet_gate import tf_hosts, run_point  # noqa: E402
from m6_fleet_gate import SshHost  # noqa: E402
from aeron_parity_gate import (  # noqa: E402
    RATES, start_aeron, stop_aeron, run_rung, fill_percentiles, sh,
    JAVA_ENV, SCRIPTS, RESULTS,
)


class EnvSshHost(SshHost):
    """SshHost whose systemd units carry extra --setenv vars (node role only
    needs it; the env is harmless on the service unit)."""

    def __init__(self, base: SshHost, env: dict):
        self.__dict__.update(base.__dict__)
        self._extra_env = env

    def start_unit(self, unit, args):
        self.kill_unit(unit)
        quoted = " ".join(f"'{a}'" for a in args)
        setenv = " ".join(f"--setenv={k}={v}" for k, v in self._extra_env.items())
        cmd = (
            f"sudo systemd-run --unit={self.unit_prefix}-{unit} --collect {setenv} "
            f"-p TimeoutStopSec=1 "
            f"-p StandardOutput=append:/opt/bench/{self.unit_prefix}-{unit}.log "
            f"-p StandardError=append:/opt/bench/{self.unit_prefix}-{unit}.log "
            f"{self.gate} {quoted}"
        )
        r = self._ssh(cmd, capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"start {unit} on {self.public_ip}: {r.stderr}")


def uc_block(hosts, label, outdir, results, attempt):
    for adm, w in ((256, 1024), (128, 1024)):
        print(f"== UC {label} adm={adm} W={w} ==", flush=True)
        row = run_point(hosts, adm, w, outdir, attempt)
        print(f"   rps={row['rps']} p50={row['p50_ms']}ms"
              + (f" INVALID:{row['invalid']}" if row.get("invalid") else ""), flush=True)
        row["label"] = label
        results.append(row)
    for h in hosts:
        h.kill_unit("node"); h.kill_unit("service")


def main():
    outdir = Path(__file__).parent.parent.parent / "bench-out" / "eventual-arm-2026-08-16"
    outdir.mkdir(parents=True, exist_ok=True)
    hosts, user, key = tf_hosts()
    h0 = hosts[0]
    print(f"hosts: {[(h.public_ip, h.private_ip) for h in hosts]}", flush=True)
    ev = {"UC2_JOURNAL_DURABILITY": "eventual"}
    ev_hosts = [EnvSshHost(h, ev) for h in hosts]

    uc_rows, aeron_rows = [], []

    # 1. fsync-on anchors
    stop_aeron(hosts)
    uc_block(hosts, "fsync_pre", outdir, uc_rows, 1)

    # 2. eventual anchors (env-carrying node units; same m5_gate binary)
    uc_block(ev_hosts, "eventual", outdir, uc_rows, 2)

    # 3. Aeron shared/b64 @ sync.level=0, IPC edge
    print("== Aeron SHARED b64 sync.level=0 (IPC edge) ==", flush=True)
    start_aeron(hosts, "shared", ipc=True, eventual=True)
    for rate in RATES:
        tag = f"aeron_ev_shared_b64_r{rate}"
        row = run_rung(h0, rate, 64, tag, outdir=outdir)
        row["mode"] = "shared-eventual"
        row["valid"] = row["rc"] == 0 and not row["failed_marker"] and row["artifact_ok"]
        print(f"   {tag}: rc={row['rc']}"
              f"{' FAIL-marker' if row['failed_marker'] else ''}", flush=True)
        aeron_rows.append(row)
    sh(h0, "sudo bash -c '" + JAVA_ENV +
       f"{SCRIPTS}/../aggregate-results {RESULTS} > /dev/null 2>&1 || true'", timeout=180)
    mode_dir = outdir / "rig-shared-eventual"
    mode_dir.mkdir(exist_ok=True)
    subprocess.run(["rsync", "-az", "-e",
                    f"ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes -i {key} -l {user}",
                    "--rsync-path", "sudo rsync",
                    f"{h0.public_ip}:{RESULTS}/", str(mode_dir) + "/"], check=False)
    for row in aeron_rows:
        if row["valid"]:
            fill_percentiles(row, mode_dir)
    stop_aeron(hosts)

    # 4. fsync-on repeat (drift bracket)
    uc_block(hosts, "fsync_post", outdir, uc_rows, 3)

    (outdir / "results.json").write_text(json.dumps(
        {"uc": uc_rows, "aeron": aeron_rows}, indent=1))

    print("\n=== UC rows ===")
    for r in uc_rows:
        print(f"{r['label']}: adm={r['adm_kib']} W={r['inflight']} "
              f"rps={r['rps']:,} p50={r['p50_ms']}ms p99={r.get('p99_ms')}ms")
    print("\n=== Aeron eventual (shared b64, level 0) ===")
    for r in aeron_rows:
        print(f"r{r['rate']}: valid={r['valid']} p50={r['p50_us']}us "
              f"p90={r['p90_us']}us p99={r['p99_us']}us")


if __name__ == "__main__":
    main()
