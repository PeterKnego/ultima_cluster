#!/usr/bin/env python3
"""Wire-level rolling-upgrade fleet row (spec 2026-10-19-uc2-wire-level-design §6.4).

One row, FUNCTIONAL clauses only, NO rate bar:
  level         every node's `uc2_wire_level` ends at 13
  gaps          each node restart's longest commit stall <= ROW_COMMIT_GAP_BAR_SECS
  fsm_agreement FSM state agrees across nodes after the roll (`m12_gate check-fsms`)

For 2.15.0 the "old" side is the SAME tree built with `--features
wire-level-override` and run with UC2_WIRE_RANGE=12,12; the roll restarts one
node at a time with UC2_WIRE_RANGE=12,13. From 2.16.0 the old side is the
previous release (parameterise the levels then).

    python3 bench-infra/scripts/wire_roll_gate.py --selftest      # no ssh
    python3 bench-infra/scripts/wire_roll_gate.py --run [--hosts pub/priv,...]

Run by the maintainer on the fleet; nothing here runs against AWS on import.
Exit codes: 0 PASS, 1 FAIL, 2 argparse usage.
"""
import argparse
import os
import re
import threading
import shlex
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

ROW_COMMIT_GAP_BAR_SECS = 5.0   # one node's restart: commit may stall while a quorum re-forms, never longer
TARGET_LEVEL = 13
OLD_RANGE = "12,12"
NEW_RANGE = "12,13"
LEVEL_WAIT_SECS = 60
SAMPLE_PERIOD_SECS = 0.1
UNIT_NODE, UNIT_SVC, UNIT_EDGE = "wire-node", "wire-svc", "wire-edge"
APP_ID = "uc2-wire-roll"
GATE_FLAGS = "wire-level-override"


def verdict_row_roll(level_by_node_final, gaps_secs, fsm_agreement):
    """Functional clauses only (spec §6.4): every node ends at the target
    level, no restart stalled commit past the bar, and the FSMs agree after the roll. No rate bar."""
    clauses = {
        "level": all(v == 13 for v in level_by_node_final.values()),
        "gaps": max(gaps_secs, default=0.0) <= ROW_COMMIT_GAP_BAR_SECS,
        "fsm_agreement": bool(fsm_agreement),
    }
    return ("PASS" if all(clauses.values()) else "FAIL"), clauses


def selftest():
    def check(name, got, want):
        assert got == want, f"{name}: {got!r} != {want!r}"
    check("pass", verdict_row_roll({0: 13, 1: 13, 2: 13}, [1.2, 0.8, 2.9], True)[0], "PASS")
    check("holdout", verdict_row_roll({0: 13, 1: 12, 2: 13}, [1.0], True)[0], "FAIL")
    check("gap", verdict_row_roll({0: 13, 1: 13, 2: 13}, [6.0], True)[0], "FAIL")
    check("agreement", verdict_row_roll({0: 13, 1: 13, 2: 13}, [], False)[0], "FAIL")
    # A stall is measured to the first sample ABOVE the mark (the upper bound,
    # at most one tick over), so a monotone run reads as one tick: the
    # documented resolution, not a stall.
    check("stall monotone = one tick", longest_stall([(i * 1.0, i) for i in range(5)]), 1.0)
    check("stall flat", longest_stall([(0.0, 1), (1.0, 2), (2.0, 2), (3.0, 2), (4.0, 2), (5.0, 3)]), 4.0)
    check("stall empty", longest_stall([]), float("inf"))
    # #183: a dip is not movement. One observer lost for a tick inside a
    # stall (5,5,3,5,5 then 6) is ONE 5 s stall from t=0 to the rise at t=5,
    # not two 1 s stalls split by the dip.
    check("stall dip inside",
          longest_stall([(0.0, 5), (1.0, 5), (2.0, 3), (3.0, 5), (4.0, 5), (5.0, 6)]), 5.0)
    # A failover steps the position back and climbs again: the stall runs
    # until the climb passes the mark (t=4, p=6 > 5), not when it first rises.
    check("stall dip then climb",
          longest_stall([(0.0, 5), (1.0, 5), (2.0, 3), (3.0, 4), (4.0, 6), (5.0, 7)]), 4.0)
    # A stretch with no sample at all (every observer silent) is stall, not a
    # gap the verdict skips over: 1 s at the mark, 8 s blind, then a rise.
    check("stall blind stretch", longest_stall([(0.0, 5), (1.0, 5), (10.0, 6)]), 10.0)
    # Stepped back and never regained the mark: the stall runs to the end.
    check("stall never regains", longest_stall([(0.0, 5), (1.0, 3), (2.0, 4), (3.0, 4)]), 3.0)
    check("sampler skipped ticks", CommitSampler.fold_tick(0.5, [None, None]), None)
    check("sampler partial tick", CommitSampler.fold_tick(0.5, [None, 7]), (0.5, 7))
    check("sampler full tick", CommitSampler.fold_tick(0.5, [9, 7]), (0.5, 9))
    print("selftest OK")


def longest_stall(samples):
    """Longest wall interval over which the commit position did not ADVANCE
    past its high-water mark. `samples` = [(t_seconds, position)] in time
    order. Only a rise above the mark ends a stall (#183): a dip — the max
    taken over fewer surviving observers for a tick, or a failover stepping
    the exported position back before it climbs again — is not movement, so
    the stall keeps running from the mark's first sample. A stall is measured
    from the first sample at the mark TO THE FIRST SAMPLE ABOVE IT (or the last
    sample, if none is): the upper bound, which over-reads by at most one
    scrape tick — the conservative direction for a FAIL bar — and counts a
    blind stretch (skipped ticks) as stall, since nothing proves commit moved
    while no observer answered. A monotone run therefore reads as one tick,
    the documented resolution."""
    if not samples:
        return float("inf")
    best, since, high = 0.0, None, None
    for t, p in samples:
        if high is None:
            since, high = t, p
        best = max(best, t - since)
        if p > high:
            since, high = t, p
    return best


# ================================================================ fleet arm
# Everything below needs ssh and the provisioned fleet; it is imported lazily
# so `--selftest` has no dependency on jumbo_gate/m6/m12/m14 (or boto).

def _mods():
    import jumbo_gate as jg
    import m6_fleet_gate as m6
    import m12_fleet_gate as m12
    import m14_fleet_gate as m14
    return jg, m6, m12, m14


def build_uc_node_override(hosts):
    """`jumbo_gate.build_uc_node` plus `--features wire-level-override`, the
    feature that makes `UC2_WIRE_RANGE` readable by the daemon."""
    jg, m6, _, _ = _mods()
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    for h in hosts:
        cmd = (f"{env} {m6.SshHost.CARGO} build --release "
               f"--manifest-path {m6.SshHost.UC_SRC}/Cargo.toml -p uc_node --bin uc2-node "
               f"--features {GATE_FLAGS} "
               f"&& test -x {h.gate} && sha256sum {h.gate} && echo NODE-OK")
        r = jg.ssh(h, cmd, label="build-node-override", timeout=1800)
        if "NODE-OK" not in (r.stdout or ""):
            raise RuntimeError(f"uc2-node build on {h.public_ip}: {(r.stderr or r.stdout)[-2000:]}")


def start_node_unit(h, cfg, wire_range):
    """`m12.start_unit` cannot pass environment (it builds a bare
    `systemd-run ... <gate> <args>` line), so build the same line with
    `m12.unit_start_cmd` and splice systemd-run's `-E KEY=VAL` in, which puts
    UC2_WIRE_RANGE in the transient unit's environment."""
    jg, _, m12, _ = _mods()
    m12.kill_unit(h, UNIT_NODE)
    cmd = m12.unit_start_cmd(h, UNIT_NODE, ["--config", cfg], nofile=True)
    cmd = cmd.replace("systemd-run ", f"systemd-run -E UC2_WIRE_RANGE={wire_range} ", 1)
    r = jg.ssh(h, cmd, label="systemd-run")
    if r.returncode != 0:
        raise RuntimeError(f"start {UNIT_NODE} on {h.public_ip}: {r.stderr}")


def start_load_side(h_gate, h_node, members_edge):
    """The service (and the TCP edge) for one node, attached to ITS node's
    instance dir. They must be re-started after that node restarts."""
    _, _, m12, _ = _mods()
    m12.start_unit(h_gate, UNIT_SVC, ["service", "--instance-dir", h_gate.dir, "--app-id", APP_ID,
                                      "--envelope", "on", "--fsm", "count"])
    m12.start_unit(h_gate, UNIT_EDGE, ["edge", "--instance-dir", h_gate.dir, "--app-id", APP_ID,
                                       "--listen", f"{h_gate.private_ip}:{m12.EDGE_PORT}",
                                       "--members", members_edge, "--envelope", "on",
                                       "--inflight", "4096"])


def wait_cluster_agent(h, secs=60):
    jg, _, _, _ = _mods()
    deadline = time.time() + secs
    while time.time() < deadline:
        v = jg.read_gauge(jg.scrape_prom(h), "uc2_agent_alive", agent="cluster")
        if v == 1:
            return True
        time.sleep(1.0)
    return False


class CommitSampler(threading.Thread):
    """Scrapes `uc2_commit_position` on every surviving node each tick and
    records the MAX, from construction-time start until `stop()`. Start it
    BEFORE the kill so the whole stall window is observed. Resolution is one
    ssh-scrape tick (~1 s for two sequential scrapes).

    A tick on which NO observer answered is a missing sample, not a max over
    nothing: it is skipped and counted in `skipped`, so a sampler that lost
    its observers shows in the per-roll INFO line instead of as a shorter
    stall. A tick on which SOME observer failed is kept (the max over the
    rest) and counted in `partial`; `longest_stall` treats the dip that can
    produce as no movement (#183)."""

    def __init__(self, observers):
        super().__init__(daemon=True)
        self.observers, self.samples, self._stop_ev = observers, [], threading.Event()
        self.skipped, self.partial = 0, 0
        self.t0 = time.time()

    @staticmethod
    def fold_tick(t, vals):
        """One tick's scrape results (`None` = that observer did not answer)
        folded to a sample, or `None` when no observer answered."""
        got = [int(v) for v in vals if v is not None]
        return (t, max(got)) if got else None

    def run(self):
        jg, _, _, _ = _mods()
        while not self._stop_ev.is_set():
            vals = [jg.read_gauge(jg.scrape_prom(o, label="gap"), "uc2_commit_position")
                    for o in self.observers]
            sample = self.fold_tick(time.time() - self.t0, vals)
            if sample is None:
                self.skipped += 1
            else:
                if any(v is None for v in vals):
                    self.partial += 1
                self.samples.append(sample)
            self._stop_ev.wait(SAMPLE_PERIOD_SECS)

    def stop(self):
        self._stop_ev.set()
        self.join(timeout=30)
        return self.samples


def run_roll(hosts, args):
    jg, m6, m12, m14 = _mods()
    nodes, load_host = hosts[:3], hosts[3]
    # The gate-binary view of the same machines (m12_gate roles), same dirs.
    gate_hosts = m6.build_fleet_hosts(m12.BUILT_GATE, args.ssh_user, args.ssh_key, args.hosts,
                                      count=4, unit_prefix="jumbo", remote_root="/opt/bench/jumbo",
                                      probe_bin=m12.BUILT_PROBE)
    gnodes = gate_hosts[:3]

    # 1. sync the tree, build uc2-node with the override feature.
    m14.sync_tree_to(nodes, args.tree, m6.SshHost.UC_SRC)
    if not args.no_build:
        build_uc_node_override(nodes)
        for h in gnodes:
            jg.ssh(h, f"sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup "
                      f"{m6.SshHost.CARGO} build --release --manifest-path {m6.SshHost.UC_SRC}/Cargo.toml "
                      f"-p uc_gateway --example m12_gate", label="build-gate", timeout=1800)

    # 2. three daemons, all pinned to the OLD level 12, metrics on.
    members = [(i, f"{h.private_ip}:19200") for i, h in enumerate(nodes)]
    cfgs = {}
    for i, h in enumerate(nodes):
        body = jg.render_node_toml(i, members[i][1], h.dir, members,
                                   metrics_addr=f"{h.private_ip}:{jg.METRICS_PORT_DEFAULT}")
        body = body.replace('app_id = "uc2-jumbo-gate"', f'app_id = "{APP_ID}"')
        body = body.replace('names = ["reg"]', 'names = ["count"]')
        cfgs[i] = f"{h.dir}/node.toml"
        jg.ssh(h, f"sudo rm -rf {h.dir} && sudo mkdir -p {h.dir} && "
                  f"sudo tee {cfgs[i]} >/dev/null <<'WCFG'\n{body}\nWCFG", label="write-config")
        m12.truncate_log(h, UNIT_NODE)
        start_node_unit(h, cfgs[i], OLD_RANGE)
    edge_members = m12.edge_members_str(gnodes)
    for h in nodes:
        assert wait_cluster_agent(h), f"cluster agent never alive on {h.public_ip}"
    for g in gnodes:
        start_load_side(g, g, edge_members)
    time.sleep(m12.EDGE_SETTLE_SECS + 3)

    # 3. the M14 row-a load shape (steady window), from the fourth host, for
    #    the whole roll: client-remote through every node's edge.
    gateways = ",".join(f"{g.private_ip}:{m12.EDGE_PORT}" for g in gnodes)
    roll_budget = int(len(nodes) * (args.per_node_secs) + LEVEL_WAIT_SECS + 60)
    lg = gate_hosts[3]
    largv = ["client-remote", "--gateways", gateways, "--app-id", APP_ID,
             "--secs", str(roll_budget), "--payload", "64", "--inflight", "256"]
    lcmd = f"sudo {lg.gate} " + " ".join(shlex.quote(x) for x in largv)
    print(f"INFO [ssh {lg.public_ip}] {lcmd}   (load for the whole roll)", flush=True)
    load = subprocess.Popen(lg.ssh + [lg.target, lcmd], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    gaps, levels, load_died = [], {}, False
    try:
        time.sleep(m12.EDGE_SETTLE_SECS + m14.WARMUP_SECS)
        # 4. roll: kill, restart at 12,13, wait for the cluster agent, and
        #    sample commit on a node that is NOT the one restarting.
        for i, h in enumerate(nodes):
            sampler = CommitSampler([n for j, n in enumerate(nodes) if j != i])
            sampler.start()
            for u in (UNIT_EDGE, UNIT_SVC):
                m12.kill_unit(gnodes[i], u)
            m12.kill_unit(h, UNIT_NODE)
            start_node_unit(h, cfgs[i], NEW_RANGE)
            ok = wait_cluster_agent(h)
            print(f"INFO roll n{i}: cluster agent {'alive' if ok else 'NOT alive'}", flush=True)
            start_load_side(gnodes[i], gnodes[i], edge_members)
            time.sleep(SAMPLE_PERIOD_SECS)
            samples = sampler.stop()
            gap = longest_stall(samples) if ok else float("inf")
            print(f"INFO roll n{i}: {len(samples)} commit samples "
                  f"({sampler.skipped} ticks skipped: no observer answered; "
                  f"{sampler.partial} partial: some observer silent), "
                  f"longest stall {gap:.2f}s", flush=True)
            gaps.append(gap)

        # 5. every node at level 13 within the budget.
        levels, deadline = {}, time.time() + LEVEL_WAIT_SECS
        while time.time() < deadline:
            levels = {}
            for i, h in enumerate(nodes):
                v = jg.read_gauge(jg.scrape_prom(h), "uc2_wire_level")
                levels[i] = int(v) if v is not None else -1
            if all(v == TARGET_LEVEL for v in levels.values()):
                break
            time.sleep(1.0)
    finally:
        # 6. stop the load; read the linearizability verdict.
        load_died = load.poll() is not None
        if not load_died:
            load.terminate()
        try:
            load.communicate(timeout=30)
        except Exception:
            load.kill()
    leader_h = gnodes[0]
    rc, out = m12.run_foreground(leader_h, ["check-fsms", "--instance-dir", leader_h.dir,
                                            "--app-id", APP_ID, "--mode", "linearizable"]  # per-node FSM agreement, not a history verdict
    , timeout=90)
    agreement = rc == 0 and re.search(r"^FSMS-OK\s", out, re.M) is not None
    if load_died:
        print("FAIL: client-remote exited before the roll ended; no load was applied", flush=True)
        agreement = False
    verdict, clauses = verdict_row_roll(levels, gaps, agreement)
    print_table(verdict, clauses, levels, gaps)
    for h in nodes:
        m12.kill_unit(h, UNIT_NODE)
    for g in gnodes:
        for u in (UNIT_EDGE, UNIT_SVC):
            m12.kill_unit(g, u)
    return verdict


def print_table(verdict, clauses, levels, gaps):
    print("\n=== wire-level rolling-upgrade row ===")
    print(f"{'clause':<14}{'bar':<30}{'observed':<28}result")
    rows = [
        ("level", f"all nodes == {TARGET_LEVEL}", str(levels)),
        ("gaps", f"max gap <= {ROW_COMMIT_GAP_BAR_SECS}s",
         f"{[round(g, 2) for g in gaps]}"),
        ("fsm_agreement", "check-fsms OK after roll", str(clauses["fsm_agreement"])),
    ]
    for name, bar, obs in rows:
        print(f"{name:<14}{bar:<30}{obs:<28}{'ok' if clauses[name] else 'FAIL'}")
    print(f"RESULT: {verdict}")


def main():
    ap = argparse.ArgumentParser(description="Wire-level rolling-upgrade fleet row")
    ap.add_argument("--selftest", action="store_true", help="verdict arithmetic only; no ssh")
    ap.add_argument("--run", action="store_true", help="run the fleet row")
    ap.add_argument("--tree", default=str(Path(__file__).resolve().parent.parent.parent),
                    help="local checkout to sync and build (default: this repo)")
    ap.add_argument("--hosts", default="", help="pub/priv,... x4 (else terraform output)")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default=os.path.expanduser("~/.ssh/id_ed25519"))
    ap.add_argument("--uc-node-bin", default=None, help="uc2-node path on the hosts")
    ap.add_argument("--per-node-secs", type=int, default=30,
                    help="commit sampling window after each node restart")
    ap.add_argument("--no-build", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest()
        return
    if not a.run:
        ap.error("one of --run or --selftest is required")
    jg, m6, m12, _ = _mods()
    hosts = m6.build_fleet_hosts(a.uc_node_bin or jg.UC_NODE_BUILT_DEFAULT, a.ssh_user, a.ssh_key,
                                 a.hosts, count=4, unit_prefix="jumbo",
                                 remote_root="/opt/bench/jumbo", probe_bin=m12.BUILT_PROBE)
    sys.exit(0 if run_roll(hosts, a) == "PASS" else 1)


if __name__ == "__main__":
    main()
