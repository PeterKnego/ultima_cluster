# UC v2 operations

Everything needed to run, change, and diagnose a cluster.

This page was a single eleven-section runbook until 2026-08-06. Its content now
lives in task-shaped guides and in reference, because the two answer different
questions: a guide tells you what to do about a goal you have, and reference
tells you what a field or a flag *is* when you need to look it up mid-task. The
path is kept because other documents and tooling cite it.

## Running a cluster

- [Run a cluster on real hosts](../how-to/run-a-cluster.md) — durable instance
  directories, the bind-address rule, client placement, process supervision,
  and restart cost. *Was §2, §4.*
- [Keep the journal from growing without bound](../how-to/bound-journal-growth.md)
  — snapshots, then purging, and confirming it works. *Was §5.*
- [Run a gateway](../how-to/run-a-gateway.md) — a TCP front door for clients
  that can't attach to shmem: start/stop, the stats line, one edge per node
  host, what a client sees on `REDIRECT`/`LEADER_CHANGED`/`RETRY`, and the
  faulted-exit/restart contract when a node's instance restarts underneath a
  running gateway.

## Observing a cluster

- [Monitor a cluster](../how-to/monitor-a-cluster.md) — enabling
  `[metrics]`, Prometheus scraping and alert rules, the Grafana dashboard,
  the `/healthz`/`/readyz` probes, and the structured JSON-lines event
  vocabulary.

## Changing a running cluster

- [Change cluster membership without downtime](../how-to/change-cluster-membership.md)
  — add, promote, demote, remove; resize; retire a leader; decommission and
  replace hardware; **signed admin requests** (`--admin-key`,
  `gen-admin-key`, the `auth_*`/`audit_failed` reason codes) and reading
  `uc2ctl audit` (M12b, `v2.6.0`). *Was §6.*
- [Encrypt traffic between nodes](../how-to/encrypt-node-traffic.md) — key
  material, the flag-day rollout, health counters, and rotation; pair with
  `[admin] auth = "hmac"` — see its "Known interaction with admin
  authentication" section. *Was §11.*
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — a scripted flag-day
  binary upgrade (`scripts/uc2_flag_day.sh`), the traffic-stop prerequisite,
  a measured downtime number, and (M12b) the `[crypto].enabled`/`[admin]`
  config-choice note every M9–M11 `node.toml` needs before it starts on
  `v2.6.0`+.

## Surviving failures

- [Back up a cluster](../how-to/back-up-a-cluster.md) — an ordered-copy
  artifact taken from a live, loaded node, verified before you trust it,
  restorable onto a new host; the minority-restore rule.
- [Recover from quorum loss](../how-to/recover-from-quorum-loss.md) — a
  majority of voters is gone: force a survivor back into service with the
  data-loss window stated up front, then wipe-and-rejoin the repaired peers.

## When something is wrong

- [Diagnose a node that is not serving](../how-to/diagnose-a-node.md) — reading
  a live node's control page. *Was §3's procedural half.*
- [Change cluster membership: read the audit log](../how-to/change-cluster-membership.md#read-the-audit-log)
  — `uc2ctl audit --instance-dir D [--tail N] [--json]`, offline, works on a
  stopped node too. Every admin decision (accepted, refused, retried) is
  recorded here before its answer is published, including which key signed
  it or `"filesystem"`/`"unverified"` when nothing did.
- [Investigate a failed correctness run](../how-to/investigate-a-failed-run.md)
  — elle's two tiers, why their assertions invert, and the checklist after
  changing a proved kernel. *Was §9, §10.*
- [Reproduce a published result](../how-to/reproduce-a-result.md) — gate
  binaries, fleet runs, and comparing honestly. *Was §8.*

## Look-up

- [Instance directory](../reference/instance-directory.md) — every file, its
  owner, and its durability class. *Was §1.*
- [The cnc control page](../reference/cnc-page.md) — the pinned layout, field by
  field. *Was §3's field tables.*
- [`uc2ctl`](../reference/uc2ctl.md) — sub-commands, arguments, response
  statuses, refusal reasons.
- [Configuration](../reference/configuration.md) — `NodeConfig`, environment
  switches, crypto file formats, cluster limits.
- [Linearizable read path](../reference/read-path.md) — how reads are certified
  and what their failure signatures mean. *Was §7.*
- [`gateway.toml`](../reference/gateway-config.md) — every gateway key, default,
  and named refusal.
- [The remote protocol](../reference/remote-protocol.md) — the framed TCP
  wire format a gateway client implements against.
- [The state-machine contract](../reference/state-machine-contract.md) —
  `RawStateMachine`/`StateMachine`, and `Sessioned<S>`'s exactly-once
  semantics.

## The two rules worth knowing before anything else

**Never put an instance directory on `tmpfs`.** Every `fsync` becomes a no-op,
the cluster appears healthy, and committed data is lost on power loss.

**One admin client per instance directory at a time.** `uc2ctl` and any harness
writing the admin band directly will interleave and compose a request neither
sent.

## Related

- [Architecture](../ARCHITECTURE.md) — why the system is shaped this way.
- [Verification](../VERIFICATION.md) — what is proved, checked, and merely
  bug-hunted.
- [Benchmarks](../BENCHMARKS.md) — every measured result and what it ran on.
