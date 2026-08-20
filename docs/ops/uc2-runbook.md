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

## Observing a cluster

- [Monitor a cluster](../how-to/monitor-a-cluster.md) — enabling
  `[metrics]`, Prometheus scraping and alert rules, the Grafana dashboard,
  the `/healthz`/`/readyz` probes, and the structured JSON-lines event
  vocabulary.

## Changing a running cluster

- [Change cluster membership without downtime](../how-to/change-cluster-membership.md)
  — add, promote, demote, remove; resize; retire a leader; decommission and
  replace hardware. *Was §6.*
- [Encrypt traffic between nodes](../how-to/encrypt-node-traffic.md) — key
  material, the flag-day rollout, health counters, and rotation. *Was §11.*
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — a scripted flag-day
  binary upgrade (`scripts/uc2_flag_day.sh`), the traffic-stop prerequisite,
  and a measured downtime number.

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
