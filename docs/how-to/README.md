# How-to guides

Task-shaped guides for people running `ultima_cluster`. Each one answers a
goal you actually have, assumes you already know your way around a distributed
system, and links out rather than teaching.

If you have never run a cluster, start with [the quickstart](../QUICKSTART.md)
instead — it is a lesson, and it leaves you with a working three-node cluster to
apply these guides to.

## Running a cluster

The two guides most people need first: getting nodes onto real machines, and
keeping the disk from filling once they are there.

- [Run a cluster on real hosts](run-a-cluster.md) — binaries and configs onto
  each machine, the address rule that causes the most confusing failure in the
  system, where client processes must live, process supervision that does not
  hang on shutdown, and what a planned restart costs.
- [Keep the journal from growing without bound](bound-journal-growth.md) —
  snapshots first, then purging, and how to tell that it is working.
- [Monitor a cluster](monitor-a-cluster.md) — Prometheus scraping, the alert
  rules, the Grafana dashboard, the `/healthz`/`/readyz` probes, and the
  structured-event vocabulary.

## Changing a running cluster

Both of these are live operations. Neither requires a restart, and both have
preconditions worth reading before you start.

- [Change cluster membership without downtime](change-cluster-membership.md) —
  grow, shrink, replace a machine, or retire the current leader.
- [Encrypt traffic between nodes](encrypt-node-traffic.md) — a flag day, not a
  rolling change: the whole cluster flips together.

## Building on it

- [Write a service binary](write-a-service-binary.md) — the lifecycle template
  for the half that runs your state machine: signal handling, apply-agent
  supervision, and why leaving either out fails quietly.

## When something is wrong

- [Diagnose a node that is not serving](diagnose-a-node.md) — what a live node
  believes about itself, read from its control page.
- [Investigate a failed correctness run](investigate-a-failed-run.md) — telling
  a real consistency violation from a harness that has lost its teeth, and how
  to judge whether a fix worked.
- [Reproduce a published result](reproduce-a-result.md) — re-run a performance
  or correctness claim on your own hardware and compare honestly.

## Related

- [Reference](../reference/) states the surfaces these guides drive: the CLI,
  the control page, configuration, the wire.
- [Architecture](../ARCHITECTURE.md) explains why the system is shaped this way.
- [Operations runbook](../ops/uc2-runbook.md) is the historical home of this
  material and now points here.
