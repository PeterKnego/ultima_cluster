# UC v2 operations

Everything needed to run, change, and diagnose a cluster.

This page was a single eleven-section runbook until 2026-08-06. Its content now
lives in task-shaped guides and in reference, because the two answer different
questions: a guide tells you what to do about a goal you have, and reference
tells you what a field or a flag *is* when you need to look it up mid-task. The
path is kept because other documents and tooling cite it.

## Getting the binaries

Since `v2.6.0` there are release artifacts, so installing is a download and a
verify rather than a build:

- **Tarballs** — `uc2-<version>-{x86_64,aarch64}-unknown-linux-gnu.tar.gz`
  from [the releases page](https://github.com/PeterKnego/ultima_cluster/releases),
  each with a `.sha256`, a `.sigstore.json` bundle, a signed `SHA256SUMS` and
  a CycloneDX SBOM (`uc2-<version>.cdx.tar.gz`). Inside: `bin/` (`uc2-node`,
  `uc2ctl`, `uc2-gateway`, `counter-service`, `counter-remote`), `packaging/`
  (example configs, systemd units, Prometheus rules, Grafana dashboard,
  `Dockerfile`, `compose.yml`, `quickstart-local.sh`), `LICENSE` and
  `README-release.md`.
- **Container image** — `ghcr.io/peterknego/uc2:<version>`, multi-arch, built
  from those same tarballs and signed by digest.
- **Verify before you run it.** Signing is keyless; a signature only means
  something when pinned to the workflow that produced it. The exact
  `cosign verify-blob` / `cosign verify` invocations are in
  [Install the binaries](../how-to/run-a-cluster.md#install-the-binaries-on-each-host)
  and in the tarball's own `README-release.md`.
- **Upgrading** an existing cluster from one of these is
  [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — the binaries change,
  the flag-day rule does not.
- **Which versions go together, and what may change under you:**
  [the semver policy](../reference/semver-policy.md). One version number
  covers the tag, all twelve crates, the tarballs and the image.
- Cutting one of these releases (maintainers): [Cut a release](../how-to/cut-a-release.md).

## Running a cluster

- [Run a cluster on real hosts](../how-to/run-a-cluster.md) — durable instance
  directories, the bind-address rule, client placement, process supervision,
  and restart cost. *Was §2, §4.*
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — the flag day, and (as
  of 2.7.0) the rule that a host's node, service, gateway and shmem clients
  restart *together*, because the ring file format changed.
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
- [Diagnose a node → Which FSM is holding the cluster up?](../how-to/diagnose-a-node.md#which-fsm-is-holding-the-cluster-up)
  — the per-FSM band (`uc2ctl status`'s services table with its
  `row=name=version=hash=` fields, the `service="<name>",row="<r>"` metric
  families, `Uc2ServiceAbsent` /
  `Uc2ServicePinnedAtLagBound`, and the `service_attached`/`service_detached`
  records).
- **Is the log's clock moving?** `uc2_log_time_ns` on every node is the highest
  leader stamp the archive has recorded; `uc2_log_time_lag_seconds` on the
  **leader only** (rendered `0` elsewhere) is wall clock minus that. A grown lag
  means the leader's clock stepped backwards (stamps hold flat until wall time
  catches up) or nothing is being appended; `Uc2LogTimeFrozen` fires above 5 s
  for 30 s. Per-row timer counters are `uc2_timers_pending`,
  `uc2_timers_fired_total`, `uc2_timers_late_total` and
  `uc2_timers_rearmed_total`; the two `[log]` records are `timer_late` (emitted
  only when a fire is late — there is deliberately no per-fire record on the
  consensus agent's hot path) and `timers_rearmed` (on a leadership loss). See
  [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md).
- **Do all nodes hold the same schedule table?** `uc2_schedule_table_position`
  is the frame-end position of the table this node has adopted (`0` = none) and
  must be identical everywhere; `Uc2ScheduleTableDiverged` fires when it is
  not. `uc2_schedule_entries` counts the adopted entries (a parked `once`
  included, unlike `uc2_timers_pending`), and
  `uc2_schedule_apply_refused_total` counts refused applies. The records are
  `schedule_table_adopted` (info, on every adoption) and
  `schedule_apply_refused` (warn, with the reason code). **A node reading
  position `0` while its peers read a nonzero one has not adopted the table** —
  it joined below the purge floor (the table is not in the snapshot stream), or
  it crashed in the narrow window between recording the frame and persisting
  it. The remedy for both is the same: re-run `uc2ctl schedule apply`.

## Changing a running cluster

- [Change cluster membership without downtime](../how-to/change-cluster-membership.md)
  — add, promote, demote, remove; resize; retire a leader; decommission and
  replace hardware; **signed admin requests** (`--admin-key`,
  `gen-admin-key`, the `auth_*`/`audit_failed` reason codes) and reading
  `uc2ctl audit` (M12b, `v2.6.0`). *Was §6.*
- **Apply a schedule table** (2.11 pending):
  `uc2ctl schedule apply <file.toml> --instance-dir D --app-id A [--admin-key K]`
  parses the TOML, stages the encoded bytes as `<instance_dir>/schedules.pending`
  (mode `0600`, fsync, rename), and sends admin op `6` carrying that file's
  SHA-256 digest in the signed request fields — so under `[admin] auth = "hmac"`
  the table's contents are authenticated even though they never fit the 64-byte
  admin line. **Run it against the leader**: the staged file is node-local, so a
  follower answers `retry` (status `2`) with the leader hint rather than
  forwarding a request whose payload the leader cannot see. The leader also
  answers `retry` while the previous table frame is still above commit (single
  in flight); `uc2ctl` does not poll through a retry — it exits non-zero and
  names the staged file, so re-run the same command. Refusals are `40 schedule_digest`,
  `41 schedule_missing`, `42 schedule_decode`, `43 schedule_unknown_fsm`
  ([`uc2ctl` § Refusal reasons](../reference/uc2ctl.md#refusal-reasons)); a
  refused or timed-out apply **leaves the staged file in place**, so a retry
  needs nothing re-staged, and the node deletes it only after a successful
  append. Every outcome is audited as `schedule_apply` (its `id`/`addr` fields
  render the digest, not an address). Applying **replaces the whole table** —
  to drop one entry, apply a file without it. Read the adopted table back with
  `uc2ctl schedule show`, which reads `<instance_dir>/state/schedules.state`,
  and see the position on `uc2ctl status`'s `config:` line as
  `schedule_position=`.
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
  owner, and its durability class, including the per-declared-FSM files since
  M14 (`svc_query.<id>.ring`, `egress_service.<id>.broadcast`,
  `service.<id>.lock`, `snapshots/<id>/`) and, since log time and timers
  (2.11 pending), `svc_sched.<id>.ring` — the first per-row ring the **node**
  consumes (service → node: schedule, cancel and consumed requests). It takes
  the per-row reservation from 5 MiB to 6 MiB. The schedule table adds two more
  paths in the same release: `state/schedules.state` (durable, the newest
  **adopted** table; copied by `backup`/`restore` with the rest of `state/`, and
  optional — it is not in the verify checklist, so a pre-2.11 artifact stays
  valid) and `schedules.pending` in the instance root (transient, written by
  `uc2ctl schedule apply`, deleted by the node after a successful append).
  *Was §1.*
- [The cnc control page](../reference/cnc-page.md) — the pinned layout, field by
  field, including cnc 3.1's per-slot name/hash line (7) and version word
  (line 0, word 1) added for FSM identity, plus the two words log time added
  in the same page version (2.11 pending): `log_time_ns` at page 1 offset
  `4048` (written by the **archive agent**, never lowered — the highest leader
  stamp recorded, and what a new leader seeds its clamp from) and
  `timers_pending` at slot line 7 `+488` (written by the **consensus agent**,
  republished every pass). Decoding them raw: they are plain LE `u64`s, so
  `od -A d -t u8 -j 4048 -N 8 cnc2.dat` reads the log clock in nanoseconds and
  `-j $((4096 + 512*ROW + 488))` reads a row's pending-timer count. *Was §3's
  field tables.* Raw-offset walkthrough:
  [Diagnose a node → Which FSM is holding the cluster up?](../how-to/diagnose-a-node.md#which-fsm-is-holding-the-cluster-up)
- [`uc2ctl`](../reference/uc2ctl.md) — sub-commands, arguments, response
  statuses, refusal reasons. `status`'s `services:` line gained
  `log_time_ns=<ns>` (raw nanoseconds since the Unix epoch, not RFC 3339 —
  there is no formatter in the binary) and each per-FSM row gained
  `timers_pending=<n>`, both since log time and timers (2.11 pending); the
  `config:` line gained `schedule_position=<n>` and the sub-command list gained
  `schedule apply` / `schedule show` with reason codes 40–43.
- [Monitor a cluster → The per-FSM families](../how-to/monitor-a-cluster.md#the-per-fsm-families-m14)
  — which metric families carry a `service` label, what the unlabeled
  aggregate means now, and the declared-set drift query. `Uc2ServiceIdentityDrift`
  fires the same way when a row's declared NAME itself differs node-to-node
  (a mis-declared `[services]` config, not a missing FSM); `Uc2ServiceVersionDrift`
  fires when an attached FSM's version differs node-to-node (expected transiently
  during a rolling upgrade, a bug if it persists).
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
- [Threat model](../security/threat-model.md) — what is defended and what is
  out of model, with [the attack surface](../security/attack-surface.md)'s
  bind-address guidance for the three listening ports. *Was §11's posture
  half.*
- [Verification](../VERIFICATION.md) — what is proved, checked, and merely
  bug-hunted.
- [Benchmarks](../BENCHMARKS.md) — every measured result and what it ran on.
