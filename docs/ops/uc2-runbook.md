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
  **leader only** (rendered `0` elsewhere) is wall clock minus that. Since
  `2.12.0` (unreleased) a backward wall-clock step no longer holds this lag
  open: the log clock smears the step instead of freezing, so
  `Uc2LogTimeFrozen`'s meaning has narrowed to "the appender is stalled" — a
  grown lag now means nothing is being appended, not a clock step. The
  per-node smear gauge (`uc2_log_clock_smear_ns`) and the `log_clock_step`
  record are covered in
  [Monitor a cluster § The log clock and the timer families](../how-to/monitor-a-cluster.md#the-log-clock-and-the-timer-families-2110),
  not repeated here. Per-row timer counters are `uc2_timers_pending`,
  `uc2_timers_fired_total` and `uc2_timers_late_total`; `uc2_timers_pending` is
  the **leader's** count and a follower exports `0`, because the timer heap is
  leader-only since the cluster FSM. The one `[log]` record is `timer_late`
  (emitted only when a fire is late — there is deliberately no per-fire record
  on the consensus agent's hot path). See
  [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md).
- **Do all nodes hold the same schedule table?** `uc2_schedule_table_position`
  is the frame-end position of the table this node's **cluster FSM** has
  applied (`0` = none) and must be identical everywhere;
  `Uc2ScheduleTableDiverged` fires when it is not. `uc2_schedule_entries`
  counts the committed entries naming a row this node declares (a parked `once`
  included, unlike `uc2_timers_pending`), read from the cluster FSM's view so
  it is identical on leader and follower alike; and
  `uc2_schedule_apply_refused_total` counts refused applies. Three records:
  `schedule_table_adopted` (info, whenever the view's table position moves,
  with `source="cluster_fsm"` — one path now) and `cluster_command_applied`
  (info, on every applied `CLUSTER` command, naming the kind and whether the
  FSM accepted it), plus at warn `schedule_apply_refused` (with the 40–43
  reason code) and `schedule_staged_file_kept` (the append succeeded but the
  staged file — `schedules.pending` or `settings.pending`, named in the `file`
  field — could not be deleted; remove it by hand). Since the cluster FSM
  (2.11.0) this alert is **narrow**: the table is state applied at
  commit, so there is no `state/schedules.state` crash window, no
  revert-on-truncation, no wipe keep-alive signature, and a below-floor join
  is not a cause — the snapshot session carries the cluster FSM's own artifact
  (`service_id = 255`), installed before the joiner's floor advances. A node
  reading a different position is a node whose `uc2-cluster` agent is not
  applying; read the position beside `uc2_commit_bytes` on that node. The
  remedy is unchanged: re-run `uc2ctl schedule apply`.
- **What settings is this cluster running?** `uc2ctl settings show` prints the
  committed `admission_bytes`, `fsm_lag`, `snapshot_interval_bytes` and
  `snapshot_target` out of the newest cluster artifact; change them with
  `uc2ctl settings apply <file.toml>` (admin op 7, refusals 44–47, audited as
  `settings_apply`). `[settings]` in `node.toml` seeds genesis only. Both
  `show` commands read a **file**, so they lag the live view and say
  `no cluster artifact yet` until the first snapshot instant completes.
- **What command size can this cluster carry?** `uc2ctl status` prints one
  `ceiling:` line — `ceiling: 8864 B (rung 8960, discovered)` or
  `ceiling: 1344 B (rung 1408, baseline)`. The ceiling is the live cnc word at
  offset 3984 and the rung is read from this node's newest cluster artifact;
  with the default snapshot cadence (`0`, instants are commanded) there may be
  no artifact yet, in which case the line **infers** `discovered` from a
  ceiling the baseline rung cannot produce and says so. A trailing
  `— capped by this node's own max_payload, not by the rung` means the binding
  half is this host's buffer bound, not the cluster's rung. In `/metrics`:
  `uc2_datagram_mtu_bytes` (the committed rung, identical cluster-wide once
  caught up), `uc2_payload_ceiling_bytes`, `uc2_probe_min_mtu_bytes` (this
  node's own proven minimum; `0` = nothing proven, including on a solo node),
  `uc2_probe_sent_total`/`uc2_probe_acked_total`, `uc2_send_emsgsize_total`
  (must be 0) and `uc2_commands_over_standard_total` (frames appended above
  the standard 1312 B ceiling — leader-only, so sum it across the fleet).
  Records: `datagram_mtu_proposed` (leader, per raise) and
  `payload_ceiling_adopted` (every node). A discovery commit is in
  `audit.jsonl` as a `settings_apply` with `source = "discovery"`, `actor =
  "node"` — the one audit line no admin request produced. **Probe traffic that
  never stops is normal on a narrow cluster**: a peer resolves only when the
  top rung is verified *and* its advertised minimum has caught up, so one
  permanently narrow path means 2–3 probe datagrams per 30 s per peer forever.
  See [Run a cluster on jumbo frames](../how-to/jumbo-frames.md).
- **What snapshot set is this node holding?** `uc2ctl snapshot show` prints
  each declared row's newest artifact position, the cluster row's, and
  `set=<P>` — the newest position present in **all** of them, which is the
  purge floor once persisted. It is offline (a directory listing) and never
  opens an artifact. A row whose `newest=` sits below the others is the row
  holding the floor back, and `uc2_snapshot_row_incomplete_total{row}` will be
  climbing for it.

## Changing a running cluster

- [Change cluster membership without downtime](../how-to/change-cluster-membership.md)
  — add, promote, demote, remove; resize; retire a leader; decommission and
  replace hardware; **signed admin requests** (`--admin-key`,
  `gen-admin-key`, the `auth_*`/`audit_failed` reason codes) and reading
  `uc2ctl audit` (M12b, `v2.6.0`). *Was §6.*
- **Apply a schedule table** (2.11.0):
  `uc2ctl schedule apply <file.toml> --instance-dir D --app-id A [--admin-key K]`
  parses the TOML, stages the encoded bytes as `<instance_dir>/schedules.pending`
  (mode `0600`, fsync, rename), and sends admin op `6` carrying that file's
  SHA-256 digest in the signed request fields — so under `[admin] auth = "hmac"`
  the table's contents are authenticated even though they never fit the 64-byte
  admin line. **Run it against the leader**: the staged file is node-local, so a
  follower answers `retry` (status `2`) with the leader hint rather than
  forwarding a request whose payload the leader cannot see. The leader also
  answers `retry` while any previous `CLUSTER` frame — a table, a settings
  record or a membership change — is still above the committed cluster view
  (single in flight, across all three); `uc2ctl` does not poll through a retry — it exits non-zero and
  names the staged file, so re-run the same command. Refusals are `40 schedule_digest`,
  `41 schedule_missing`, `42 schedule_decode`, `43 schedule_unknown_fsm`
  ([`uc2ctl` § Refusal reasons](../reference/uc2ctl.md#refusal-reasons)); a
  refused or timed-out apply **leaves the staged file in place**, so a retry
  needs nothing re-staged, and the node deletes it only after a successful
  append. Every outcome is audited as `schedule_apply` (its `id`/`addr` fields
  render the digest, not an address). Applying **replaces the whole table** —
  to drop one entry, apply a file without it. Read the committed table back with
  `uc2ctl schedule show`, which reads this node's newest cluster artifact under
  `<instance_dir>/snapshots/cluster/`, and see the position on `uc2ctl
  status`'s `config:` line as `schedule_position=`. Both lag the live view —
  an artifact appears at a snapshot **instant** — so on a cluster that is not
  snapshotting yet they say `no cluster artifact yet` and
  `uc2_schedule_table_position` from `/metrics` is the live reading.
- **Take a snapshot** (2.11.0): `uc2ctl snapshot --instance-dir D
  --app-id A [--admin-key K] [--standby]` (admin op `8`, audited as
  `snapshot`). **Run it against the leader** — a follower answers `retry` (status `2`);
  `uc2ctl status`'s `leader_hint` says where — and it prints `instant=<P>`, the frame-end position every
  declared row and the cluster FSM freeze at. When they have all published
  `snap-<P>`, that node's purge floor moves to P. Refused
  `48 snapshot_unsupported` naming any declared row started with plain
  `start()` rather than `start_with_snapshots()` (it would ignore the frame,
  so the set could never complete), and `49 snapshot_no_learner` for
  `--standby` with no learner in the committed membership. A cadence is the
  replicated `snapshot_interval_bytes` (`0`, the default, means
  operator-commanded only).
  **`--standby` freezes only the learners**, which is how you avoid the
  commit stall a large state's freeze causes on a quorum (`P + fsm_lag` until
  the slowest freeze ends). The set comes back to a voter with
  `uc2ctl snapshot fetch --from <learner-id> [--position P]` (admin op `9`,
  **node-local — never forwarded**, so run it against the voter you want it
  on; audited as `snapshot_fetch`). That writes the artifacts **store-only**:
  no state machine is touched, and the floor advances through the ordinary
  completeness path. Status `0` means the pull is underway, not that it
  arrived — poll `uc2_snapshot_fetched_position` or `snapshot show`. Refused
  `50 snapshot_above_durable` for a position above this node's durable
  frontier. Until a voter has fetched, a joiner below its floor is
  **redirected** to a learner that holds the set, so nothing wedges.
  → [Keep the journal from growing without bound](../how-to/bound-journal-growth.md)
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

- **A node refuses to start with `cluster_artifact_corrupt`** (2.11.0).
  The `uc2-cluster` agent found its newest `snapshots/cluster/snap-<pos>.ultcluster`
  and the image failed a check — magic, version, CRC32, or a bounds check
  inside it. The node fail-stops rather than starting: silently rolling the
  cluster row back to an older membership, schedule table and settings record
  would be worse than not starting. **The record names the file.** Remove
  exactly that file and restart. The node recovers from the artifact beneath
  it and replays the gap from the journal, so nothing is lost — **when there
  is one**: since coordinated snapshot instants (2.11.0) retention is
  the node's and keeps the set at the persisted floor plus everything newer,
  so an older artifact exists whenever a newer instant has completed since the
  floor was last persisted, and does not when the corrupt file *is* the floor
  set. In that case, and if a second restart names the next file down, stop:
  that is a storage problem, not a UC one, and the answer is
  [wipe-and-rejoin](../how-to/recover-from-quorum-loss.md), which rebuilds the
  cluster row from a peer's snapshot session. Never edit an artifact in place;
  the CRC is over the whole image.
- **`Uc2PathBelowMtu` fires** (`increase(uc2_send_emsgsize_total[5m]) > 0`).
  With do-not-fragment set, the kernel refused a datagram for size: a path has
  degraded below the rung the cluster committed, or below the 1408 B baseline.
  The committed rung is **monotone and cannot be lowered**, so the only remedy
  is the path — check `ip link` on both ends and the VPC/subnet MTU. Probe
  refusals are counted separately and never fire this rule.
- **`Uc2MtuDiscoveryStalled` fires** (`uc2_probe_min_mtu_bytes >
  uc2_datagram_mtu_bytes` for 60 s). This node proved more than the cluster
  committed, so some *other* member is holding discovery back: read
  `uc2_probe_min_mtu_bytes` on every node — `1408` names the node whose path to
  a peer is narrow, `0` names one with a peer that has not answered at all.
  Whether a cluster that cannot beat the baseline fires this depends on where
  the narrowness is: a narrow **member** (one host's interface MTU) never fires
  it, because it pins every node's own minimum too; a single narrow **path**
  between two members fires it permanently on every node NOT on that path (A–B
  narrow, A–C and B–C jumbo → C has proven the jumbo rung while the committed
  rung stays at the baseline, on a cluster that is at its correct rung). Fix the
  link, or silence the rule for that node.
- **A node refuses to start with `path_below_committed_mtu`, or will not
  serve.** The cluster has committed a jumbo rung and this node's path to some
  member answered below it — either that path is narrow or **this host's own
  interface MTU** cannot carry the rung (the kernel refused the larger probes
  for size); the node cannot tell the two apart, so the refusal names both.
  Fix the MTU and restart: the remedy is never a wipe, because nothing in the
  instance directory is wrong. A member that answers *nothing* (down, slow,
  replaying) never refuses anything — the node keeps replicating and voting —
  and it does not let the node serve on its own either: the gate **holds**
  (`/readyz` 503, `uc2_jumbo_gate_pending = 1`) until the voters this node has
  proven the rung to form a **quorum with it** — self plus one on three
  voters, self plus two on four or five; a joining learner has no vote of its
  own and needs a plain majority of the voters — and then passes with proof
  (`jumbo_gate_passed`, with `proven_voters`/`voters` on the record). There is
  no timer, and none is needed: a node that cannot get a probe ack from a
  quorum of voters cannot get commit acks from them either. So a **rolling
  restart on a cluster with one dead host is not an outage**: the restarted
  survivor proves the rung to the other survivor within a probe round and
  serves. A member ANSWERING below the rung holds serving the same way while
  its ladder runs (~5 s), then refuses by name. A peer that answered once and
  then stopped answering is treated as silent, not narrow: the refusal needs a
  CURRENT answer at a rung below the committed one, and an outlived 1408 ack
  is no proof of 8960 either. On a hold that does not clear, the
  `jumbo_join_gate_armed` record names the committed rung; check
  `uc2_probe_min_mtu_bytes` and which voters are up before anything else.
  `uc2ctl remove <dead-id>` is accepted while
  a gate is pending — admin handling keys on the leader flag, not on the gate.
  `uc2_jumbo_gate_pending` is `1` on a held node, which is what separates this
  from `Uc2LeaderNotServing`'s other cause (an uncommitted `NewTerm`). Under
  `force_jumbo_frames` the same gate fail-stops after 30 s instead, as
  `jumbo_path_too_narrow` (a peer answered below 8832) or `jumbo_peer_silent`
  (a liveness fact, not an MTU one: start the member). Details and remedies:
  [Run a cluster on jumbo frames](../how-to/jumbo-frames.md#6-when-a-node-refuses-to-join).
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
  (2.11.0), `svc_sched.<id>.ring` — the first per-row ring the **node**
  consumes (service → node: schedule, cancel and consumed requests). It takes
  the per-row reservation from 5 MiB to 6 MiB. Since the cluster FSM (2.11.0)
  `svc_sched.<id>.ring` is written **only by a leading node's
  service** and drained only while leading. The same release adds
  `snapshots/cluster/` (durable — `snap-<pos>.ultcluster`, the cluster FSM's
  own artifact holding membership, the schedule table and the settings record
  as of `<pos>`; `uc2ctl backup` **does** copy it, as an artifact family of
  its own, and `verify-backup` decodes the newest one through the same image
  decoder a joiner installs it with) and two transient staged
  payloads in the instance root, `schedules.pending` and `settings.pending`,
  each written by its `uc2ctl … apply` and deleted by the node after a
  successful append. There is no `state/schedules.state`. *Was §1.*
- [The cnc control page](../reference/cnc-page.md) — the pinned layout, field by
  field, including cnc 3.1's per-slot name/hash line (7) and version word
  (line 0, word 1) added for FSM identity, plus the two words log time added
  in the same page version (2.11.0): `log_time_ns` at page 1 offset
  `4048` (written by the **archive agent**, never lowered — the highest leader
  stamp recorded, and what a new leader seeds its clamp from) and
  `timers_pending` at slot line 7 `+488` (written by the **consensus agent**,
  republished every pass). Decoding them raw: they are plain LE `u64`s, so
  `od -A d -t u8 -j 4048 -N 8 cnc2.dat` reads the log clock in nanoseconds and
  `-j $((4096 + 512*ROW + 488))` reads a row's pending-timer count. cnc **3.2**
  (jumbo frames, `2.12.0`) adds one more live word in the same band:
  `payload_ceiling` at offset **3984** (consensus-agent-written, re-published
  when the committed datagram rung moves; `od -A d -t u8 -j 3984 -N 8`), which
  is the door every client and the gateway edge reads per submit. *Was §3's
  field tables.* Raw-offset walkthrough:
  [Diagnose a node → Which FSM is holding the cluster up?](../how-to/diagnose-a-node.md#which-fsm-is-holding-the-cluster-up)
- [`uc2ctl`](../reference/uc2ctl.md) — sub-commands, arguments, response
  statuses, refusal reasons. `status`'s `services:` line gained
  `log_time_ns=<ns>` (raw nanoseconds since the Unix epoch, not RFC 3339 —
  there is no formatter in the binary) and each per-FSM row gained
  `timers_pending=<n>`, both since log time and timers (2.11.0); the
  `config:` line gained `schedule_position=<n>` and the sub-command list gained
  `schedule apply` / `schedule show` with reason codes 40–43. Jumbo frames
  (`2.12.0`) add a `ceiling:` line — the live payload ceiling, its datagram
  rung, and whether that rung is `baseline` or `discovered`.
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
