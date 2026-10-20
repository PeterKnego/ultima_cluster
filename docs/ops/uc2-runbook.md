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
  [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — stop-all for a flag
  day, or one node at a time since `2.15.0` for a release within one wire
  level of the cluster's.
- **Which versions go together, and what may change under you:**
  [the semver policy](../reference/semver-policy.md). One version number
  covers the tag, all fourteen crates, the tarballs and the image.
- Cutting one of these releases (maintainers): [Cut a release](../how-to/cut-a-release.md).

## Running a cluster

- [Run a cluster on real hosts](../how-to/run-a-cluster.md) — durable instance
  directories, the bind-address rule, client placement, process supervision,
  and restart cost. *Was §2, §4.*
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — the flag day, the
  rolling upgrade one node at a time under load (since `2.15.0`, for a
  release within one wire level of the cluster's), and (as of 2.7.0) the
  rule that a host's node, service, gateway and shmem clients restart *together*
  whenever the ring format or the cnc minor changes.
- [Keep the journal from growing without bound](../how-to/bound-journal-growth.md)
  — snapshots, then purging, and confirming it works. *Was §5.*
- [Run a gateway](../how-to/run-a-gateway.md) — a TCP front door for clients
  that can't attach to shmem: start/stop, the stats line, one edge per node
  host, what a client sees on `REDIRECT`/`LEADER_CHANGED`/`RETRY`, and the
  faulted-exit/restart contract when a node's instance restarts underneath a
  running gateway.

### Boot order: nodes first, then services (2.13.0)

A node does not open its attach door until it has **joined its cluster** — it
knows a leader, has learned a commit position, and its cluster FSM has applied
every committed `CLUSTER` frame (upgrade pins included). Until then a service
or client attaching there is refused `NodeBooting`, which
`ServiceBuilder::start*`, `Client::connect` and `Engine::attach` wait out
internally for `boot_wait` (default **10 s**, `Duration::ZERO` = no wait).

- **Start every node, wait for a leader, then start the services.** Under
  systemd on separate hosts this happens by itself; the case that bites is a
  harness or embedded deployment bringing up several nodes **in one process**
  and interleaving node-then-service — the first service burns its whole
  `boot_wait` waiting for a leader that cannot be elected until a peer that
  has not been started yet exists.
- **`declared=[]` with no FSM rows on a configured node means "not joined
  yet", not "misconfigured."** So does `uc_services_declared 0` with no
  per-FSM series on `/metrics`. One `services_declared_published` record marks
  the transition; it should follow `node_listening` within a pass or two.
- **A saturated node wants a longer `boot_wait`.** The cluster FSM applies at
  `min(commit, durable)`, so a node whose own durable position persistently
  trails commit keeps the gate shut.
- **Clients wait for every declared row's version (wire `0.10.0`, #33).**
  The leader admits no client write until every declared row has a committed
  running version, which it records from **its own** attached service for
  that row. A declared row whose service never starts on the leader keeps the
  whole cluster closed to writes — clients see back-pressure, not an error —
  and the leader logs `version_gate_waiting` every 5 s naming the row and
  `leader_service_attached`. Start every declared row's service on every
  node, on the same major.minor: a follower's service on another line is
  refused `VersionMismatch`, or stops at the genesis record with
  `version_superseded`. See
  [Upgrade an application § The version rules](../how-to/upgrade-an-application.md#the-version-rules).

Why the gate exists: an upgrade pin committed *above* the artifact a
restarting node recovered from is invisible on that node for a few passes,
and a service attaching in that window would read "no pin" and replay from
genesis under a new binary. See
[Upgrade a cluster](../how-to/upgrade-a-cluster.md) and
[Configuration § Attaching a service or a client](../reference/configuration.md).

### Placing the processes on cores

Two deploy-time choices move the commit path's own latency more than any
transport setting, both measured closed-loop at inflight 1 on 8-vCPU
`c6id.2xlarge` hosts
([the service-time record](../benchmarks/uc2-service-time-2026-09-16.md)):

- **Pin the node and the service to disjoint physical cores.** Unpinned,
  the shipped path read p50 123 µs with a 92 µs spread across repeats;
  pinned, 109 µs with no spread, and 41 µs less mean. On an 8-vCPU host
  whose SMT siblings are `(0,4) (1,5) (2,6) (3,7)`, the rig's layout is the
  node on `0,1,4,5` (both threads of two cores, one per busy-spin agent
  pair), the service on `2`, and clients or a gateway on `3,7` — as
  `CPUAffinity=` on the systemd units. Check the sibling map with
  `lscpu -p=CPU,CORE` first; a wrong map pins agents onto each other's
  siblings and costs throughput.
- **The service's apply agent idles on a ladder since that record.** Up to
  `2.12.0` its idle was a flat 50 µs sleep, which at low load landed on
  about half of all responses as a second mode ~100 µs above the first
  (p90 194 µs against p50 109 on that record; 117 µs with the sleep gone).
  The default is now spin → yield → the same 50 µs sleep, which keeps the
  agent awake across a client round trip after each frame and costs
  nothing when the service is truly idle or fully busy. `UC2_APPLY_IDLE=spin`
  in the service's environment still buys the last few microseconds for one
  pegged core per service, and `sleep:50` restores the old posture. Set it
  per service process, not cluster-wide — it is a capacity decision for
  that host. See
  [the environment switches](../reference/configuration.md#environment-switches).

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
  `2.12.0` a backward wall-clock step no longer holds this lag
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
  Beside those three, the FSM upgrade lifecycle (`2.13.0`) adds
  `uc2_upgrade_pin_origin`, `uc2_upgrade_pin_version` and
  `uc2_snapshot_hash_mismatch`, per row; `uc2ctl upgrade show` reads the same
  committed state.
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
  `snapshot_target` out of the running node's live cluster view (the newest
  cluster artifact when the node is stopped); change them with
  `uc2ctl settings apply <file.toml>` (admin op 7, refusals 44–47, audited as
  `settings_apply`). `[settings]` in `node.toml` seeds genesis only.
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
- **What wire level is this cluster at, and can it rise?** `uc2ctl status`
  prints `wire: level=L binary=MIN..MAX` — the committed level (cnc
  `wire_level`, offset 3992) and this node's binary range (cnc `wire_binary`,
  4000). `uc2ctl wire show` adds the raise mode and, **on the live leader
  only**, each member's reported wire max and the verdict (`raisable to N`,
  `at level`, or the holdout). In `/metrics`: `uc2_wire_level`,
  `uc2_wire_binary_min`/`_max`, `uc2_wire_member_max{peer}` (leader-only;
  `0` = no report this term) and `uc2_wire_datagrams_refused_total{reason}`.
  `Uc2WireLevelBelowMembers` fires after 30 min below every binary's max: in
  manual mode the raise is due, in auto mode a member is not reporting. See
  [The committed cluster wire level, explained](../notes/uc2-wire-level-explained.md).
- **What snapshot set is this node holding?** `uc2ctl snapshot show` prints
  each declared row's newest artifact position, the cluster row's, and
  `set=<P>` — the newest position present in **all** of them. It is offline
  (a directory listing) and never opens an artifact. A row whose `newest=`
  sits below the others is the row holding the floor back, and
  `uc2_snapshot_row_incomplete_total{row}` will be climbing for it. Since
  `0.11.0` (the snapshot catalog, `2.14.0`) `set=` is the node-local,
  on-disk reading only — see the next bullet for the cluster's own view,
  which is what actually bounds the purge floor.
- **What does the cluster agree it can rebuild from?** The snapshot catalog
  (`0.11.0`, since `2.14.0`,
  [design spec](../superpowers/specs/2026-10-01-uc2-snapshot-catalog-design.md))
  replaces "the newest set that exists on disk" with "the newest set every
  declared row and the cluster artifact **agree** on" as the purge floor and
  every install source. A node's own effective floor is the newest agreed
  set it actually holds, which can sit below the cluster-wide one — the
  normal state of a voter on a learner-only cluster (`snapshot_target =
  "learners"`), not a stall. **Read it live with `uc2ctl catalog`** (or `GET
  /cluster` on `[metrics]`): every set with its state, `agreed`, per-row
  verdicts and hashes, who holds it complete (on the leader), the backup
  tiers, the purge gate, and this node's own holdings. When the node is
  stopped the same command reads the newest cluster artifact and says so.
  Fields and conventions: [The cluster view](../reference/cluster-view.md);
  what each `attention` entry means and what to do: [Cluster view attention
  codes](#cluster-view-attention-codes) below. The five gauges stay the
  **alerting** surface: `uc2_catalog_sets` (how many sets are listed),
  `uc2_catalog_agreed_position` (the cluster floor), `uc2_catalog_empty`
  (`1` while no set has ever completed — the flag-day/genesis fallback
  window, during which the floor and retention both run on today's
  pre-catalog rule instead), `uc2_catalog_stalled` and `uc2_catalog_diverged`
  (how many commanded or disagreeing sets are visible).
  `Uc2SnapshotSetDiverged` keys on `uc2_catalog_diverged` rather than
  comparing per-node gauges directly.
- **What does the whole cluster look like right now?** `uc2ctl cluster
  --instance-dir D --app-id A` prints membership, settings, the schedule
  table, per-row upgrade state, the catalog, tiers, the purge gate, this
  node's services and holdings, and an `attention` list; `--json` prints the
  `uc2.cluster-view/1` document. Ask the **leader** for the per-member
  holdings (`nodes`). Offline (node stopped) it reads the newest cluster
  artifact and prints `offline: newest artifact at P`.

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
  `uc2ctl schedule show`, which reads the running node's live cluster view
  (this node's newest cluster artifact under
  `<instance_dir>/snapshots/cluster/` when it is stopped), and see the
  position on `uc2ctl status`'s `config:` line as `schedule_position=`, which
  still reads the artifact and so lags until a snapshot **instant**;
  `uc2_schedule_table_position` from `/metrics` is the live gauge.
- **Take a snapshot** (2.11.0): `uc2ctl snapshot --instance-dir D
  --app-id A [--admin-key K] [--standby]` (admin op `8`, audited as
  `snapshot`). **Run it against the leader** — a follower answers `retry` (status `2`);
  `uc2ctl status`'s `leader_hint` says where — and it prints `instant=<P>`, the frame-end position every
  declared row and the cluster FSM freeze at. When they have all published
  `snap-<P>`, that node's purge floor moves to P. Refused
  `48 snapshot_unsupported` naming any declared row without the snapshot
  capability bit (it would ignore the frame, so the set could never
  complete — since #67 every Rust SDK service sets this bit unconditionally,
  so it now only fires for a service attached without the current Rust SDK (a non-Rust attacher, or a binary built before #67)), and
  `49 snapshot_no_learner` for `--standby` with no learner in the committed
  membership. A cadence is the
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
- **Restarts start from the newest agreed set** (snapshot lifecycle). An
  unpinned row that restarts installs the node's **start set** when it is
  ahead of the row's own state, then replays only the tail; the service
  prints `uc_service: row N started from snap-P` on stderr, once per
  position. The node publishes the start set in two cnc slot words, `+264`
  (position) and `+272` (version), readable with the cnc decode. A pinned
  row takes it only once its pin is **complete** — the catalog lists an
  agreed set the pin's `to` line built above the pin record, and only such a
  set is published for a pinned row; until then the pin's origin wins.
- **Completing an upgrade pin.** After upgrading every instance of a pinned
  row, take an instant (`uc2ctl snapshot`). Once it agrees, the pin is
  complete — an agreed set above the pin record, on the `to` line (for a
  `--patch` pin an old same-line build's set counts too): the row resumes
  normal start (from that set) and normal retention, and the origin's set is
  released — retired by the catalog, and pruned on each node once that node
  holds the completion set (its own freeze or an auto-fetch) and its `to`
  instance has caught up to it.
  Until then **every** node keeps the origin's set and holds its journal at
  the origin (`snapshot_floor_held_for_pin`), because an instance not yet
  upgraded still has to install the origin and replay from it; a cluster
  that never snapshots after an upgrade grows its journal without bound.
- **Reading a refused fetch.** The node's own background fetch logs
  `snapshot_fetch_requested` with `actor=auto`; a miss logs
  `snapshot_fetch_timeout`, `snapshot_fetch_no_holder` (every candidate was
  tried; the next attempt waits 30 s) or `snapshot_fetch_skipped_no_space`
  (once per set). `snapshot_fetch_free_unknown` (warn, once per node start)
  says this node's free-space reading never succeeded, so fetches proceed
  with the space check off and `Uc2SnapshotWontFit` cannot fire for it.
  `uc2_snapshot_auto_fetch_total{outcome}` counts each of
  `ok`, `refused` (this node could not even issue the request), `timeout`
  (a holder stayed silent), `no_space` and `no_holder`. Residual: a new
  fetch drops the receiver's one parked late-answer slot, so a very late
  datagram from an earlier holder can be lost; auto-fetch waits at least 1 s
  after a timeout to keep that rare.
- [Encrypt traffic between nodes](../how-to/encrypt-node-traffic.md) — key
  material, the flag-day rollout, health counters, and rotation; pair with
  `[admin] auth = "hmac"` — see its "Known interaction with admin
  authentication" section. *Was §11.*
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — both procedures: the
  rolling upgrade within one wire level (next bullet) and the scripted
  stop-all flag day (`scripts/uc2_flag_day.sh`) with its traffic-stop
  prerequisite and a measured downtime number, and (M12b) the `[crypto].enabled`/`[admin]`
  config-choice note every M9–M11 `node.toml` needs before it starts on
  `v2.6.0`+.
- **Roll an upgrade one node at a time** (since `2.15.0`). Permitted when the
  new release's wire minor is the cluster's level (`uc2ctl wire show` →
  `level=`) or one above it. Want a rollback window? Add `wire_raise =
  "manual"` to the cluster's **full** settings file (the one last applied, or
  what `uc2ctl settings show` prints) and `uc2ctl settings apply` it **before
  the first restart** — the apply replaces the whole record, and every absent
  key except `wire_raise` reverts to its default. Then, per
  host: stop the node and everything attached to it, install, start the
  node, wait for `can_serve`, start its services. After the last host,
  `uc2ctl wire show` on the leader should say `verdict: raisable to N`; auto
  mode raises within a second, manual waits for `uc2ctl wire raise` (admin
  op 12, leader-only, audited `wire_raise`; a cluster already at level answers
  `ok` and appends nothing; refused `63 wire_holdout` while a member has no
  report this term or reports below the target). The raise is the point of
  no return. Procedure:
  [Upgrade a cluster § Rolling upgrades since 2.15.0](../how-to/upgrade-a-cluster.md#rolling-upgrades-since-2150).

## Surviving failures

- [Back up a cluster](../how-to/back-up-a-cluster.md) — an ordered-copy
  artifact taken from a live, loaded node, verified before you trust it,
  restorable onto a new host; the minority-restore rule.
- [Recover from quorum loss](../how-to/recover-from-quorum-loss.md) — a
  majority of voters is gone: force a survivor back into service with the
  data-loss window stated up front, then wipe-and-rejoin the repaired peers.

## When something is wrong

- **`Uc2BackupBehind` fires** (backup tier). The tier named by the `tier`
  label (16 hex digits; `uc2ctl tier show --target T --instance-dir D`
  prints the same id as `0x…`) is more than 256 MiB of
  committed log behind for 15m. The alert keys on the level of
  `uc2_backup_lag_bytes`, which only the leader exports; it does not fire
  because commit moved while `uc2_backup_through` stood still — an idle
  cluster's commit creeps on watermark and snapshot-report frames and the
  writer cuts no chunk for them. Find the writer node (the one whose
  `uc2_backup_bytes_written_total` is moving, or should be), then read its
  `uc2_backup_errors_total{reason}` and its `uc2-backup` log lines: an
  unreachable or full target is `unavailable`, a journal it cannot read is
  `journal_unavailable`. If no node runs a writer for that tier, nothing
  advances it — start one or `uc2ctl tier forget` the tier. **On a fork, or a
  cluster restored from the tier whose writers moved to a new target**, the
  inherited entry for the old tier is never advanced and this alert fires by
  design: `uc2ctl tier forget --tier-id 0x…` that old id.
- **`Uc2BackupHoldReleased` fires.** A writer node ran low on disk and let go
  of the journal it was holding for its tier (`backup_hold_released` in its
  log), so ordinary purging resumed and the tier will have a hole. Free disk
  on that node first. When the writer resumes it counts
  `uc2_backup_chain_gaps_total` and the tier's chain restarts from a later
  set — earlier positions are no longer restorable from that tier. Fix what
  held the writer back (`Uc2BackupBehind`, the errors counter) so it does not
  recur.
- **`Uc2BackupGateReleased` fires.** A node with backup-gated purge on
  (`purge_requires_tiers`) ran low on disk and let go of the gate
  (`backup_gate_released` in its log), so it purged journal that fewer than K
  tiers confirm. Free disk on that node first; `uc2_backup_gate_released`
  shows whether the release still holds (`backup_gate_rearmed` when it ends).
  Then fix what held the tiers back (`Uc2BackupBehind`, the errors counter),
  or the cluster will do it again. Positions before that node's journal start
  are restorable only from the tier.
- **`backup_readback_failed` appears.** The writer put an object in the tier
  that did not read back clean, so its report does not advance past it
  (`uc2_backup_errors_total{reason="readback"}`, `Uc2BackupWriterError`). It
  retries each pass. If it persists, run `uc2ctl tier verify --deep --target
  …`: it names the object; the target's storage is the usual cause.
- **`tier_install_declined` appears** on a node that fell below the leader's
  purge floor. The leader asked it to install from its tier and it could not;
  `reason` says why (`no_source`: no `[tier_source]`/`[backup]` on this node;
  `not_registered`: the tier is unreadable or another cluster's; `no_set`:
  the tier holds no usable set at or above the floor; `verify`: a copied
  object disagreed with the leader's hash, or the set's term-map line
  disagreed with the leader's, `detail="lineage_mismatch"`). The leader opens an ordinary
  snapshot session after it, so nothing is lost. A node that should have
  installed from the tier: check the mount and `tier verify`.
- **`tier_set_deferred` appears** on a writer: it has put a set off ten
  passes running. Look at whether the node recently restarted; it clears when
  the writer's term map reaches below the set.
- **`Uc2BackupWriterError` fires.** The `reason` label decides. `foreign`:
  the target's `TIER` object names another cluster — the writer has STOPPED
  and will not touch it; point `[backup] target` at the right place and
  restart the node. `overlap_mismatch`: an object already in the tier
  disagrees with this node's journal — also STOPPED; do not overwrite the
  tier, investigate which cluster wrote it. `unavailable`: target I/O, retried
  on a ladder; check the target's reachability, permissions and space.
  `journal_unavailable`: this node's own journal could not supply a range
  (read error, or purged under the cut); retried. `hash_mismatch`: a local
  snapshot set does not match the catalog's agreed hashes, so it was skipped —
  run `uc2ctl upgrade show` and treat it as a divergent replica. `fenced`: a
  restore rolled this target's history back (`fence/<Q>`; `tier show` lists
  it) — STOPPED; point `[backup] target` at a NEW, empty directory and
  restart the node (never delete the fence to reuse the old tier: its
  history past Q is not this cluster's). `no_hardlinks`: the target's
  filesystem cannot hard-link (SMB/CIFS, some FUSE mounts) — STOPPED; move
  the target to a filesystem that can. This alert looks back 10 minutes;
  a stopped writer keeps `Uc2BackupWriterStopped` firing.
- **`Uc2BackupWriterStopped` fires.** `uc2_backup_writer_stopped == 1`: the
  node's writer refused its target for good (`foreign`, `overlap_mismatch`,
  `fenced` or `no_hardlinks` — the errors counter and the
  `backup_writer_stopped` log line say which) and holds nothing, so the node
  purges as if it had no writer and the tier falls behind. Act on the reason
  as under `Uc2BackupWriterError`, then restart the node; the gauge clears on
  restart.
- **After a whole-cluster restore from a tier** (`uc2ctl tier restore` on
  every member, same `--at Q`, each into an empty instance dir — keep the
  moved-aside originals until the restored cluster is verified). If `Q` was
  below the tier's reach, the restore fenced the tier: before starting the
  nodes, point every writer's `[backup] target` at a new, empty directory.
  Once the new tier reports, `uc2ctl tier forget` the old tier's id, or
  `Uc2BackupBehind` fires on it. `on_committed` runs again for the commands
  between the start set and `Q` (at-least-once).

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
  voters, self plus two on four or five; a joining **learner** has no vote
  and needs no quorum, so it passes on ONE proven voter — and then passes
  with proof (`jumbo_gate_passed`, `gate = join`, with
  `proven_voters`/`voters`/`self_vote` on the record). There is no timer, and
  none is needed: durable reports and votes are pairwise-sealed exactly like
  probes, so a node that cannot get a probe ack from a quorum of voters
  cannot get their commit acks or votes either. So a **rolling restart on a
  cluster with a single dead host is not an outage**: the restarted survivor
  proves the rung to the other survivor within a probe round (1 s on the
  fast ladder; up to 30 s on the slow cadence once the ladder is spent) and
  serves. A member ANSWERING below the rung does not hold on its own either —
  only the missing quorum holds — and once its ladder runs out (~5 s) it
  refuses by name. A peer that answered once and then stopped answering is
  treated as silent, not narrow: the refusal needs a CURRENT answer at a rung
  below the committed one, and an outlived 1408 ack is no proof of 8960
  either. **A hold that does not clear is not silent**: every 30 s the node
  writes `jumbo_join_gate_holding` (warn) with the committed rung, the quorum
  terms it is short of and every member short of the rung (`id:carried`,
  `0` = silent), and `Uc2JumboGateHeld` (warning) fires after 5 min in any
  role. Read that record first, then `uc2_probe_min_mtu_bytes` on the
  members it names; with wire crypto on, a member whose pairwise session
  never came up looks exactly like a dead one here, so check
  `uc_crypto_handshake_failures_total` too.
  `uc2ctl remove <dead-id>` is accepted while
  a gate is pending — admin handling keys on the leader flag, not on the gate.
  `uc2_jumbo_gate_pending` is `1` on a held node, and `Uc2LeaderNotServing`
  excludes that case, so a held leader raises `Uc2JumboGateHeld` and nothing
  else. `force_jumbo_frames` changes none of this once THIS node has learned
  the committed rung: from then on the join gate takes precedence, so a
  silent member is a hold, never a `jumbo_peer_silent` fail-stop. Before it
  has (a replay that has not reached the `CLUSTER` frame, a fresh joiner
  waiting on its snapshot) a forced node runs the force gate's rule and can
  fail-stop `jumbo_peer_silent` / `jumbo_path_too_narrow` at 30 s. A learner's
  one proven voter need not be the leader, whose path its frames actually
  cross: a silent leader is not tested by a learner's pass.
  Details and remedies:
  [Run a cluster on jumbo frames](../how-to/jumbo-frames.md#6-when-a-node-refuses-to-join).
- **A node stops with `WireLevelAboveBinary`, `WireLevelBelowBinary` or
  `ClusterRecordUndecodable`** (`2.15.0`). The three wire-level fail-stops,
  siblings of `path_below_committed_mtu`'s `PathBelowCommittedMtu`: the
  `uc2-cluster` agent stops, the daemon exits 1 and the name is on stderr
  (`cluster fatal (fail-stop): …`, with a `wire_level_fault` or
  `cluster_record_undecodable` record). `WireLevelAboveBinary` — the cluster
  has committed a wire level above this binary's max; it is checked at every
  `Settings` apply, artifact install and boot. The binary is too old:
  install a release whose wire minor is at least the level, then start it.
  A node rolled back to the old binary after the raise lands here, which is
  the point. `WireLevelBelowBinary` — the committed level is below this
  binary's min, judged only once the node has joined (leader known, commit
  learned, the cluster agent's walk at that commit), so a replay through old
  history never trips it. The binary is too new: install the release the
  message names (level + 1) first, stop-all upgrades included. Neither needs
  a wipe; nothing in the instance directory is wrong. `ClusterRecordUndecodable`
  — a committed `CLUSTER` record no correct peer could have written at the
  committed level (an unknown kind, or a `Settings` version above the
  level's; the record's `reason` says which): the journal is corrupt or a
  peer broke the level. A journal that reaches back before `2.11.0` holds
  pre-cluster-FSM `CONFIG` frames that can read this way — give it a cluster
  artifact first (`uc2ctl snapshot` on a node that has one); otherwise
  restore from backup. A known kind with a malformed payload is not this
  stop: it is the cluster FSM's ordinary refusal 42. **A node that never joins and prints
  `wire_refused` lines instead** is two or more levels away from its peers
  (refused both ways on header byte 13): it waits in `NodeBooting` and does
  not fail-stop. Install a binary within one level of the cluster's.
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

### Cluster view attention codes

The `attention` list of `uc2ctl cluster` / `uc2ctl catalog` / `GET /cluster`
names only conditions the node already detects. Each code, and what to do:

- **`catalog_empty`.** No snapshot set has ever completed, so the purge floor
  and retention run on the pre-catalog rule. Normal on a new cluster; take a
  first instant with `uc2ctl snapshot` (or set `snapshot_interval_bytes`).
  Same source as `uc2_catalog_empty`. Never raised from an offline read.
- **`set_stalled`** (context `set`). A set was commanded and a newer one has
  been commanded since, yet it never completed: some row or the cluster FSM
  did not freeze it. Run `uc2ctl snapshot show` on each node and find the
  row whose `newest=` trails; check that its service is attached
  (`uc2ctl status`). The newest commanded set is "in progress", not
  stalled, so this differs from `uc2_catalog_stalled`, which counts every
  commanded entry. Alert: `Uc2SnapshotStalled`.
- **`set_diverged`** (context `set`, `row`). Replicas reported different
  artifact hashes for that row (`"cluster"` is the cluster artifact). The set
  is not `agreed`, so it cannot become the floor or an install source. Treat
  the minority replica as divergent; run `uc2ctl upgrade show` and compare
  `artifact_hash=` per node in `uc2ctl status`. Alert:
  `Uc2SnapshotSetDiverged`.
- **`purge_gate_released`.** This node ran low on disk and let go of the
  backup-gated purge hold, so it purged journal that fewer than K tiers
  confirm. See `Uc2BackupGateReleased` under [When something is
  wrong](#when-something-is-wrong).
- **`tier_behind`** (context `tier`, `lag_bytes`). That tier's confirmed
  `through` trails this node's commit by more than 256 MiB. See
  `Uc2BackupBehind` under [When something is wrong](#when-something-is-wrong);
  unlike the alert there is no 15-minute hold, so a brief spike shows here
  first.
- **`node_stale`** (context `node`; leader's document only). The leader has
  not heard that member's holdings for more than three maximum election
  timeouts (or never). Check the member's reachability and that it is running
  (`uc2ctl status` on it); `nodes[].holdings` for it is old or `null`.

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
  decoder a joiner installs it with — and the zero-byte `snap-<pos>.foreign`
  marker beside a set this node received whole instead of building, which
  backup and restore carry with it). **Operator rule:** the marker is how a
  node knows a set is a copy and must not be reported as its own
  observation. If you copy a set's artifacts into `snapshots/` by hand,
  create `snapshots/cluster/snap-<pos>.foreign` first (before copying any
  of that set's files in), or that node becomes a spurious attester —
  its reports for the set agree with the source by construction and count
  as an independent builder in the catalog's verdict. And two transient staged
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
  is the door every client and the gateway edge reads per submit. cnc
  **3.5** (the wire level, `2.15.0`) adds three: `wire_level` at **3992**
  (the committed cluster wire level, `uc2-cluster`-agent-written at every
  `Settings` apply; `od -A d -t u8 -j 3992 -N 8`), `wire_binary` at
  **4000** (node-written at boot, `(WIRE_MIN << 8) | WIRE_MAX` — `3084` is
  `12..12`, `3085` is `12..13`) and, in each peer slot, `wire_max` at `+8`
  (that member's reported wire max, consensus-agent-written on change,
  leader-authoritative; `0` = no report this term). *Was §3's field tables.* Raw-offset walkthrough:
  [Diagnose a node → Which FSM is holding the cluster up?](../how-to/diagnose-a-node.md#which-fsm-is-holding-the-cluster-up)
- [`uc2ctl`](../reference/uc2ctl.md) — sub-commands, arguments, response
  statuses, refusal reasons. `status`'s `services:` line gained
  `log_time_ns=<ns>` (raw nanoseconds since the Unix epoch, not RFC 3339 —
  there is no formatter in the binary) and each per-FSM row gained
  `timers_pending=<n>`, both since log time and timers (2.11.0); the
  `config:` line gained `schedule_position=<n>` and the sub-command list gained
  `schedule apply` / `schedule show` with reason codes 40–43. Jumbo frames
  (`2.12.0`) add a `ceiling:` line — the live payload ceiling, its datagram
  rung, and whether that rung is `baseline` or `discovered`. The wire level
  (`2.15.0`) adds a `wire:` line after it and the `wire show` / `wire raise`
  sub-commands (admin op 12, refusal 63).
- [Monitor a cluster → The per-FSM families](../how-to/monitor-a-cluster.md#the-per-fsm-families-m14)
  — which metric families carry a `service` label, what the unlabeled
  aggregate means now, and the declared-set drift query. `Uc2ServiceIdentityDrift`
  fires the same way when a row's declared NAME itself differs node-to-node
  (a mis-declared `[services]` config, not a missing FSM); `Uc2ServiceVersionDrift`
  fires when an attached FSM's version **line** (major.minor) differs
  node-to-node — patch differences are allowed and do not fire — and
  `Uc2RowVersionMismatch` when a live attached FSM (heartbeat under 10 s) is
  off its row's committed running line (since wire `0.10.0`, a stop that did
  not happen; a stopped service not yet replaced after a pin does not fire). See [Upgrade an application § The
  version rules](../how-to/upgrade-an-application.md#the-version-rules).
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
