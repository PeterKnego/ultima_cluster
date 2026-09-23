# Releases

What each release introduced, newest first — one or two sentences per point,
each linking to the doc that explains it. The full engineering record of every
release (mechanics, rulings, evidence tables) is
[`docs/releases.md`](docs/releases.md); the per-milestone proof records are in
[`docs/benchmarks/`](docs/benchmarks).

## v2.13.0 — 2026-09-22 — the FSM upgrade lifecycle

Upgrading a state machine becomes a pinned, per-row procedure the platform
enforces instead of a flag day run from memory.
[Full record](docs/releases.md#v2130--2026-09-22--the-fsm-upgrade-lifecycle).

- **Upgrade flag day:** wire `0.8.0` → `0.9.0` and cnc `3.2` → `3.3` — stop
  every node before starting any, and clear `snapshots/<row>/` once per node
  (a purge-on cluster with an in-memory state machine cannot yet).
  → [Upgrade a cluster § 2.13.0](docs/how-to/upgrade-a-cluster.md#wire--cnc-change-in-2130-upgrade-pins-and-snapshot-reports-090-cnc-33)
- **Upgrade pins.** `uc2ctl upgrade pin` commits a row's upgrade origin to the
  log, so every instance of the new version starts from the same snapshot.
  → [`uc2ctl` § `upgrade pin`](docs/reference/uc2ctl.md#upgrade-pin) ·
  [The cluster FSM § Pins and reports](docs/notes/uc2-cluster-fsm-explained.md#pins-and-reports-2130)
- **Pinned install at attach.** A pinned row installs the origin snapshot
  before applying anything, and the old binary is refused by name.
  → [State-machine contract § Attaching](docs/reference/state-machine-contract.md#attaching-the-node-must-have-joined-its-cluster-first) ·
  [SDLC standard § S4 Pin the origin](docs/reference/application-sdlc.md#s4-pin-the-origin)
- **Live snapshot-hash reports.** Every node's artifact hash at every instant
  is compared on the log, and a divergent replica is named by a gauge and the
  `Uc2SnapshotHashDiverged` alert.
  → [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **A readiness gate at attach, and `boot_wait`.** Services and clients attach
  only once their node has joined its cluster, so start every node before
  attaching any service.
  → [Configuration § `boot_wait`](docs/reference/configuration.md#attaching-a-service-or-a-client-boot_wait)
- **Diff replay.** The new `uc2-diffreplay` replays one corpus through two
  builds and fails on any difference not declared in advance; `pin-verify`
  rehearses a pinned upgrade against a live node.
  → [Diff replay](docs/how-to/diff-replay.md) ·
  [`uc_diffreplay` README](uc_diffreplay/README.md)
- **The lifecycle, written down.** An SDLC standard with the change taxonomy
  and per-row upgrade steps S1–S9, a rewritten upgrade how-to, and a
  `diff-replay-judge` agent skill.
  → [SDLC standard](docs/reference/application-sdlc.md) ·
  [Upgrade an application](docs/how-to/upgrade-an-application.md)
- **A worked example and a tutorial.** [`examples/kv`](examples/kv), a
  replicated key-value store, and a design-to-upgrade tutorial built on it,
  plus two clean-room experience reports.
  → [Build an application](docs/tutorials/build-an-application.md) ·
  [builder report](docs/notes/uc2-dogfood-kv-builder-report.md) ·
  [operator report](docs/notes/uc2-dogfood-kv-operator-report.md)
- **Lower low-load latency.** The apply agent idles on a spin → yield → sleep
  ladder instead of a flat 50 µs sleep, which took the shipped posture's p90
  from 219 to 118 µs on the fleet.
  → [Service time § 4.5](docs/benchmarks/uc2-service-time-2026-09-16.md#45-re-run-with-the-ladder-as-the-default--2026-09-17)
- **Fixed:** an apply pass that overran the log buffer could replay forever in
  silence; plus a test flake, a service that did not retry a booting node, and
  CI gaps.
  → [Full record § Fixed on the way](docs/releases.md#fixed-on-the-way)
- **Changed reading and API notes:** `uc2_cluster_fsm_position` now reports
  the cluster agent's walk cursor; three config structs gain a public
  `boot_wait` field and `ApplyCtx::ids` takes `&mut self`.
  → [Semver policy § 2.13.0](docs/reference/semver-policy.md#2130-api-notes)
- **Performance:** no fleet gate ran; this is a control-plane and tooling
  release.

## v2.12.0 — 2026-09-13 — jumbo frames, and the monotonic log clock

The command payload ceiling is discovered from the network, and the log clock
no longer freezes on a backward wall-clock step.
[Full record](docs/releases.md#v2120--2026-09-13--jumbo-frames-and-the-monotonic-log-clock).

- **Upgrade flag day:** wire `0.7.0` → `0.8.0`, cnc `3.1` → `3.2` — stop every
  node before starting any, and delete `max_payload` from every `node.toml`
  first.
  → [Upgrade a cluster § 2.12.0](docs/how-to/upgrade-a-cluster.md#wire--cnc-change-in-2120-jumbo-frames-080-cnc-32)
- **Jumbo frames.** Nodes probe their paths and commit the largest datagram
  every member carries, so a 9001 B path carries 8896 B commands instead of
  1344 B; the raise is one-way, and a node host must now run Linux.
  → [Explainer](docs/notes/uc2-jumbo-frame-discovery-explained.md) ·
  [Run a cluster on jumbo frames](docs/how-to/jumbo-frames.md) ·
  [Limits § hard limits](docs/reference/limits.md#hard-limits)
- **Monotonic log clock.** Log time is `CLOCK_MONOTONIC` plus a sampled epoch
  offset, so a backward NTP step slows the clock at 500 ppm instead of
  freezing it.
  → [Explainer](docs/notes/uc2-log-time-and-timers-explained.md#the-log-clock)
- **Fixed:** attaching to a restarting node could panic on a torn cnc header,
  and attaching during a node's boot gap could adopt the wrong FSM set for
  life.
  → [Full record § fixed after the 2.11.0 tag](docs/releases.md#fixed-after-the-2110-tag)
- **Performance:** jumbo discovery converges within ~2 s and the refusal rows
  pass; both throughput bars are inconclusive, and no bar was moved.
  → [Jumbo gate](docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md) ·
  [log-clock gate](docs/benchmarks/uc2-log-clock-gate-2026-09-08.md)

## v2.11.0 — 2026-09-08 — FSM identity, log time, the cluster FSM, and coordinated snapshots

Five features on one flag day: state machines carry their identity in code,
frames carry time, cluster data moves into an internal FSM, and snapshots
become coordinated instants.
[Full record](docs/releases.md#v2110--2026-09-08--fsm-identity-log-time-the-cluster-fsm-and-coordinated-snapshots).

- **Upgrade flag day:** wire `0.6.0` → `0.7.0`, cnc `3.0` → `3.1`, with a
  same-length header relayout — stop every node before starting any, and make
  three `node.toml` edits on every host.
  → [Upgrade a cluster § 2.11](docs/how-to/upgrade-a-cluster.md#wire--cnc-change-in-2110-fsm-identity-log-time-and-the-cluster-fsm-070-cnc-31)
- **FSM identity.** A state machine declares `const NAME` and `VERSION` in
  code, and a mismatched cluster is refused by name; `ApplyCtx` replaces the
  bare `position` argument and `IdGen` derives deterministic ids.
  → [Explainer](docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md) ·
  [semver carve-out](docs/reference/semver-policy.md#fsm-identity-a-breaking-trait-and-config-change-riding-as-a-minor)
- **Log time and timers.** Every frame carries the leader's timestamp, giving
  `apply` a deterministic "now", and a state machine can schedule its own
  callbacks.
  → [Explainer](docs/notes/uc2-log-time-and-timers-explained.md) ·
  [Schedule work inside a state machine](docs/how-to/schedule-work-in-a-service.md)
- **A replicated schedule table.** Operators declare recurring ticks in a TOML
  file and apply it with one signed command.
  → [Run work on a schedule](docs/how-to/run-work-on-a-schedule.md)
- **The cluster FSM.** Membership, the schedule table and four replicated
  settings live in one internal state machine with its own snapshot artifact.
  → [Explainer](docs/notes/uc2-cluster-fsm-explained.md) ·
  [`uc2ctl` § `settings apply`](docs/reference/uc2ctl.md#settings-apply)
- **Coordinated snapshot instants.** `uc2ctl snapshot` freezes every row at one
  log position, and `--standby` freezes only learners so commit never stalls.
  → [Explainer § Instants](docs/notes/uc2-cluster-fsm-explained.md#instants-one-position-one-set) ·
  [Keep the journal from growing without bound](docs/how-to/bound-journal-growth.md)
- **Fixed:** a restarted ex-leader could run one config version behind, a
  learner could wedge below a climbing purge floor, and a default node could
  not apply a full schedule table.
  → [Full record](docs/releases.md#v2110--2026-09-08--fsm-identity-log-time-the-cluster-fsm-and-coordinated-snapshots)
- **Performance:** three rows pass, two are honest failures (timer precision,
  the snapshot arm's introduction cost) and four were inconclusive; no bar was
  moved.
  → [FSM identity gate](docs/benchmarks/uc2-fsm-identity-gate-2026-09-02.md) ·
  [time-and-timers gate](docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md)

## v2.10.0 — 2026-08-31 — one log stream, config from the environment, and a weak-memory fix

Operator-facing hygiene plus a memory-ordering fix a loom model found.
[Full record](docs/releases.md#v2100--2026-08-31--one-log-stream-config-from-the-environment-and-a-weak-memory-fix).

- **Upgrade:** a plain binary swap, but the daemons no longer write to stdout.
  → [Upgrade a cluster § 2.10.0](docs/how-to/upgrade-a-cluster.md#stdout-is-now-empty-2100)
- **One log stream.** Every daemon record is a JSON line on stderr, and stdout
  is empty.
  → [Monitor a cluster § Structured records](docs/how-to/monitor-a-cluster.md#structured-records)
- **Environment overrides.** Eleven `UC2_*` variables override deploy-varying
  config keys, so one image can run every node.
  → [Configuration § Environment overrides](docs/reference/configuration.md#environment-overrides)
- **Config identity.** Every node logs the SHA-256 of the config it loaded.
  → [Record a release](docs/how-to/record-a-release.md)
- **New crate `uc_obs`**, the structured log format — 13 publishable crates.
  → [Architecture](docs/ARCHITECTURE.md)
- **SMR, explained.** A plain-language explainer is now the single source for
  the concept.
  → [State machine replication, explained](docs/notes/state-machine-replication-explained.md)
- **Fixed:** the Broadcast ring's seqlock could let a torn response through on
  aarch64; the fix is one fence, free on x86_64.
  → [The broadcast seqlock, explained](docs/notes/uc2-broadcast-seqlock-explained.md)
- **Removed (breaking):** `uc_service`'s `ultima_db` feature, which nothing in
  the tree used.
  → [Semver policy](docs/reference/semver-policy.md)
- **Performance:** unchanged; CPU pinning was evaluated and not adopted, and a
  node needs 4 physical cores.
  → [Pinning](docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md) ·
  [Core-count sweep](docs/benchmarks/uc2-node-core-count-sweep-2026-08-31.md)

## v2.9.0 — 2026-08-30 — one prefix: every crate is now `uc_*`

A package rename and nothing else: no behaviour, wire, config or binary name
changed.
[Full record](docs/releases.md#v290--2026-08-30--the-uc_-crate-rename).

- **Renamed crates.** `uc2_*` and `ultima-journal` became `uc_*`, and the
  `uc2ctl` package became `uc_ctl`; source that names the old crates needs a
  mechanical `sed`.
  → [Full record](docs/releases.md#v290--2026-08-30--the-uc_-crate-rename)
- **Unchanged for operators.** Binaries, units, the image, the instance-dir
  layout and every metric name are as `2.8.1` shipped.
  → [Run a cluster](docs/how-to/run-a-cluster.md)
- **First crates.io publish.** All 12 crates went live at `2.9.0` under their
  new names.
  → [Cut a release § 6](docs/how-to/cut-a-release.md)
- **Why a minor.** Nothing had been published to crates.io before; that
  exception is now spent.
  → [Semver policy § the carve-out](docs/reference/semver-policy.md#the-one-carve-out-the-290-crate-rename)

## v2.8.1 — 2026-08-30 — the multi-service proof pass (M14c2)

Proof-only: no feature, config, wire or cnc change.
[Full record](docs/releases.md#v281--2026-08-30--m14c2-the-multi-service-proof-pass).

- **Two-FSM capstones.** Linearizability, partition, `SIGKILL` and Elle tiers
  now run with two state machines per node, plus a replication-equivalence
  oracle shown to catch a divergent FSM.
  → [VERIFICATION § 11](docs/VERIFICATION.md#11-what-is-not-verified) ·
  [How multi-service works](docs/notes/uc2-m14-multi-service-explained.md)
- **Lockstep under oversubscription is an operating envelope.** No candidate
  fix cleared the bar, so lockstep needs a free CPU per declared FSM.
  → [The experiment](docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md)
- **A CPU-pinned fleet rig** (`--pin`, off by default).
  → [Fleet pinning](docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md)
- **Fixed:** `uc_service_lag_waits_total` counted nothing in the common bounded
  case, and a stuck snapshot intake is now abandoned after 60 s.
  → [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **`v2.8.1` replaces the `v2.8.0` pre-release as Latest**; `v2.8.0` keeps
  its pre-release flag as the record of what it lacked.

## v2.8.0 — 2026-08-30 — several state machines behind one log (M14)

Up to eight state machines per node, fed by one replicated log.
[Full record](docs/releases.md#v280--2026-08-30--m14-multi-service-one-log-n-state-machines).

- **Upgrade flag day:** wire `0.6.0`, cnc `3.0`; a single-service config needs
  no change.
  → [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md#wire-change-in-280-snap_begin-carries-every-fsms-snapshot-060)
- **`[services]`.** Declare N FSMs, kept within a byte bound of each other or
  in lockstep.
  → [Configuration § `[services]`](docs/reference/configuration.md#services)
- **Per-FSM routing and fan-in.** Submit or query one FSM, or all of them with
  one ticket.
  → [How it works § routing and fan-in](docs/notes/uc2-m14-multi-service-explained.md#routing-and-fan-in)
- **Per-FSM snapshots, observability, and backup.** A snapshot session ships
  every FSM's artifact, and metrics, alerts and backups are per FSM.
  → [Monitor a cluster](docs/how-to/monitor-a-cluster.md) ·
  [Back up a cluster](docs/how-to/back-up-a-cluster.md)
- **Performance:** five of six gate rows passed; row d failed as specified and
  passed on a re-specified re-run.
  → [M14 gate](docs/benchmarks/uc2-m14-gate-2026-08-29.md)
- **Published as a GitHub pre-release** pending the two-FSM proofs, which
  `2.8.1` carries.

## v2.7.0 — 2026-08-26 — the remote path at the cluster's speed (M13)

The remote path runs at the backend's rate and degrades instead of collapsing
under oversubscription.
[Full record](docs/releases.md#v270--2026-08-26--m13-remote-path-performance-and-flow-control).

- **Upgrade:** a same-host restart (node, service, gateway and local clients
  together), not a cluster flag day.
  → [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
- **A rebuilt remote client.** The same blocking API over an `Engine`-shaped
  split, removing a 7× bottleneck.
  → [Remote protocol](docs/reference/remote-protocol.md)
- **An ingress ring that cannot convoy.** Producers commit per record, so none
  waits on another.
  → [The MPSC publish convoy, explained](docs/notes/uc2-m13-mpsc-publish-convoy-explained.md)
- **A global credit budget at the gateway.**
  → [The grant budget](docs/reference/gateway-config.md#the-grant-budget-270)
- **Fixed:** the `2.6.0` gateway collapse, correctly diagnosed as the ring
  convoy, not the credit budget.
  → [the correction](docs/notes/uc2-m12a-edge-flow-control-gap.md)
- **Performance:**
  → [M13 gate](docs/benchmarks/uc2-m13-gate-2026-08-24.md) ·
  [per-hop bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)

## v2.6.0 — adoptable cluster (M12) — *shipped as `v2.6.0-rc.1`; superseded by v2.7.0, no final tag*

The cluster becomes adoptable by someone who is not its author.
[Full record](docs/releases.md#v260--m12-adoptable-cluster--shipped-as-v260-rc1-superseded-by-v270-no-final-tag).

- **Upgrade:** add `[crypto]` and `[admin]` sections to every `node.toml`, or
  the node refuses to start by name.
  → [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
- **Two-tier state-machine contract.** A raw bytes-in/bytes-out tier under the
  typed one, for services that want to skip the codec.
  → [State-machine contract](docs/reference/state-machine-contract.md) ·
  [Two tiers, one contract](docs/notes/uc2-two-tier-state-machine-contract.md)
- **Exactly-once over a remote hop** with `Sessioned<S>`.
  → [State-machine contract](docs/reference/state-machine-contract.md)
- **A remote protocol, client and gateway.** Clients can reach a cluster over
  TCP from another host.
  → [Remote protocol](docs/reference/remote-protocol.md) ·
  [Run a gateway](docs/how-to/run-a-gateway.md)
- **Admin authentication and an audit log.** Mutating `uc2ctl` verbs are
  HMAC-signed and every request is recorded.
  → [Who may change the cluster](docs/notes/uc2-admin-authentication.md)
- **Signed release artifacts.** Tarballs, an SBOM and a container image, all
  cosign-signed.
  → [QUICKSTART](docs/QUICKSTART.md)
- **A semver policy and a security package**, with a fuzz tier that found
  real defects.
  → [Semver policy](docs/reference/semver-policy.md) ·
  [`docs/security/`](docs/security) · [SECURITY.md](SECURITY.md)
- **Fixed:** a replayable admin request after restart, two `Sessioned` defects
  found by fuzzing, and panicking UDP readers.
  → [security self-assessment § 2](docs/security/self-assessment.md#2-findings)
- **Performance:** remote-path batching gave ~+40 % per connection; the leader
  uses about a quarter of its NIC at peak.
  → [M12 gate record](docs/benchmarks/uc2-m12-gate-2026-08-22.md)

## v2.5.0 — 2026-08-21 — survivable cluster (M11)

Back up, restore, recover from quorum loss and upgrade on a measured schedule.
[Full record](docs/releases.md#v250--2026-08-21--m11-survivable-cluster).

- **Upgrade:** a node now needs ~78 MiB free in its instance dir before it
  boots.
  → [Instance directory](docs/reference/instance-directory.md#on-disk-footprint)
- **Offline backup, verify and restore.**
  → [Back up a cluster](docs/how-to/back-up-a-cluster.md)
- **Quorum-loss recovery** with `uc2ctl force-single-member`.
  → [Recover from quorum loss](docs/how-to/recover-from-quorum-loss.md)
- **Full-disk fail-stop.** A node halts by name instead of acking writes it
  cannot persist.
  → [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Measured flag-day upgrades** with `scripts/uc2_flag_day.sh`.
  → [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
- **Fixed:** four pre-existing journal-layer defects.
  → [M11 explained § 5](docs/notes/uc2-m11-survivable-cluster-explained.md)
- **Performance:** fleet flag-day downtime 14.0 s and 14.7 s against a 60 s bar.
  → [M11 gate record](docs/benchmarks/uc2-m11-gate-2026-08-20.md)

## v2.4.0 — 2026-08-20 — observable cluster (M10)

A running cluster can be watched, probed and alerted on.
[Full record](docs/releases.md#v240--2026-08-20--m10-observable-cluster).

- **`/metrics`, `/healthz`, `/readyz`** in the daemon, off unless `[metrics]`
  is configured.
  → [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Structured transition logging**, one JSON line per state change.
  → [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Alert rules and a dashboard**, each rule proven to fire.
  → [`packaging/`](packaging)
- **Fail-fast daemon.** An internal agent failure exits the process for
  systemd to restart.
  → [Run a cluster](docs/how-to/run-a-cluster.md)
- **Performance:** scraping costs ~1.7 % of throughput.
  → [M10 gate record](docs/benchmarks/uc2-m10-gate-2026-08-20.md)

## v2.3.0 — 2026-08-19 — deployable node (M9) + rollup

The first tag since `v2.1.0`, shipping everything landed in between.
[Full record](docs/releases.md#v230--2026-08-19--m9-deployable-node).

- **Upgrade flag day:** wire `0.5.0` — upgrade all nodes together.
  → [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
- **A `uc2-node` daemon** with a TOML config, named startup refusals and
  systemd units.
  → [Run a cluster](docs/how-to/run-a-cluster.md) ·
  [Configuration reference](docs/reference/configuration.md)
- **A service-binary template.**
  → [Write a service binary](docs/how-to/write-a-service-binary.md)
- **Wire crypto (M8)**, opt-in and off by default.
  → [Encrypt node traffic](docs/how-to/encrypt-node-traffic.md)
- **Content-attested durable reports**, a consensus safety fix.
  → [Explainer](docs/notes/uc2-term-map-window-loss-explained.md)
- **A pipelined client SDK.**
  → [QUICKSTART](docs/QUICKSTART.md)
- **A batched linearizable read barrier**, cutting its throughput cost from
  ~58 % to ~0 %.
  → [Read path reference](docs/reference/read-path.md)
- **Fixed:** three consensus-safety windows found by the Lean proof effort.
  → [Verification overview](docs/VERIFICATION.md)
- **Performance:** 1.48 M responses/s at p99 0.905 ms through the pipelined
  client.
  → [M9 gate](docs/benchmarks/uc2-m9-gate-2026-08-19.md) ·
  [wire 0.5.0 fleet gate](docs/benchmarks/uc2-protocol-050-fleet-gate-2026-08-17.md) ·
  [Aeron scorecard](docs/benchmarks/uc2-aeron-parity-2026-08-15.md)

## v2.1.0 — 2026-07-14 — live reconfiguration (M7)

[Full record](docs/releases.md#v210--2026-07-14).

- **Live membership changes.** Promote, demote, add or remove one member at a
  time under load with `uc2ctl`.
  → [Change cluster membership](docs/how-to/change-cluster-membership.md)
- **Fixed:** an MPSC ingress-ring underflow under contention.
- **Performance:** every transition's commit-rate dip stayed ≤ 4.7 %.
  → [M7 gate record](docs/benchmarks/uc2-m7-gate-2026-07-13.md)

## v2.0.0 — 2026-07-13 — the v2 core (M1–M6)

The Aeron-shaped rewrite: UC owns consensus, elections and transport.
[Known issues](docs/releases.md#v200--known-issues).

- **The SMR core.** Single-writer polling agents over a shared-memory log,
  replicated over UC's own reliable UDP.
  → [Architecture](docs/ARCHITECTURE.md)
- **The end-to-end SDK.** A sync, deterministic `StateMachine` in your own
  process.
  → [QUICKSTART](docs/QUICKSTART.md)
- **Elections and failover**, with zero committed-write loss.
  → [M4 gate record](docs/benchmarks/uc2-m4-gate-2026-07-11.md)
- **Snapshots, learners and journal purge** (purge off by default).
  → [Bound journal growth](docs/how-to/bound-journal-growth.md)
- **Linearizable reads.**
  → [Read path reference](docs/reference/read-path.md)
- **Performance:** 1.64 M responses/s at p50 0.600 ms end-to-end.
  → [M5 gate record](docs/benchmarks/uc2-m5-gate-2026-07-12.md) ·
  [M6 gate record](docs/benchmarks/uc2-m6-gate-2026-07-12.md)
