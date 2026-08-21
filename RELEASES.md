# Releases

What each release introduced, newest first. Every feature links to the doc
that explains it in full. The deep engineering record — complete safety-fix
analyses, wire-version mechanics, upgrade remedies — is
[`docs/releases.md`](docs/releases.md); the per-milestone proof records
(pre-committed bars, fleet runs) are in
[`docs/benchmarks/`](docs/benchmarks).

## Unreleased — v2.5.0 pending — survivable cluster (M11)

Merged on `main`; every gate row now passes (fleet flag-day downtime 14.0 s
and 14.7 s against a 60 s bar; true-`ENOSPC` proven against a real loopback
fixture after the fixes described below). Nightly's independent confirmation
of the `ENOSPC` row is the last thing outstanding before the tag. Full
record, including two honest FAILs and what they exposed:
[gate record](docs/benchmarks/uc2-m11-gate-2026-08-20.md). Design deep-dive:
[M11 explained](docs/notes/uc2-m11-survivable-cluster-explained.md).

- **Offline backup, verify, and restore** (`uc2ctl backup / verify-backup /
  restore`): safe against a *running* node's purge and snapshot churn via an
  enforced copy ordering, with `verify` asserting the coverage invariant
  instead of trusting the operator. →
  [Back up a cluster](docs/how-to/back-up-a-cluster.md)
- **Quorum-loss recovery** (`uc2ctl force-single-member`): when a majority of
  the cluster is permanently gone, rebuild from one surviving node — offline,
  provably non-persisting until confirmed, with the data-loss window stated
  before anything is written. →
  [Recover from quorum loss](docs/how-to/recover-from-quorum-loss.md)
- **Full-disk fail-stop, observed end-to-end**: a node that hits the disk wall
  halts loudly instead of acking writes it cannot persist, naming the errno
  (`StorageFull` / `os error 28`) so the operator knows to free space; a new
  `uc2_free_disk_bytes` metric and the `Uc2DiskLow` alert give the early
  signal before the wall. Proving this end-to-end exposed two real defects,
  both fixed here: the node's mmapped IPC files were sparse, so a full disk
  killed whichever process (node, service, or client) next touched an
  unbacked page with `SIGBUS` — bypassing the fail-stop path entirely — and
  the journal's segment preallocator discarded the underlying errno, so even
  a correct fail-stop said only "segment preallocation failed". **Operational
  consequence:** those files now reserve their blocks at startup, so a
  default instance dir needs ~78 MiB free before a node will boot, and a node
  that cannot reserve it refuses to start with a named error. →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md) ·
  [gate record, row 3b](docs/benchmarks/uc2-m11-gate-2026-08-20.md)
- **Measured flag-day upgrades** (`scripts/uc2_flag_day.sh`): the
  stop-all/upgrade/start-all procedure as a script with preflight refusals,
  an un-upgrade path, and a printed downtime number. →
  [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
- **Fixed bugs**: four pre-existing journal-layer defects surfaced by the
  backup work's adversarial testing — a healable crash state that refused
  boot, a heal-residue permanent wedge, a masked acked-durability hole at
  segment rolls, and a latent writer panic. →
  [M11 explained §5](docs/notes/uc2-m11-survivable-cluster-explained.md) ·
  [gate record](docs/benchmarks/uc2-m11-gate-2026-08-20.md)

## v2.4.0 — 2026-08-20 — observable cluster (M10)

A running cluster can now be watched, probed, and alerted on without touching
the source — and it costs the hot path ~1.7%.

- **In-daemon observability endpoint**: `GET /metrics` (Prometheus text, 60+
  metric families), `/healthz` (liveness), `/readyz` (role-aware readiness —
  keyed on `can_serve`, so an elected-but-not-yet-serving leader is correctly
  not ready). Zero new dependencies; enabled by the `[metrics]` config
  section, off when absent. →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Transition-triggered structured logging** (`[log]` config section): one
  JSON line per state transition — election, truncation, snapshot install,
  config adoption — never one per operation. →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Shipped alert rules and dashboard**: 13 Prometheus alert rules (every one
  proven to fire against a deliberately broken cluster) and a Grafana
  dashboard, under [`packaging/`](packaging). →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Fail-fast daemon**: an internal agent failure now exits the daemon (for
  systemd to restart) instead of lingering as a healthy-looking zombie. →
  [Run a cluster](docs/how-to/run-a-cluster.md)
- **Performance**: the fleet gate measured scrape cost at median 0.983
  on/off throughput ratio (≈1.7%, bar ≥ 0.95) under a 1 s all-nodes scrape,
  with zero false alerts over a 10-minute healthy soak. →
  [M10 gate record](docs/benchmarks/uc2-m10-gate-2026-08-20.md)

## v2.3.0 — 2026-08-19 — deployable node (M9) + rollup

The first tag since v2.1.0, so it ships everything landed in between: the
deployable daemon, wire crypto, a consensus safety fix, the pipelined client,
and the batched read barrier.

- **A real `uc2-node` daemon**: starts from a TOML config file; every config
  mistake is a *named startup refusal* (a typo names the key, a semantic
  error names the rule) instead of a later failure that looks like something
  else. Clean `SIGTERM` drain-and-stop so planned restarts replay a journal
  tail instead of paying reconstruction; packaged systemd units. →
  [Run a cluster](docs/how-to/run-a-cluster.md) ·
  [Configuration reference](docs/reference/configuration.md)
- **Service-binary template**: the shape a user's crate instantiates —
  SIGTERM handling, supervision, the `counter-service` example. →
  [Write a service binary](docs/how-to/write-a-service-binary.md)
- **Wire crypto (M8) — opt-in, off by default**: authenticated + encrypted
  node↔node UDP (Noise `IK` over an X25519 allowlist, AES-256-GCM, a rotating
  group key for the fan-out, anti-replay). A cluster runs all-encrypted or
  all-cleartext — no mixed mode. →
  [Encrypt node traffic](docs/how-to/encrypt-node-traffic.md)
- **Content-attested durable reports (wire protocol 0.5.0)**: a consensus
  safety fix that upgrades commit ranking from a position quorum to a content
  quorum. **Flag day**: upgrade all nodes together — a mixed cluster stalls
  commits rather than committing unsoundly. →
  [the plain-language explainer](docs/notes/uc2-term-map-window-loss-explained.md) ·
  [wire protocol reference](docs/reference/wire-protocol.md)
- **Pipelined client SDK**: `uc2_client`'s public `Engine` (split send/poll
  halves, exactly-once correlation) and `PipelinedClient` with an
  `await`-able `Ticket` per request. →
  [QUICKSTART — beyond one-shot CLI calls](docs/QUICKSTART.md) ·
  [API docs](https://peterknego.github.io/ultima_cluster/)
- **Batched linearizable read barrier**: linearizable reads ride shared probe
  rounds; the barrier's throughput cost fell from ~58% to ~0% at ~953k
  linearizable reads/s. →
  [Read path reference](docs/reference/read-path.md) ·
  [the read-barrier explainer](docs/notes/uc2-read-barrier-explained.md)
- **Fixed bugs**: three consensus-safety windows found by the Lean proof
  effort before any production deployment existed — a Raft Figure-8
  acked-write-loss window in commit ranking, a candidate intake-gate reopen,
  and a boot-open gate phantom commit (Findings #6b, #9, #5). →
  [detailed record](docs/releases.md) ·
  [verification overview](docs/VERIFICATION.md)
- **Performance**: planned leader restart under load stops in 0.042 s and is
  back at baseline ≤ 10.5 s (M9 gate); end-to-end 1.48 M responses/s @ p99
  0.905 ms through the public pipelined client; UC leads Aeron Cluster
  1.3–1.8× on the matched-durability scorecard. →
  [M9 gate](docs/benchmarks/uc2-m9-gate-2026-08-19.md) ·
  [client re-run + A/B](docs/benchmarks/uc2-m5-engine-gate-2026-08-15.md) ·
  [Aeron scorecard](docs/benchmarks/uc2-aeron-parity-2026-08-15.md)

## v2.1.0 — 2026-07-14 — live reconfiguration (M7)

- **Single-server membership changes, live, under load**: promote / demote /
  add / remove one member at a time via the `uc2ctl` admin CLI — no restarts,
  no joint consensus (adjacent configs differ by one member, so majorities
  always intersect). Removed ids are tombstoned forever; a returning host
  rejoins as a fresh id. →
  [Change cluster membership](docs/how-to/change-cluster-membership.md) ·
  [uc2ctl reference](docs/reference/uc2ctl.md)
- **Fixed bugs**: the v2.0.0 MPSC ingress-ring free-space underflow under
  producer contention (spurious backpressure, not corruption). →
  [detailed record](docs/releases.md)
- **Performance**: the 5-host fleet gate held every membership transition's
  commit-rate dip ≤ 4.7% (bar < 10%) with a 3.22 s leader self-removal
  handoff and zero loss or divergence. →
  [M7 gate record](docs/benchmarks/uc2-m7-gate-2026-07-13.md)

## v2.0.0 — 2026-07-13 — the v2 core (M1–M6)

The Aeron-shaped rewrite: UC owns consensus, elections, and transport
directly (the openraft-based v1 is retired).

- **The SMR core**: four single-writer polling agents per node over a
  shared-memory log buffer and control page; replication is a byte-stream
  fan-out over UC's own reliable UDP; monotonic byte positions instead of log
  indices. →
  [Architecture](docs/ARCHITECTURE.md)
- **The end-to-end SDK**: you write a sync, deterministic `StateMachine` in
  your own process; the service SDK applies committed commands and the client
  SDK submits and queries over shared memory. →
  [QUICKSTART](docs/QUICKSTART.md)
- **Leader elections and failover**: automatic, with zero committed-write
  loss across repeated leader kills. →
  [Architecture](docs/ARCHITECTURE.md) ·
  [M4 gate record](docs/benchmarks/uc2-m4-gate-2026-07-11.md)
- **Snapshots, learners, and journal purge** (purge off by default): a node
  below the purge floor converges by snapshot install + tail replay; learners
  replicate without counting toward quorum. →
  [Bound journal growth](docs/how-to/bound-journal-growth.md)
- **Linearizable reads**: a quorum read barrier plus a service-epoch check
  that closes the TOCTOU against a service crashing mid-query. →
  [Read path reference](docs/reference/read-path.md)
- **Performance**: 1.64 M responses/s @ p50 0.600 ms end-to-end (M5 gate,
  pre-pipelined client); learner join under load dipped commit rate 0.9%
  (M6). →
  [M5 gate record](docs/benchmarks/uc2-m5-gate-2026-07-12.md) ·
  [M6 gate record](docs/benchmarks/uc2-m6-gate-2026-07-12.md)
- **Known issue at release**: the MPSC ingress underflow, fixed in v2.1.0. →
  [detailed record](docs/releases.md)
