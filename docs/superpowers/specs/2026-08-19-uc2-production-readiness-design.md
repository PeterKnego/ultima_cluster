# UC v2.2+ — production readiness: from proven library to deployable cluster

**Date:** 2026-08-19
**Status:** approved design (repo audit 2026-08-19; scope confirmed with the
maintainer); next step = M9 implementation plan
**Baseline:** v2.1.0 (M1–M7 released) plus unreleased wire 0.4.0 (M8 crypto) and
0.5.0 (content-attested durable reports). Nothing in this document changes the
wire protocol, the consensus core, or the cnc page layout outside its reserved
band.

## 1. Goal and locked decisions

Close the gap between what UC has *proved* and what a stranger can *run*. The
audit that produced this spec found no correctness gap and no missing consensus
work — every item below is operational surface that was deferred, correctly, while
the safety story was built.

Decisions locked at scope-setting:

| Decision | Choice |
|---|---|
| Target audience | **Turnkey cluster.** Someone who is not the author downloads it, runs three nodes on three hosts, points a service at it, and operates it without reading the source. The widest of the three candidate targets; chosen deliberately. |
| Milestone split | **M9 deployable → M10 observable → M11 survivable → M12 adoptable.** Each is independently shippable and leaves a coherent product if the next never happens. |
| Service binary shape | **Template, not host.** `uc2-service` ships as a documented binary template the user's crate instantiates. Dynamic loading of user state machines is rejected: it buys nothing and costs the sync/deterministic apply contract its clarity. |
| Graceful shutdown | **Clean stop + archive drain only.** `Node::stop()` already exists and is graceful; M9 adds the signal handler that calls it and the drain that makes restart cheap. |
| Leadership transfer | **Deferred, and not to M9.** A pre-shutdown handoff needs a new protocol message (Raft's `TimeoutNow` analog) and touches `ElectionSm`. It is a consensus change wearing an operations hat; it gets its own spec or none at all. A planned leader stop costs one election timeout (150–300 ms) until then, which is acceptable and measurable. |
| Upgrade path | **Script the flag day now; defer version negotiation.** M11 delivers a tested procedure with a published downtime number. A one-version-skew negotiation window is real design work — the negotiated floor becomes consensus-relevant state — and is explicitly out of scope here. |
| Metrics transport | **In-daemon HTTP endpoint over a read-only cnc attach.** Not a sidecar: the daemon already holds the page, and a second process attaching read-only is a new failure mode for no gain. |
| Gateway placement | **A client of the cluster, never a member.** It holds no consensus state, so it stays stateless and horizontally scalable. Correctness obligations stay behind `uc_client::Engine`'s existing slot-correlation boundary. |
| Crypto default | **Revisit at M12, not before.** Flipping the default is a posture change that needs the M8 fleet ratio (still open) and the packaging story to land first. |
| Verification posture | **Unchanged.** No milestone here weakens a gate, and none is blocked on the open Lean obligations. |

### Why operations and not more proof

The honest reason the open `leader_completeness` obligation does not appear in
any milestone below: a stranger cannot currently deploy the system at all, and
no additional theorem changes that. The proof work is tracked in
`docs/VERIFICATION.md` and stays on its own track.

## 2. As-built baseline — what the audit found

Evidence gathered 2026-08-19 against `main` at `4d5655f`. Stated as findings,
because several are sharper than expected in both directions.

| Finding | Evidence |
|---|---|
| **No node binary exists.** The only `main.rs` in the workspace is `uc_ctl/src/main.rs`. | `docs/QUICKSTART.md:106` starts a cluster with `cargo run -p counter --bin counter-node` — an example. Fleet gates run roles built from `uc_node/examples/m{4..7}_gate.rs`. |
| **The docs already describe a binary the build does not produce.** | `docs/how-to/run-a-cluster.md` writes `/path/to/uc2-node` in its systemd example. |
| **Graceful stop is implemented; nothing calls it.** This is smaller than it first appears. | `Node::stop()` (`uc_node/src/node.rs:1402`) signals and joins all four agents; `Node::crash()` is the deliberate no-flush counterpart; `Service::stop()` (`uc_service/src/lib.rs:314`) mirrors it. `examples/counter/src/bin/counter-node.rs` ends in `loop { sleep(100ms) }` and installs no signal handler, so `SIGTERM` kills agents mid-cycle and restart pays reconstruction. |
| **`stop()` does not drain the archive.** Not a safety issue — un-recorded bytes were never durable, so never acked — but it is a restart-cost issue. | No flush API on `uc_log::archive` or the journal; agents exit at the top of a duty cycle. |
| **Configuration is a Rust struct.** Changing `buffer_bytes` means recompiling. | `NodeConfig` at `uc_node/src/node.rs`; `docs/reference/configuration.md` documents fields, not a file. |
| **A documented silent-data-loss trap is not asserted in code.** | `run-a-cluster.md`: an instance dir on `tmpfs` makes every `fsync` a no-op and the cluster "will appear to work and will lose committed data on power loss." `bench-infra/scripts/m6_fleet_gate.py:119` (`assert_durable_fs`) already refuses this for gates; the node does not. |
| **Zero telemetry.** No Prometheus, OTel, statsd, or `/metrics` anywhere. Library code contains **zero** `tracing::` calls; total logging is 27 `eprintln!` + 8 `println!`; no subscriber is initialised. | Workspace-wide grep. |
| **The scrape source is already built and pinned.** | `docs/reference/cnc-page.md`: counters at 256/320/384/448/512, `term` 704, `node_flags` 768, `leader_hint` 832, heartbeats 896/960, `config_version` 3456, `admission_bytes` 3712, `seal_failures` 3776, per-peer band at 1408. Accessors: `Node::reports_unattested`, `crypto_handshake_failures`, `crypto_stats`, sender/receiver `stats()`. |
| **No backup, restore, or quorum-loss procedure exists.** | `grep -ri 'backup\|restore\|disaster' docs/` returns nothing operational. The inputs are ready: `docs/reference/instance-directory.md` already classifies `journal/`, `state/`, `snapshots/` as must-survive-power-loss and everything else as rebuilt on boot. |
| **`ENOSPC` is unhandled anywhere in the tree.** Purge is `Disabled` by default, so journals grow unbounded unless enabled. | Workspace-wide grep. Likely lands in one of the `.expect(...fail-stop)` paths in `uc_log/src/archive.rs` — arguably correct, but untested, undocumented, and with no low-disk warning ahead of it. |
| **Every protocol change costs a full-cluster outage.** Correct in isolation; unstated as a product property. | `docs/releases.md`: 0.5.0 is a flag day, a mixed cluster *stalls commits*; crypto is a flag day with no mixed mode. The 0.5.0 fleet gate explicitly does not test mixed-version operation. |
| **Clients must be co-located with a node.** | `uc_client` has no sockets. Writes are leader-only; a follower returns `Outcome::NotLeader { hint }` that the *caller* must act on, and cannot reroute because it cannot reach another host. |
| **The admin plane has no access control.** Membership change is *safe* (one at a time, documented refusal table) but not *authorised*. | `uc2ctl` writes the cnc admin band (3584/3648); node forwards as kinds 16/17. Anyone with write access to the instance directory can remove voters. No audit record. |
| **No supply-chain or toolchain gating.** | No `cargo-deny`, `cargo-audit`, SBOM, or fuzz targets. `rust-toolchain.toml` pins `channel = "stable"` — floating — over ~130 `unsafe` blocks concentrated in `uc_log/src/cnc.rs` (38), the three rings, and `uc_log/src/buffer.rs`. |
| **Version identity is inconsistent.** | Workspace version `0.1.0`; tags `v2.0.0`/`v2.1.0`. Only `uc_lincheck` and `uc2ctl` are `publish = false`, so every other crate is *intended* to publish and none has. |

## 3. Non-goals

Named so they are not re-litigated per milestone:

- **No consensus changes.** No milestone alters `uc_consensus`, the wire
  protocol, or the cnc layout outside the reserved band.
- **No leadership-transfer protocol** (see §1).
- **No mixed-version operation** (see §1). Flag days stay flag days; M11 makes
  them cheap and documented, not absent.
- **No sharding or multi-raft.** One cluster is one state machine.
- **No dynamic loading of user state machines.**
- **No new remote-admin surface** beyond authenticating the one that exists.
- **No weakening of any existing gate** to make a milestone pass.

## 4. M9 — Deployable node

**Scope.** A `uc2-node` binary that starts from a TOML config file, validates it
with useful errors, and stops cleanly on `SIGTERM`. A `uc2-service` binary
template. Documentation stops referencing example binaries.

**Design decisions.**

- **Config file mirrors `NodeConfig` one-to-one**, plus `[log]` and
  `[metrics]` sections reserved for M10. `serde(deny_unknown_fields)` so a typo
  is a startup refusal rather than a silently-ignored setting — the same posture
  as the crypto boot refusal.
- **Validation is a preflight, not a panic.** Every rule that today produces a
  confusing downstream failure becomes a named startup error: `buffer_bytes` a
  power of two, `max_payload` well under it, learner ids disjoint from members,
  this node's id present in `members`, and `bind` **identical** to this node's
  own `members` entry — the mismatch `run-a-cluster.md` documents as producing a
  leader that elects but never commits.
- **The `tmpfs` check is code.** Lift `assert_durable_fs`'s rule into the node:
  refuse to start when the instance directory is on a RAM-backed filesystem,
  with an env override (`UC2_ALLOW_VOLATILE_FS=1`) for the test suites that
  legitimately want it.
- **Shutdown is signal handler → drain → `Node::stop()`.** The drain is new: wait
  for `durable` to reach `append` under a bounded deadline before signalling the
  archive agent, so a restarted node rejoins from the journal instead of paying
  reconstruction. On deadline expiry, log what was left and stop anyway — a
  shutdown that hangs is worse than one that costs a replay.
- **Seed derivation ships.** `counter-node.rs`'s `seed_for()` comment explains
  that identical seeds livelock a cluster through vote splits. That belongs in
  the daemon, not in an example.

**Acceptance gate (pre-committed).** `systemctl stop uc2-node` on a leader under
load completes in **< 1 s**, exits **0**, and the restarted node rejoins
**without a snapshot install** (asserted on `incoming_snapshot_pos` staying
unchanged). Cluster commit-rate dip across the stop/start cycle is measured and
recorded to the M7 transition-dip standard. Every validation rule has a test
asserting the node refuses to start with a message naming the offending field.

## 5. M10 — Observable cluster

**Scope.** Metrics, structured logging, health probes, alert rules.

**Design decisions.**

- **The exporter reads the cnc page the daemon already holds.** No sidecar. The
  page is designed for concurrent readers and its offsets are pinned with
  regression tests, so a read-only scrape cannot perturb the hot path — but the
  gate proves that rather than assuming it.
- **Derived series, not just raw fields:** commit lag (`append - commit`), apply
  lag (`commit - service_applied`), per-peer replication lag
  (`commit - reported_durable`), admission-window saturation, heartbeat staleness
  for both processes, and rates for `reports_unattested`,
  `append_pos_unknown_source`, `naks_plus_replay`, `seal_failures`.
- **Logging is transition-triggered, never per-operation.** The four polling
  agents must not take an allocation per record. Elections, truncations, snapshot
  installs, config transitions, NAK storms, seal failures, and fail-stops each
  emit one structured record carrying node id, term, and position.
- **Readiness keys on `can_serve`, not on the leader flag.** The `0x01` state
  documented in `diagnose-a-node.md` — elected but NewTerm not yet
  quorum-committed — is exactly what a naive `leader == true` probe gets wrong.
  Liveness and readiness are separate probes with separate semantics per role.
- **Alert rules ship as a file.** `diagnose-a-node.md` already contains the
  interpretations; M10 turns them into Prometheus rules and a dashboard JSON in
  the repo.

**Acceptance gate.** `/metrics` covers every field above, scraped by Prometheus
in a fleet run; the M5 throughput gate is **re-run with scraping active and must
not regress beyond its existing noise band**; a Kubernetes-style probe regime
survives a leader kill without routing traffic to a node in state `0x01`; every
shipped alert rule has been fired once against a deliberately broken cluster.

## 6. M11 — Survivable cluster

**Scope.** Backup/restore, quorum-loss recovery, `ENOSPC`, the scripted upgrade.

**Design decisions.**

- **Backup is defined against the durable/volatile split** already published in
  `instance-directory.md`: `journal/`, `state/`, `snapshots/` are the artifact;
  everything else is rebuilt.
- **The purge interaction is the subtle part and gets an explicit ordering
  rule.** Under `PurgePolicy::BelowSnapshot`, a backup that captures the snapshot
  and the journal at different instants can straddle a purge and produce an
  artifact with a hole in it. The rule is stated in the spec's own terms and
  asserted by the tool, not left to the operator.
- **Quorum-loss recovery is deliberately awkward.** Forcing a survivor into a
  new single-member configuration discards acked writes it may not hold. The
  command requires an explicit confirmation flag naming the cluster, and the
  procedure states the data-loss window in terms an operator can reason about.
  `NoCommonPrefix` = wipe-and-rejoin covers the *node* case; this is the
  *cluster* case, and nothing covers it today.
- **`ENOSPC` fails stop, loudly, with warning ahead of it.** Fail-stop is
  probably already the behaviour and is arguably correct; the defect is that it
  is untested, undocumented, and unheralded. A free-space counter lands in the
  cnc reserved band and feeds an M10 alert, so the operator sees the wall before
  hitting it.
- **The flag day gets a script and a number.** Quiesce, verify every node at the
  same durable position, stop all, upgrade, start all, verify, resume — with
  measured downtime published under a pre-committed bar like every other gate
  here. M9's graceful shutdown is what makes this cheap, which is why it sits in
  M9.

**Acceptance gate.** A node is backed up under load, its host destroyed, a new
host restored from the backup alone, and it rejoins and converges — **as a CI
test, not a documented procedure**. A node driven into `ENOSPC` fails in an
asserted way, the cluster keeps serving, and the node recovers when space is
returned. The upgrade procedure is executed on a fleet with downtime recorded.

## 7. M12 — Adoptable cluster

**Scope.** The reference gateway, admin authn/authz/audit, packaging,
publishing, security posture.

**Design decisions.**

- **The gateway is the adoption unlock and the largest item here.** It serves a
  remote protocol, discovers and follows the leader across failover, and holds
  no consensus state. `uc_client::Engine` already carries the exactly-once slot
  correlation; the gateway must not accumulate correctness obligations of its
  own. Note the crate docs already describe `Engine` as what "a max-throughput
  RPC gateway or the `m5_gate` measurement harness runs on directly" — the
  harness is in the repo, the gateway is not.
- **Admin operations need a credential distinct from filesystem access**, plus
  an append-only audit log recording actor, operation, and outcome for both
  accepted and refused requests. Separately, the reference docs must state
  plainly that `app_id` is a wrong-cluster guard and **not** a credential — a
  turnkey product will have it mistaken for one.
- **Packaging targets an operator with no Rust toolchain**: signed release
  binaries, a container image, systemd units, and a quickstart that works from
  those artifacts alone.
- **Version identity is reconciled and the crates are published**, with a stated
  semver policy for the public API (`StateMachine`, `SnapshotStateMachine`,
  `OutputHandler`, `NodeConfig`, the three client tiers). Until then every
  adopter pins a git SHA with no breakage signal.
- **The security posture moves into the README's own words**: crypto is off by
  default, so the default deployment parses untrusted bytes in the
  `uc_protocol::v2` decoders with nothing in front of them; and a malicious
  cluster member can forge fan-out traffic as any node, because the group key is
  symmetric. Both are already honestly recorded in `VERIFICATION.md` §10 — the
  change is prominence. A finding that is *disclosed* survives a security
  review; the same finding *discovered* does not.

**Acceptance gate.** A `uc2-gateway` follows a leader across failover without
dropping acknowledged writes, and its throughput cost against direct `Engine`
use is measured and published. Admin operations require a credential and are
audited. A tagged release brings up the quickstart cluster from published
artifacts with no toolchain. An external security review has been run against
the stated threat model.

## 8. Standing hygiene — a parallel track, not a milestone

Each of these is roughly an afternoon and none should wait for a milestone slot.

- **Pin the toolchain and declare an MSRV.** `channel = "stable"` is floating
  over ~130 `unsafe` blocks in the rings and the cnc accessors. A compiler that
  changes under the atomics without anyone deciding to change it is a real risk.
- **`cargo-deny`, `cargo-audit`, SBOM in `ci.yml`** — while shipping crypto.
  `Cargo.toml`'s existing comments show deliberate care to keep one AES-GCM
  implementation in the binary rather than two; that judgement deserves a gate
  enforcing it.
- **Fuzz the protocol decoders.** The datagram header, the frame decoders,
  `AppendPositionBody`, and the snapshot-session framing parse bytes straight off
  the network — and with crypto off, the default, those bytes are entirely
  untrusted. This is the cheapest high-value gap in the document, and it
  compounds with the crypto default in §7.
- **Miri over the rings and cnc accessors.** `loom` covers frame visibility,
  which is the hardest part; Miri covers a different class (provenance,
  alignment, UB) that nothing covers today.
- **`cargo fmt`.** Deliberately unenforced with ~800 hunks of drift and a stated
  rationale. Defensible solo; friction under a turnkey target. One formatting
  commit and a gate, once the branches in flight land.
- **The open reconfig flake.** `resize_3_to_5_to_3` at 5/86 (5.8%), matching the
  pre-fix baseline exactly — pre-existing, correctly not argued away. Membership
  change is what operators do at 3am under this target, so it should not stay
  open indefinitely.

## 9. Product limits to state up front

Legitimate constraints, not defects. They belong in the README's scope section
so an adopter meets them before a proof-of-concept rather than during one.

- Hard cap **8 total members** (voters + learners), from the cnc observability band.
- **One node per instance directory**, enforced by an exclusive flock.
- **One cluster is one state machine** — one leader, one apply thread, one core's
  worth of apply throughput.
- **All fleet measurements are single-AZ** (`c6id.2xlarge`, cluster placement
  group). No cross-AZ or cross-region characterisation exists, and failover
  timing is latency-sensitive.
- **Clients are co-located with a node** until M12's gateway lands.

## 10. Milestone shape and sequencing

| Milestone | Delivers | Gets to production |
|---|---|---|
| **M9** deployable | daemon, config file, validation, clean stop | a startable, stoppable process |
| **M10** observable | exporter, logging, health, alert rules | **you**, operating it |
| **M11** survivable | backup/restore, DR, `ENOSPC`, scripted upgrade | an **ops team**, operating it |
| **M12** adoptable | gateway, admin authn, packaging, publish | a **stranger**, operating it |

Sequencing notes:

1. M9 first, without exception — everything downstream assumes a process that
   exists and can be stopped.
2. Within M10, the exporter outranks logging: the cnc page has already done the
   hard part, so it is the highest value per hour in the whole document.
3. Within M11, `ENOSPC` outranks backup: it is a silent death today.
4. M12's gateway is the single largest item and the actual adoption unlock.
5. Standing hygiene (§8) runs throughout; the fuzz targets and the toolchain pin
   should land during M9 regardless.

One implementation plan per milestone, written when that milestone starts —
decisions taken in M9 shape M10's plan, and writing all four now would bake in
guesses. Plan for M9:
`docs/superpowers/plans/2026-08-19-uc2-m9-deployable-node.md`.

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| **The exporter perturbs the hot path.** UC's headline is a latency number; an observability feature that costs it is a bad trade. | Read-only attach to a page already built for concurrent readers; M10's gate re-runs the M5 throughput gate with scraping active and treats a regression beyond the noise band as a failure. |
| **Logging allocates on the duty cycle.** The four agents are single-writer busy-spin loops. | Transition-triggered records only; explicit constraint in M10's plan; same throughput gate catches it. |
| **The drain hangs shutdown.** A bounded wait that is not actually bounded is worse than no drain. | Hard deadline, log what was left, stop anyway. Asserted by the M9 gate's `< 1 s` bar. |
| **The `tmpfs` refusal breaks the test suites.** Several legitimately use RAM-backed paths. | `UC2_ALLOW_VOLATILE_FS=1` override; the suites already route journal-bearing dirs to `CARGO_TARGET_TMPDIR` per CLAUDE.md, so the blast radius is small. |
| **The gateway accretes correctness obligations** and quietly becomes a consensus participant. | Locked as a stateless client; exactly-once correlation stays behind `Engine`'s existing boundary; it holds no durable state by construction. |
| **Scope creep from "turnkey"** turns M12 into an unbounded product backlog. | M9–M11 are independently shippable and each leaves a coherent product. M12 can be re-scoped to a narrower audience without invalidating anything before it. |
| **Operational work displaces the open proof obligations** indefinitely. | They stay tracked in `VERIFICATION.md` on their own track, with the reasoning for the ordering recorded in §1 rather than left implicit. |
