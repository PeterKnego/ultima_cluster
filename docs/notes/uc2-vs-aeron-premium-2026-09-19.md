# UC v2 against Aeron Premium — what we have, what we lack, what is worth adding

**Date:** 2026-09-19
**Updated:** 2026-10-17, for 2.14.0
**Status:** assessment note (feature map + ranked recommendation)
**Related:** `uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` (the Standby
comparison this extends), `uc2-network-and-ipc-share-of-smr-latency.md`
(the kernel-bypass reading), `uc2-aeron-cluster-upgrade-model.md`,
`docs/BACKLOG.md` §3, §4 and §6.
**Source:** the Aeron Premium overview
(https://aeron.io/premium-docs/overview/index.html) and its seven component
overview pages, read 2026-09-19 and **not** re-fetched for the update.
Sub-pages (changelogs, DPDK counters and troubleshooting, Insights CLI
reference, SBE mapper samples) were never fetched. UC statements reflect
`main` at `0ffb73cc` plus the 2.14.0 release docs (`RELEASES.md`,
`docs/releases.md`). Every UC statement cites the file it was read from;
nothing was re-run for this note.

## Headline

Aeron Premium is seven components. UC ships the equivalent of four in full
or better, covers most of one, has nothing for one, and one does not apply
to a Rust code base. The one that is mostly covered is Cluster Standby: a
learner now answers snapshot and read-your-writes reads, so the query
offload that is Standby's headline use is there. What Standby still has and
UC lacks is learner-as-relay and a warm DR failover.

No item on the list is now large and clearly worth doing. The next ones are
small: a key-generation CLI, and a staleness bound on reads off a learner.
TLS on the gateway link follows if a remote client ever crosses an
untrusted path.

## The map

| Aeron Premium | What it is (from its overview page) | UC equivalent | Coverage |
|---|---|---|---|
| Cluster Standby | Async non-voting log replica, typically in another region; standby snapshots triggered by the leader, executed on the standby; `TransitionModule` promotion; daisy-chaining; offload of heavy query/egress services | Learners (non-quorum, never back-pressure) that answer snapshot and read-your-writes reads, `uc2ctl snapshot --standby`, automatic background fetch of agreed sets (`auto_fetch`), `uc2ctl snapshot fetch`, in-protocol M7 promote, node replacement from a backup tier | read replica yes; relay and warm DR no |
| Data Retention Regulator | Tree of eight policies (retain N snapshots, min length, age, regex-matched streams, cascade, max-of), dry-run, `SegmentFileBackingUpStrategy` hook before delete | Purge below the newest **agreed** snapshot set; `retain_sets` (1..48); the backup tier with `backup_retain_sets` / `backup_retain_age`; backup-gated purge (`purge_requires_tiers`) | core and the common policies yes; dry-run and policy combinators no |
| Insights (beta) | Prometheus exporters, a sample Grafana dashboard, an analysis CLI | `/metrics` `/healthz` `/readyz`, `packaging/grafana/uc2-dashboard.json`, `packaging/prometheus/uc2-alerts.yml`, the cluster view (`GET /cluster`, `obs.sock`, `uc2ctl cluster` / `catalog`), `uc_obs` JSON-lines logs | at or above parity |
| Selector | `aeronsd`, an epoll daemon so an application thread sleeps on a *set* of subscriptions instead of spinning; Linux only | Client-side futex park (`RingWaitHandle`), the spin → yield → park ladder in `uc_client/src/wait.rs`, the apply agent's `UC2_APPLY_IDLE` backoff ladder | covered, one narrow gap |
| Transport Security | OpenSSL 3; RSA-4096 identity keys + ECDHE + HKDF; AES-256-GCM with the frame header as AAD; SHA-256 fingerprints; shared-key mode for multicast/MDC; C media driver only; covers every publication by default | M8 wire crypto: Noise IK over X25519, pairwise keys + a rotating cluster group key, AES-256-GCM with the datagram header as AAD, RFC-6479 anti-replay, allowlist | node↔node at parity; the gateway link is uncovered |
| Kernel Bypass (DPDK) | `aeronmd_dpdk` replacing the media driver; IPv4 unicast only, one port per driver, DEDICATED threading with pinned cores, huge pages | none | none, and already declined with data |
| SBE Domain Mapper | Java 17 annotation processor generating SBE ↔ domain adapter/proxy classes | Typed `StateMachine` tier over serde, `RawStateMachine` via the blanket impl | not applicable |

## Component by component

### 1. Cluster Standby — read replica yes, relay and DR no

The July note found that a UC learner is already most of a Standby, and the
reasons still hold: `FlowControl::limit()` is an order statistic over voters
only and a learner's advert is never consulted, and `follower_slot` returns
`None` for a learner so its report never reaches the `CommitTracker`
(`uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` § "Forward-looking sketch").
A lagging learner cannot stall the leader, which is the property Standby
exists to provide.

A learner is also a read replica. The node forwards a snapshot read
straight to its service with no role check, and admits a read-your-writes
read against its own durable and applied positions, again with no role
check (`uc_node/src/node.rs`, `drain_query_ring` and
`admit_min_position_read`). `uc_node/tests/lin_backup_tier.rs` reads a
learner's state with a snapshot read. The read-your-writes spec says
learners "serve min-position reads like any node"
(`docs/superpowers/specs/2026-10-08-uc2-read-your-writes-design.md` §6.1),
and the release record says the same (`RELEASES.md`, 2.14.0). That is
Standby's headline use, query services "too resource-intensive for live
clusters", answered from the learner's own lagging apply.

UC has Standby's snapshot verbs too:

- `uc2ctl snapshot --standby` (admin op 8, `FLAG_SNAPSHOT_STANDBY`) freezes
  **only the learners**, which is how a snapshot is taken off the live path
  (`docs/ops/uc2-runbook.md`, "Snapshots" entry). Aeron's overview says the
  same thing in its words: standby snapshots "don't stop the cluster" and
  are "triggered via the cluster leader but executed on standby
  infrastructure".
- Every node, voters included, fetches the newest **agreed** set in the
  background after a free-space check (`auto_fetch`, on by default), holds
  it, and purges below it (`docs/how-to/bound-journal-growth.md` §
  "Learner-only clusters"). `uc2ctl snapshot fetch` (admin op 9) remains
  the manual pull. Its Premium analog is `PremiumClusterTool
  replicate-standby-snapshot`; whether Premium also replicates
  automatically was not read.

Promotion is where UC is stronger, not weaker: M7 `promote` is one committed
config frame that can also *add* a seat, against Standby's
`TransitionModule`, which can only put a standby into a **failed member's
existing seat** (`transition-as-member <id>`) after the dead box is
stopped and its host name repointed. The page is explicit that the standby
must carry "the same cluster member information as the rest of the
cluster"; the member list never changes. See "The inverse" § A for the
verbatim preconditions. UC can also build a replacement node from a backup
tier with no copy of the log from a peer, then add and promote it
(`docs/how-to/back-up-to-a-tier.md` § "Replace a node from the tier").

What UC does not have:

- a **staleness bound** on a learner read. A query's answer carries the
  position it was applied at (`uc_client/src/engine.rs`, the `Completion`
  built from the response's position prefix), but nothing reports the gap
  to commit or refuses an answer older than a bound (option J in
  `smr-read-options-compared.md`). This is what is left of `docs/BACKLOG.md`
  §4's Phase A;
- **learner-as-relay** for daisy-chaining ("a Cluster Standby node can
  retrieve its log information from another Cluster Standby"), Phase B; a
  grep for a relay over `uc_node/src` and `uc_net/src` finds none;
- a **warm DR failover** to a separate cluster, Phase C, which Aeron's page
  itself flags as lossy ("Replication is asynchronous, creating data loss
  risk during failover"). UC's cold equivalent is a restore or fork from a
  backup tier at another site, which loses whatever the cluster committed
  past the tier's reach (`docs/how-to/back-up-to-a-tier.md` § "Restore").

Phase C is a consistency weakening and stays a product decision before it
is code, as the backlog says. Phase B matters only once a cross-region
learner exists.

### 2. Data Retention Regulator — the core is stronger, most of the policy is there

UC's retention is safer than DRR's by construction. A purge floor follows
the newest snapshot set that every reporting node **agrees** on, row by row
and hash by hash, not merely the newest set whose files are on disk
(`uc2-snapshot-catalog-explained.md`). Snapshots are mandatory, so every
service can produce one: `ServiceBuilder::start()` requires
`SnapshotStateMachine` (`docs/reference/state-machine-contract.md`).
DRR's `RetainEnoughDataForRecoveryPolicy` ("the last N snapshots plus
necessary log data") is the same idea expressed as one policy among eight,
with no cross-node check behind it.

DRR's policies, against UC:

- **retain-N** is the replicated `retain_sets` (1..48). Above 1, the journal
  is purged only below the oldest retained set, so every retained set is a
  start point (`docs/reference/configuration.md`, the `retain_sets` row).
- **age** exists for the backup tier, `backup_retain_age`, beside
  `backup_retain_sets` (same file). Local sets have no age rule.
- **backup-before-delete** is stronger than a hook. A writer node copies
  agreed sets and the committed journal to a tier, and with
  `purge_requires_tiers = K` every node holds its purge floor at or below
  the K-th highest position its tiers have confirmed
  (`docs/how-to/back-up-to-a-tier.md` § "Backup-gated purge"). DRR's
  `SegmentFileBackingUpStrategy` is a callback before delete, default
  no-op; UC's gate makes the purge wait for confirmed copies.
- **dry-run** (`aeron.data.retention.dry.run=true`) has no UC counterpart.
  The cluster view shows where a purge would stop (`catalog.floor` and
  `cluster.purge_gate.position`, `docs/reference/cluster-view.md`) but not
  the list of files it would delete.
- regex-matched policies, a cascade and a max-of combinator have no
  counterpart, and need none: the eight-policy tree is over-built for one
  log plus N artifact directories.

**Low priority:** a dry-run listing is the one item left here that an
operator would touch.

### 3. Insights — at or above parity

Aeron's page lists three things, in beta: Prometheus exporters, one sample
Grafana dashboard, and a CLI "for deeper analytical capabilities" whose
scope the overview does not describe. UC ships all three shapes plus what
Insights' page does not mention at all, a rule file:
`packaging/prometheus/uc2-alerts.yml` holds 34 rules, cross-checked by
`scripts/m10_alert_fire.sh`. The last recorded fire-test run passed 33
rules at PR #100's head; the run on the release head is still pending
(`docs/releases.md`, 2.14.0 evidence table). The structured log stream
(`uc_obs`) has no counterpart on the Insights page.

UC's analysis surface is the cluster view: one versioned JSON document
(`uc2.cluster-view/1`) of the committed cluster state, this node's holdings
and, on the leader, every member's holdings, with an `attention` list of
the conditions the node already detects. It is served on an always-on
`obs.sock`, on `GET /cluster` when `[metrics]` is configured, and printed by
`uc2ctl cluster` and `uc2ctl catalog` (`docs/reference/cluster-view.md`).
Because the Insights CLI's scope is not described, the two are not compared
feature by feature.

**Nothing to take.** UC's own observability friction list is the operator
dogfood's tickets #40–#42, and those matter more than anything here.

### 4. Selector — covered, with one narrow gap

Selector solves "sleep instead of spin" for application threads waiting on
several subscriptions, via an epoll daemon. UC's answer is in-process and
already measured:

- the client's driver uses the spin → yield → park ladder, ported from
  `ultima_rings` with its topology-sweep findings quoted in
  `uc_client/src/wait.rs` ("on a BUSY machine `Park` is the FASTEST
  strategy (5-24x)");
- `Ticket::wait` always parks;
- the apply agent's default idle is the `UC2_APPLY_IDLE` backoff ladder
  (`uc_service/src/lib.rs`, `APPLY_IDLE`), with `spin` / `yield` /
  `sleep:<µs>` overrides.

Node agents are busy-spin by design, exactly like Aeron's media driver, and
Selector does not target them either.

The one Selector-shaped gap is the **await-a-set** case.
`uc_client/src/engine.rs` (the `RingWaitHandle` doc, "M14b deviation 3")
records that a handle "is ONE futex, so a parked driver" waiting on another
FSM's ring "resolves at the park timeout instead (≤ 1 ms)". A per-ring
wakeup word plus a single multi-word wait would close it. **Only worth doing
if a multi-FSM parked client appears in practice.**

### 5. Transport Security — parity on strength, not on reach

On the node↔node plane the two are equivalent where it matters. Both pin
peer public keys in an operator-distributed list (ATS: RSA public keys and
SHA-256 fingerprints; UC: base64 X25519 keys in the allowlist,
`docs/how-to/encrypt-node-traffic.md`). Both seal with AES-256-GCM and
authenticate the frame/datagram header as AAD. UC additionally rotates a
cluster group key on three triggers and carries RFC-6479 anti-replay
windows; ATS's page describes neither. ATS's multicast shared-key mode has
no UC counterpart because UC has no multicast.

Two gaps are real:

- **No key-generation CLI.** The how-to opens with "there is no
  key-generation CLI yet" and hands the operator `head -c 32 /dev/urandom`
  plus a programmatic step for the public half; `uc_ctl` has no `keygen`
  verb. A `uc2ctl keygen` is small and worth doing regardless.
- **The gateway link is cleartext and unauthenticated.**
  `docs/security/threat-model.md` states it: "There is no client
  authentication and no TLS — see §5", an M12 non-goal. ATS covers ingress
  because an Aeron client is an ordinary publication and ATS "secures all
  Aeron publications and subscriptions by default". This is a revisit of a
  recorded non-goal, moderate cost, and it matters only once a remote
  client crosses an untrusted path.

### 6. Kernel Bypass (DPDK) — none, and already declined with data

UC has no bypass path. The decision is on record and rests on numbers, not
taste:

- `uc2-network-and-ipc-share-of-smr-latency.md` §2d reads Adaptive's own AWS
  results: at 100 k msg/s DPDK's transport p50 is 24 µs against Java's 21,
  "bypass barely moves the transport RTT on modern AWS"; its effect is on
  the tail and on the 1 M msg/s knee.
- `docs/BACKLOG.md` §6 declines "kernel bypass and the net-decomp brief's
  WIRE(P) instrument" because "the whole round trip is 33.5 µs", and notes
  that ENA has none of the NICs the ABTRDA3 campaign measured, so the fleet
  cannot even run the experiment.

Aeron's own page makes the operational price explicit: DEDICATED threading
only, sender/receiver/conductor pinned to separate cores, huge pages, IOMMU
or unsafe mode, IPv4 unicast, one interface per driver. **Not now.** The
condition that reopens it is a user needing multi-million msg/s with a
sub-millisecond p99 on bare metal; isolated transport ladders then belong in
the sibling `hi-perf-cmp` grid, and only a UC-budget-share measurement
belongs here.

### 7. SBE Domain Mapper — not applicable

A Java 17 annotation processor that generates SBE ↔ domain adapters. In
Rust the same job is `#[derive(Serialize, Deserialize)]`, and UC fixed its
contract in the 2026-08-22 codec spike (`2026-08-22-codec-budget-spike.md`):
bytes-in/bytes-out at the core, a typed serde adapter on top. The adjacent
gap that *is* real, two versions of one row applying side by side during a
rolling upgrade, is `docs/BACKLOG.md` §3 (issue #66) and the SBE
`sinceVersion` discussion in `uc2-aeron-cluster-upgrade-model.md` §D, not a
mapper. Today a row runs exactly one committed version and changes it
through a pinned upgrade ("The inverse" § A).

## Ranked recommendation

1. **`uc2ctl keygen`** — small, closes the encryption how-to's opening
   caveat.
2. **A staleness bound on learner reads** — the rest of Standby Phase A
   (backlog §4). The answer already carries its applied position; what is
   missing is the gap to commit, or a bound the node enforces. Small and
   additive.
3. **Gateway TLS + client authentication** — a recorded M12 non-goal; take
   it up when a remote client crosses an untrusted path, not before.
4. **Learner-as-relay** (Standby Phase B) — when a cross-region learner
   exists, not before.
5. **Retention dry-run** — a listing of what a purge would delete; cheap,
   low priority.
6. **Multi-ring wait** for parked multi-FSM clients — only on demand.
7. **Warm DR failover** (Standby Phase C) — a product decision before it is
   code; the backup tier's restore and fork are the cold path today.
8. **DPDK** — declined; reopen only on the condition in §6.
9. **SBE Domain Mapper** — not applicable.

Done since this note was first written: read-your-writes reads on a
learner, retain-N (`retain_sets`), an age rule and backup-before-delete (the
backup tier and its purge gate). Snapshot reads on a learner were already
possible then, since the query path never checked the node's role; the
first version of this note missed that.

## The inverse — what UC has that Aeron does not

Checked against the local Aeron checkout at `f0366beca8` (2026-08-27, five
commits past 1.53.0, the same tree `uc2-aeron-cluster-upgrade-model.md`
reads), plus the seven premium pages above. Three groups, then what was
deliberately **not** claimed.

### A. No Aeron equivalent, OSS or Premium

- **Live in-protocol membership change — add, remove, resize.** M7's
  promote / demote / add / remove are committed config frames, one at a
  time, under load, and a 3 ⇄ 5 resize is two add+promote pairs. Aeron
  removed dynamic join: the tree still carries
  `ClusterEventCode.DYNAMIC_JOIN_STATE_CHANGE_UNUSED` and a "Removed Dynamic
  Join" comment in `BoundedLogAdapter.java:255`. What Premium Standby
  *does* give back is **dynamic replacement of one failed member's
  identity**, and it is fair to call that dynamic: the surviving members
  keep running while a pre-provisioned standby takes over the dead node's
  member id (`transition-as-member 1 60s`). The page's own preconditions
  (§ "Replacing a Single Node", read from the raw HTML 2026-09-19): the
  standby's consensus-module config must "use the same cluster member
  information as the rest of the cluster"; members must be addressed by
  host name; the failed machine must be stopped; if it was the leader,
  wait for a new one; repoint the name to the standby's IP via DNS,
  `/etc/hosts` or a custom resolver; then run the tool on the standby.
  The member list and its ids are static config throughout, so the count
  never changes and no member is ever added or removed; and the page's
  other transition, DR failover, creates a **new** cluster ("that system
  now provides the source of truth") rather than changing membership of
  the old one. So the distinction is: Aeron can swap the box behind a
  fixed seat, UC can change the seats.
- **A read API: linearizable, read-your-writes and snapshot reads.** UC's
  linearizable `Query` goes through the `READ_PROBE`/`ACK` quorum barrier
  with the service-epoch backstop (`uc2-read-barrier-explained.md`); a
  read-your-writes read carries a position token and is answered by any
  node that has applied it (`uc2-read-your-writes-explained.md`); a
  snapshot read is answered locally on any node. Aeron Cluster has no read
  API: a grep for `linearizab`, `read barrier`, `readIndex` or `ReadProbe`
  over `aeron-cluster/src/main/java` returns nothing. Every read is an
  ingress message that rides the log, or runs on a Premium Standby.
- **Cross-node snapshot agreement.** Every node hashes each row of each
  snapshot set and reports it; the cluster FSM records the hashes and a
  verdict, agreed or diverged, per set, and the purge floor follows only
  agreed sets (`uc2-snapshot-catalog-explained.md`). That is a determinism
  check at every instant. A grep for `checksum`, `digest` or `crc` over
  `aeron-cluster/src/main/java` returns nothing.
- **A pinned, per-row upgrade with a committed running version.** Every
  row has a committed running version; a service off that line is refused
  at attach, and an old service stops at exactly the record that
  supersedes it (`uc2-row-running-version-explained.md`). An upgrade names
  a coordinated instant as its origin, `uc2ctl upgrade pin`, and every
  instance installs that one artifact before applying anything
  (`docs/how-to/upgrade-an-application.md`). Aeron's one gate is a
  cluster-wide `appVersion` checked by a pluggable `VersionValidator`,
  major equality by default, with no committed upgrade record
  (`uc2-aeron-cluster-upgrade-model.md` §C;
  `ConsensusModuleAgent.java:635`).
- **A diff-replay harness.** `uc2-diffreplay` replays a corpus through two
  builds of a state machine and attributes every difference in responses,
  timers, state and side effects to a declared intent
  (`docs/how-to/diff-replay.md`). Nothing in `ClusterTool`'s command list
  (`ClusterTool.java:135-224`) replays a log through two builds.
- **Point-in-time restore and fork.** A backup tier restores the whole
  cluster as of any frame end it reaches, fences the abandoned history so
  no writer mixes the two, forks a copy under a new `app_id` with output
  progress set so side effects are not repeated, and exports a diff-replay
  corpus (`uc2-backup-tier-explained.md`). Aeron's OSS `ClusterBackup`
  starts its copy from `BEGINNING` or `LATEST_SNAPSHOT`
  (`ClusterBackup.java`, `ReplayStart`), and no `ClusterTool` command
  restores to a chosen position or forks. Whether either can be done by
  hand with Aeron's archive tools was not checked.
- **Per-FSM command routing and fan-in.** Aeron's `SessionMessageHeader`
  (`aeron-cluster-codecs.xml:132-138`) carries only `leadershipTermId`,
  `clusterSessionId` and `timestamp`, so every service receives every
  message. UC routes a command to one row and fans responses in, and
  `TIMER` is a per-FSM frame in a broadcast log
  (`uc2-m14-multi-service-explained.md`).
- **Replicated cluster settings.** `uc2ctl settings apply` commits
  `fsm_lag`, `admission_bytes`, the snapshot cadence and the retention and
  backup settings through the cluster FSM, so every node runs one committed
  value (`docs/reference/configuration.md`). Aeron's settings are per-node
  properties; nothing in `ConsensusModule.java` replicates configuration.
- **A committed, declarative schedule table.** `every` / `at` / `once`
  rules applied by an operator, replicated, adopted from the artifact
  (`uc2-log-time-and-timers-explained.md` § "The schedule table"). Aeron
  has programmatic `Cluster.scheduleTimer` from inside a service only.
- **Jumbo-frame discovery.** The payload ceiling is probed per path and
  committed cluster-wide, monotone (`uc2-jumbo-frame-discovery-explained.md`).
  Aeron's MTU is a static channel parameter; a grep for MTU discovery over
  `aeron-driver` and `aeron-client` returns nothing.
- **A monotonic log clock with smearing.** `MillisecondClusterClock.time()`
  is bare `System.currentTimeMillis()`. Whether the consensus module clamps
  it monotone was not verified. UC smears a backward step at 500 ppm
  (`uc_node::log_clock`).
- **FSM identity checked at snapshot install.** Per-row name hash and
  version on `SNAP_BEGIN`, refused by name
  (`uc2-fsm-identity-and-deterministic-ids-explained.md`). Aeron's gate is a
  single `appVersion` with major-equality by default
  (`uc2-aeron-cluster-upgrade-model.md` §C).
- **Named startup refusals and fail-stop reservation.** Refused-by-name
  config keys, the fallocated instance dir so ENOSPC is a boot refusal, and
  a mixed-version cluster that stalls rather than mis-decodes
  (`uc2-m11-survivable-cluster-explained.md`). Aeron's mark files check
  major version at open; the rest surfaces as a runtime error.

### B. In UC's OSS core where Aeron charges Premium

- Wire crypto with group-key rotation and anti-replay (Aeron: ATS, §5).
- Query offload onto non-voting replicas: snapshot and read-your-writes
  reads on a learner (Aeron: Cluster Standby, §1).
- Agreed-set purge with `retain_sets`, tier retention by count and age, and
  backup-gated purge (Aeron: Data Retention Regulator, §2). The OSS cluster
  itself never purges: a grep for `purge` over
  `aeron-cluster/src/main/java` returns nothing.
- Alert rules with a fire-test script, and a JSON cluster view, not only
  exporters and a sample dashboard (Aeron: Insights, §3).
- A concrete HMAC admin authenticator with an fsync-per-record audit log
  (`uc2-admin-authentication.md`). Aeron's OSS tree ships the
  `Authenticator` / `AuthorisationService` interfaces with trivial defaults
  (`aeron-client/.../security/SimpleAuthenticator.java`,
  `SimpleAuthorisationService.java`) and
  `AllowBackupAndStandbyAuthorisationService`
  (`aeron-cluster/src/main/java/io/aeron/cluster/`), which admits only the
  backup-query, heartbeat and standby-snapshot messages; none checks a
  credential.

### C. The proof surface

Deterministic sim with safety invariants, Lean proofs with a Rust
conformance checker, four loom models (two of the log buffer,
`uc_log/tests/loom_frame.rs` and `loom_reserve.rs`, and two of the rings,
`uc_protocol/tests/loom_mpsc.rs` and `loom_broadcast.rs`), 26 fuzz targets
(`fuzz/fuzz_targets/`), Elle, the WGL lincheck capstones and the SIGKILL
crashtest, mapped in `docs/VERIFICATION.md`. The Aeron tree has no proof,
sim, TLA+, Lean, Jepsen or fuzz directory at its root. Not a feature, but
the largest asymmetry.

### Not claimed, because Aeron has it too

- **Multiple services per node** exist (`aeron.cluster.service.count`,
  `ConsensusModule.java:695`), so M14 is distinct in routing, not in count.
- **Timers** exist. `Cluster.scheduleTimer`'s javadoc states no delivery
  guarantee either way, so UC's exactly-once `Timed<S>` is not claimed as
  stronger without reading the consensus module's timer handling.
- **Coordinated snapshots** across services exist via Aeron's
  `SnapshotMarker`.
- **Restart from the latest snapshot** exists: Aeron recovers from a
  recovery plan of snapshot plus log (`ClusterTool recovery-plan`). UC's
  difference is only that the start set must be an agreed one
  (`uc2-snapshot-lifecycle-explained.md`).
- **Continuous off-node backup** exists as the OSS `ClusterBackup`, a
  process that runs remote from the cluster and "polls for snapshots and
  replicates the log" (`ClusterBackup.java`, class javadoc), plus
  `ClusterTool`. UC's backup tier is continuous too; what it adds is in §A
  (point-in-time restore, fork, corpus export) and §B (purge gating), not
  the copying itself.
- **Content-attested durable reports** (wire 0.5.0): Aeron's
  `AppendPosition` codec was not located in this pass, so unverified.

## Why the split falls this way

Aeron's premium tier is mostly operability around an unchanged consensus
core: retention, metrics, wait strategies, wire security, a faster NIC
path. UC absorbed those into its OSS milestones, M8, M10 and M11 and then
2.14.0's storage service (catalog, lifecycle, backup tier, cluster view),
because each was gated on being operable, so they are not gaps. The one
premium component that is architectural, Cluster Standby, Aeron built
*outside* consensus after removing in-protocol dynamic join. UC kept
membership in the log (M7) and got Standby's non-back-pressuring half for
free from the flow-control split. Its read path never checked the node's
role, so a learner is a read replica as well. What remains is relay and a
warm failover, not a replication engine beside the cluster.
