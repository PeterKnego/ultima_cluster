# UC v2 against Aeron Premium — what we have, what we lack, what is worth adding

**Date:** 2026-09-19
**Status:** assessment note (feature map + ranked recommendation)
**Related:** `uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` (the Standby
comparison this extends), `uc2-network-and-ipc-share-of-smr-latency.md`
(the kernel-bypass reading), `uc2-aeron-cluster-upgrade-model.md`,
`docs/BACKLOG.md` §3, §4 and §6.
**Source:** the Aeron Premium overview
(https://aeron.io/premium-docs/overview/index.html) and its seven component
overview pages, fetched 2026-09-19. Sub-pages (changelogs, DPDK counters and
troubleshooting, Insights CLI reference, SBE mapper samples) were **not**
fetched. Every UC statement cites the file it was read from; nothing was
re-run for this note.

## Headline

Aeron Premium is seven components. UC already ships the equivalent of three
in full or better, has the mechanism but not the product for two, has
nothing for one, and one does not apply to a Rust code base. The one item
worth taking up is already ranked: the stale-read mode off a learner
(`docs/BACKLOG.md` §4, Phase A). Two smaller items follow it: a key-generation
CLI and, if a remote client ever crosses an untrusted path, TLS on the
gateway link.

## The map

| Aeron Premium | What it is (from its overview page) | UC equivalent | Coverage |
|---|---|---|---|
| Cluster Standby | Async non-voting log replica, typically in another region; standby snapshots triggered by the leader, executed on the standby; `TransitionModule` promotion; daisy-chaining; offload of heavy query/egress services | Learners (non-quorum, never back-pressure), `uc2ctl snapshot --standby`, `uc2ctl snapshot fetch`, in-protocol M7 promote | mechanism yes, geo product no |
| Data Retention Regulator | Tree of eight policies (retain N snapshots, min length, age, regex-matched streams, cascade, max-of), dry-run, `SegmentFileBackingUpStrategy` hook before delete | Purge below the complete snapshot set with `slack_bytes`; node-owned delete-only retention; `uc2ctl backup` / `verify-backup` / `restore` | safety core yes, policy language no |
| Insights (beta) | Prometheus exporters, a sample Grafana dashboard, an analysis CLI | `/metrics` `/healthz` `/readyz`, `packaging/grafana/uc2-dashboard.json`, `packaging/prometheus/uc2-alerts.yml`, `uc2ctl status` + cnc decode, `uc_obs` JSON-lines logs | at or above parity |
| Selector | `aeronsd`, an epoll daemon so an application thread sleeps on a *set* of subscriptions instead of spinning; Linux only | Client-side futex park (`RingWaitHandle`), the spin → yield → park ladder in `uc_client/src/wait.rs`, the apply agent's `UC2_APPLY_IDLE` backoff ladder | covered, one narrow gap |
| Transport Security | OpenSSL 3; RSA-4096 identity keys + ECDHE + HKDF; AES-256-GCM with the frame header as AAD; SHA-256 fingerprints; shared-key mode for multicast/MDC; C media driver only; covers every publication by default | M8 wire crypto: Noise IK over X25519, pairwise keys + a rotating cluster group key, AES-256-GCM with the datagram header as AAD, RFC-6479 anti-replay, allowlist | node↔node at parity; the gateway link is uncovered |
| Kernel Bypass (DPDK) | `aeronmd_dpdk` replacing the media driver; IPv4 unicast only, one port per driver, DEDICATED threading with pinned cores, huge pages | none | none, and already declined with data |
| SBE Domain Mapper | Java 17 annotation processor generating SBE ↔ domain adapter/proxy classes | Typed `StateMachine` tier over serde, `RawStateMachine` via the blanket impl | not applicable |

## Component by component

### 1. Cluster Standby — mechanism yes, product no

The July note found that a UC learner is already most of a Standby, and the
reasons still hold: `FlowControl::limit()` is an order statistic over voters
only and a learner's advert is never consulted, and `follower_slot` returns
`None` for a learner so its report never reaches the `CommitTracker`
(`uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` § "Forward-looking sketch").
A lagging learner cannot stall the leader, which is the property Standby
exists to provide.

Since `2.11.0` UC also has Standby's two operator verbs:

- `uc2ctl snapshot --standby` (admin op 8, `FLAG_SNAPSHOT_STANDBY`) freezes
  **only the learners**, which is how a snapshot is taken off the live path
  (`docs/ops/uc2-runbook.md`, "Snapshots" entry). Aeron's overview says the
  same thing in its words: standby snapshots "don't stop the cluster" and
  are "triggered via the cluster leader but executed on standby
  infrastructure".
- `uc2ctl snapshot fetch` (admin op 9) pulls a learner's artifact
  **store-only** to a voter, the analog of `PremiumClusterTool
  replicate-standby-snapshot`.

Promotion is where UC is stronger, not weaker: M7 `promote` is one committed
config frame that can also *add* a seat, against Standby's
`TransitionModule`, which can only put a standby into a **failed member's
existing seat** (`transition-as-member <id>`) after the dead box is
stopped and its host name repointed. The page is explicit that the standby
must carry "the same cluster member information as the rest of the
cluster"; the member list never changes. See "The inverse" § A for the
verbatim preconditions.

What UC does not have is the product on top:

- a **stale-read query mode** answered from the learner's own lagging apply
  (Standby's headline use: query and persistent-egress services "too
  resource-intensive for live clusters");
- **learner-as-relay** for daisy-chaining ("a Cluster Standby node can
  retrieve its log information from another Cluster Standby");
- **DR failover** to a separate cluster, which Aeron's page itself flags as
  lossy ("Replication is asynchronous, creating data loss risk during
  failover").

`docs/BACKLOG.md` §4 already phases these as A / B / C and records the
crypto prerequisite as met. **Phase A is the one item in this note worth
taking up**: it is additive, the largest capability gap against the stated
comparator, and the mechanism underneath it is fleet-proven. Phase C is a
consistency weakening and stays a product decision before it is code, as
the backlog says.

### 2. Data Retention Regulator — the core is stronger, the policy language is absent

UC's retention is safer than DRR's by construction: a purge floor moves only
on a **complete snapshot set** at one commanded instant, and retention is
node-owned and delete-only because only the node can see a set
(`docs/how-to/bound-journal-growth.md`). DRR's
`RetainEnoughDataForRecoveryPolicy` ("the last N snapshots plus necessary
log data") is the same idea expressed as one policy among eight.

What DRR adds is expressiveness and operator ergonomics:

- **retain-N-sets** as an explicit knob rather than an implicit pruning
  rule;
- **dry-run** (`aeron.data.retention.dry.run=true`) that lists what would be
  deleted;
- a **backup-before-delete hook** (`SegmentFileBackingUpStrategy`, default
  no-op) for offloading to external storage;
- age-based and regex-matched policies, a cascade and a max-of combinator.

The eight-policy tree is over-built for one log plus N artifact directories.
The first two items are cheap and operator-facing and would fit `uc2ctl`.
The hook is what `uc2ctl backup` already is, run by hand. **Low priority.**

### 3. Insights — at or above parity

Aeron's page lists three things, in beta: Prometheus exporters, one sample
Grafana dashboard, and a CLI "for deeper analytical capabilities" whose
scope the overview does not describe. UC ships all three shapes from M10
plus what Insights' page does not mention at all, a rule file:
`packaging/prometheus/uc2-alerts.yml`, cross-checked by
`scripts/m10_alert_fire.sh` (CLAUDE.md records the last local run as
2026-09-07, 23/23 rules firing; not re-run for this note). The structured
log stream (`uc_obs`) has no counterpart on the Insights page.

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
  plus a programmatic step for the public half. A `uc2ctl keygen` is small
  and worth doing regardless.
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
gap that *is* real, command schema evolution across a rolling upgrade, is
`docs/BACKLOG.md` §3 and the SBE `sinceVersion` discussion in
`uc2-aeron-cluster-upgrade-model.md` §D, not a mapper.

## Ranked recommendation

1. **Standby Phase A, the stale-read mode off a learner** — backlog §4,
   additive, the largest gap against the comparator, mechanism already
   fleet-proven.
2. **`uc2ctl keygen`** — small, closes the how-to's opening caveat.
3. **Gateway TLS + client authentication** — a recorded M12 non-goal; take
   it up when a remote client crosses an untrusted path, not before.
4. **Retain-N-sets + dry-run** for retention — cheap operator ergonomics,
   low priority.
5. **Multi-ring wait** for parked multi-FSM clients — only on demand.
6. **DPDK** — declined; reopen only on the condition above.
7. **SBE Domain Mapper** — not applicable.

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
- **A linearizable read path.** UC's typed `Query` goes through the
  `READ_PROBE`/`ACK` quorum barrier with the service-epoch backstop
  (`uc2-read-barrier-explained.md`). Aeron Cluster has no read API: a grep
  for `linearizab`, `read barrier`, `readIndex` or `ReadProbe` over
  `aeron-cluster/src/main/java` returns nothing. Every read is an ingress
  message that rides the log.
- **Per-FSM command routing and fan-in.** Aeron's `SessionMessageHeader`
  (`aeron-cluster-codecs.xml:132-138`) carries only `leadershipTermId`,
  `clusterSessionId` and `timestamp`, so every service receives every
  message. UC routes a command to one row and fans responses in, and
  `TIMER` is a per-FSM frame in a broadcast log
  (`uc2-m14-multi-service-explained.md`).
- **Replicated cluster settings.** `uc2ctl settings apply` commits
  `fsm_lag`, `admission_bytes` and the snapshot cadence through the cluster
  FSM, so every node runs one committed value. Aeron's settings are per-node
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
- Complete-set-gated purge with `slack_bytes` (Aeron: Data Retention
  Regulator, §2).
- Alert rules with a fire-test script, not only exporters and a sample
  dashboard (Aeron: Insights, §3).
- A concrete HMAC admin authenticator with an fsync-per-record audit log
  (`uc2-admin-authentication.md`). Aeron ships the `Authenticator` /
  `AuthorisationService` interfaces with trivial defaults
  (`aeron-client/.../security/SimpleAuthenticator.java`,
  `SimpleAuthorisationService.java`) and the premium
  `AllowBackupAndStandbyAuthorisationService`.

### C. The proof surface

Deterministic sim with safety invariants, Lean proofs with a Rust
conformance checker, loom models on three rings, 24 fuzz targets, Elle, the
WGL lincheck capstones and the SIGKILL crashtest, mapped in
`docs/VERIFICATION.md`. The Aeron tree has no proof, sim, TLA+, Lean, Jepsen
or fuzz directory at its root. Not a feature, but the largest asymmetry.

### Not claimed, because Aeron has it too

- **Multiple services per node** exist (`aeron.cluster.service.count`,
  `ConsensusModule.java:695`), so M14 is distinct in routing, not in count.
- **Timers** exist. `Cluster.scheduleTimer`'s javadoc states no delivery
  guarantee either way, so UC's exactly-once `Timed<S>` is not claimed as
  stronger without reading the consensus module's timer handling.
- **Coordinated snapshots** across services exist via Aeron's
  `SnapshotMarker`.
- **Backup** exists as the OSS `ClusterBackup` live node plus `ClusterTool`.
  UC's offline `backup` / `verify-backup` / `restore` is a different shape,
  not a strict superset.
- **Content-attested durable reports** (wire 0.5.0): Aeron's
  `AppendPosition` codec was not located in this pass, so unverified.

## Why the split falls this way

Aeron's premium tier is mostly operability around an unchanged consensus
core: retention, metrics, wait strategies, wire security, a faster NIC
path. UC absorbed those into the OSS milestones M8, M10 and M11 because
each milestone was gated on being operable, so they are not gaps. The one
premium component that is architectural, Cluster Standby, Aeron built
*outside* consensus after removing in-protocol dynamic join. UC kept
membership in the log (M7) and got Standby's non-back-pressuring half for
free from the flow-control split, which is why what remains is a read mode
on top of a learner rather than a replication engine beside the cluster.
