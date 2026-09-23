# How Aeron Cluster handles software upgrades — the versioning model, read from source

**Status:** research note, 2026-09-13; revised 2026-09-19 (the SBE
extension rules in §D are now quoted from fetched pages, §B gains a second
rolling-upgrade claim and a wire-retirement example, and §G separates the
three upgrade axes — cluster jars, FSM, client — that Aeron's docs run
together, with the rules on each). Companion to
`uc2-rolling-upgrade-compatibility-assessment.md`, whose Aeron row and
"FSM `const VERSION`" paragraph this extends (and corrects in one place, §C).

**Source read:** the local checkout `/home/claude/ultima/aeron` at
`f0366beca8 2026-08-27 (HEAD -> master, origin/master)`, `git describe` =
`1.53.0-5-gf0366beca8` — i.e. five commits past the 1.53.0 release of
2026-08-26 (`CHANGELOG.adoc:8`). Every `path:line` below is against that
tree. Web sources are cited by URL; anything not fetched is marked
**not verified**.

## The one-paragraph answer

Aeron versions five things, each by its own mechanism, and gates only on
**semantic-version major equality** wherever it gates at all. Between
consensus-module peers there is **no version negotiation and no version
check**: the consensus protocol carries a `protocolVersion` that nothing
reads (§C), and mixed-version peers stay decodable only because every
consensus message is SBE with additive `sinceVersion` fields (§D). The one
cluster-wide gate is the application's own `appVersion`, stamped into the
log at every term boundary and into every snapshot and checked on
**read** by a pluggable `VersionValidator` (§C). The supported upgrade
procedure is **not written down anywhere public** I could reach; what the
tree and changelog say is: one node at a time is *possible* for
schema-additive Aeron releases, and the maintainers name "clean shutdown
with a snapshot, restart the whole cluster" as the procedure when a fix
changes log semantics (§B). What the tree cannot say, and the talk does
(§B, testimony): running **different versions of nodes in one cluster is
business as usual** for Aeron's own operators — the tolerance comes
entirely from protocol discipline, not from any mechanism the code
enforces, which is exactly why no check exists to find.

## A. What Aeron versions, and at what granularity

| layer | carrier | granularity / current value | check | where |
|---|---|---|---|---|
| UDP data/control frames (transport) | 1-byte `version` in every frame header | `CURRENT_VERSION = 0x0`, never bumped | exact equality; mismatch → frame dropped, `invalidPackets++` | `aeron-client/.../protocol/HeaderFlyweight.java:110`; `aeron-driver/.../media/UdpChannelTransport.java:366-402` |
| client ↔ media driver (`cnc.dat`) | `int` semver at offset 0 of the CnC metadata | `CNC_VERSION = 0.2.0` | Java: **major** equality (`checkVersion`); C client: major equality **and** `file.minor >= client.minor` ("Driver version insufficient") | `aeron-client/.../CncFileDescriptor.java:89,466-474`; `CommonContext.java:1117,1158,1311,1431`; `aeron-client/src/main/c/aeron_cnc_file_descriptor.h:26`, `aeron_context.c:607-626` |
| cluster client ↔ consensus module (ingress) | `SessionConnectRequest.version` (`version_t`, `sinceVersion="2"`) | `AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION = 0.3.0` | **major** equality on the leader; else `session.reject(EventCode.ERROR, "invalid version …")` (also for backup sessions) | `aeron-cluster-codecs.xml:161-170`; `client/AeronCluster.java:1209-1229,2464`; `SessionManager.java:270-275,394-402` |
| consensus module ↔ consensus module | `CanvassPosition.protocolVersion`, `RequestVote.protocolVersion` (`sinceVersion="9"`) | `ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION = 1.0.0` | **none** — carried, event-logged, passed to `Election`, never compared (§C) | `aeron-cluster-codecs.xml:444-462`; `ConsensusModule.java:393-416`; `ConsensusPublisher.java:79,117`; `ConsensusModuleAgent.java:903-918,967-972`; `Election.java:289,339` |
| archive client ↔ archive | `version` on the archive connect request | `AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION = 1.12.0` | **major** equality in `ArchiveConductor` | `aeron-archive/.../client/AeronArchive.java:2633-2652`; `ArchiveConductor.java:484` |
| every SBE message (all of the above) | SBE header `schemaId ‖ templateId ‖ blockLength ‖ version` | cluster schema `id="111" version="17"`; mark-file schema `id="110" version="2"`; node-state schema `id="112"` | `schemaId` exact (else `ClusterException("expected schemaId=…")` — or, on the log, dispatch to an extension); `version` is the SBE *acting version*: fields with `sinceVersion` above it decode as their `nullValue` | `aeron-cluster-codecs.xml:2-8`; `aeron-cluster-mark-codecs.xml:2-8`; `ConsensusAdapter.java:85-113`; `LogAdapter.java:197-206`; `BoundedLogAdapter.java:237-241` |
| persistent per-node files | `ClusterMarkFile` (cluster-mark.dat), `NodeStateFile`, `ArchiveMarkFile`, archive `Catalog` header | cluster mark `0.3.0`; archive mark `3.1.0`; catalog stamped with the archive mark version | **major** equality at open, `ClusterException("mark file major version … does not match software")` / `ArchiveException("incompatible catalog file version …")` | `service/ClusterMarkFile.java:56-68,167-171,687-691`; `NodeStateFile.java:176-180`; `aeron-archive/.../ArchiveMarkFile.java:55-65,509-512`; `Catalog.java:206-211,290-294` |
| the cluster's own `recording.log` | fixed 4 KiB entries, `ENTRY_TYPE_TERM/SNAPSHOT/STANDBY_SNAPSHOT` + an invalid flag | **no version field** of its own — additive by entry type | none | `RecordingLog.java:432-498` |
| the application (business logic) | `appVersion` (`version_t`, `int` semver, default `0.0.1`) in `NewLeadershipTerm` (consensus, `id=12`, no `sinceVersion`), `NewLeadershipTermEvent` (log, `sinceVersion="4"`), `SnapshotMarker` (`sinceVersion="4"`) | user-assigned; `SemanticVersion.compose(major, minor, patch)`, `maxValue=16777215` | `VersionValidator.isVersionCompatible(ctx.appVersion(), seen)`; default `AppVersionValidator` = **major** equality | `aeron-cluster-codecs.xml:72-73,262,489,677`; `ConsensusModule.java:1596,2308-2334`; `AppVersionValidator.java:45-48`; `VersionValidator.java:26-38` |

Two notes on the table. (1) `version_t` has `nullValue="0"` and
`presence="optional"`, so a message from a peer whose schema predates a
version field decodes as `0`, and `SemanticVersion.major(0) == 0` — which
is why a default `0.0.1` cluster passes every gate against a peer that
sends nothing (`aeron-cluster-codecs.xml:72-73`). (2) The `1.0.0` consensus
protocol version's own javadoc says "If these don't match then consensus
modules are not compatible" (`ConsensusModule.java:393-397`) — a statement
of intent that no code enforces.

## B. The supported upgrade procedure

**There is no published procedure.** The Cluster Tutorial wiki
(https://github.com/aeron-io/aeron/wiki/Cluster-Tutorial) has 34 sections
and none on upgrades; the wiki index
(https://github.com/aeron-io/aeron/wiki) has no upgrade or migration page;
aeron.io's "Operating Aeron Cluster"
(https://aeron.io/docs/aeron-cluster/operating-aeron-cluster/) documents
`ClusterTool` `snapshot`/`shutdown`/`describe` as ordinary operations, not
as an upgrade sequence; its troubleshooting page says nothing about
"incompatible version". The repo's `aeron-cluster/README.md` has zero hits
for `upgrade`. aeron.io's marketing page claims "Snapshotting and rolling
upgrades allow for 24×7 operational models, enabling upgrades with zero
downtime" (https://aeron.io/aeron-open-source/) with no procedure behind
it — the same finding the companion note recorded. The talk page "Fault
Tolerant 24/7 Ops with Aeron Cluster" (Todd Montgomery,
https://aeron.io/resources/fault-tolerant-operations-aeron-cluster-todd-montgomery/,
fetched 2026-09-19) is the closest thing to a statement of the model:
"Aeron Cluster supports rolling upgrades, where components are updated one
at a time … This method requires careful planning and implementation of
protocols that support backward and forward compatibility, allowing
different versions of the system to coexist temporarily. Semantic
versioning and protocol design play crucial roles in this process." That
is a description of the *discipline* (§D), placed on the application; it
names no procedure, no gate and no check. Neither claim says **which
upgrade** it means — the Aeron jars or the application on top of them —
and the two are different axes with different mechanisms (§G). The one
procedural sentence that exists, 1.47.0 below, is evidence for the
*cluster* axis only.

**From the video itself** (watched by this repo's maintainer 2026-09-19;
there is no transcript to fetch, so this is recorded as **testimony**,
not as a quoted page):

1. Aeron's operators **run different versions of nodes in one cluster as
   business as usual.** It is not an exceptional procedure; the system is
   built to tolerate it. This is the fact the tree cannot show — no test
   runs mixed versions and no check detects them (§A, §C) — and it
   changes the reading of §A: the absence of a peer version check is not
   an omission but the *consequence* of a design in which a mixed
   cluster is the normal state and every message must survive it.
2. The tolerance is achieved by **protocol design**, named as four
   things: forward *and* backward compatibility of every message;
   **version everything** — messages, data, snapshots; **semantic
   versioning with strong rules** for what is and must stay compatible
   across versions; and SBE's **ignore bits / optional fields** — a
   reader skips what it does not know and nulls what the sender did not
   send (§D).

Read against the code, the talk and the tree agree: §A–§D is the
mechanism the talk describes, and the 1.47.0 note is the exception the
talk's rules imply — a fix that changes *semantics* is the one case
extension rules cannot cover, so it is stop-the-world. The calibration
line in issue #31 ("Aeron Cluster 1.53 has **no** supported rolling
upgrade") is therefore too strong as written: Aeron has no rolling-upgrade
*mechanism* — no gate, no negotiation, no committed level — but the
*practice* is supported, by discipline, and is routine.

What *can* be pinned down, by case:

- **(i) Aeron library bump.** Nothing in the tree forbids a mixed-version
  cluster and nothing detects one (§C, §D). The cluster codec schema is at
  `version="17"` with every addition since `version="2"` marked
  `sinceVersion` and `presence="optional"`
  (`aeron-cluster-codecs.xml:153-714`), which is the discipline that makes
  a one-node-at-a-time roll *decodable*. Whether a given release is
  *semantically* safe to roll is decided per release in the changelog: for
  the 1.47.0 fix "duplicate service messages during failover/restart when
  using multiple services" the maintainers wrote **"Upgrade procedure:
  Those affected will need to do a clean shutdown (with a snapshot) and
  restart the whole cluster with the fix"** (`CHANGELOG.adoc:1409`, under
  `== 1.47.0 (2025-01-17)`). That is the only upgrade-procedure sentence in
  the 3 800-line changelog, and it is stop-the-world. The per-node files
  gate on **major** version (table A), and the cluster mark file is still
  `0.x`, so no shipped Aeron release has ever refused an older node's
  cluster directory at open. A search-engine summary attributed to a
  maintainer chat — "the entire node (all services as well as the consensus
  module) must be stopped for upgrades … one way is to add a member, then
  stop, upgrade and bring back each member in turn, finally the leader" —
  could not be traced to a page (the Gitter permalink redirects to an app
  shell): **not verified**, reported here only because it is the sole
  description of a rolling sequence that surfaced. How Aeron *retires*
  wire surface is visible in the same changelog: 1.53.0 removed the
  archive's unauthenticated `ConnectRequest` (`templateId = 2`) as a
  **Breaking** entry, justified by "This message was not used by official
  Archive clients used since 1.24.0 (2019-11-24)" (`CHANGELOG.adoc:30-32`)
  — a template is dropped only after every shipped client has stopped
  sending it for years, which is the SBE "new message type, old one
  lingers" rule (§D) run to completion.
- **(ii) Application upgrade.** This is what `appVersion` exists for
  (§C). Same major → nodes may be rolled and the gate is silent; a major
  bump → the first new-major leader's `NewLeadershipTerm` / log event
  **terminates every old-major node** (`unexpectedTermination`), so a
  major bump is stop-the-world by construction, not by procedure.
- **(iii) Snapshot-format change.** The snapshot payload is the service's
  own; Aeron owns only the `SnapshotMarker` envelope, which carries
  `appVersion` + `timeUnit` and is checked at load
  (`ConsensusModuleAgent.java:632-648`, `ClusteredServiceAgent.java:977-983`).
  A service that changes its snapshot encoding is expected to bump the
  major or install a custom validator; there is no framework migration of
  a service snapshot. Issue #1671's reporter asked for exactly the missing
  piece — a way to validate log and snapshot versions differently
  (https://github.com/aeron-io/aeron/issues/1671) — and the answer shipped
  as the `VersionValidator` interface in 1.52.0 (`CHANGELOG.adoc:192-194`).

## C. `appVersion` and `VersionValidator`: a gate, not a negotiation

It is a **read-side gate**, checked in four places, all against the
reader's *configured* `ctx.appVersion()`; nothing is ever negotiated or
written back:

1. **Consensus, at election** — the elected leader sends `NewLeadershipTerm`
   with `ctx.appVersion()` (`Election.java:1281-1296`,
   `ConsensusPublisher.java:186-220`); a receiving consensus module runs
   the validator **before** handing the term to its `Election`, and on
   failure logs `"incompatible version: <mine> log=<theirs>"` as a
   `ClusterException(FATAL)` and calls `unexpectedTermination`, which
   sends the services a termination position, stops log recording, moves
   the module to `CLOSED` and throws `ClusterTerminationException`
   (`ConsensusModuleAgent.java:1032-1038,3297-3307`). The follower
   *exits*; the leader is unaffected. Note the same field on the
   consensus wire was **not encoded until 1.47.0** ("Send `appVersion` in
   the `NewLeadershipTerm`", `CHANGELOG.adoc:1581`; issue #1671's
   `log=0.0.0`), so from 1.40.0 to 1.46.x this path compared against 0.
2. **Consensus, on log replay** — `onReplayNewLeadershipTermEvent`, same
   error text, same `unexpectedTermination`
   (`ConsensusModuleAgent.java:1618-1624`).
3. **Consensus, on snapshot load** — `onLoadBeginSnapshot` throws
   `ClusterException(FATAL)` (`ConsensusModuleAgent.java:632-640`).
4. **Service container** — on the log's `NewLeadershipTermEvent` it throws
   `AgentTerminationException` (`ClusteredServiceAgent.java:592-598`); on
   snapshot load, `ClusterException` (`:977-983`). Each container carries
   its own `appVersion` + validator (`ClusteredServiceContainer.java:751,786,1080-1122`).

The default policy is one line —
`SemanticVersion.major(contextAppVersion) == SemanticVersion.major(appVersionUnderTest)`
(`AppVersionValidator.java:45-48`) — and since 1.52.0 the pluggable type
is the `VersionValidator` interface, with `AppVersionValidator` its
singleton default (`VersionValidator.java:26-38`, `e0a4988d62 2026-06-03`,
`CHANGELOG.adoc:192-194`). **Correction to the companion note:** it names
`AppVersionValidator` as the pluggable type; since 1.52.0 that class is
`final` and the extension point is `VersionValidator`. History: the field
and the major-equality check arrived together in 1.23/2019
("supporting an application version for cluster nodes to check
compatibility", `6d0b2a0b2d 2019-08-12`, plus `26ff464e73` and `95d8f0d58e`
the next day for the container and snapshots); the pluggable validator in
1.40.0 (`299a3001a1 2022-07-18`, `CHANGELOG.adoc:2404`); the consensus
`protocolVersion` fields one month later (`41007fc1d8 2022-08-22`,
`CHANGELOG.adoc:2378`).

What `appVersion` is **not**: it is not Aeron's own version (Aeron never
sets it; default `0.0.1`, `ConsensusModule.java:1596`), it is not compared
between peers at canvass or vote time (`CanvassPosition`/`RequestVote`
carry `protocolVersion`, not `appVersion`, and neither is checked —
`Election.java:289-360` reads the parameter for nothing), and it does not
select behaviour anywhere: no code path branches on the value, only on
the validator's boolean.

## D. SBE schema versioning between peers of different Aeron versions

Every consensus, log, ingress, egress and snapshot message is decoded by
wrapping the codec with the header's `blockLength` and `version` — the SBE
"acting" values — after an exact `schemaId` check
(`ConsensusAdapter.java:85-113`, `LogAdapter.java:197-206`,
`ConsensusModuleSnapshotAdapter.java:69-72`). That gives both directions:

- **new reader, old message** — a field whose `sinceVersion` exceeds the
  acting version decodes as its `nullValue` ("The decoder must return the
  null representation for the extension fields when acting as a previous
  version", https://github.com/aeron-io/simple-binary-encoding/wiki/Message-Versioning);
  every such field in the cluster schema is `presence="optional"` with an
  explicit null, e.g. `NewLeadershipTerm.commitPosition sinceVersion="15"`
  (`aeron-cluster-codecs.xml:491`), `appVersion sinceVersion="4"` (`:262,677`);
- **old reader, new message** — the reader positions past the sender's
  `blockLength`, so appended root-block fields are skipped. Both
  directions are the stated design goal, fetched 2026-09-19 from the SBE
  wiki's Design Principles page
  (https://github.com/aeron-io/simple-binary-encoding/wiki/Design-Principles):
  "an older system should be able to read a newer version of the same
  message and vice versa … An extension mechanism is designed into SBE
  which allows for the introduction of new optional fields within a
  message that the new systems can use while the older systems ignore
  them until upgrade." (The generated decoders that implement the
  old-reader half are build outputs and not in the checkout, so the
  mechanism is still inferred from the adapters' use of the header's
  `blockLength`, not read from generated code.)

The extension rules themselves, from the Message Versioning page
(https://github.com/aeron-io/simple-binary-encoding/wiki/Message-Versioning,
fetched 2026-09-19 — it had failed to load for the 2026-09-13 pass):

- a field is added "by creating a new `messageSchema` and increasing the
  `version` number", and its `sinceVersion` is "the version number that
  has been used for the new schema";
- fields may only be added "at the end of the root block in the `message`
  or the end of a block in a `group`", and must be `presence="optional"`;
- "Messages cannot remain backwards compatible if existing fields are
  modified or removed";
- "It is **not** possible to add fields to a `composite` type without
  creating a new message template and schema version";
- a new decoder handling an older message must "act like a previous
  version to ensure it does not read beyond the end of an existing
  block", returning "the null representation for the extension fields";
- and the boundary of the mechanism, from Design Principles: "If new
  mandatory fields are required or a fundamental structural change is
  required then a new message type must be employed because it is no
  longer a semantic extension of an existing message type."

The `deprecated` attribute (six cluster messages carry `deprecated="12"`,
`aeron-cluster-codecs.xml:351,532,540,550,558,575`) is an annotation on
the template: no cluster adapter in `aeron-cluster/src/main/java`
branches on it (grep, 2026-09-19), so a deprecated message still decodes
and dispatches exactly as before. Together these
are the whole of Aeron's "schema migration": there is no migration, only
additive extension of a message and eventual replacement of the template.

The schema history is consistent with a project that relies on this: 17
schema versions and every post-`2` field is `sinceVersion`-tagged
(`aeron-cluster-codecs.xml:153,155,166,169,249,261-262,345,366,383,451,461,491,501,600-601,617,649,676-677,712-714`).
But I found **no statement either way** — no "all nodes must run the same
Aeron version" anywhere in the tree, wiki, docs or changelog, and no test
that runs mixed versions. The mechanism that would make a mixed cluster
*unsafe* is not decoding but semantics: a new field that an old peer
nulls out (e.g. `commitPosition` on `NewLeadershipTerm`, or the 1.47.0
service-message ordering fix) — and that is precisely the case the
changelog answered with stop-the-world (`CHANGELOG.adoc:1409`).

## E. Snapshots and the recording log across versions

- **Cluster:** `recording.log` has no version field; entries are typed and
  the newest entry type (`ENTRY_TYPE_STANDBY_SNAPSHOT = 2`) was added
  without a format break (`RecordingLog.java:437-453`). `ClusterTool` has
  no `migrate`; its commands are `describe`, `recovery-plan`,
  `recording-log`, `sort-recording-log`, `seed-recording-log-from-snapshot`,
  `invalidate-latest-snapshot`, `snapshot`, `suspend`/`resume`,
  `shutdown`/`abort`, `describe-latest-cm-snapshot`,
  `validate-recording-log` (`ClusterTool.java:135-224`). The
  consensus-module snapshot is itself an SBE stream (`SnapshotMarker`,
  `ClusterSession`, `Timer`, `ConsensusModule`, `ClusterMembers`,
  `PendingMessageTracker` — `aeron-cluster-codecs.xml:668-725`) and so
  gets the same acting-version tolerance; the `ConsensusModule` message
  gained three `sinceVersion="3"` optional fields that way (`:712-714`).
  Cluster mark file and node-state file gate on major (`0`) only.
- **Archive:** the only real migration tooling in Aeron. `ArchiveTool
  migrate` prints mark-file, catalog and latest versions, then runs the
  planned steps (`ArchiveTool.java:759-790`); the planner holds three
  steps, `0→1`, `1→2` (`MINIMUM_VERSION = 2.0.0`), `2→3` (`3.0.0`), and
  applies every step whose minimum exceeds the on-disk version
  (`ArchiveMigrationPlanner.java:33-55`, `ArchiveMigration_1_2.java:41`,
  `ArchiveMigration_2_3.java:41`). Without a migration the archive refuses
  to open a catalog or mark file of another major
  (`Catalog.java:206-211`, `ArchiveMarkFile.java:509-512`). This is
  per-node, offline, and orthogonal to consensus: a rolling upgrade across
  an archive major is "stop node, `migrate`, start node".
- **Service snapshots:** payload is the service's; envelope check = the
  `appVersion` validator + `timeUnit` equality (§B(iii)). No migration
  tooling exists or is planned in-tree.

## F. Mapped onto UC's Level 1 / 2 / 3

| UC concept (companion note) | Aeron has… | maps to |
|---|---|---|
| Level 1: unknown kinds dropped **and counted**; bodies grow at the tail; a version byte in every datagram | frame `version` byte, drop + `invalidPackets`; SBE `sinceVersion` + optional-null; exact `schemaId` refusal | **Level 1, fully** — Aeron's whole peer-compat story is Level 1 discipline, done by a codegen tool rather than by hand |
| Level 2: a committed, monotone cluster-wide wire level (etcd/CockroachDB/Kafka shape) | **nothing.** No committed feature level, no floor record, no "cluster is at protocol X" state; `protocolVersion` on canvass/vote is written and never read | no counterpart — Aeron does not have a Level 2 |
| Level 3: a layout selected per term via `NEW_TERM`, and an FSM-version validator | `appVersion` in `NewLeadershipTerm` / `NewLeadershipTermEvent` / `SnapshotMarker`, read-side major-equality gate that *terminates* the reader | **the carrier half of Level 3 only** — a term-boundary stamp exists, but it selects nothing; it is a refusal, and a per-reader one |
| UC's `SNAP_BEGIN` FSM-`VERSION` equality refusal | `SnapshotMarker.appVersion` + `VersionValidator`, default major-equality | the same gate one notch looser (major, not exact) and pluggable |
| UC cnc page `version_compatible` (newer attacher on older page) | Java `cnc.dat`: major only; C client: major **and** `file.minor >= client.minor` (a newer client refuses an older driver) | Aeron's C rule — "the file may be newer than the attacher, never older" — is the swap-order rule the companion note wants, written as a check |
| journal/`StableValue` `format_ver` exact + per-node boot migration | `ArchiveTool migrate` (0→1→2→3), catalog/mark-file major gate; no cluster-side migration at all | same shape: per-host, offline, orthogonal to consensus |

The comparison in one line: **Aeron is per-message SBE versioning plus a
per-reader app-major gate and nothing in between** — no committed
cluster-wide level, no negotiation, no written procedure — and the
maintainers fall back to "snapshot, stop the whole cluster, restart" when
a change is semantic rather than syntactic. UC's Level 2 is the piece
Aeron lacks, and it is the piece that would have turned the 1.47.0 note
from a procedure into a refusal.

## G. Three upgrade axes, and the rules on each

Sections A–F are read from the framework's side. Turned around — "I run
an Aeron Cluster deployment; what are the rules when I change something?"
— there are **three separate axes**, and Aeron's docs never name them
because in Aeron they usually ship as one process: the consensus module,
the service container and the `ClusteredService` run in the same JVM, so
"upgrade the node" means all three at once. The *mechanisms* are
separate, and so are the rules. (UC separates them by construction —
node, service and client are three processes — which is why this
section is split the way it is.)

### G.1 Cluster axis: the Aeron jars themselves

Consensus module, service container, archive, media driver. The
application has no say here and no hook.

- Mixed Aeron versions between consensus modules are **undetected**:
  `protocolVersion` is carried and never compared (§A, §C). Decodability
  across a roll comes from Aeron's own SBE `sinceVersion` discipline (§D).
- Per-node files gate on **major** at open (§A), and every cluster mark
  file ever shipped is `0.x`, so no release has refused an older node's
  directory.
- Whether a release is *semantically* safe to roll is a per-release
  changelog judgement. The one time the maintainers made it (1.47.0) the
  answer was "clean shutdown with a snapshot, restart the whole cluster"
  (§B(i)).

### G.2 Application axis: the FSM, its commands, its snapshot

- **Aeron passes the command payload through as bytes.** The framework's
  `SessionMessageHeader` is SBE and carries only `leadershipTermId`,
  `clusterSessionId` and `timestamp` (`aeron-cluster-codecs.xml:132-138`);
  the service sees `onSessionMessage(session, timestamp, buffer, offset,
  length, header)` (`service/ClusteredService.java:82-88`) and the
  container never decodes what follows the header. SBE is what Aeron
  itself uses and recommends, **not a requirement** — but its extension
  rules (§D: append optional `sinceVersion` fields, never modify or
  remove one, new template for a mandatory or structural change, old
  template kept until every producer has stopped) are the discipline the
  application needs under whatever encoding it picks, because of the
  next rule.
- **A new FSM must replay the old log from the latest snapshot.**
  Recovery is "load the newest snapshot, replay the terms after it"
  (`RecordingLog` recovery plan, §E); the framework migrates nothing in
  between. Snapshot before upgrading so the replay window is short. Every
  message in that window is decoded by the *new* code, so an added
  field's absent/null value must mean "the old behaviour" inside the FSM.
- **The snapshot payload is yours, and only its envelope is checked.**
  `SnapshotMarker.appVersion` + `timeUnit` (§B(iii)). A snapshot-encoding
  change is either an app-major bump (stop-the-world by construction,
  §C) or a custom `VersionValidator` plus your own old-format decoder.
  If you want the log and the snapshot to carry different compatibility
  rules — issue #1671's ask — the validator is the only hook, and it sees
  one `int` per check with no indication of which of the four call sites
  it is (`VersionValidator.isVersionCompatible(context, underTest)`,
  `VersionValidator.java:37`).
- **Same app major → restart nodes one at a time; different major → the
  first new-major leader terminates every old-major node and service.**
  There is no order rule from the framework; the unverified maintainer
  sequence in §B(i) ("each member in turn, finally the leader") is the
  only one that surfaced.

### G.3 Client axis: the processes that submit commands

Two layers again, one per axis above.

- **Framework layer.** The cluster client's ingress protocol
  (`AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION = 0.3.0`) is
  gated at **major** by the leader on `SessionConnectRequest`
  (`SessionManager.java:270-275`, table A). The gate is one-sided: the
  leader's `SessionEvent` carries its own `version` back
  (`aeron-cluster-codecs.xml:153`, `sinceVersion="6"`), and
  `client/AeronCluster.java` never compares it — the class imports
  `SemanticVersion` and uses only the SBE acting version (grep,
  2026-09-19). So a newer client is refused by an older cluster; an older
  client is accepted by a newer one as long as the major holds.
- **Application layer.** The payload the client encodes is the same
  bytes the FSM decodes, so the G.2 extension rules apply **in both
  directions during a roll**: an old client's message must still decode
  on a new FSM (the replay case), and a new client's message with
  appended fields must be harmless on an old leader that skips them.
  Aeron gives the client side no version hook at all — no `appVersion`
  on the ingress session, nothing in the egress — so client/FSM
  compatibility is entirely the application's own contract.

### Mapped onto UC

For the reader coming from `docs/how-to/upgrade-a-cluster.md`:

| Aeron axis | UC surface |
|---|---|
| G.1 cluster: undetected mixed versions, per-release judgement, stop-the-world when semantic | the node↔node wire + cnc **flag day**; the committed wire level of issue #31 is the proposed replacement, the piece Aeron never built (§F) |
| G.2 payload as bytes, SBE recommended | `AppCommand = Bytes` end-to-end; the typed tier is a serde adapter, the encoding is the service's |
| G.2 replay the old log through new code | `install_snapshot(P)` + journal tail-replay through the new apply loop; Level 1's tail-growth and unknown-kind discipline |
| G.2 snapshot envelope check, major-only, pluggable | `ULTSNAP1 ‖ P` + the `SNAP_BEGIN` per-row `VERSION` refusal — **exact** today; the `const VERSION` validator in the FSM-identity spec is the reserved slot for Aeron's looser, pluggable rule |
| G.2 same-major roll of the FSM | no UC counterpart yet — a per-row `VERSION` inequality, where both sides report a nonzero one, refuses the snapshot session (`uc_net/src/receiver.rs:478,623`), so a service upgrade that bumps `VERSION` is a flag day too |
| G.3 framework layer, leader-side major gate | the client↔gateway remote protocol v1 (separate from the node wire, unchanged since M12) and the shmem `cnc` attach checks |
| G.3 application layer, no hook | the same: UC checks nothing about the command bytes a client sends |

## Sources not reachable or not verified

- Any aeron.io or wiki page describing a rolling-upgrade *procedure*:
  none exists among the pages fetched (wiki index, Cluster Tutorial,
  Operating Aeron Cluster, Cluster Troubleshooting, Cluster Standby Change
  Log, RAFT feature page, theaeronfiles.com consensus-module pages).
- The maintainer chat quote in §B(i): search-engine summary only,
  permalink unreadable — **not verified**.
- SBE forward-compatibility (old reader, newer message): the *rule* is
  now quoted from the fetched Design Principles and Message Versioning
  pages (§D, 2026-09-19); the *mechanism* in generated decoder code is
  still inferred, since the checkout holds no generated codecs.
- GitHub issue search: `gh search issues --repo aeron-io/aeron "rolling
  upgrade"` returned nothing; the web UI query showed one unrelated PR
  (#2093). Issue #1671 (fetched) is the only issue found that discusses
  `appVersion` semantics.
