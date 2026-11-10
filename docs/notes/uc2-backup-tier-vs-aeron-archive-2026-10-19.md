# The UC backup tier against Aeron Archive — a feature comparison, read from source

**Date:** 2026-10-19
**Status:** comparison note (feature map, no recommendation ranked)
**Related:** [The backup tier, explained](uc2-backup-tier-explained.md),
[How to back up to a tier](../how-to/back-up-to-a-tier.md),
[The snapshot catalog, explained](uc2-snapshot-catalog-explained.md),
`uc2-vs-aeron-premium-2026-09-19.md` (the component-level comparison this
sits under), `uc2-m7-vs-aeron-cluster-standby-2026-07-24.md`.
**Source:** the local Aeron checkout `/home/claude/ultima/aeron` at
`f0366beca8 2026-08-27`, `version.txt` = `1.54.0-SNAPSHOT`; paths below
written `A/` mean `aeron-archive/src/main/java/io/aeron/archive/`, `C/` mean
`aeron-cluster/src/main/java/io/aeron/cluster/`, and `X` means
`aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml`. UC
statements reflect `main` at `0a9bed82` (the 2.14.0 writeup merged) and cite
the file they were read from; nothing was re-run for this note.

## Headline

The two are not the same kind of thing, and the comparison only makes sense
once that is said. **Aeron Archive is a stream recorder**: it copies raw
Aeron frames from a publication into position-named segment files and
replays them onto a channel. It has no notion of a state machine, a commit,
or a snapshot. Aeron Cluster builds its durable log, its snapshots, its
follower catch-up and its off-cluster `ClusterBackup` on top of that
primitive. **UC's backup tier is the other way round**: a cluster-level
product that already knows what commit means, with its raw storage primitive
(`uc_journal` plus the tier's write-once objects) kept internal.

So the fair pairing is UC's tier against Aeron Archive **plus**
`ClusterBackup` and `RecordingLog`. Where a row below says "Aeron", read it
as that combination unless it names the Archive alone.

In one line each: UC is ahead on *what a backup means* (agreed history, a
replicated recovery point, fences, pin-aware retention, backup-gated purge);
Aeron is ahead on *moving data* (archive-to-archive replication with a live
merge, replay-then-merge consumers, an authenticated control plane). Neither
has an object store or encryption at rest; only UC compresses.

## Feature by feature

| Axis | UC backup tier (2.14.0) | Aeron Archive (1.54-SNAPSHOT) |
|---|---|---|
| Unit of capture | Coordinated snapshot sets (every row plus the cluster FSM at one position P) and committed journal chunks between them (`sets/<P>/`, `journal/<from>-<to>.ultj`) | Recordings: one per publication image, raw frames, position-addressed (`X:94-96`); a "snapshot" is just another recording the application writes (`C/service/ClusteredServiceAgent.java:995`) |
| What bytes are admitted | Only `min(commit, durable)` (`uc_node/src/tier/writer.rs:22-24`); only sets whose per-row hashes agreed across replicas, checked against the catalog before copying | Whatever the image delivered; the archive cannot tell committed from uncommitted. Cluster bounds the backup replay by the leader's commit-position counter (`C/ClusterBackupAgent.java:837-845`) |
| Capture mode | Continuous, on a background thread (`uc2-backup`) reading already-durable files; never on a polling agent, so it cannot stall commit | Live, inside the messaging fabric: a spy (LOCAL) or a subscriber (REMOTE) (`A/ArchiveConductor.java:558-560`). A LOCAL spy recording is part of the publisher's limit and can backpressure it (`aeron-driver/.../NetworkPublication.java:802-818`) |
| Restore-point granularity | Any frame end Q in `(S, reach]`; chunks cut at 64 MiB or a 60 s quiet interval, and one chunk ending exactly at each new set | Any frame-aligned position inside a recording |
| Storage backend | A directory on a mount that supports hard links (NFS, EFS, a block volume); no S3, no network service | A local directory (`aeron.archive.dir`), `java.io` only; no object store, no remote backend |
| Object format | Write-once objects, 1 MiB blocks, CRC32 over the raw bytes, zstd per block (level 3 default) | Preallocated segment files, 128 MiB default, a power of two between 64 KiB and 1 GiB, in Aeron frame format (`A/Archive.java:336, 1480-1486`); no compression |
| Encryption at rest | Not present (the operator secures the mount) | Not present |
| Catalog | Replicated in the cluster FSM: per instant the position, kind, log time, per-row version, majority hash, size and verdict; the tier adds a `SET` label per set and a committed watermark per tier, up to four tiers (`CLUSTER` kind 7) | A memory-mapped local file of SBE descriptors (start/stop positions, timestamps, term and MTU geometry, channel, sessionId), one per archive, not replicated (`A/Catalog.java`, `X:325-367`) |
| Integrity | CRC per block; every object read back before it counts; overlap byte-equality between writers; a tier install hash-checked against the leader's committed verdict; `uc2ctl tier verify [--deep]` | Optional per-frame CRC stored in the frame's `sessionId` field, so stored data is no longer byte-identical to the stream; checked on replay (`A/RecordingWriter.java:198-210`, `A/ReplaySession.java:416-418`); `ArchiveTool verify` walks headers and CRCs and marks bad entries INVALID |
| Durability meaning | A position in the tier is committed, fsync-durable on the node, and read back | `rec-pos` is set after `FileChannel.write` returns, so it means "in the page cache" unless `aeron.archive.file.sync.level` is 1 or 2 (`A/Archive.java:352-369`); after a crash the stop position is rebuilt by scanning frames (`A/Catalog.java:1053-1097`) |
| Restore | An offline builder writes a fresh instance dir: journal `[S,Q)`, floor S, term map from the `SET` lineage, no vote file; whole-cluster, new node, fork to a new `app_id`, or a diff-replay corpus | Replay onto a channel, bounded or open-ended; Cluster rebuilds state by replaying snapshot recordings then the log recording per the `RecordingLog` recovery plan (`C/RecordingLog.java:880-900`, `C/LogReplay.java`) |
| Rollback safety | A restore below the reach writes `fence/<Q>`; every writer refuses the fenced tier by name, so two histories never mix | Not present at the archive layer; Cluster's election can `truncateRecording` a follower's log (`C/ConsensusModuleAgent.java:1884`) |
| Retention | Replicated `backup_retain_sets` and `backup_retain_age` on log time; the newest set and a pin's rollback set are always kept; only verified sets count; any writer deletes, idempotently | Entirely explicit: truncate, purge, detach, purge-segments, attach, migrate-segments (`A/ArchiveConductor.java:1199-1619`); no time- or size-based policy |
| Backup-gated purge | `purge_requires_tiers = K`: no node purges below the K-th highest confirmed tier reach; released on low disk, with an alert | Not present; log retention is the operator's job |
| Cross-site copy | Not present; the only lever is several writers on one or several mounts | Pull-based `ReplicationSession` between archives, able to follow a live recording and merge onto the live stream; `ClusterBackup` is built on it (see below) |
| Seeding a new member | Online: a learner with `[tier_source]` installs from the tier before asking a peer; offline: `uc2ctl tier restore --new-node` | Follower catch-up is an archive replay from the leader (`C/ConsensusModuleAgent.java:1163-1164`); a backup node replicates snapshots and the live log |
| Live tail from the backup | Not present; the tier serves sets, the log above a peer's floor always comes from peers | Yes: `ReplayMerge` and `PersistentSubscription` switch a consumer from history to live (`A/client/ReplayMerge.java`, `A/client/PersistentSubscription.java`) |
| Admin and auth | Offline verbs rely on filesystem permissions; `tier forget` is admin op 11, HMAC-signed, audited | An SBE control protocol over Aeron (schema 101, version 14), a pluggable authenticator with challenge/response, per-template authorisation (`A/Archive.java:590-608`); the default is no auth |
| Versioning | Tier format `uc2-tier-v1`, object magics `ULTTOBJ1`/`ULTJCHK1`, `ULTSNAP1` refused; cluster image v2 to v6 loads older images in place | Mark file and catalog at 3.1.0 with a major-version check; `ArchiveTool migrate` runs 0→1→2→3; client protocol 1.12.0, a major mismatch is refused |
| Observability | `uc2_backup_through`, `uc2_backup_lag_bytes`, errors and gate metrics; five alerts; named log events; `GET /cluster` tiers block | Counters (recording position, bytes, cycle times), a recording-events stream, recording signals, a mark-file error log, a logging agent |
| Multi-writer | Yes, lock-free: hard-link placement, `SET` written last, overlap compare, canonicalised padding; a disagreeing writer stops and alerts | One archive per directory, enforced by mark-file liveness (10 s) |
| Linearizability proof | `uc_node/tests/lin_backup_tier.rs`: a restore at Q is WGL-linearizable with nothing after Q visible | Not present in the archive; Cluster's own tests |

## Where each one is ahead

**UC is ahead on what a backup means.** The tier stores the cluster's
agreed history, not a node's disk. The recovery point is replicated state
(the committed watermark), so every node answers "how far are we backed up"
identically. Fences, the lineage in `SET`, pin-aware retention and
backup-gated purge are all things an archive cannot do, because it does not
know what commit or a pin is. Aeron Cluster has no equivalent of the fence: a
`ClusterBackup` archive that kept recording after a rollback restore would
hold two histories.

**Aeron is ahead on movement of data.** Archive-to-archive replication with
a live merge is the feature UC lacks most clearly, and the one real gap this
comparison surfaces: UC's only answer to "copy this backup somewhere else" is
a second mount. The replay-then-merge consumer story has no UC counterpart
either, and the control plane is authenticated and authorised per request,
where UC's offline verbs trust the filesystem.

**Integrity and durability defaults differ in UC's favour.** UC's zstd per
block and CRC-on-raw-bytes is strictly more than Aeron's raw preallocated
segments, and read-back plus overlap compare is a stronger story than an
optional CRC that rewrites a header field. UC never counts an object until it
is read back, and the admitted bytes are already fsync-durable on the node;
Aeron's default sync level is 0, so an archive can report a position that a
power failure then loses (`A/Archive.java:364-368` says so).

**The deepest difference is who owns the "what is this?" question.** Aeron
Archive pushes it up: the archive stores positions in a stream, and
`RecordingLog` in aeron-cluster is the file that says which recording is a
snapshot and which is the log. UC pushes it down: the catalog of snapshot
instants lives inside the replicated cluster FSM, so the backup writer,
retention and the purge gate all consume a committed verdict instead of
local bookkeeping. That is why UC can offer "never delete a pin's rollback
set" with no operator step, and also why UC cannot cheaply add cross-archive
replication: a second tier would need its own committed watermark semantics,
not just a byte copy.

## What cross-archive replication is

Aeron's way of copying a recording from one archive to another over the
network, with no shared filesystem and no external copy tool. The caller
asks the **destination** archive to replicate recording X from a **source**
archive, and the destination does all the work through a `ReplicationSession`
(`A/ReplicationSession.java:61-75` lists its states), which behaves as an
ordinary archive client of the source:

1. Connect to the source's control channel; fetch X's descriptor.
2. Ask the source for X's current recording position, to learn whether X is
   still live.
3. Create a new local recording (or extend one, with `dstRecordingId`) with
   X's term geometry.
4. Ask the source to **replay** X from the destination's current position
   onto the destination's replication channel
   (`aeron.archive.replication.channel`).
5. Record that replay locally, like any subscription; report progress as
   `RecordingSignal` events `REPLICATE`, `SYNC`, `MERGE`, `REPLICATE_END`
   (`X:42-50`).
6. If the source recording has a stop position, stop there. If the source is
   still being recorded, enter `CATCHUP` then `ATTEMPT_LIVE_JOIN`: once the
   replay has caught up, add the live multicast destination and drop the
   replay, so the destination is now recording the live stream directly.

The replicated recording is an ordinary recording on the destination with
its own recording id; source and destination ids differ (`X:111`).

Three uses in the Aeron ecosystem:

- **Off-cluster backup.** `ClusterBackup` (`C/ClusterBackup.java:107-137`)
  replicates the cluster's snapshot recordings and then follows the live log
  recording from the leader's archive, bounded by the leader's commit
  counter. The result is a second archive, on another host or site, that can
  boot a cluster.
- **Standby and follower catch-up.** A follower that fell far behind, or a
  new member, replicates the log recording from the leader's archive rather
  than reading a shared disk.
- **Moving history between archives** for any reason, such as migrating an
  archive to a new box while it keeps recording.

Properties worth knowing: it is pull-based, so the source needs nothing
beyond allowing the control connection; it carries credentials in
`ReplicationParams`, so an authenticated source works; the live join needs
the source stream to be multicast or multi-destination; and it is **not a
byte copy of the archive directory** but a replay recorded again, so the
destination's segment layout, ids and (if enabled) CRC headers are its own.

UC's nearest things are the tier as a shared mount that several writers
fill, and the tier-as-source install, which lets a node pull a snapshot set
from the mount instead of from a peer. Both move data through a filesystem,
not from archive to archive.

## Open items on the UC side this comparison surfaces

All already recorded in `docs/BACKLOG.md` or the how-to's
[What the tier does not do yet](../how-to/back-up-to-a-tier.md#what-the-tier-does-not-do-yet):

- no object-store target, no tier-to-tier replication;
- the tier's journal is never served live, only its sets;
- R10a, retention's view gate at the origin-set edge, is still open;
- a silently changed object is detected by `tier verify --deep` but not
  repaired.

## Aeron facts an operator is most likely to misread

- **`max.catalog.entries` is deprecated** in favour of
  `aeron.archive.catalog.capacity` (default 1 MiB, grows about 1.5× up to
  2 GiB) (`A/Archive.java:463-487`, `A/Catalog.java:117`).
- **The `capacity` ArchiveTool command is listed in the help text but has no
  branch in `main()`**; it exists only as the static Java API
  (`A/ArchiveTool.java:390, 406, 1757`).
- **The README says the C async client is unimplemented**, but
  `aeron_archive_async_client.{c,h}` now exists, so that line looks stale.
- **Default concurrency limits are 20 recordings and 20 replays**
  (`A/Archive.java:429-452`); a wildcard subscription can multiply recordings.
