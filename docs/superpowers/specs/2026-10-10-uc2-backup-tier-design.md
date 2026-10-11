# UC2 backup tier (3a: write, watermark, restore) — design

**Status:** design approved in conversation on 2026-10-10; this document is
the written spec for review. **Project 3a** of the storage-service direction
(1 catalog, 2 node-local lifecycle, **3 backup tier**, 4 decision surface).
Project 3 was split in brainstorming: **3a** (this spec) writes the cluster's
agreed history to an off-node tier, records how far it reaches, and restores
from it; **3b** (its own spec, after 3a lands) makes the tier a *holder* —
nodes prune and purge below what the tier has confirmed, and fetch from it.
Depends on the catalog
(`2026-10-01-uc2-snapshot-catalog-design.md`) and the lifecycle
(`2026-10-09-uc2-snapshot-lifecycle-design.md`); read their errata first.

## 1. Problem

The only off-node copy UC knows how to make is `uc2ctl backup`: one node's
instance directory, copied by hand (`uc_node/src/backup.rs`,
`docs/how-to/back-up-a-cluster.md`). That leaves four gaps:

- **No recovery point is recorded.** Nothing in the cluster says how far any
  backup reaches, so "what do we lose if every node dies now" has no answer
  short of inspecting someone's backup directory.
- **It is one node's disk, not the cluster's history.** A node's journal ends
  in bytes that are durable but not committed — an election may truncate
  them — and its `state/` holds that node's private vote. Restoring a whole
  cluster from one node's copy clones both.
- **No history.** A copy is a point, overwritten by the next; "the cluster as
  of last Tuesday" (for an investigation, a diff-replay corpus, or a rollback)
  needs many sets and the journal between them.
- **The post-pin rollback is manual.** The upgrade how-to names "the
  pre-upgrade off-node backup restored on every node" as the only way back
  after a pin, and leaves taking it to the operator.

## 2. Scope

**In (3a):**

- A **tier format** in a filesystem directory: agreed snapshot sets and the
  committed journal between them, position-keyed, safe for several writers
  without locks, zstd-compressed per block (§4).
- A **writer**: a background agent on nodes configured with `[backup]` (§5),
  including a **purge hold** on the writer's own node (§5.4).
- The **watermark**: `CLUSTER` kind `7` `BackupWatermark`, one entry per tier,
  reported over a new pairwise datagram (§6).
- **Tier retention**, replicated in `Settings` (§7).
- **Restore**: one builder that materialises an instance directory as of a
  position Q, used four ways — whole-cluster restore, a new node, a fork, and
  a diff-replay corpus export — plus `tier show` and `tier verify` (§8).

**Out:**

- **3b**: any node other than the writer acting on the watermark — pruning or
  purging below `retain_sets` because the tier holds a set, fetching a set or
  journal from the tier at start or join, a purge floor over K holders, and
  the D3 revisit (lifecycle spec §14).
- **Object storage.** The target is a directory behind a small trait (§4.4);
  an S3-shaped implementation is a later, separate item.
- **Encryption at rest** and **access control** on the target. The tier holds
  the same bytes the nodes' disks hold, under the same trust; the operator
  secures the mount.
- **Continuous sub-segment streaming** of the journal tail. The recovery
  point is one chunk or one quiet interval (§5.3).
- The operator / AI surface beyond `tier show` (project 4).

## 3. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **The tier holds the cluster's agreed history in its own format** (approach A), not a mirror of one node's instance directory (approach B). | Only committed bytes and agreed sets are the cluster's history; a node's directory adds uncommitted tail bytes and private state (vote, config shadow). A position-keyed format lets every restore and 3b share one reader. B reused more code but would need the same committed bookkeeping and gives segment-coarse (64 MiB) points in time. |
| D2 | **Target = a filesystem directory behind a `BackupTarget` trait**; one implementation. | No new heavy dependency; any mount works (NFS, EFS, a block volume); tests run on local disk. The trait's four operations (§4.4) are what an object-store implementation would need. |
| D3 | **The writer is a node agent, opt-in per node** (`[backup]` in `node.toml`). | The node already holds the sets, the journal and the commit position; a separate daemon would re-derive them and need admin authentication to report. Per-node because mount paths are per host. |
| D4 | **Agreed sets + committed journal**, cut at `min(commit, durable)` into chunks. | Point-in-time restore to any frame end ≤ `through`; committed bytes are never truncated, so nothing in the tier is ever retracted. |
| D5 | **Write-once objects, a set's `SET` label last.** No locks. | Several writers and crashed writers are safe by construction; a reader never sees half a set. |
| D6 | **The writer's node holds its purge for the tier, released when space runs low.** | A backup that falls behind would otherwise lose history silently whenever purge outruns it. The release keeps UC's rule that a node never fills its own disk (M11 fail-stop): the backup degrades, the service does not. Maintainer's call, 2026-10-10. |
| D7 | **The watermark is replicated** (`CLUSTER` kind 7), reported by the writer, appended by the leader. | It is a fact every node and every operator must agree on, and it survives in the cluster artifact. Same report path as `SNAP_REPORT`. |
| D8 | **Tier retention is a replicated policy the writer applies; the default keeps everything.** | One policy for several writers; a new backup never deletes until told to. Pinned data follows the maintainer's node rule: kept while the pin is incomplete, then ordinary retention. |
| D9 | **One builder, four restores.** A whole-cluster restore and a fork require explicit confirmation; a single-node restore is **always under a new node id**. | A restored node's vote is empty by design; under a reused id that is a forgotten vote, which the replace-hardware rule already forbids (`change-cluster-membership.md` § Replace hardware). |
| D10 | **A fork replays no side effects; every other restore replays them from the start set** (at-least-once). | A fork is a copy for inspection; its outputs already happened in the real world. Recovery keeps the existing at-least-once contract — an effect may repeat, none is skipped. |
| D11 | **The tier compresses with zstd, per 1 MiB block, on by default; the node's own journal and snapshots stay uncompressed.** Checks are over raw bytes. | The tier is written once, read rarely and kept long — where CPU-for-space pays. Journal frames (32 B alignment padding, repeated headers) are the likely win; snapshots depend on the app, and incompressible blocks are stored raw. The node's journal is on the commit path and is the retransmit source, so compressing it is a separate measured question. Maintainer's call, 2026-10-10. |

## 4. The tier format

### 4.1 Layout

```
<target>/
  TIER                          tier identity (§4.2)
  sets/<P>/                     one agreed set, P = the instant's position (20-digit, zero-padded)
      row-<id>.ultsnap          each declared row's artifact (ULTSNAP2 envelope included), in a tier object (§4.1.1)
      cluster.ultcluster        the cluster FSM's artifact at P, in a tier object
      SET                       the set's label (§4.3) — written LAST
  journal/<from>-<to>.ultj      committed log bytes [from, to) (20-digit, zero-padded)
  .tmp/                         in-progress writes; anything here is garbage
```

A `.ultj` chunk holds exactly `to − from` bytes of the log, starting and
ending on frame boundaries (both positions are frame ends, multiples of
`FRAME_ALIGNMENT`), in a tier object. Its raw bytes are the log's own frames,
so a reader walks them with the existing frame iterator.

#### 4.1.1 Tier objects and compression

Every data file in the tier (artifact or chunk) is a **tier object**: a
48-byte header, then a sequence of blocks.

```
magic [u8;8] (ULTTOBJ1 artifact | ULTJCHK1 chunk) ‖ codec u8 (0 none, 1 zstd) ‖
reserved [u8;7] ‖ raw_len u64 ‖ crc32(raw bytes) u32 ‖ reserved u32 ‖
from u64 ‖ to u64                      (chunk: the range; artifact: both 0)
block := stored_len u32 ‖ raw_len u32 ‖ crc32(raw block) u32 ‖ stored bytes   (raw_len ≤ 1 MiB)

CRC is CRC-32/IEEE via `crc32fast`, the cluster image's own checksum.
```

- **Codec per object**, chosen by the writer (`[backup] compression`, §5.1);
  every reader handles both. A zstd block is one independent zstd frame, so a
  restore stopping at Q inside a chunk decompresses only the blocks it needs,
  and damage costs one block. Restore streams block by block and never holds
  a whole object in memory.
- **Every check is over the raw bytes**: the header CRC, a chunk's frame walk,
  the agreed artifact hash, the `Exists` comparison between writers (§5.2) and
  `tier verify --deep`. Two writers with different codecs or levels therefore
  produce equal objects by every rule a reader applies.
- **Incompressible blocks are stored raw** (a zstd block whose output is not
  smaller is written with `stored_len == raw_len` and the raw bytes, flagged
  by equality), so an app that compresses its own snapshots pays almost
  nothing.
- **The node's own journal and snapshots are not compressed** — only the tier
  is. The node's journal is on the commit path and is the retransmit source;
  compressing it is a separate, measured question (`docs/BACKLOG.md`).
- Dependency: the `zstd` crate (BSD-3/MIT; builds bundled C `zstd-sys`, the
  workspace's first C dependency outside examples), admitted through
  `deny.toml` and the MSRV 1.89 gate.

### 4.2 `TIER`

Text, `key=value` lines like `MANIFEST`: `format=uc2-tier-v1`, `app_id`,
`tier_id` (a random non-zero u64, hex), `created_ns`. The first writer to
find no `TIER` creates it (write-once, §4.4); every later writer and every
reader reads it. A writer whose `app_id` differs refuses the target by name
(`backup_target_foreign`) and writes nothing.

### 4.3 `SET`

Text, `key=value`: `position`, `kind` (`full`/`standby`), `time_ns` (the
catalog entry's log time), then per row `row.<id>.hash`, `row.<id>.version`,
`row.<id>.size`, and `cluster.hash`, `cluster.size` (sizes are RAW lengths,
as the catalog records them; the stored length is the object's own business). The hashes are the
**catalog's agreed hashes**, not the writer's recomputation; the writer
checks its local files hash to them before copying (§5.2). A set directory
without `SET` is not a set.

### 4.4 The `BackupTarget` trait

```rust
pub trait BackupTarget: Send + Sync {
    /// Create `path` iff absent: the callback writes a .tmp/ file; the target
    /// fsyncs it, links it into place (refusing an existing name), fsyncs the
    /// directory. Present already → `Exists` (the caller compares).
    fn put_if_absent(&self, path: &str, write: &mut dyn FnMut(&mut File) -> Result<u64, TierError>) -> Result<Put, TierError>;
    fn list(&self, dir: &str) -> Result<Vec<String>, TierError>;
    fn open(&self, path: &str) -> Result<Option<File>, TierError>;
    fn delete(&self, path: &str) -> Result<(), TierError>;   // absent → Ok
    fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError>;
}
```

`FsTarget` is the one implementation. Link-into-place is the atomicity
primitive (a hard link then unlink of the temporary name, so an existing
final name is never replaced — POSIX `rename` would replace it); a crash leaves only `.tmp/` entries, which a writer removes when
it starts (only entries older than one hour, so a concurrent writer's
in-flight file is not taken from under it).

### 4.5 Invariants a reader may rely on

1. A file at its final name is complete and never changes.
2. A set is complete iff its `SET` exists; every file `SET` names is present
   while `SET` is (deletion removes `SET` first, §7.2).
3. Journal chunks may overlap (two writers cut at different points). Committed
   bytes are identical everywhere, so overlapping ranges must be byte-equal;
   a reader that finds them unequal stops with `tier_overlap_mismatch` — it
   means two clusters share a target or the tier is corrupt.
4. **The chain** from a set S is the longest run of chunks covering
   `[S, X)` without a gap; X is that set's reach.

## 5. The writer

### 5.1 Configuration and placement

```toml
[backup]
target = "/mnt/uc-backup"        # required to enable; absent section = no writer
chunk_bytes = "64MiB"            # cut a journal chunk at this much committed history
quiet_interval = "60s"           # or after this long with any uncut history
compression = "zstd"             # "zstd" (default) | "none" — the tier objects this writer writes
compression_level = 3            # zstd level, 1..=19
```

Node-local (`node.toml`), not replicated: paths differ per host. Parsed into
`StartupOptions` and handed to the node through `StartOpts::backup` — not
`NodeConfig`, which has no `Default` and ~50 literal call sites, and for the
same reason `StartOpts::admin` lives there. The writer
runs as its own thread, `uc2-backup`, outside the five polling agents, with
the sleeping idle strategy the holdings runner uses; its I/O never runs on a
hot agent. A learner is the recommended writer (operator docs); any member
may be one; several may share a target.

### 5.2 Sets

Each pass, for every catalog entry that is **agreed** (catalog verdict, the
same predicate `newest_agreed` uses) and complete on this node's disk and not
complete in the tier: hash the local files and compare with the catalog's
agreed hashes; on match copy each artifact with `put_if_absent`, then write
`SET`. A local mismatch is not copied (`backup_set_hash_mismatch`, once per
set) — the node holds a diverged copy, and another writer or a later fetch
will supply the agreed one. Sets awaiting agreement wait; diverged sets are
never copied. An `Exists` answer is compared by **envelope position and
payload hash** (`artifact_hash_of`, which covers the payload only) and is a
silent success when both match, `tier_overlap_mismatch` when not. Byte
equality is deliberately not required: every member that took the instant
builds its own copy of the set at P, and agreeing copies have identical
payloads, but their `ULTSNAP2` envelopes may carry different patch versions
of the same line (the lifecycle's start rule admits `same_line`, errata P3).
So whichever writer finishes first supplies the tier's one copy per P, any
agreeing copy is as good as any other, and `SET` records the version the
catalog lists for the row, not the envelope's.

### 5.3 Journal

The writer reads the tier's chain end E for the newest set it can extend
(§4.5) and its own local committed frontier C = `min(commit, durable)`. When
`C − E ≥ chunk_bytes`, or `C > E` and `quiet_interval` has passed since the
last cut, it reads `[E, C)` from its journal (`replay_journal_from`), and
writes one chunk (or several, each ≤ `chunk_bytes`). Chunk ends are frame
ends; C is always one.

**A gap.** If this node's journal no longer reaches E (`first_base > E`), the
writer cannot extend that chain. It starts a new chain at the newest set it
holds whose position ≥ `first_base`, copying that set first if absent. The old
chain's reach stays where it was; the watermark (§6) reports only the chain
it extends, so a gap is visible as `oldest_set` jumping and a
`backup_chain_gap{from, to}` event.

### 5.4 The purge hold

On the writer's node only, the purge floor is clamped to the writer's
**local** progress — the end of the chain it last confirmed in the tier
(its own record, not the replicated watermark, which lags) — the same way
`hold_floor_for_pins` clamps it for pins. The hold is released while the
node's free space is below the auto-fetch headroom rule
(`auto_fetch::fits`, lifecycle spec §7.3, applied to one `chunk_bytes`): the
node purges as usual, the writer records a gap when it resumes, and
`backup_hold_released` is logged once per episode with the
`Uc2BackupHoldReleased` alert. A writer that cannot reach its target at all
holds on the same terms. Purge is off by default, so the hold matters only
where `[purge]` is on.

### 5.5 Reporting

After every pass that advanced anything, the writer reports its chain to the
leader (§6.2).

## 6. The watermark

### 6.1 The record

`CLUSTER` kind `7` (reserved by the catalog spec for exactly this),
`BackupWatermark`:

```
op u8 (0 = report, 1 = forget) ‖ reserved [u8;7] ‖ tier_id u64 ‖
oldest_set u64 ‖ newest_set u64 ‖ through u64 ‖ reporter u64 (node id)
```

The cluster FSM keeps up to **4** tiers (`MAX_BACKUP_TIERS`), each
`{tier_id, oldest_set, newest_set, through, reporter, updated_pos}`, in the
cluster image (version 5 → 6, a trailing blob; a v5 image loads with no
tiers). Meaning: **tier `tier_id` can restore any frame end in
`[oldest_set, through]`**, from the chain the reporter extends.

Apply rules (deterministic, at commit):
- `report` for a known tier: `through` and `newest_set` move only forward;
  `oldest_set` may move forward (retention, a gap) but never above
  `newest_set`. A report that would move `through` backward is a no-op
  (counted, `backup_report_stale`) — a slower writer's older news.
- `report` for an unknown tier when 4 are held: a no-op, counted
  (`backup_tiers_full`).
- `forget`: removes the tier's entry (§6.3).

### 6.2 The report path

A new pairwise datagram, `DGRAM_KIND_BACKUP_REPORT = 27`, body = the record's
fields after `op` (40 B), sent to the known leader like `SNAP_REPORT` 26
(`deliver_snapshot_reports`); dropped when no leader is known and resent on
the next pass. A leader reports in-process. The leader coalesces pending
reports per tier (newest `through` wins) and appends one kind-7 command per
tier, under the existing single-in-flight rule for `CLUSTER` commands.

### 6.3 Forgetting a tier

`uc2ctl tier forget <tier_id>` — admin op **11**, leader-only, audited
`tier_forget`; refusal **62 `tier_unknown`** when no entry has that id. The
tier's files are untouched; a writer still pointed at it re-registers it on
its next report.

### 6.4 What reads it

- `/metrics`: `uc2_backup_through{tier}`, `uc2_backup_oldest_set{tier}`,
  `uc2_backup_lag_bytes{tier}` (`commit − through`, from the leader's view),
  and on writer nodes `uc2_backup_hold_released_total`,
  `uc2_backup_bytes_written_total{kind=set|journal}`, `uc2_backup_errors_total{reason}`.
- Alerts: `Uc2BackupBehind` (lag above a rule threshold, or `through` flat for
  15 m while commit moves), `Uc2BackupHoldReleased`, `Uc2BackupWriterError`.
- `uc2ctl tier show` and `settings show`-style reads of the committed
  artifact. Project 4's live catalog read will include it.
- **No node acts on it in 3a.** Even the writer's purge hold uses the
  writer's own progress, not the watermark (§5.4). Acting on it is 3b.

## 7. Retention in the tier

### 7.1 The policy

`Settings` v4 → **v5** (`SETTINGS_LEN` 36 → 46), two trailing fields:

| field | type | meaning | default |
|---|---|---|---|
| `backup_retain_sets` | u16 | always keep at least this many newest sets | `0` = no count limit |
| `backup_retain_age_secs` | u64 | always keep sets whose log time is younger than this | `0` = no age limit |

Older versions decode both as `0`. No door refusal beyond the existing
reserved-sentinel rule: `backup_retain_age_secs == u64::MAX` is refused with
`47 settings_bounds`. `uc2ctl settings apply` accepts `[backup] retain_sets`
and `retain_age` (a duration string).

### 7.2 The rule

A set S in the tier is **deletable** iff all hold:
1. at least one limit is non-zero, and S is outside both: not among the
   `backup_retain_sets` newest complete sets in the tier (when non-zero), and
   older than `backup_retain_age_secs` by log time against the newest set's
   log time (when non-zero; the tier's own clock, not a wall clock);
2. S is not the newest complete set;
3. S is not **pin-protected**: for every row whose newest pin is incomplete
   (the completion rule C1 of the pin-completion rulings,
   `hold_floor_for_pins`'s predicate), the pin's origin set and the newest set
   strictly below the pin's record position (the rollback point) are
   protected, together with the journal from that rollback set to the record
   position.

Deletion order, any writer, idempotent: remove `SET`, then the set's files;
then remove every chunk whose `to` ≤ the new oldest complete set's position
and is not inside a protected span. The writer then reports the new
`oldest_set`. A restore reading a set while it is being deleted fails on the
missing file by name (§8.1 step 1) and never builds from half a set.

## 8. Restore

### 8.1 The builder

`uc_node::tier::materialize(target, q, out_dir, mode) -> Materialized`, all
offline. It refuses an `out_dir` whose `instance.lock` is held (the running-
node probe `restore_artifact` uses) or whose `journal/`, `state/` or
`snapshots/` is non-empty (`TargetNotEmpty`, as today).

1. **Start set.** S = the newest complete set with `S ≤ Q` whose chain reaches
   Q. Every artifact is hashed against `SET`; a missing or mismatched file is
   a named refusal.
2. **Q.** Default `through` of the chain; an explicit `--at Q` must be a frame
   end inside `[S, reach]`, else `restore_point_unreachable` naming the
   restorable range.
3. **Journal.** A fresh `uc_journal` whose floor is S (`Archive::adopt_floor`)
   and whose blocks are the chain's bytes `[S, Q)`, verified chunk by chunk
   (CRC, frame walk, overlap equality).
4. **Snapshots.** `snapshots/<row>/snap-S.ultsnap` and
   `snapshots/cluster/snap-S.ultcluster`, copied.
5. **State** — written by the builder, never copied from anywhere:
   - `snapshot.state` = S;
   - `term_map.state` = derived from the frame headers in `[S, Q)` (the
     builder computes it; boot's `rederive_term_map` then has nothing to add);
   - `config.state` = the membership carried by the cluster artifact at S,
     `position = S`; boot's `rederive_config` folds in the membership frames
     in `[S, Q)` as it does today;
   - `vote.state` = **absent** (no vote);
   - `output_progress.state` = per D10: S for recovery and new-node, Q for a
     fork.
6. A `RESTORED` file (`key=value`: tier_id, S, Q, mode, created_ns) for the
   audit trail; nothing reads it.

### 8.2 The four uses

| command | does | guard |
|---|---|---|
| `uc2ctl tier restore --target T [--at Q] --instance-dir D --confirm-cluster <app_id>` | **Whole-cluster restore**: run on every node, same T and Q. History after Q is gone. | `--confirm-cluster` must equal the tier's `app_id` (as `force-single-member`). Every node starts from identical history with no vote, so the first election is ordinary. The doc states the data-loss boundary and that this is the quorum-loss domain, not the minority-restore rule. |
| `uc2ctl tier restore --new-node --node-id N …` | **One replacement node** seeded from the tier instead of a peer's snapshot session. | Refused when `N` appears in the set's membership or tombstones (`restore_id_reused`): a new id only, then `uc2ctl add`. |
| `uc2ctl tier fork --target T [--at Q] --instance-dir D --app-id NEW` | **A separate cluster** as of Q. Materialises with `output_progress = Q`, then applies the `force-single-member` plan to the result. | `NEW` must differ from the tier's `app_id` (`fork_same_app_id`). The operator grows the fork with add/promote. |
| `uc2ctl tier export-corpus --target T --row R [--from S] --to Q --out D` | **A diff-replay corpus**: the builder's output plus `CORPUS` (`origin = S`, `end = Q`, `row`, `version` from `SET`). | `uc2-diffreplay` reads it unchanged. |

### 8.3 Read-only verbs

- `uc2ctl tier show --target T` — `TIER`, every set (position, kind, time,
  rows), every chain's range and gaps, and the restorable ranges; with
  `--instance-dir` of a live node, the committed watermark beside it.
- `uc2ctl tier verify --target T [--deep]` — chunk CRCs and frame walks, chain
  continuity, overlap equality, every `SET`'s files present with the stated
  sizes; `--deep` re-hashes every artifact against its `SET` hash. Exit non-
  zero on any finding, each named.

## 9. Wire, image and settings changes — inside the unreleased flag day

| Surface | Change |
|---|---|
| `CLUSTER` kind 7 | `BackupWatermark` (§6.1); `ClusterKind::from_u8` admits 7 |
| pairwise datagram 27 | `DGRAM_KIND_BACKUP_REPORT` (§6.2) |
| `Settings` (kind 3) | v5, `SETTINGS_LEN` 46 (§7.1) |
| cluster image | `CLUSTER_IMAGE_VERSION` 5 → 6, trailing backup-tier blob |
| admin | op 11 `tier_forget`, refusal 62 `tier_unknown` |
| cnc | unchanged |

Wire `0.11.0` is unreleased (the catalog opened it; 2.14.0 not yet cut), so
these fold into it: **no new version and no extra flag day.** The
`version.rs` 0.11.0 comment gains the two additions. A 2.13.0 peer is already
excluded by the 0.11.0 flag day.

## 10. Failure handling

| Condition | Behaviour |
|---|---|
| target unmounted / unwritable | writer logs `backup_target_unavailable` once per episode, retries on a 1 s → 60 s ladder; purge hold per §5.4; `Uc2BackupWriterError` |
| `TIER` names another `app_id` | writer refuses the target by name, writes nothing, alerts; never "fixes" it |
| target full (`ENOSPC` on the target) | the write fails, the `.tmp` entry is removed, retry ladder; the node's own disk is unaffected |
| overlap or `Exists` mismatch | `tier_overlap_mismatch`, the writer stops writing to that target (alert) — it is evidence of two clusters on one target or corruption, and continuing would mix them |
| writer crash mid-file | only `.tmp/` residue; cleaned at next start (§4.4) |
| two writers | both safe (§4.5); their reports coalesce at the leader |
| leader unknown | reports dropped and resent next pass |
| restore hits a missing or bad file | named refusal before anything is written to `out_dir` beyond `.tmp` — the builder assembles under `out_dir/.restore-tmp` and renames into place last |

## 11. Proof

1. **Format** (`uc_node::tier` unit tests): tier-object round trip for both
   codecs (incompressible block stored raw; partial read to a block boundary;
   a corrupted block fails its CRC by name), write-once, `Exists` comparison
   across codecs,
   `.tmp` cleanup age rule, `SET`-last visibility, chunk header + CRC round
   trip, chain computation with gaps and overlaps, overlap mismatch.
2. **Writer** (in-process, `uc_node/tests/backup_tier.rs`): sets copied only
   when agreed; diverged and pending sets never; journal chunks contain only
   bytes ≤ `min(commit, durable)` (a test that drives a leader past commit and
   truncates on failover asserts the truncated bytes never reach the tier);
   quiet interval cut; gap → new chain; two writers into one target.
3. **Purge hold**: purge on, writer behind → floor clamped; free space faked
   low (`set_free_bytes_for_test`) → hold released, gap recorded.
4. **Watermark** (cluster FSM unit tests + sim): forward-only `through`,
   `oldest_set` bounds, stale no-op, 4-tier cap, forget; the image v5 → v6
   load.
5. **Retention**: count, age, both, defaults keep all; pin protection holds
   while incomplete and lifts on completion; deletion order under a
   concurrent reader.
6. **Restore round trip under the linearizability checker** (the capstone,
   `uc_node/tests/lin_backup_tier.rs`): run `lin_v2`'s register load with a
   writer and failovers → stop every node → `tier restore` every node at Q →
   restart → the restored register history equals the recorded history up to
   Q (WGL-linearizable, and no op after Q visible) and the cluster accepts new
   writes.
7. **Fork**: boots under the new `app_id` as one node, accepts writes, and its
   `on_committed` replays nothing below Q.
8. **Corpus**: an exported corpus passes `uc2-diffreplay` determinism mode on
   the kv example.
9. **New node**: seeded from the tier, added, catches up; `restore_id_reused`
   refuses an existing id.
10. Fuzz: a `uc_node_tier_object` target over the tier-object header, block
    decode (both codecs) and a chunk's frame walk.
11. Size smoke (not a gate): raw vs stored bytes for the kv example's journal
    and snapshots, recorded in the plan's ledger so the default is grounded.

## 12. Documentation

- New how-to `docs/how-to/back-up-to-a-tier.md` (set up a writer, choose
  retention, read the watermark, verify, the four restores); `back-up-a-
  cluster.md` points at it as the recommended path and keeps the per-node
  tool as the low-level one.
- `upgrade-an-application.md`: the rollback point after a pin is the tier's
  protected rollback set (§7.2), restored with `tier restore --at <record
  position>`.
- References: configuration (`[backup]`, the two settings), uc2ctl (the six
  verbs, op 11, refusal 62), metrics and alerts, instance-directory
  (`RESTORED`), wire protocol (kind 7, datagram 27).
- `docs/notes/` explainer: what the tier holds and why it is the cluster's
  agreed history rather than a node's disk.
- Release entries at the 2.14.0 cut.

## 13. Risks

- **A restored cluster must commit from a cold start.** Every node starts with
  services at S and history to Q. `docs/BACKLOG.md` records "a whole-cluster
  cold restart may never commit" when the log end sits past `fsm_lag` at a
  cold start (unverified on main). The restore round trip (§11.6) exercises
  exactly this; if the defect is real it blocks 3a's whole-cluster restore and
  is fixed first, in its own ticket.
- **The fork's cluster FSM membership.** `force-single-member` writes only
  `config.state`; the cluster artifact at S still carries the old membership,
  and `uc_sim`'s inv12 requires the FSM's membership to be a committed prefix
  of the kernel's. Whether today's quorum-loss procedure reconciles the two
  (and how) is **not verified**; the plan's pre-flight establishes it before
  the fork task, and the fork may need a membership command appended by the
  single node at its first leadership.
- **The leader trusts the writer's claim.** A buggy writer can overstate
  `through`. `tier verify` is the check; 3b, which will delete on the
  watermark, must verify before acting.
- **The writer reads the journal on a busy node.** It is the same read path a
  deep-NAK replay uses, on its own thread; a learner writer keeps it off the
  voters.

## 14. Open questions for 3b

- Whether a node may retire sets below `retain_sets` and purge below its
  floor once the watermark covers them, and with how many tiers confirming.
- Fetching a set or journal span from the tier at start or join, and whether
  that replaces every-node copies (D3 of the lifecycle spec).
- Whether 3b's deletions require a verify pass of the tier first (§13).

## Review focus (for the plan)

1. A failover that truncates bytes above commit while the writer is cutting a
   chunk — the chunk must end at or below the commit the writer read, never
   include truncated bytes.
2. Two writers, one behind on a purged journal (gap) and one ahead — the
   watermark must follow the extending chain and never report a reach across
   the gap.
3. Retention deleting the set a concurrent `tier restore` has chosen — the
   restore must refuse by name, not build from a partial set.
4. A pin made, then completed, while the tier's retention is tight — the
   rollback set is protected exactly until completion, then ages out.
5. A whole-cluster restore at a Q far past the start set (more than `fsm_lag`
   of replay) — the restored cluster must elect and commit (§13).

#### Errata (as built, 2026-10-11)

Where the build differs from or refines this spec; numbered as in the plan
(`docs/superpowers/plans/2026-10-10-uc2-backup-tier.md`) and its execution
ledger (PF = pre-flight, R = execution rulings, by task).

- **PF1, R3a — the chain rule, first step included (§4.5).** A chunk extends
  a chain at cursor c iff `from ≤ c < to`; among those the greatest `to`
  wins, in both `reach_from` and `plan_chain`. "The chunk starting at the
  cursor" was loose prose: chunks are cut at chain ends, not at sets, so a set
  usually falls INSIDE a chunk, and a begin-at-start rule would leave every
  newer set reaching nothing — restore would fall back to the oldest set, and
  once retention deleted it the tier would restore nothing.
- **PF2, R8a — raw journal blocks, padding bodies zeroed (§5.3).** The writer
  does not re-serialise frames from `replay_journal_from` (`Replay` skips
  padding, so the log could not be reproduced). It reads the archived blocks
  raw (`find_block` + `Journal::read`), slices `[E, end)` and walks frame
  headers only to find the last frame end ≤ C. Padding-frame bodies and
  alignment slack are zeroed in the chunk: those bytes are not replicated
  (padding travels header-only and each node archives its own leftovers), so
  byte-exact copies would trip `tier_overlap_mismatch` between two honest
  writers. Every other byte is the log's own.
- **R8b, R11a(ii) — bookkeeping-only quiet cuts (§5.3).** A quiet cut whose
  range holds only padding and `BackupWatermark` frames is skipped — else
  each report's own kind-7 frame would re-trigger a cut forever on an idle
  cluster. One such cut IS allowed when the tier's `through` equals its newest
  complete set's position, so a freshly copied set on an idle cluster gets a
  chunk past it and becomes restorable; after that cut `through` is past the
  set and the skip applies again. (In practice an instant's own
  `SnapshotReport` frames already follow S, so the exception is defensive.)
  `Uc2BackupBehind` keys on the level of `uc2_backup_lag_bytes` (> 256 MiB),
  not on "commit moved while `through` stood still", for the same reason.
- **R8c — the gap event (§5.3).** `backup_chain_gap` also fires when the
  watermark jumps past the old reach, not only when the writer's own journal
  no longer reaches E; the writer's shared state gains `stopped` and `gaps`.
- **R8d, superseded by R9b — CUT TO THE SET (§5.2/§5.3).** Copying a set above
  the chain end would start a new bare chain (`watermark_of` prefers the
  set's reach) and abandon `[E_old, P)` at every instant. R8d deferred such a
  set until the chain passed it; with instants more frequent than chunk cuts
  and `retain_sets = 1` every deferred set left the catalog first, so the tier
  never gained a second set. As built: a set is copied when the tier has no
  watermark yet, when P ≤ `through`, or in the gap case (the newest held set ≥
  `first_base`); otherwise the writer's next cut ends exactly at P (several
  chunks if `P − through > chunk_bytes`), then copies P. Smaller chunks at
  instant boundaries are the cost.
- **R8e — the leader keeps a report that moves ANY field (§6.2).** The
  leader's filter dropped a report unless `through` moved, which blocked a
  late set copy (`newest_set`) and retention's `oldest_set` advance at an
  unchanged `through` — on an idle cluster indefinitely. As built the leader
  keeps a report iff it moves any of `through`, `newest_set`, `oldest_set`
  forward against the committed entry, and coalescing keeps per-field maxima
  (`oldest_set` clamped ≤ `newest_set`). A report whose `through` is BEHIND the
  committed entry is still dropped whole (the FSM would no-op it). A follower
  with no known leader puts the claim back in its slot, and the writer
  re-offers an unchanged claim every 10 s, because a leader change drops the
  leader's pending reports.
- **R5a — the `oldest_set` clamp (§6.1).** The clamp of `oldest_set` to
  `newest_set` at apply is defensive: the record decoder already refuses
  `oldest_set > newest_set` (42), and for any accepted record the clamp
  cannot bind. It is tested through `ClusterState::put_backup_watermark`.
- **R9a — the purge hold (§5.4).** The hold is keyed on the TIER's `through`
  (any writer's chunks count — bytes in the tier are backed up whoever wrote
  them), not on this writer's own chain alone, and it clamps the journal purge
  target (sets are covered by the writer's keep list). A STOPPED writer
  (foreign target, overlap mismatch) RELEASES its hold: it can never progress,
  and `Uc2BackupWriterError` already alerts.
- **R10a — retention waits for the view (§7.2).** A writer plans retention only
  once its committed cluster view has consumed at least to the newest complete
  set in the tier. Any pin whose rollback set is in the tier has its record
  below that set, so a view past it has applied the pin; a lagging view would
  not yet hold the pin that protects a set another writer copied.
- **The copy filter never re-copies a set retention would delete (§5.2/§7).**
  A catalog set the policy (with pin protection) would delete the moment it
  landed is not copied — whether it lies below the oldest set or above a
  protected one — or every deleted set would come straight back. Incomplete
  set directories (no `SET`) below the oldest kept set are removed once their
  newest entry is older than one hour (`BackupTarget::newest_entry_age`), so a
  partially deleted set does not stay a `tier verify` finding forever.
- **R10b — `SET` only after every object is confirmed (§4.5.2).** Retention's
  age guard cannot see an upload in flight, so a set directory being filled can
  lose objects to a concurrent deletion. Before putting `SET`, `copy_set`
  re-lists the directory and confirms every object it is about to name is
  present with a matching header length, re-placing any that vanished. A
  delete landing between the last check and the `SET` put remains a residual
  window, bounded by the placement rounds and the retry ladder.
- **`BackupConfig::retention_interval` (§5.1).** The retention pass runs every
  60 s (`DEFAULT_RETENTION_INTERVAL`). This is a field of `BackupConfig` for
  tests to shorten, NOT a `[backup]` key — the loader always takes the default.
- **`[backup]` via `StartOpts` (§5.1).** As specified, and as built:
  `StartupOptions` → `StartOpts::backup: Option<BackupConfig>`, never a
  `NodeConfig` field. The writer thread `uc2-backup` sleeps 100 ms when idle
  and is appended LAST to the node's agents, so `/healthz`'s indices 0–4 are
  unchanged.
- **D11 as built — the tier object (§4.1.1).** CRC-32/IEEE via `crc32fast`
  (now a regular dependency of `uc_node`), over the raw bytes: once per object
  in the header and once per block, so damage is located to one ≤ 1 MiB block.
  A reader refuses a raw total that differs from the header's `raw_len`.
- **R11a(i) — restore points are `(S, reach]` (§6.1, §8.1).** A restore point Q
  is a frame end with `S < Q ≤ reach`: the start set strictly below Q. Q == S
  is refused, because an empty restored journal reopens at durable 0. The
  watermark therefore means "any frame end in `(oldest_set, through]`", not
  `[oldest_set, through]`.
- **R11c — the new-node id check (§8.2).** `--new-node` must check the id
  against the membership the restored node will BOOT with — the artifact at S
  plus every `Membership` frame in `[S, Q)`, the same fold `rederive_config`
  does — not the membership at S alone: an id added in `(S, Q]` would boot as an
  existing member with an empty vote, D9's forbidden case. (Moot in 3a after
  R12a, kept for 3b.) The builder also writes a `RESTORING` marker first and
  removes it last, and holds `instance.lock` for the whole build.
- **R11d — there is no `uc2ctl add` (§8.2).** The verbs are `add-learner` then
  `promote`; the docs and hints name both.
- **R12a — `--new-node` is refused, deferred to 3b (§8.2).** A tier-seeded node
  joining a LIVE cluster is cut to 0 by term-map reconcile (its restored map
  `[(t, S)]` against the leader's `[(t, 0)]` has no alignment →
  `Truncate{to: 0}`), then fail-stops the archive on `PositionPurged`.
  `uc2ctl tier restore --new-node` refuses by name (`new_node_unsupported`,
  pointing at the ordinary snapshot-session join); the fix is node-side
  lineage seeding and is 3b's. Its e2e stays `#[ignore]`d as 3b's acceptance
  test. Whole-cluster restore is unaffected (every node shares `[(t, S)]`).
- **PF-fork, R12b, R12c — the fork's membership (§8.2, §13).**
  `force_single_member` works and the fork leads alone, but its cluster FSM
  still names the old voters. As built the leader appends the kernel's config
  as a `Membership` command at first leadership when the FSM's version is
  older — once the cluster agent has consumed past the kernel record,
  single-in-flight, refused (and given up for the incarnation) unless the FSM
  validates it — which also heals the existing force-single-member inv12
  hole. Until that check has latched, admin membership requests answer retry
  (an earlier reconfig would break the FSM's version chain for good), and the
  check runs under CONSENSUS serving, not client serving, so a jumbo join gate
  pending on a silent member cannot block removing it (R12c). The fork's
  inherited `backups` entry is cosmetic (the purge hold is writer-local) and
  documented: the operator may `uc2ctl tier forget` it (R12b).
- **R7a — `--tier-id` (§6.3).** `uc2ctl tier forget --tier-id <id>` (hex
  `0x…` or decimal) is a flag, not the positional sketched in §6.3.
- **PF3 — `CORPUS` (§8.2).** `uc_ctl → uc_diffreplay` with
  `default-features = false` is acyclic, so export-corpus writes `CORPUS`
  through `CorpusManifest::write` — one writer of the format.
- **PF4 — test scratch.** Tier unit tests use `cluster_agent.rs`'s `tempdir()`
  pattern (`CARGO_TARGET_TMPDIR`, else the target tree), prefix `uc2-tier-`;
  never `/tmp`.
- **PF-cold — the cold start (§13).** The risk was real: a whole-cluster cold
  restart with the log end well past `fsm_lag` beyond applied deadlocked
  (attach waits for join, commit waits for apply) — #95, fixed by PR #96 before
  the capstone (§11.6) ran.
- **R14a — the capstone's start set (§11.6).** A set frozen before a row has
  applied anything (cursor `None`) is refused fatally by the start-set install
  — #98, outside the tier. The capstone takes its first set after 20 acked
  mutations; no assertion was loosened.
