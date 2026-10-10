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
reserved [u8;7] ‖ raw_len u64 ‖ crc32c(raw bytes) u32 ‖ reserved u32 ‖
from u64 ‖ to u64                      (chunk: the range; artifact: both 0)
block := stored_len u32 ‖ raw_len u32 ‖ stored bytes      (raw_len ≤ 1 MiB)
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
pub trait BackupTarget: Send {
    /// Write `src` (exactly `len` bytes) at `path` iff absent: under .tmp/, fsync, rename,
    /// fsync the directory. Present already → `Exists` (caller compares).
    fn put_if_absent(&self, path: &TierPath, src: &mut dyn Read, len: u64) -> io::Result<Put>;
    fn list(&self, dir: &TierPath) -> io::Result<Vec<String>>;
    fn open(&self, path: &TierPath) -> io::Result<Box<dyn Read + Send>>;
    fn delete(&self, path: &TierPath) -> io::Result<()>;   // absent → Ok
}
```

`FsTarget` is the one implementation. Rename-into-place is the atomicity
primitive; a crash leaves only `.tmp/` entries, which a writer removes when
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

Node-local (`node.toml`), not replicated: paths differ per host. The writer
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
