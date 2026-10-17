# The snapshot catalog, explained

**Status:** written 2026-10-16 for the `2.14.0` cut, where the catalog ships as part of
wire `0.11.0`. Spec:
[`2026-10-01-uc2-snapshot-catalog-design.md`](../superpowers/specs/2026-10-01-uc2-snapshot-catalog-design.md).
Read its two errata blocks and erratum R37 (with its R38–R42 amendments)
before the body: a good part of what follows is errata, not spec text, and
the 2026-10-05 lock-discipline addendum is retracted. This note gives the
reasoning in plain language. The operator surfaces are in
[`uc2ctl catalog`](../reference/uc2ctl.md#catalog) and
[How to keep the journal from growing without bound](../how-to/bound-journal-growth.md).

The catalog is project 1 of the four-part storage service. The
[snapshot lifecycle](uc2-snapshot-lifecycle-explained.md),
the [backup tier](uc2-backup-tier-explained.md) and the
[cluster view](../reference/cluster-view.md) are built on top of it.

## The problem in one sentence

A node purged its journal below any snapshot set whose files were all on its
disk, even when the nodes disagreed about what was in that set, and nothing
in the cluster recorded which sets existed or where they were.

## What was missing

Since `2.11.0` a snapshot is a **set**: at one coordinated instant P, every
declared row and the cluster FSM freeze, and a node holds "the set at P" once
every `snap-<P>.ultsnap` and `snapshots/cluster/snap-<P>.ultcluster` is on
its disk ([The cluster FSM, explained § Instants](uc2-cluster-fsm-explained.md#instants-one-position-one-set)).
`2.13.0` added a determinism check on top of that: every node hashes its
artifacts and reports the hashes, and the cluster FSM records them per row.

Three gaps remained.

1. **The floor ignored the hashes.** A node marked a set complete when its
   last file landed and moved its snapshot floor there, which is also the
   purge floor. If the nodes' hashes for that set disagreed (one replica's
   state machine had diverged), the node still deleted the journal below a
   set no one could vouch for. The journal is the only thing that can rebuild
   a state the snapshot got wrong.
2. **The cluster forgot.** The FSM kept only the newest report per row. A
   newer set replaced the record of the older one, and nothing named a set
   that was commanded but never completed.
3. **Nobody could see the whole cluster.** A node knew its own disk. Its
   heartbeat carried no snapshot positions. "What can this cluster rebuild
   from, and from which node?" meant logging into every host. The features
   planned next (start a restarting service from a snapshot, fetch sets
   automatically, back them up off-node) all need that answer.

## The idea: agreements replicated, holdings advertised

The catalog splits what the cluster knows into two kinds of fact, because
the two behave differently.

- **Agreements** are facts that must not drift: "the set at P exists, row 2
  of it hashed to H on every node that built it, and it was built by version
  1.4". These go into the **cluster FSM**, applied at commit on every node,
  so every node reads the same answer and a node that restarts or joins
  learns it with the rest of the cluster state.
- **Holdings** are facts that change all the time and can become false
  without warning: "node 3 has set P on disk, its journal starts at Q, it has
  40 GiB free". A disk can be lost and nobody writes a log record for that.
  Replicating these would cost a consensus write for every segment rotation
  and would still lag reality. So each node **advertises** them on its
  heartbeat, and the leader keeps the latest copy per node in memory.

Kafka's tiered storage makes the same split: segment metadata is
replicated, while what each broker holds is soft state.

## The replicated half: one entry per set

The catalog is a list in the cluster FSM's state, oldest first, with one
entry per coordinated instant. Each entry has:

- the position P (the key), whether it was a **full** or a **standby**
  (learners-only) instant, and the instant's log time;
- a state, `Commanded` or `Complete`;
- per row, and for the cluster artifact as "row 255": the version that built
  it, the majority hash, the artifact's size, and a **verdict**: `Agreed`,
  `Diverged`, `NoMajority` or `Unreported`.

Where the entries come from:

- **A `SNAPSHOT` frame creates one.** Every node's cluster agent already
  reads that frame, because it freezes on it, so it also adds a `Commanded`
  entry. No new record is needed, and a commanded instant that never
  completes now has a durable name instead of vanishing. This makes the
  cluster FSM depend on one frame type that is not `CLUSTER`. It records the
  frame on every path it walks, live or journal catch-up, so the catalog
  depends only on the committed log and not on how a node read it.
- **`SnapshotReport` records fill it in.** The leader collects each node's
  hash for each row (pairwise `SNAP_REPORT`) and appends one
  `SnapshotReport` (`CLUSTER` kind 5) per row per instant. Applying it
  computes the verdict. Once every row and the cluster artifact have a
  verdict, the entry becomes `Complete`.
- **The cluster artifact is checked too.** `0.11.0` admits row `255` in
  reports: each node hashes its own `snap-<P>.ultcluster`. Before this, the
  cluster FSM's own artifact had no determinism check.

**What "agreed" means.** A row is `Agreed` when every node that reported it
gave the same hash. `Diverged` means a majority exists but some node
differs. `NoMajority` means no hash has a majority. A set is **agreed** when
it is `Complete`, the cluster artifact is `Agreed`, and no row reads
`Diverged` or `NoMajority`. One dissenting node is therefore enough to keep a
set from ever agreeing. That is deliberate: a disagreement means some
replica's state is wrong, and the platform cannot tell which.

**Agreement is decided once (ruling R9).** It is judged when the entry turns
`Complete`, against the rows declared at that moment. A row added to the
cluster later shows as `Unreported` in older entries but does not un-agree
them. If it did, adding a row would empty the catalog and could let
retention drop a pinned origin.

**The version is the one that built the artifact (R4).** A pin at origin P
commits *after* P, so a report for P can apply after the pin. The catalog
therefore records the version in force strictly below P, worked out from the
row's pin history, not the version running when the report applies. The
history keeps four pins per row, so a set more than four pins back may name
the wrong builder.

### Waiting for reports, and not waiting forever (R37, R38)

The leader appends a row's record when **every voter has reported** or when
**five seconds of log time** have passed since the instant
(`SNAP_REPORT_TIMEOUT_NS`), whichever comes first. A two-of-three matching
record appended by timeout is agreed (R41).

The first build kept one pending instant per row and let a newer instant
replace it. Under instants that came faster than the timeout, a voter one
instant behind starved agreement forever: the others' reports for P were
thrown away when they reported P+1, and the laggard's late report for P was
dropped as old. The floor stayed at 0. `lin_v2` found it before merge. The
fix keys evidence by `(row, P)`, each with its own clock. That clock is the
instant's own committed log time, so a leader change does not restart it. On
every leader change, each node also re-offers the reports for sets the
catalog still lists as not `Complete`.

### A copy is not a witness (R42)

A report means "I built this and got hash H". A set that reached a node
through a snapshot session (a below-floor install, or `uc2ctl snapshot
fetch`) was built elsewhere, and reporting it would count one builder twice.
The receiver writes a zero-byte marker `snapshots/cluster/snap-<P>.foreign`
before any of the session's files become visible, and a node never reports
a marked set. The marker is removed with the set, and backup and restore
carry it. One case is not covered: an operator who copies a set in by hand
without the marker. That node will attest a set it did not build, and no
code can tell.

## The advertised half: `Holdings` on `STATUS`

Every node's `STATUS` heartbeat grows from 16 B to 144 B and carries a
`Holdings` record:

- its journal span (`journal_first`, `durable`) and `commit`;
- `applied` for each of the eight rows;
- `free_bytes`, `journal_bytes` and `snapshots_bytes`;
- `sets_held`, a bitmap over the catalog's entries saying which sets this
  node holds complete on disk, stamped with a hash of the catalog it was
  computed against (`catalog_position`). A bitmap read against a different
  catalog would point at the wrong sets, so it is ignored.

None of this is computed on the consensus pass. The snapshot writers and the
pruner keep a cached struct up to date, and the byte counts come from a 1 Hz
probe on its own `uc2-holdings` thread (R27). The first build put that probe
on the archive agent, assuming a slow filesystem would only delay archiving.
That assumption was wrong: `durable` gates commit, so a directory walk there
would couple commit to the snapshot directory's metadata locks.
`journal_bytes` is an estimate from positions, not a file walk (R17).

The leader keeps each node's latest `Holdings` in a soft table. It counts
itself too, although it never sends itself a `STATUS` (R15). An entry is
stale after `3 × election_timeout_max` (900 ms by default) and drops out of
every answer. The table is never persisted or replicated, and a new leader
rebuilds it within one status cadence.

The queries every consumer shares are pure functions over the two halves
(`uc_node::catalog::CatalogQuery`): `newest_agreed`, `agreed_for(row,
version)`, `holders(P)`, `journal_covers(P, Q)`, `stalled`, `diverged` and
`coverage_gaps`. The last one names spans between agreed sets that no live
holder and no live journal can rebuild.

## What changes for a node: the floor follows agreement

A node's **effective floor**, which sets both its snapshot floor and its
journal purge floor, is now **the newest agreed set this node holds complete
on disk**. Two consequences:

- **A diverged set is never the floor.** The previous agreed set stays the
  floor, and the journal above it stays, until a newer set agrees. A joiner
  below the floor is served the set at the floor, so it too gets an agreed
  set.
- **The catalog's newest agreed set is a target, not permission.** A node
  that does not hold that set does not move its floor. This is the normal
  state of a voter on a learner-only cluster (`snapshot_target = learners`),
  where only learners build sets. Without the catalog such a voter could
  never safely purge. In `2.14.0` the lifecycle's auto-fetch pulls each new
  agreed set to every node in the background, and the floor then moves
  ([learner-only clusters](../how-to/bound-journal-growth.md#learner-only-clusters-voters-fetch-and-purge-by-default)).

The pruner deletes only artifacts below the floor that the catalog no longer
lists. A stalled or diverged set therefore stays on disk while it is listed,
so it can still be seen and alerted on.

### Retention: `retain_sets`

How many agreed sets the cluster keeps is now a replicated setting,
`retain_sets` (`Settings` v3), because every node has to keep the same sets
for "the cluster holds P" to mean anything. The default is `1`, which is the
retention UC always had. It is set with
[`uc2ctl settings apply`](../reference/uc2ctl.md#settings-apply), or with
`[settings] retain_sets` in `node.toml` for genesis only. `0` (which would
retire the floor) and anything above `48` are refused at the door with
`47 settings_bounds`.

The rule (R21) keeps the newest `retain_sets` agreed sets **plus** every
pinned origin, and a pinned origin never counts toward the number. Since the
lifecycle, an origin is kept only until its pin completes. Entries older
than the oldest kept agreed set are dropped, whatever their state. When a
set turns `Complete`, every older `Commanded` entry is dropped (R25). The
list has a hard cap of 64 entries (`MAX_CATALOG_SETS`), and the bound of 48
leaves room under it for eight pins and eight instants in flight (R29).

A snapshot is a start point only together with the journal after it, so
with `retain_sets` above `1` the purge stops at the **oldest** retained agreed
set the node holds, not at the effective floor. The floor itself still
follows the newest set. A retained set whose journal was already purged
before it was retained (after raising `retain_sets`, say) is skipped rather
than waited on. Each extra set therefore costs one snapshot interval of
journal as well as its own bytes, and buys a place to start a node, or a
diff-replay corpus, further back.

### The `Empty` window

A cluster at genesis, or one whose cluster artifact predates the catalog,
starts with an empty list. The `0.11.0` flag day migrates nothing, because
that would mean reading every artifact on every node. While **no listed
entry is `Complete`** the catalog is `Empty` and every reader falls back to
the old rule: the floor is the node's own newest complete set, and the
pruner prunes the way it used to.

`Empty` means "nothing has completed", not "nothing has agreed" (R26). A
catalog whose every complete set diverged is not `Empty`: its floor is `0`
and nothing moves. The cost is that the journal grows on such a cluster;
nothing is lost. `uc2_catalog_empty` reads `1` for the whole window, so you
can see when the cluster has actually started keeping the record, as
opposed to merely running the new binary.

## What an operator sees

- **`uc2ctl catalog`** (and the catalog part of `uc2ctl cluster`, or
  `GET /cluster`): the sets with their per-row verdicts and holders, the
  floor, `retain_sets`, this node's holdings, the per-member table, and the
  storage `attention` codes (`catalog_empty`, `set_stalled`,
  `set_diverged`, ...). It reads live, from before the first artifact
  exists. See [`uc2ctl catalog`](../reference/uc2ctl.md#catalog) and
  [the cluster view](../reference/cluster-view.md).
- **Metrics**: `uc2_catalog_sets`, `uc2_catalog_agreed_position` (the
  cluster floor, correct on a learner-only cluster where
  `uc2_snapshot_set_position` is not), `uc2_catalog_empty`,
  `uc2_catalog_stalled` and `uc2_catalog_diverged`. `Uc2SnapshotSetDiverged`
  now fires on `uc2_catalog_diverged > 0` instead of comparing per-node
  gauges. For which node differs, `uc2ctl upgrade show` still prints the
  per-node hash matrix. See
  [Monitor a cluster § The snapshot families](../how-to/monitor-a-cluster.md#the-snapshot-families-2110).
- **`uc2_status_refused_total`**: `STATUS` bodies dropped because they had
  the old 16 B layout. Nonzero means an old node is in the cluster (see
  below).
- **Pins** now need an agreed origin. The lifecycle added refusal
  `61 pin_origin_not_agreed`, which closed the gap the catalog's own errata
  had left open.

## Upgrading: one flag day, nothing wiped

The full procedure is
[Upgrade a cluster § 0.11.0](../how-to/upgrade-a-cluster.md#wire-change-after-0100-the-snapshot-catalog-0110).
In short:

- **Stop every node before starting any.** Node↔node datagrams carry no
  version word, so an old peer cannot be refused by name. A `0.11.0` leader
  drops an old follower's 16 B `STATUS` (counted in
  `uc2_status_refused_total`), and replication to that follower stalls. An
  old peer applies the row-255 report and the wider `Settings` record as
  undecodable, and its cluster FSM diverges silently (R30).
- **Take one full instant just before the window**, and check that every
  node's newest `snap-*.ultcluster` is at the same P. A node with an older
  artifact replays `SNAPSHOT` frames the others do not, so the nodes'
  catalogs differ until the next set completes.
- **Commit one `uc2ctl settings apply` afterwards**, so that every node takes
  `retain_sets` from the log rather than from an old image or `node.toml`.
  An old `Settings` record reads `retain_sets = 1` (R24).
- **Nothing on disk is cleared.** The cluster image gains a catalog blob, and
  older images load with an empty catalog. `cnc` does not change for the
  catalog. The wire changes are in
  [the wire protocol reference](../reference/wire-protocol.md).

## Limits: what it does not do

- **No minimum number of builders.** A standby set built by one learner is
  "agreed" over one reporter, so divergence cannot be detected by
  construction. Two learners is the minimum for the verdict to mean
  anything. Nothing enforces it; auto-fetch logs
  `snapshot_fetch_single_reporter`.
- **It does not say which node is wrong.** The catalog stores a verdict. The
  per-node hashes stay in the per-row report that `uc2ctl upgrade show`
  prints.
- **Holdings are hints.** Nothing is decided on an advertisement alone: a
  fetch from a holder that no longer has the set fails by name, and the next
  holder is tried.
- **A lying member is out of scope**, as for the rest of the threat model.
  With crypto off, a forged report above the leader's log extent is dropped
  (R39). A forged report at an unlisted position is ignored by the catalog.
- **One faulty replica stops the floor.** That is the point of the design,
  but it means a cluster whose sets keep diverging keeps its journal until a
  set agrees. Watch `uc2_catalog_diverged`.
- **It changes no restart behaviour by itself.** The catalog provides the
  answers. The rule that uses them to start a restarting service from the
  newest agreed set, and to fetch sets automatically, is the snapshot
  lifecycle, which ships in the same release.

## Where to go next

- [`uc2ctl catalog`](../reference/uc2ctl.md#catalog) and
  [`uc2ctl cluster`](../reference/uc2ctl.md#cluster): the readouts.
- [How to keep the journal from growing without bound](../how-to/bound-journal-growth.md):
  purge and retention in practice.
- [Upgrade a cluster § 0.11.0](../how-to/upgrade-a-cluster.md#wire-change-after-0100-the-snapshot-catalog-0110):
  the flag-day procedure.
- [The wire protocol](../reference/wire-protocol.md): the `STATUS`,
  `SNAP_REPORT` and `Settings` layouts.
- [The cluster FSM, explained § Instants](uc2-cluster-fsm-explained.md#instants-one-position-one-set):
  what a set is.
- [The backup tier, explained § The tier as a holder](uc2-backup-tier-explained.md#the-tier-as-a-holder):
  the next project, which copies agreed sets off-node.
