# UC2 snapshot catalog — design

**Status:** design approved in conversation on 2026-10-01; this document is
the written spec for review. **Project 1 of 4** of the storage-service
direction (catalog, node-local lifecycle, backup tier, decision surface);
the other three get their own specs and depend on this one.

## 1. Problem

Nothing in UC lists the snapshot sets and journal spans the cluster holds.
The facts exist, scattered and partial:

- The cluster FSM holds one `SnapshotReport` per row — the **newest** set's
  position and every node's hash of it (`uc_protocol/src/v2/upgrade.rs:78`,
  `uc_node/src/cluster_fsm.rs:126`). It forgets the previous set the moment a
  newer report lands, and it covers only sets a node built itself.
- A node knows only its own disk. Heartbeats carry no snapshot positions
  (`StatusBody` is `contiguous_position` + `receive_window`,
  `uc_protocol/src/v2/datagram.rs:878`). A joiner below the floor does not
  choose a source: the peer it NAKed decides, or redirects it to the one
  learner the leader last addressed a standby instant to (`node.rs:1795`).
- `uc2ctl snapshot show` parses one node's directory names; `uc2ctl backup`
  records one node's state in a `MANIFEST`. Neither sees the cluster.
- Retention is hard-coded: the newest complete set plus pinned origins
  (`prune_snapshots_below`, `node.rs:6763`), and purge below it when
  `[purge]` is on. There is no replicated retention setting.
- **A set becomes the purge floor on file presence alone.**
  `check_set_completeness` (`node.rs:6278`) marks a set complete when its
  last artifact is on disk; the hash verdict is never consulted. A set whose
  rows diverged across nodes can therefore become the floor, and the journal
  below a set no quorum agrees on is deleted.

The consequences: an operator cannot answer "what can this cluster rebuild
from, and from where" without visiting every node; a restarting service
replays the whole journal from 0 rather than from the newest snapshot
(`replay_into` installs only when its gap guard fires); #48 (auto-fetch after
a standby instant) and #66 (rolling application upgrades) both need to know
which node holds which set at which version, and cannot; and the backup tier
(project 3) has nothing to protect because nothing names what exists.

## 2. Scope

**In:**

- A replicated **catalog of snapshot sets** in the cluster FSM: position,
  kind, log time, state, and per row the building version, the agreed hash
  and the verdict, plus the cluster artifact's hash.
- A **replicated retention policy** (`retain_sets`) that drives both snapshot
  pruning and the purge floor.
- A **soft, per-node advertisement** of what each node holds (journal span,
  sets on disk, applied per row, bytes), aggregated by the leader.
- A **query interface** over the two: pure functions every consumer shares.
- The purge floor and every install source become the newest **agreed** set.
- One flag day: `SNAP_REPORT`, `STATUS`, `Settings` and the cluster artifact.

**Out (named so the boundaries are explicit):**

- **The lifecycle rule** that uses the catalog at restart, catch-up, join and
  upgrade ("load the newest agreed set ahead of me, replay from there", when
  a short catch-up should replay instead, source preference among holders).
  Project 2. This spec provides the queries; it does not change
  `replay_into` or `attach`.
- **The backup tier** (long retention off-node, restore from it, the
  "backed up through P" watermark). Project 3. This spec reserves a record
  kind for the watermark and nothing more.
- **The operator / AI surface** (`uc2ctl catalog`, JSON, metrics, a live
  read path that does not wait for the first cluster artifact). Project 4.
  This spec names the live-read requirement (§6.3) and leaves the mechanism.
- Diagnosing *which* node diverged. The per-node hash matrix stays in
  today's per-row `SnapshotReport`; the catalog stores the verdict.
- A snapshot-storage trait (a backend other than the local directory).
  `SnapshotStore` stays a concrete struct.

## 3. Decisions

| # | decision | why |
|---|---|---|
| D1 | **Hybrid**: agreements replicated in the cluster FSM, observations advertised as soft state. | A set's agreed hash and the retention policy cannot drift and must be safe to act on; which node holds what on disk changes constantly and can become false silently (a lost disk). Replicating the second class costs consensus writes per segment rotation and still lags reality. Same split as Kafka tiered storage: segment metadata replicated, broker contents soft. |
| D2 | **Sets, not rows.** The catalog is keyed by set position P; rows are fields of a set. | Every consumer asks about a set at P (install it, purge below it, fetch it). The per-row `SnapshotReport` record stays as the diagnostic matrix. |
| D3 | **Record *commanded*, derived from the `SNAPSHOT` frame itself.** No new record. | A stalled instant then has a durable, cluster-wide name. Every node's cluster agent already sees the frame (it freezes on it), so deriving the entry costs no frame. The cost is that the cluster FSM becomes a function of one non-`CLUSTER` frame type; accepted over an extra leader-appended record per instant. |
| D4 | **Every install source is an agreed set, and the purge floor is bounded by `newest_agreed`.** A diverged set is visible and fetchable, never installed, never the floor. A node's *effective* floor is the newest agreed set **it holds on disk** (§4.4). | Closes the file-presence floor (§1). A set no quorum agrees on is not evidence of anything — and a node must never purge below a set it cannot rebuild from, which with learner-only snapshots (§4.6) is every voter until it fetches. |
| D5 | **Retention is a replicated setting**, `retain_sets ≥ 1`; pinned origins are always kept. | Every node must keep the same sets or the catalog's "exists cluster-wide" is false. `0` is refused at the door: it would retire the floor. |
| D6 | **The soft advertisement is built off the consensus hot path.** | A directory listing per pass would be a hot-loop cost for a value that changes seconds apart. The builder and the pruner update a cached struct; the sender ships it at the status cadence. |
| D7 | **Genesis is empty; a named fallback covers the window** until the first agreed set. | The flag day cannot migrate sets built before the catalog existed without reading every artifact on every node. Today's behaviour (newest complete on disk, newest-only retention) is correct in that window and is what runs. |
| D8 | **No performance bar.** The apply hop is untouched; the consensus pass gains one cached-struct encode at the status cadence. | The 2.11.0–2.12.0 gates showed rate bars under the rig's variance cannot be adjudicated. `STATUS` body size before/after is recorded as a number, not a bar. |

## 4. The catalog (replicated)

### 4.1 State

Added to `ClusterState` (`uc_node/src/cluster_fsm.rs:87`):

```rust
pub struct Catalog {
    /// Oldest first. Bounded by `retain_sets` + pinned origins + commanded
    /// sets not yet complete (see §4.4).
    pub sets: Vec<SetEntry>,
}

pub struct SetEntry {
    pub position: u64,          // P, the artifact tag; the key
    pub kind: SetKind,          // Full | Standby
    pub time_ns: u64,           // the SNAPSHOT frame's log time
    pub state: SetState,        // Commanded | Complete | Retiring
    pub rows: [RowEntry; 8],    // index = row; unset rows are `Unreported`
    pub cluster: RowEntry,      // the cluster artifact, "row 255"
}

pub struct RowEntry {
    pub version: u32,           // the FSM version that built it (packed)
    pub hash: u64,              // the majority hash (0 while unreported)
    pub verdict: Verdict,       // Agreed | Diverged | NoMajority | Unreported
}
```

A set is **agreed** when every *declared* row and the cluster artifact are
`Agreed`. A `Standby` set is agreed on the same rule; its holders are
learners only.

### 4.2 Transitions

Applied by `ClusterFsm::apply` at commit, deterministic, on every node:

| Event | Effect |
|---|---|
| `SNAPSHOT` frame at END position P (D3) | Push `SetEntry { position: P, kind from FLAG_SNAPSHOT_STANDBY, time_ns from the header, state: Commanded, rows: Unreported }`. A frame at a position already present is ignored (ruling P10's "act on the last" already dedups at the agent). |
| `SnapshotReport` (kind 5) for row r at P | Set `rows[r] = { version, hash: majority, verdict }` on the entry at P, computed by today's `verdict()` (`upgrade.rs:170`). A report for a P with no entry (a report that outran its frame cannot happen — the frame commits first — but a report after the entry was dropped can): ignored, as `ReportStale` (59) already refuses at the door. When every declared row and the cluster artifact are reported, `state = Complete`. |
| `SnapshotReport` for "row 255" | The cluster artifact's hash, reported by the same path (§5.1). Fills `cluster`. |
| An entry becomes `Complete` and **agreed** | Retention runs (§4.4). |
| `Settings` with a new `retain_sets` | Retention runs. |
| `UpgradePin` naming origin P | No catalog change; §4.4 reads pins from `membership`/the pin state as today. |

### 4.3 Verdict and "agreed"

`verdict()` is unchanged: the majority hash among the reporters that are
current members, `Agreed` when all reporters match, `Diverged` when a
majority exists and some differ, `NoMajority` otherwise. One reporter is
agreed over one node, as today (`node.rs:13455`). The catalog adds nothing
to the rule; it stores the result.

**The recorded `version` is the version IN FORCE STRICTLY BELOW P, not the
running version at the moment the report applies.** §4.2's reports row
leaves `version` unexplained; it is derived from the row's pin history,
oldest → newest: the EARLIEST pin with `origin >= P` names its `from` (that
pin's install had not taken effect at P); otherwise the LATEST pin's `to`;
otherwise the row's running version (`0` with none recorded). The cluster
row's version is always `0` — its artifact is versioned by the image
layout, not an FSM version. This matters because a pin at origin P commits
*after* P: a report for P can land after the pin, and the running version
at report-apply time would then name the pin's `to` for an artifact the
pin's `from` actually built. The per-row pin history is bounded at four
entries (`MAX_PINS_PER_ROW`), so a set more than four pins back reads the
oldest RETAINED pin's `from`, which may not be the version that built it.

### 4.4 Retention

Runs after an agreed completion and after a settings change. Let `A` be the
agreed sets in position order.

1. If `|A| > retain_sets`, mark the oldest `|A| − retain_sets` agreed sets
   `Retiring`, **except** any whose position is a pinned origin of some row
   (kept until the pin is superseded, exactly as `prune_snapshots_below`'s
   `keep` list does today, `node.rs:6791`).
2. Drop from `sets` every `Retiring` entry older than the oldest kept agreed
   set, and every `Commanded` or non-agreed `Complete` entry older than it.
   (A stalled or diverged set is kept while it is newer than the floor, so
   it stays visible and alertable; once superseded it is history.)
3. The **cluster floor** is `newest_agreed()`: the youngest agreed set. A
   node's **effective floor** is the newest agreed set *it holds complete on
   disk*, which is at or below the cluster floor. The purge driver purges
   below the effective floor only (today it purges below
   `snapshot_set_position`, `node.rs:5287`, which is the same quantity
   without the agreement check), and `prune_snapshots_below`'s argument
   becomes "everything the catalog does not list", not "everything below
   the newest". `newest_agreed()` is a ceiling and a target to fetch, never
   a licence to purge what this node cannot rebuild from.

Raising `retain_sets` retires nothing until the list grows. Lowering it
retires at the next apply. `retain_sets = 0` is refused at the leader's door
with reason 47 (`settings_bounds`), the existing bounds reason.

### 4.5 Genesis and the `Empty` fallback (D7)

A cluster artifact at the new layout starts with `sets = []`. While the
catalog has **no agreed set**, every reader takes the `Empty` branch:

- `newest_agreed()` answers what `snapshot_set_position` answers today.
- The pruner deletes **nothing** (not "newest only": nothing), because the
  catalog cannot say what the newest is.
- `holders()`/`journal_covers()` still work (soft state is independent).

The first instant that completes and agrees seeds the catalog, and the
fallback ends. `Empty` is a named state, exported as a gauge, so the window
is visible rather than implicit.

### 4.6 Learner-only snapshots

The common production shape is that only learners snapshot, so voters
never pause: `snapshot_target = learners` (replicated, `Settings`) or
`uc2ctl snapshot --standby`. The catalog supports it without a special
case:

- The standby set is catalogued with `kind: Standby`; its reports come from
  learners, which are members, so it becomes *agreed* and the cluster has a
  named, hashed record of it — which it does not have today.
- Voters hold no artifact at P. Their effective floor (§4.4) stays at the
  last set they hold, so they purge nothing until a copy lands; a joiner is
  redirected to a holder as today (`node.rs:1795`), and `holders(P)` is the
  piece #48's automatic fetch needs to choose that holder.
- **"I do not hold the set the catalog names" is the normal state of a
  voter, not an error.** Project 2's lifecycle rule must treat it as *fetch
  from a holder, then install*, never as a gap.
- With **one** learner, a set's hash is agreed over one reporter (today's
  rule, `node.rs:13455`) and divergence is undetectable by construction;
  **two** learners is the minimum for the verdict to mean anything. The
  catalog does not enforce a minimum; `uc2ctl` should say it.

## 5. The soft side (advertised)

### 5.1 What a node advertises

`StatusBody` (`datagram.rs:878`) grows from 16 B to carry:

| Field | Source |
|---|---|
| `journal_first` | the cnc `archive_first_base` word (offset 1344) |
| `durable`, `commit` | cnc counters |
| `applied[8]` | cnc service slots' `applied` |
| `sets_held` | positions from the catalog this node holds complete on disk, as a bitmap over the catalog's entries (oldest first) plus the catalog position it was computed against |
| `free_bytes`, `journal_bytes`, `snapshots_bytes` | the filesystem under the instance dir |

The cluster artifact's hash joins the completeness report as "row 255": a
node that completes a set reports `(255, hash of snap-P.ultcluster)` beside
its rows, through `send_snapshot_reports` (`node.rs:6360`). Row 255 is the
`service_id` the snapshot session already uses for the cluster artifact.

### 5.2 How it is built (D6)

A `Holdings` struct cached in the node. Writers: the completeness check
(`check_set_completeness`) when a set lands; the pruner when it deletes;
the archive agent's first-base mirror; a once-per-second filesystem probe for
the byte counts. The consensus pass encodes it into `STATUS` at the existing
status cadence. No directory listing happens on the pass.

### 5.3 How the leader keeps it

A `HashMap<NodeId, (Holdings, last_seen_ns)>`. An entry is **stale** when
`last_seen_ns` is older than the liveness timeout the node already uses for
heartbeats, and stale entries are excluded from every query. The table is
rebuilt within one status cadence after a leader change; it is never
persisted and never replicated.

## 6. The query interface

Pure functions over `(view: &ClusterView, soft: &SoftTable, now_ns)`.
Shared by the node, `uc2ctl` and any future surface, so every consumer gets
the same answer from the same inputs.

| Function | Answer |
|---|---|
| `newest_agreed(at_most: u64) -> Option<P>` | the youngest agreed set ≤ `at_most`; the purge floor is `newest_agreed(u64::MAX)` |
| `agreed_for(row, version) -> Vec<P>` | agreed sets whose `rows[row].version` is on `version`'s line (`same_line`) |
| `holders(P) -> Vec<NodeId>` | non-stale nodes advertising P complete |
| `journal_covers(P, Q) -> Vec<NodeId>` | non-stale nodes with `journal_first ≤ P` and `durable ≥ Q` |
| `stalled(timeout_ns) -> Vec<P>` | `Commanded` sets whose `time_ns + timeout < now` |
| `diverged() -> Vec<(P, row)>` | every `Diverged`/`NoMajority` row of every listed set |
| `retiring() -> Vec<P>` | sets a node may delete |
| `coverage_gaps() -> Vec<(P, Q)>` | spans between consecutive agreed sets that no holder and no journal span covers: history nobody can rebuild |

### 6.1 Two consequences

- **The purge floor is bounded by `newest_agreed`** and, per node, by what
  it holds (§4.4). A diverged set never moves the floor; the previous
  agreed set stays until a newer agreed one exists.
- **Install sources are agreed sets only.** Project 2's rule and the pinned
  install both draw from `agreed_for`/`newest_agreed`.

### 6.2 What it does not answer

Which holder to prefer (project 2 / #48), and anything about off-node
copies (project 3, through its own record).

### 6.3 The live-read requirement

`uc2ctl` reads the cluster artifact, which exists only after the first
instant (`docs/BACKLOG.md`'s recorded gap). The catalog is what an operator
asks about *before* and *during* an instant, so project 4 must give the
queries a live path (an admin op answered over the admin band, or a
cnc-mapped copy of the view). This spec does not choose; it records that
the artifact alone is not enough.

## 7. Wire and layout changes — one flag day

| Surface | Change |
|---|---|
| `SNAP_REPORT` (pairwise 26) / `SnapshotReport` (CLUSTER kind 5) | row `255` admitted, carrying the cluster artifact's hash |
| `STATUS` body (kind 4) | 16 B → the §5.1 fields; a `0.11.0` leader drops a `0.10.0` follower's 16 B body (counted, `status_refused` — corrected by R30; there is no wire version to refuse it by name) |
| `Settings` (CLUSTER kind 3) | `+ retain_sets: u16`, refused at the door when `0` (47) |
| cluster artifact (`snapshots/cluster/*.ultcluster`) | `ClusterState` gains `Catalog`; `CLUSTER_IMAGE_VERSION` 3 → 4 with a trailing catalog blob. The image codec already reads every older version with the missing trailing blobs empty (`cluster_image.rs:186`), so a v3 artifact loads with an **empty catalog** — the `Empty` state of §4.5 — and **no wipe is needed**. A v4 artifact is refused by a v3 reader, as every newer layout is today |
| `CLUSTER` kind `7` | **reserved** for project 3's backup watermark; refused as unknown until then |

Wire `0.10.0` → `0.11.0`, cnc unchanged (the catalog is not on the page;
`Empty` and the gauges go through `/metrics`). Stop every node, start every
node, as for every flag day; nothing on disk is cleared. `read_status_body`
today accepts any body of at least 16 B (`datagram.rs:892`), so the new body
gets its own length floor. (Corrected by R30: there is no wire-version
word on node↔node datagrams, so a `0.10.0` peer is not refused by the wire
version. A `0.11.0` leader DROPS a `0.10.0` follower's 16 B `STATUS`
(counted, `status_refused`), so that follower's flow-control window never
opens and replication to it stalls; a `0.10.0` leader accepts a 144 B body
and ignores the tail — mixing is unsound in both directions and the
procedure forbids it.)

## 8. Error handling

| Case | Behaviour |
|---|---|
| **Diverged set** | `Complete` with the row `Diverged`. Never `newest_agreed`, never the floor, never installed; kept and visible while newer than the oldest kept agreed set (§4.4). `Uc2SnapshotSetDiverged` keys on `diverged()` instead of comparing per-node gauges. |
| **Stalled set** | Stays `Commanded`; retention ignores it; the next instant proceeds; `stalled()` names it after the timeout; dropped once below the oldest kept agreed set (§4.4). |
| **Stale or wrong soft state** | Nothing is decided on an advertisement alone. A fetch from a holder that no longer has P fails by name and the chooser tries the next; a short journal falls into today's gap guard. A node whose status stops arriving leaves `holders()`/`journal_covers()` after the liveness timeout. |
| **Flag-day window** | `Empty` (§4.5): the pre-catalog cluster artifact loads with an empty catalog; today's behaviour, and the pruner deletes nothing, until the first agreed set. |
| **Retention lowered** | Oldest agreed sets retire at the next apply, pinned origins excepted. **Raised**: nothing retires until the list grows. **`0`**: refused, 47. |
| **Voter without the agreed set** (learner-only snapshots) | Not an error: its effective floor does not move, a restart of its service fetches from `holders()` (project 2), a joiner is redirected. |
| **A node lies** | Out of the threat model (a compromised member), as the fan-out key residual is. |

## 9. Migration

- **Operators**: the flag-day procedure in `docs/how-to/upgrade-a-cluster.md`
  gains a `0.11.0` section; nothing on disk is cleared. `node.toml`'s
  `[purge]` stays (it is *whether* to purge; `retain_sets` is *what to
  keep*).
- **Code**: `snapshot_set_position` (`node.rs:2759`) stays as the node's own
  newest-complete reading and feeds the `Empty` fallback; the purge driver
  switches to the view. `prune_snapshots_below` keeps its shape and takes
  the catalog's listed positions as its keep-set.
- **Metrics**: `uc2_catalog_sets` (listed), `uc2_catalog_agreed_position`
  (= the floor), `uc2_catalog_empty` (the fallback gauge),
  `uc2_catalog_stalled`, `uc2_catalog_diverged`. `uc2_snapshot_set_position`
  keeps its meaning (this node's newest complete on disk).
- **Alerts**: `Uc2SnapshotSetDiverged` re-sourced to `uc2_catalog_diverged`;
  `Uc2SnapshotStalled` can key on `uc2_catalog_stalled` (kept as is if its
  current source is simpler).

## 10. Proof

1. **Unit**, `cluster_fsm.rs`: every §4.2 transition, table-driven over
   scripted frame sequences: commanded from a `SNAPSHOT` frame; complete and
   per-row verdict from reports; row 255; retention on overflow with a pinned
   origin kept; `Empty`; `retain_sets` lowered, raised, `0` refused. Every §6
   function against hand-built views and soft tables, including: a diverged
   set is never `newest_agreed`; `holders()` excludes stale nodes.
2. **Codecs**: round-trip and refuse-by-name for the extended `SNAP_REPORT`,
   `STATUS` and `Settings`, in the shape of `upgrade.rs`'s tests.
3. **Sim** (`uc_sim`): `uc_sim` does not run `ClusterFsm`; it models the
   cluster FSM's membership and snapshot sets abstractly (`world.rs:2023`,
   inv11 `check_set_alignment`). The new invariant (inv13) is written the same
   way: the catalog each node would derive from the `SNAPSHOT` frames and
   set completions at or below its commit is identical across nodes at equal
   commit, and no node's purge floor exceeds the newest agreed set it holds.
   Seeded fuzz over partitions and crashes with it on. Byte-equality of the
   real FSM is a unit test in `cluster_fsm.rs` (two instances, one frame
   sequence, equal images).
4. **Fuzz**: `uc_node_cluster_artifact` covers the new layout; a new target
   decodes the extended `STATUS` body.
5. **End to end**, `uc_node/tests`:
   - divergence injected (two FSMs on one row with different hashes): the set
     is complete-diverged, the floor does not move, a joiner is served the
     previous agreed set, the alert source reads diverged;
   - stalled (a row without the capability bit): commanded-not-complete,
     `stalled()` after the timeout, the next instant completes;
   - `retain_sets = 2` with purge on: the third set retires the first, the
     pinned origin survives, the journal floor follows `newest_agreed`;
   - the flag-day window: pre-existing sets on disk and an empty catalog;
     the fallback serves a joiner and deletes nothing; the first instant
     seeds the catalog and the fallback ends;
   - soft state: a killed node leaves `holders()` after the timeout and a
     fetch routes to the surviving holder;
   - learner-only: `snapshot_target = learners`, purge on; the standby set
     becomes agreed, `holders()` names the learner only, the voters' journals
     are **not** purged, a `uc2ctl snapshot fetch` lands the set on a voter
     and only then does that voter's floor move.
6. **Regression, unchanged**: `lin_v2`, `lin_partition_v2`, hard-crash,
   `pin_verify`, Elle. None should change: the catalog adds no commit-path
   semantics.
7. **Performance** (D8): no bar. A review check that `Holdings` is written
   off the pass; the `STATUS` body size before/after recorded as a number.

## 11. Risks

- **The FSM reads a non-`CLUSTER` frame (D3).** The cluster agent already
  acts on `SNAPSHOT` frames (it freezes); the FSM now also records them. The
  sim invariant (§10.3) is what proves this stays deterministic.
- **`STATUS` grows** from 16 B to roughly 100 B at the status cadence. Not
  on the replication data path; recorded, not barred.
- **Retention with `retain_sets > 1` costs disk** (the journal is kept down
  to the oldest kept set). Until the backup tier exists the practical value
  on a node is small; the default is `1`, today's behaviour.
- **`Empty` lasts until an instant agrees.** On a cluster with a row that
  cannot complete, the fallback runs indefinitely: visible through the gauge
  and `stalled()`, and no worse than today.

## 12. Open questions for the next projects

- Project 2: the restart rule's threshold for "far enough behind to install
  rather than replay", and holder preference (locality, learner-first).
- Project 3: the watermark record's shape (kind 7 reserved here), and
  whether a node may retire below the catalog's `retain_sets` once the tier
  confirms a set.
- Project 4: the live-read mechanism (§6.3) and the JSON shape an AI reads.

#### Errata (plan review, 2026-10-04)

- **`SetState::Retiring` is removed.** §4.4 step 2 drops a retiring entry in
  the same apply that marks it, so the state was never observable and
  `retiring()` would always answer nothing. A retired set is simply removed
  from the list; `SetState` is `Commanded | Complete`. "What may this node
  delete" is a node-local reading — every artifact on disk whose position
  the catalog does not list — so `retiring()` leaves the query interface and
  becomes the pruner's own rule (§4.4 step 3 already says "everything the
  catalog does not list").
- **`declared_mask` is derived from `running`.** The FSM does not hold
  `[services] names`; since #33 every declared row has a committed running
  version before it serves, so "every declared row" in `is_agreed` means
  every row with a `running` entry.
- **`retain_sets = 0` is refused at `apply` unconditionally** (47); genesis
  seeds `1`. A `0` reaches the state only through an installed v1–v3 image,
  where `retain_sets()` reads it as `1`.
- **The soft table's staleness timeout** is `3 × election_timeout_max_ns`
  (900 ms by default): no per-peer heartbeat timestamp existed to reuse.
- **`uc2_catalog_stalled`** counts commanded-not-complete sets with no
  timeout; the timeout judgement is the `stalled()` query's.

#### Errata (as built, 2026-10-04)

- **§4.4 retention keeps a pinned origin BESIDE `retain_sets`, never
  counted toward it — as built (R21).** §4.4 step 1 reads as excepting a
  pinned origin from the retiring sweep; through Task 13 `retire()` instead
  counted every agreed entry — pinned or not — against `retain_sets` and
  only refused to pick a pinned entry as the *victim* (the literal reading
  of R3), so a pin shrank effective retention by one for as long as it
  stood. R21 restates the rule as "keep the newest `retain_sets` agreed
  sets, PLUS every pinned origin", and the final fix wave builds it:
  `retire()` counts only agreed entries that are NOT pinned origins; while
  that count exceeds `retain_sets` it removes the oldest unpinned agreed
  entry that is not the newest agreed set; a pinned origin is never counted
  and never the victim; the "drop everything older than the oldest
  remaining agreed set, pins excepted" step is unchanged. Covered by
  `a_pinned_origin_does_not_count_toward_retain_sets` and Task 12 test 3
  (`[p1, p2 (pinned), p3]` keeps all three; `p4` retires `p1` and `p3`
  survives on disk).
- **§4.2's reports row does not say which version is recorded (ruling
  R4).** "Set `rows[r] = { version, hash: majority, verdict }`" names the
  field but not its value. As built, `version` is the version IN FORCE
  STRICTLY BELOW P — `ClusterState::version_at(row, p)`
  (`uc_node/src/cluster_fsm.rs:298-310`): the earliest pin with `origin >=
  p` names its `from`; else the latest pin's `to`; else the row's running
  version; the cluster row always records `0`. Not the running version at
  report-apply time, because a pin at origin P commits *after* P — a report
  for P landing after the pin would otherwise record the pin's `to` for an
  artifact its `from` actually built. Bounded by the per-row pin history
  (`MAX_PINS_PER_ROW = 4`): a set more than four pins back reads the oldest
  retained pin's `from`, which may not be the version that built it. Added
  to §4.3's body as its own paragraph.
- **§4.4 names no upper bound on `retain_sets`; the door enforces one,
  `MAX_RETAIN_SETS = 48` (rulings R8, R29).** D5 says only `retain_sets ≥
  1`. The catalog's list rides inside the cluster IMAGE, not a `CLUSTER`
  frame, so its own retention bound is `MAX_CATALOG_SETS = 64` on the list
  itself. R8 refused `retain_sets` above `MAX_CATALOG_SETS − 8 = 56`; once
  R21 kept pinned origins IN ADDITION to `retain_sets`, 56 retained + up to
  8 pins + commanded headroom exceeded 64 and `cap_catalog` could evict a
  retained agreed set, so R29 lowers it to `MAX_CATALOG_SETS − 16 = 48` —
  eight entries for pins (one per row) and eight for commanded instants in
  flight, pinned by a compile-time assert in `uc_protocol::v2::catalog`. `cap_catalog()` is
  the backstop this bound is meant to make unreachable: it evicts the
  oldest non-agreed entry first, then the oldest agreed entry that is
  neither a pinned origin nor the newest agreed set, and never the newest
  entry outright (evicting it would drop every later instant on arrival and
  freeze the floor).
- **§4.1/§4.3's "agreed" is FROZEN at completion, not recomputed against the
  live declared mask (ruling R9).** As drafted, `is_agreed` reads as a
  live predicate — "every *currently* declared row and the cluster artifact
  are `Agreed`" — which would let a row added to a running cluster
  retroactively un-agree every earlier set (no artifact for the new row
  exists at those positions). As built, a `Complete` entry's agreement is
  judged once, against the rows it had to cover *at the moment it turned
  `Complete`*; `SetEntry::is_agreed` takes no mask argument, and a row
  declared later is `Unreported` in an older entry without un-agreeing it.
  Completeness itself (`Commanded` → `Complete`) is still judged against the
  live running-derived mask at report time — only *agreement*, once
  reached, is frozen. Otherwise adding a row would empty the catalog on the
  spot and let retention drop a pinned origin out from under its pin.
  **This SUPERSEDES ruling R7.** R7 had accepted, as a limitation, that "a
  row added later un-agrees earlier sets and the catalog reads `Empty`
  until the next instant agrees" — true of a live-mask `is_agreed`, which is
  what R7 was ruled against before R9 replaced it. Under frozen agreement
  that no longer happens: a row added later reconstructs from its own
  genesis above the earlier sets, which stay agreed and never drop out from
  under retention or a pin.
- **§4.4/§7's "`retain_sets = 0` is refused at the door" elides apply's own
  handling of a replicated `0` (ruling R10).** The flag day clears nothing
  on disk, so a `0` can reach `apply` two ways that are not an operator's
  request: a v1/v2 `Settings` record replayed from the journal (where `0`
  always meant "unset"), or the image's own `ClusterState::retain_sets`
  before anything has ever set it. `validate_replicated` (the replicated
  half every node runs identically) refuses only `retain_sets >
  MAX_RETAIN_SETS`; a replicated `0` is **normalised at `apply`** to the
  *current* `retain_sets` (retention is simply unchanged by that record).
  Only the leader's pre-append door (`validate`) refuses an operator's own
  `0` unconditionally, with `47`, so no operator can ever stage one.
  **SUPERSEDED by R24 below** — "keep the current value" made the result
  depend on how a node reached its state, which is the Critical defect R24
  fixes.
- **§4.4 step 3's "holds complete on disk" is a cache read, not a file
  check, outside one seam (ruling R12).** As drafted this reads as "ask the
  filesystem". As built, a node's effective-floor candidate is read from
  `holdings_held` — the same cache `Holdings.sets_held` advertises,
  maintained by the writers §5.2 already names — and a filesystem check
  (`holds_set`) runs only once, at the uncommon view-change edge where a
  newly-listed agreed set has no cache entry yet (see ruling R18 below);
  never on a steady pass.
- **§4.5's `Empty` fallback keeps today's pruning rule, not "nothing"
  (ruling R13).** As drafted, the pruner "deletes nothing" while `Empty`.
  As built, it keeps TODAY's rule instead — delete below this node's own
  newest complete set, pinned origins kept — because "nothing" protected
  nothing real (every set §4.5 meant to protect already sits below the
  node's own newest complete set, which was always prunable) while
  breaking every existing purge test, none of which has an agreed set to
  offer.
- **§5.2's once-per-second filesystem probe runs on the ARCHIVE agent, not
  wherever "off the pass" was read to mean (ruling R14).** The spec's "off
  the consensus hot path" (D6) is a correct constraint but names no agent;
  an early draft of the probe landed inside the node's own per-pass
  `do_work`, which is itself the consensus pass and contradicts D6 outright
  for a directory walk. As built through Task 13 it ran on the **archive
  agent**'s duty cycle — the agent that already owns disk I/O (journal
  recording). The reasoning recorded here then, "so a slow filesystem
  stalls archiving, never commit", was **false**: `durable` gates commit
  through the report ceiling, so the archive agent IS the commit path.
  **SUPERSEDED by R27 below** — the probe now runs on its own thread.
- **§5.1's `catalog_position` stamp is a content hash, not a position
  (ruling R16).** The spec's `sets_held` row names "the catalog position it
  was computed against" without saying what that quantity is; the natural
  reading is a walk cursor or list length. As built it is
  `catalog_version_of(sets)`, an FNV-1a-64 **content hash** over the set
  list's wire encoding, computed in `ClusterView::publish` on the cluster
  agent and stored as `ClusterView::catalog_version`. `Holdings.catalog_position`
  keeps the wire field name but carries this hash — two nodes holding the
  same catalog agree on it regardless of where their own walk cursor
  stands, which a position-based stamp would not guarantee.
- **§5.1's `journal_bytes` is an O(1) estimate, not a filesystem reading
  (ruling R17).** "The filesystem under the instance dir" reads as a walk.
  As built it is computed from positions alone —
  `(durable − archive_first_base)` rounded up to whole `segment_size_bytes`,
  plus one more segment when `preallocate_segments` is on — because the
  journal has block sequence numbers, not a segment count, and a per-file
  walk would scale with the journal itself (tens of thousands of files at
  1 TB) on the same agent that advances `durable`.
- **§5.2's holders list implies the disk-presence seed runs continuously;
  it runs once, at boot (ruling R18).** The writers §5.2 names (the
  completeness check, the pruner, the archive's first-base mirror, the
  probe) are all steady-state edges that never touch the filesystem to
  decide "held". The one writer that DOES check the filesystem,
  `holds_set` (ruling R12), runs **only** at boot/recovery, gated by a
  `holdings_seeded` flag the first `refresh_from_view` consumes; after
  that, a newly-listed set becomes "held" only through this node's own
  completion edge, so a view change on the consensus pass performs no
  filesystem call.
- **§10.3's inv13 covers clause (a) only, not both halves (ruling R19).**
  The proof plan asks for both "the catalog each node would derive … is
  identical across nodes at equal commit" (a) and "no node's purge floor
  exceeds the newest agreed set it holds" (b) in one invariant. `uc_sim`
  has no purge floor to check (b) against — the sim models the cluster
  FSM's catalog abstractly and never runs a node's actual pruner — so inv13
  as shipped sweeps clause (a) alone (`World::check_catalog_determinism`);
  clause (b)'s coverage is Task 8's node unit tests and Task 12's end-to-end
  tests, not the sim.
- **§4.1's "every declared row" presumes the node and the FSM already
  agree on what "declared" means (ruling R1).** As built, both read the
  same mask — derived from the FSM's `running` (bit r ⇔ `running[r]` is
  set), never from a node's local `services.ids()` — because a row between
  attach and genesis would otherwise make the node and the FSM disagree on
  which sets are complete or agreed. `prune_snapshots_below_in`
  (`uc_node/src/node.rs:7160-7165`) states the equivalence explicitly: "the
  same set `check_set_completeness` reads… on a real node (which always
  declares `[services] names`) the two are equal." Largely moot after R9
  freezes agreement at completion rather than recomputing it live, but
  stated here because it is still what "declared" means at the point a set
  turns `Complete`.
- **§7's "`install_snapshot` refuses a mis-tagged artifact" is one of
  several codec-level refusals the catalog adds, not stated (ruling R5).**
  `install_snapshot` also refuses a cluster image whose catalog blob is not
  strictly increasing by position — `"cluster image: catalog out of
  order"` (`uc_node/src/cluster_fsm.rs`, the `install_snapshot` leaf) —
  same class as the image's other structural checks (CRC, framing, pin and
  report list bounds), since `retire` and every reader rely on the list
  staying ordered oldest-first.
- **§4.2's `SNAPSHOT`-frame row describes the live arm only; the journal
  catch-up path shares it (ruling R11).** `on_snapshot_frame` is recorded
  on EVERY path the cluster agent walks a `SNAPSHOT` frame on — the live
  arm and `replay_from_journal`'s catch-up walk alike
  (`uc_node/src/cluster_agent.rs`) — for every `SNAPSHOT` frame at or below
  the walk's target, independent of whether THIS node's own freeze
  decision (`last_actionable_instant`'s pick) acts on that particular
  frame. The catalog is a function of the committed prefix and must not
  depend on which path a node took to reach it; `on_snapshot_frame` is
  idempotent on `end`, so recording the same frame twice — once from the
  live arm, again if a later overrun replays the same ground through the
  journal — costs nothing.
- **§5.3's leader-side soft table is silent on whether the leader counts
  itself (ruling R15).** A node never sends itself a `STATUS` datagram, so
  a literal reading of "built from inbound `STATUS` datagrams" would leave
  the leader absent from its own `holders()`. As built, `Node::soft_table()`
  stamps and records its own cached `Holdings` under its own node id before
  returning the table — the leader is as much a legitimate holder and fetch
  source as any follower, and omitting it would make a single-voter
  cluster, or a quorum that happens to include the leader, read as having
  no holder for a set it plainly has.
- **`retain_sets` had no operator surface in the plan this spec was
  reviewed against (ruling R6).** D5/§4.4 assume an operator can set
  `retain_sets`, but neither `node.toml`'s `[settings]` nor `uc2ctl
  settings apply`'s TOML carried the key — an omission Task 12 test 3 and
  this spec's own `uc2ctl` documentation would otherwise have had nothing
  to point at. Added as Task 8b (after Task 8): `[settings] retain_sets`
  for genesis, the `retain_sets` key in the `settings apply` TOML, and
  `settings show`'s rendering of it.
- **R24 — a v1/v2 `Settings` record reads `retain_sets = 1`, not `0`
  (supersedes R10's "keep current"; final fix wave, Critical C1).** As built
  through Task 13, a v1/v2 record decoded as `0` and `apply` kept the
  current value; a node that installed a pre-flag-day v3 image (settings
  tail a v2 record) then held `0` in its state forever while a node that
  walked from genesis held `1` — every later cluster image differed, so the
  cluster row read diverged at every instant. As built now:
  `decode_settings` maps a v1/v2 record to `retain_sets = 1` (the
  newest-only retention such a cluster actually ran); `install_snapshot`
  normalises any `0` to `1`; `apply` normalises a `0` (reachable only on a
  crafted v3 record — the door refuses an operator's) to `1` **from the
  record alone**, never from the current state, so a replayed old record
  wins like every other replayed field. Regression:
  `a_v3_image_with_a_v2_settings_tail_converges_with_genesis` (byte-equal
  images, both `1`). Upgrade step: commit one `uc2ctl settings apply` after
  the flag day so every node's settings come from the log.
- **R25 — an entry turning `Complete` drops every OLDER `Commanded` entry
  that is not a pinned origin (final fix wave, Important I1).** §4.4 step 2
  drops a stalled entry only once an AGREED set passes it. A node whose
  newest v3 artifact was older than another's replays pre-flag-day
  `SNAPSHOT` frames into `Commanded` entries no node ever reports on, so the
  two catalogs differed for as long as retention took (~64 instants at the
  cap). As built: `put_report`, on the edge where its entry becomes
  `Complete` (agreed or not), drops older non-pinned `Commanded` entries.
  Trade-off: a stalled instant stays visible until the NEXT set
  **completes**, not until the next set agrees. Upgrade step: take one full
  instant right before stopping and confirm every node's newest
  `snap-*.ultcluster` is at the same P.
- **R26 — `Empty` means "no `Complete` entry", not "no agreed entry"
  (amends §4.5 and R13; final fix wave, Important I2).** As built through
  Task 13, `effective_floor_in` treated a catalog with nothing AGREED as
  `Empty` and fell back to `own` — so a cluster whose every complete set
  diverged purged below a diverged set, breaking D4 outside the flag-day
  window. As built now: `Empty` ⇔ no listed entry is `Complete`
  (`ClusterState::catalog_empty`, published as
  `ClusterView::catalog_has_complete`); a catalog whose complete sets all
  diverged is not `Empty`, its agreed search finds nothing, and the
  candidate is `0` — nothing moves. `uc2_catalog_agreed_position == 0`
  still means "nothing agreed"; `uc2_catalog_empty` now reads
  `catalog_has_complete` and is `1` iff no entry is `Complete`. Cost: journal
  growth (never loss) on a cluster whose every set diverges.
- **R27 — the `Holdings` probe runs on its own `uc2-holdings` thread
  (supersedes R14; final fix wave, Important I3).** R14 put the 1 Hz
  `statvfs` + `snapshots/` walk on the archive agent on the premise that a
  slow filesystem would stall archiving, never commit. The premise is wrong:
  `durable` gates commit through the report ceiling, so a directory walk
  there couples commit to the snapshots directory's metadata locks. As
  built: `HoldingsProbe` is driven by a sixth thread, `uc2-holdings` — an
  `AgentRunner` with `IdleStrategy::Sleep(50 ms)` whose work closure calls
  `maybe_probe` (still at most once per second) and never reports progress,
  so it sleeps between wakes rather than spinning. It has no consensus role,
  is not one of the five `/healthz` agents, and is stopped and joined with
  the node like the others. It reads two atomics (`durable`,
  `archive_first_base`) and writes the `Holdings` cell's three byte fields.
- **R28 — the cluster artifact's hash is published at freeze, not re-read
  on the pass (final fix wave, M1).** As built through Task 13,
  `send_snapshot_reports` did an `fs::read` of `snap-P.ultcluster` on the
  consensus pass, once per completed set, to hash row 255. As built: the
  `uc2-cluster` agent hashes the image it is writing (`freeze_and_write`),
  the image it installs (`install_from`) and, once at boot, the recovered
  artifact, and publishes `(position, hash)` through
  `cluster_agent::ClusterArtifactHash` — a two-word seqlock, published
  BEFORE `cluster_snapshot_pos` moves — and the report edge reads
  `hash_at(P)`, skipping row 255 when the word names another position.
- **R29 — `MAX_RETAIN_SETS = MAX_CATALOG_SETS − 16 = 48` (final fix wave,
  M6).** See the R8 bullet above: after R21, 56 retained sets plus eight
  pins plus commanded headroom exceeded the 64-entry list and
  `cap_catalog` would evict a retained agreed set. Cost: an operator who
  wanted 49–56 retained sets (none exist before the backup tier).
- **R30 — mixed-version `STATUS` is DROPPED and counted, not "refused by
  name" (final fix wave, M3).** §7 and the release docs said a `0.10.0`
  peer is "refused by name / by the wire version". No wire-version word
  rides node↔node datagrams: `read_status_body` refuses a body without the
  layout-2 word, and the receiver silently dropped it. As built: the
  receiver counts every such body (`FollowerStats::statuses_refused`,
  exported as `uc2_status_refused_total`) and names the source on stderr
  as `status_refused` at most once a minute per source (the
  `note_cleartext_peer` throttle shape; `uc_net` has no `uc_obs`
  dependency). The truthful statement, now in §7 and every doc that made
  the claim: a `0.11.0` leader drops a `0.10.0` follower's 16 B `STATUS`, so
  that follower's flow-control window never opens and replication to it
  stalls; a `0.10.0` leader accepts a 144 B body and ignores the tail —
  mixing is unsound both ways and the procedure forbids it.
- **The pin door does not require an AGREED origin (open for project 2).**
  `uc2ctl upgrade pin` (admin op 10) still checks only that the origin is
  this node's newest COMPLETE set, as it did before the catalog; it does not
  consult the catalog's verdict. A pin can therefore name a set that later
  reads `Diverged`, and retention keeps it (a pinned origin is never
  retired, R21) — but the floor never moves onto it (D4, R26), so nothing is
  lost. Requiring `is_agreed()` at the door is a project-2 question (the
  chooser decides what a pinned attach may install from), not built here.

#### Addendum (2026-10-05): lock discipline of the published view — RETRACTED

**Retracted the same day (ruling R37).** The addendum's finding rested on
one maximum sample per run. A five-run probe at the same commit (Task 15,
`~/scratch/t15/`) measured the `ClusterView` mutex's longest acquisition
wait at 1.07–1.51 µs while the bad runs (21 and 13 fault ticks, 5 and 3
abandoned instants) still occurred, and the four design items below, once
built, left the tick distribution unchanged (5/8/11/7/13). The lock is not
the carrier; the commit that built this addendum (5a7a8c2) is reverted.
The actual mechanism is the report-aggregation liveness gap recorded in
the erratum that follows this addendum. The text below is kept as the
record of a wrong diagnosis and binds nothing.

**Finding (retracted).** `lin_v2::two_fsm_bounded` is deterministic on `main` (5 fault
ticks) and non-deterministic on this branch (5–31 ticks, WGL stack overflow in
3/5 runs). Bisected to the row-255 report × the catalog-aware floor. Probes
show the mechanism is **contention on the `ClusterView` mutex**: acquisition
waits of ~35 ms (good runs) to ~100 ms (bad runs) with ~3 µs medians — a lock
convoy of short holders preempted on an oversubscribed box. The branch added
takers on both sides: the consensus pass clones `ClusterViewInner` for the
floor every 100 ms while the floor is stuck (D4/R12), and the cluster agent
publishes on every `SNAPSHOT` frame (D3) and appends one more report per
instant (row 255). A delayed agent delays a restarted row's attach (the
readiness gate waits on the agent's walk), the row misses its freeze at P, the
next command supersedes the instant, and the fault loop's history grows.
Disabling either taker alone restores `main`'s behaviour; the pruner is not
involved; no filesystem I/O or snapshot install is involved at HEAD.

**Rule.** The consensus pass takes **no lock in steady state**, and no holder
of the view's lock does more than a pointer copy while holding it.

**Design.**
1. `ClusterView.inner` becomes `Mutex<Arc<ClusterViewInner>>`. `publish`
   builds the new `ClusterViewInner` **outside** the lock, then swaps the
   `Arc` under it; the atomics are stored after the swap, `position` last, as
   today. `snapshot_inner()` returns `Arc<ClusterViewInner>` (an `Arc` clone
   under the lock — nanoseconds, no allocation); callers read through it.
2. The floor path caches its last computation on
   `(own, catalog_version, persisted_floor)` and recomputes only when one of
   the three moves; a stuck floor costs the pass one compare per throttle
   tick and no view read.
3. Every `snapshot_inner()` call reachable from the consensus pass or from
   the leader's per-report path (`on_snap_report` → `report_position_for`) is
   audited: served by an atomic where one exists, otherwise by the `Arc`
   (never a clone of the contents).
4. The cluster agent publishes only when the state it publishes changed
   (a `SNAPSHOT` frame changes the catalog, so it still publishes; an
   applied `CLUSTER` command that left the state equal does not).

**Proof.** `publish`'s mutex-wait max measured in `two_fsm_bounded` before and
after (the probe in `~/scratch/lin-m2-report.md`): after must be < 1 ms;
`two_fsm_bounded` 5/5 at 5 ticks with 0 `snapshot_instant_abandoned`; the
full proof stack. **No bar** on anything else (D8).

#### Erratum (2026-10-05, ruling R37): the leader's report aggregation must stay live under a lagging voter

**What the spec assumed.** §4.2 takes the `SnapshotReport` record as given
and §4.5 makes the purge floor "the newest AGREED set this node holds". Both
inherit, unexamined, the aggregation rule plan B3 built for live
nondeterminism detection (lifecycle spec §6.5.2, `Consensus::on_snap_report`):
the leader keeps **one** pending instant per row; a report for a NEWER
instant replaces that entry outright and restarts the 5 s
`SNAP_REPORT_TIMEOUT_NS` clock; a report for an OLDER instant than the one
pending is dropped; and both leader exits clear the map. Before the catalog
that rule only cost divergence coverage: a node's floor moved on its own
complete set. The catalog made the floor depend on an agreed record, so the
rule's liveness became the floor's liveness.

**What breaks.** With instants arriving faster than the timeout, a voter that
is ONE instant behind the others starves agreement for every instant: the
others' reports for P are discarded when they report P+1, the laggard's
report for P is then "older than pending" and dropped, and the clock never
runs 5 s because every instant restarts it. `lin_v2::two_fsm_lockstep`
(one instant per 1.2 s fault tick, one node always restarting) shows it in
full — 83 instants, 33 leader changes, 501 reports sent, 147 superseded,
0 timed out, 3 appended, floor 0 for the whole budget — and
`two_fsm_bounded`'s 5-to-21-tick spread is the same mechanism: each run ends
at its FIRST agreed instant, and how many instants that takes is luck. In
production the same shape is a slow voter under a short
`snapshot_interval_bytes`: purge freezes cluster-wide and
`uc2_catalog_stalled` is the only symptom. This is a liveness regression
against `main`, found by the proof stack, and it is fixed on this branch
before merge.

**Rule.** A report is evidence about one `(row, instant)`; a newer instant
never discards an older one's evidence. The leader keeps pending evidence
keyed by `(row, position)`, every entry with its own clock, and appends an
entry when **every voter has reported it OR its own clock reaches
`SNAP_REPORT_TIMEOUT_NS`**, whichever is first — ascending by position
within a row, rows ascending, one append per pass (single-in-flight is
unchanged). `verdict()` and `is_agreed()` are unchanged: two matching voters
of three appended by timeout are `Agreed`, so a lagging voter delays the
floor by at most the timeout instead of forever.

**Mechanics.**
1. `pending_snapshot_reports` becomes a map from row to an ordered list of
   `PendingSnapshotReport { position, first_seen_ns, hashes }`, ascending
   by position. A report for a position at or below the row's COMMITTED
   report position is still dropped (unchanged, `held_report_position`). Any
   other position gets or joins its own entry; a report never moves between
   entries and never restarts another entry's clock.
2. When an entry for `(row, P')` is appended, the row's pending entries
   BELOW `P'` are removed and counted under the existing
   `snapshot_report_superseded` event: once `P'` commits they would be
   refused as stale at the door (§4.2 / refusal 59). That is now the ONLY
   place the event fires. A lost agreement opportunity exists here (P could
   still have completed by timeout while P' waits on another row) and is
   accepted: ascending order means P' appends before P only when P is not
   ready, and §4.2's completion drop (R25) removes P's `Commanded` entry
   once P' completes anyway.
3. Bound: at most `MAX_CATALOG_SETS` (64) pending instants per row; a new
   instant beyond the bound evicts the row's OLDEST pending entry, counted
   under the same event. Memory is ≤ 64 × 9 rows × ~120 B; the append rate
   is unchanged from a healthy cluster's (one record per row per instant).
4. Both leader exits still clear the map — a leader's evidence is a leader's
   to place — and the evidence is RE-OFFERED instead of lost: a node
   re-sends the reports for its NEWEST complete set (`snapshot_set_position`,
   when non-zero) each time it learns a DIFFERENT leader (the cnc
   `leader_hint` changes to another node id, or this node becomes leader, in
   which case it feeds itself in-process as `send_snapshot_reports` already
   does for a leader). Once per leader change; through the existing
   `send_snapshot_reports(p)`, whose `snapshot_pos == p` guard skips any row
   that has since frozen a newer instant.
5. `uc2_snapshot_reports_timed_out_total` keeps its meaning (an append with
   fewer voters than required); no new metric, no wire or layout change; the
   `SnapshotReport` record is unchanged. Not a flag day.

**Proof (binding for Task 16).** (a) A unit test on a 3-voter leader that
replays the starvation: instants every 1 s of `pass_mono_ns`, voter C always
one instant late — under the old rule no instant appends; under the new rule
every instant appends `by: all_voters`. (b) Per-entry clocks: P then P+1 a
second later; P appends by timeout at 5 s, P+1 not before its own 5 s. (c)
The appended-P' drop of older pending and the 64-entry eviction, each with
the superseded event. (d) The re-send on a changed leader hint (and the
in-process self-report on becoming leader). (e) `lin_v2::two_fsm_bounded`
5 runs: the test's own tick count in every run equals `main`'s (5) within
±1, with the per-run count of `snapshot_report_appended` recorded; (f)
`lin_v2::two_fsm_lockstep` 3/3 pass; (g) the all-15 `lin_v2` suite once; the
usual gates. No performance bar (D8).

**Amended (ruling R38, 2026-10-05, after Task 16 round 1).** Round 1 built
mechanics 1–5 as written and removed the starvation (the headline test went
from 0 appends to 7; `two_fsm_lockstep` 3/3), but proof (e) missed its bar
(ticks 5/5/7/5/12): under one fault per 1.2 s most instants complete on two
of three voters and must wait out the timeout, the leader changes every 3–5
ticks, and each change cleared the map and restarted every clock. Two
mechanics change, both from replicated data, no wire or layout change:
- **The clock is the log's, not the leader's.** A pending `(row, P)` times out
  when the leader's current LOG time is at or past `SetEntry(P).time_ns +
  SNAP_REPORT_TIMEOUT_NS` — the instant's own stamp, read from the committed
  catalog — so a leader change does not restart it. Only a `P` the catalog
  does not list yet (its frame not yet committed on this node) falls back to
  `first_seen_ns` in the pass's monotonic clock. The two clocks are never
  compared with each other. Cost accepted: a voter whose freeze takes longer
  than the fastest node's freeze plus 5 s is left out of the record a little
  earlier than before; the floor is unaffected.
- **The re-offer covers every held set the catalog still lists as not
  `Complete`,** above the row's committed report position, not only the
  newest. As built, the cnc slots name only the newest instant's hashes, so a
  node keeps a bounded (64-set) in-memory cache of each completed set's
  `(row, hash)` list from its completion edge and re-offers older sets from
  it; a node restarted after completing a set has no cache for it and
  re-offers only the set its slots still name. The newest set is re-offered
  even when the node's own catalog view lags its completion edge. Once per
  distinct `(leader, term)`; a candidate does not re-offer (its hint is
  stale); the normal send does not mark the leader as offered, so a set that
  completes in the pass of a leader change still gets the full re-offer, at
  the cost of at most one duplicate report per row per new leader.

Result: proof (e) ticks 5/6/6/5/6, bar met; `two_fsm_lockstep` 3/3; the
all-15 `lin_v2` green (Task 16 round 2). Open at the task review: a new
leader appends its OWN report for an instant already older than the timeout
at once, before followers' re-offers arrive, so more records name one or two
voters than before (9 of 30 appends in (e)). §4.3's "one reporter is agreed"
makes such a record move the floor — no less safe than `main`'s node-local
floor, but thinner divergence coverage under leader churn; a short grace on
a fresh leader is under ruling.

**Settled (rulings R39–R41, 2026-10-05, Task 16 fix round).** (R39) The leader
drops a report whose position is above its OWN log extent — an honest
reporter applied to P, so P is committed, so the leader holds those bytes —
counted as `snapshot_report_dropped reason=above_extent`, once a minute per
reporter; this turns a forged position (crypto off) into a no-op instead of a
permanent wedge of that row's agreement. A forged report below the extent at
an unlisted position still appends by fallback timeout and is ignored by the
catalog (§4.2, no entry); that is the crypto-off posture
`docs/security/attack-surface.md` now names. (R40) The completed-set hash
cache a node re-offers from is seeded from the artifacts it holds by the
`uc2-holdings` thread — a row file's payload after the 24-byte `ULTSNAP2`
envelope hashes to the slot's `artifact_hash`; row 255's bare image to the
published word — one probe after a set is first seen complete, never from the
consensus pass. A restarted node therefore re-offers its full evidence, and
the one-voter records of round 2 are gone; the records that still name two of
three voters are those where the third voter never built the set. (R41) Such
a two-of-three matching record IS agreed and moves the floor: §4.3 already
accepts one reporter, the purging node must additionally hold the set, and
the absent voter has nothing a third report could have compared. A grace
period on a fresh leader was analysed and declined: it would have changed
none of the thin records.
