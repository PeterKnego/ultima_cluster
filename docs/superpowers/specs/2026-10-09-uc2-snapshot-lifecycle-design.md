# UC v2 — snapshot lifecycle: start from the newest agreed set (storage-service project 2)

**Status:** design approved in conversation 2026-10-09; this document is the
written spec for review. Project 2 of the four-project storage-service
direction (1 catalog — shipped on `main`, 2 this, 3 backup tier, 4 operator
surface). It builds on
[the snapshot catalog](2026-10-01-uc2-snapshot-catalog-design.md) — read that
spec's errata and its R37–R42 amendments first — and closes the "Out" item
that spec named for project 2 (§2) and its §12 open question.

## 1. Problem

The catalog (project 1) knows which snapshot sets exist, which are **agreed**
(every reporting node's hash matches), how big they are not yet, and which
nodes hold them. Nothing uses that to decide where a row **starts**:

- A service that attaches or restarts resumes at its state machine's own
  `last_applied` — `0` for an in-memory state machine — and **replays the
  whole journal**. It installs a snapshot only when the journal has a hole
  (`uc_service::replay` gap guard) or when an upgrade pin forces one
  (`uc_service::attach`, plan B2).
- On a **learner-only** cluster (`snapshot_target = learners`, or
  `uc2ctl snapshot --standby` — catalog spec §4.6, "the common production
  shape") voters never hold a set. Their purge floor only moves on a set they
  hold, so their journals grow without bound until an operator runs
  `uc2ctl snapshot fetch` **on each voter** (#48).
- `uc2ctl upgrade pin` accepts any origin. A pin forces every instance of the
  row to install that exact artifact through a one-way door; nothing checks
  that the cluster ever agreed on it.

Rolling application upgrades (#66) need the first point fixed: a node
restarted with a new binary needs a known-good starting set, not a full
replay of history.

## 2. Scope

**In:**

- A per-row **start set**, computed by the node and published on the cnc
  page: the newest agreed set this node holds.
- The **start rule** in the service: install the start set when it moves the
  row forward, then replay only the tail.
- **Auto-fetch**: a node fetches the newest agreed set it does not hold from a
  holder, in the background (absorbs #48). A replicated switch,
  `snapshot.auto_fetch`, default on.
- **Artifact sizes in the catalog** and a **free-space check** before a fetch,
  with an early warning when the newest set would not fit.
- The **pin rule**: a pin's origin must be an agreed set.

**Out:**

- **On-demand fetch at start** (a restarting service waiting for a remote set)
  and a **purge floor based on K remote holders**. Considered and deferred
  (D3); recorded for Phase 3 (§14).
- The **backup tier** as a holder or source. Project 3.
- **Widening the version rule** so a binary can start from a set another
  version built. That belongs to #66's feature-level design (D5).
- A snapshot cadence by time, an application API to request an instant, and
  a policy for occasional all-nodes "audit" instants. Separate small items.
- The operator surface (`uc2ctl catalog`, JSON). Project 4.

## 3. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Start from the newest eligible set whenever it moves the row forward** — no byte threshold, no cost model. | The most predictable restart time; a threshold or cost model optimises a cost that is small either way and adds a second code path. |
| D2 | **Eligible = agreed** (catalog verdict), held complete on local disk, at or below `min(commit, durable)` — the node publishes the newest such set (§4.1); the **service** additionally requires the set's building version to equal its own (D5, §5). An **empty catalog** (no Complete entry, catalog spec R26) keeps today's behaviour. | A set nobody verified is never a starting point; replay is always available as the safe path. Held sets count regardless of how they arrived (built, fetched, session-installed); R42's foreign mark governs reporting, not starting. |
| D3 | **Every node keeps its own copy by default** (background auto-fetch); `snapshot.auto_fetch = false` turns it off. Service start **never waits for the network**. The purge rule is unchanged: a node purges only below an agreed set it holds. | The maintainer's call (2026-10-09): real snapshots are small, so per-instant copies are cheap and restarts stay local. The alternative — copies only on producers, on-demand fetch at start, a purge floor over K remote holders — was costed (§15) and **deferred to Phase 3**, where the backup tier changes the availability argument. |
| D4 | The node computes the start set and **publishes it in the row's cnc slot** (approach A). | The catalog and the disk are both on the node; reading the catalog from the newest cluster artifact (approach B) is one instant stale by construction — the artifact at P was frozen before P's agreement existed. |
| D5 | **Keep today's version rule.** A set built by another version is eligible only as this row's pinned origin. | Widening it is #66's feature-level design; deciding "compatible" here would guess that design. |
| D6 | **A pin's origin must be agreed** (refusal 61), except on an empty catalog. | A pin is the most consequential start point and a one-way door; an unverified or diverged origin would spread a possibly bad state to every node. |
| D7 | **Sizes are catalogued**, and a node **never starts a fetch that would not fit** with headroom; an alert warns before any download. | UC fail-stops a node on a full disk (M11); a snapshot download must never be what fills it. |

## 4. The start set (node)

### 4.1 Eligibility

For row `r`, the start set is the newest catalog entry `e` such that:

1. `e.is_agreed()` (the catalog's frozen verdict: Complete, cluster row
   Agreed, every reported row Agreed);
2. `e.rows[r].verdict == Agreed` — a row not reported in that set is **not**
   eligible for it;
3. this node holds `e` complete on disk (the same `holdings_held` the
   effective floor and the STATUS holdings bits use);
4. `e.position <= min(commit, durable)` on this node.

If no entry qualifies — including an empty catalog — the start set is `0`.

### 4.2 Publication

Two new words on the row's **`snapshot_pos` line** of the cnc service slot
(cnc 3.4, unreleased — folded into it):

| offset in slot | word | writer | readers |
|---|---|---|---|
| `+256` (exists) | `snapshot_pos` | service snapshot builder, once per instant | node consensus agent (every pass), `uc2ctl status`, `/metrics` |
| `+264` | `start_set_pos` (u64, 0 = none) | node consensus agent, on change | service apply thread, at attach and overrun recovery |
| `+272` | `start_set_version` (low 32 = packed version that built row `r`'s artifact) | same | same |

Both writers are rare (once per instant / per agreement), so the line stays
uncontended; the per-frame apply path never reads it. The pair is written
`version`, then `pos` (Release), and read `pos`, `version`, `pos` again —
the same coherent-pair pattern `send_snapshot_reports` uses on the slot.

### 4.3 Recompute

The publisher runs on the consensus agent and recomputes only when the
catalog's content stamp (`catalog_version`) or this node's held-set list
changes, or when `min(commit, durable)` passes the position of a newer agreed
held set. A steady pass costs one compare.

## 5. The start rule (service)

Runs at **attach** and in the **overrun recovery** path (`replay_into`, before
the journal scan):

1. A **pinned row** takes its pin path, unchanged, with priority.
2. Otherwise, if `start_set_pos > resume` (where the row would otherwise
   resume) and `start_set_version == S::VERSION`: install the artifact at
   `start_set_pos` through the existing `SnapshotStore` / `ULTSNAP2` checks,
   resume the follower at `start_set_pos`, replay the tail.
3. Otherwise, today's behaviour: resume from the state machine's own position,
   replay, install only on a journal gap.
4. If the install fails (artifact missing or unreadable), log once by name and
   fall back to 3. Replay is always safe; a journal that cannot cover the gap
   meets today's gap handling and fail-stop.

A durable state machine already at or past the start set is never rewound
(rule 2's strict `>`).

## 6. Auto-fetch (node)

When `snapshot.auto_fetch` is on (default), on every node including learners:

- **Trigger:** the catalog's newest agreed set `N` is not held here, and
  `N <= durable` on this node (a set ahead of the local log waits, as
  `start_fetch` already requires).
- **Only the newest:** when a newer set becomes agreed, the pending target
  moves to it; older unheld sets are never fetched.
- **Holder:** from `holders(N)` (live soft-table entries), learners first,
  then lowest node id, never self.
- **Stagger:** the first attempt for a given `N` waits `node_id × 250 ms`.
- **Retry:** a refusal or the 60 s fetch timeout (`FETCH_TIMEOUT_NS`) moves to
  the next holder, backing off from 1 s, doubling to 30 s.
- **Mechanism:** the existing store-only fetch (`start_fetch`,
  `PendingFetch`, `IntakeMode::StoreOnly`), one at a time per node; the
  completed set enters holdings through the existing completeness path, which
  moves the node's effective floor and therefore its purge.
- **Space check:** §7.3, before every attempt.
- **Visibility:** the existing `snapshot_fetch` audit record with
  `actor = "auto"`; a new counter `uc2_snapshot_auto_fetch_total{outcome}`
  (`ok`, `refused`, `timeout`, `no_space`, `no_holder`).
- **Setting:** `Settings` record **v4** adds `auto_fetch: bool`, default
  `true`; v1–v3 records decode as `true`; `[settings] auto_fetch` seeds
  genesis; `uc2ctl settings apply` changes it.

With the switch **off** on a learner-only cluster, voters hold nothing and
never purge. That is the documented trade of the switch.

## 7. Sizes and free space

### 7.1 Reports carry sizes

- `SNAP_REPORT` datagram (kind 26) body: 24 B → **32 B**, adding the
  artifact's byte size (`size u64 @24`).
- `SnapshotReport` record (`CLUSTER` kind 5): entries become
  `(node_id u32, hash u64, size u64)`, still strictly increasing by node id.
- Both change inside the unreleased wire 0.11.0.

### 7.2 The catalog stores them

- `RowEntry` gains `size: u64` (the size reported with the **majority** hash;
  identical hashes imply identical sizes). `ROW_ENTRY_LEN` 13 → 21,
  `SET_ENTRY_LEN` 135 → 207.
- A set's total size is the sum of its rows' and its cluster artifact's sizes.
- `size = 0` means **unknown** (a set catalogued before sizes existed).
- Cluster image **v5** carries the new layout; v1–v4 images decode with every
  size `0`. No wipe.

### 7.3 The fetch check

Before every auto-fetch attempt, using the free-space figure the `uc2-holdings`
probe already measures once a second (`preflight::free_disk_bytes`):

```
free_bytes >= total_size + max(total_size / 4, 1 GiB)
```

If not, the attempt is skipped (`outcome = "no_space"`) and an obs event
`snapshot_fetch_skipped_no_space` names the set, its size and the free bytes,
once per set. A set of **unknown** size (`0`) is fetched without the check, as
today, with a log line saying the size is unknown. The headroom is a fixed
default, not a setting.

### 7.4 The early warning

- Gauge `uc2_snapshot_newest_agreed_bytes`: the newest agreed set's total
  size (0 when unknown or none).
- Alert `Uc2SnapshotWontFit`: the newest agreed set would fail §7.3's check on
  this node. Fires on every node, including learners that build rather than
  fetch, and on nodes with the switch off — it warns before any download.

## 8. The pin rule

`uc2ctl upgrade pin` (admin op 10) is refused with a new code **61
`pin_origin_not_agreed`** when the origin is not an agreed catalog entry. The
message tells the operator the set may still be collecting reports (up to
about 5 s after the instant) and to retry. On an **empty catalog** the pin is
allowed as today, with a log line saying agreement could not be checked.
Every existing pin rule (52–60) is unchanged.

## 9. Wire, page and image changes — all inside the unreleased flag day

`main` carries wire `0.11.0`, cnc `3.4` and cluster image v4, none released.
This project changes, inside those:

| surface | change |
|---|---|
| `SNAP_REPORT` datagram (kind 26) | body 24 → 32 B (size) |
| `CLUSTER` kind 5 `SnapshotReport` | entries gain `size u64` |
| `Settings` record | v4: `auto_fetch` |
| cluster image | v5: catalog row entries gain `size`; v1–v4 decode with size 0 |
| cnc service slot | `start_set_pos @+264`, `start_set_version @+272` (cnc 3.4) |
| admin refusals | 61 `pin_origin_not_agreed` |

Operators upgrading from `2.13.0` already stop every node for 0.11.0; nothing
here adds a step. Dev clusters built from `main` between the catalog merge and
this change must stop every node too (a v4-era report datagram is 24 B and is
refused by length, like any mixed flag day).

## 10. Failure handling

| situation | behaviour |
|---|---|
| Start set pruned or deleted between publish and install | install fails by name; fall back to replay |
| Start set built by another version, row unpinned | replay; one log line naming both versions |
| State machine already at or past the start set | no install |
| Old node, new service | the words read 0; replay as today |
| New node, old service | the words are ignored |
| Holder disappears mid-fetch | 60 s timeout; next holder with backoff |
| No live holder | `no_holder`; retry at the 30 s ceiling; the node is safe, it just does not purge |
| Set ahead of this node's log | wait; never fetch above `durable` |
| Many instants in a burst | one fetch per node at a time, chasing the newest |
| Not enough space | `no_space`, obs event, alert; no download |
| Disk fills anyway (another writer) | today's ENOSPC fail-stop |
| Pin origin not agreed yet | refusal 61; wait for that set to agree, or take a new instant and pin that (a diverged set never agrees) |
| Empty catalog | start rule and pin door keep today's behaviour |

## 11. Proof

**Unit tests, each watched failing first:**

- Publisher eligibility table: agreed and held, not held, above
  `min(commit, durable)`, row unreported in the set, diverged set, empty
  catalog, recompute only on a stamp or holdings change.
- Start-rule matrix: pinned, ahead, behind or equal, version mismatch, install
  failure falling back, overrun recovery jumping forward.
- Auto-fetch: trigger, holder order, stagger, refusal and timeout moving on
  with backoff, chase-newest, switch off, waiting above durable, the space
  check exactly at its threshold, unknown size.
- Codecs: `SNAP_REPORT` 32 B, report record with sizes, `RowEntry` 21 B,
  image v5 and v4-decode-with-zero-sizes, `Settings` v4 and v1–v3 decode.
- Majority-size rule; pin door 61 and its empty-catalog escape.

**End-to-end (`uc_node/tests/catalog.rs`):**

1. Learner-only cluster, switch on: after a standby instant every voter
   fetches and holds the set and purges below it.
2. Same, switch off: voters hold nothing and never purge.
3. A voter's in-memory service restarted after an instant starts from the
   local set: `applied` jumps to the set's position rather than climbing from 0.
4. A pin naming a not-yet-agreed origin is refused with 61, then accepted.
5. A node whose free space is below the check skips the fetch, raises the
   event, and its gauge reports the set's size.

**Proof stack:** workspace, `lin_v2`, `lin_partition_v2`, the hard-crash tests
(whose restarts now take the start-set path), pin-verify, catalog and learner
e2e, clippy and the 1.89 MSRV gate, fmt, doc links, **fuzz seed
regeneration** for every changed encoding (ruling R45), and a **nightly
dispatched on the branch** — this week showed the CI runner exposes timing the
dev box does not.

## 12. Documentation

- `docs/how-to/upgrade-a-cluster.md` — the 0.11.0 section gains settings v4,
  the 32 B report and image v5.
- `docs/reference/configuration.md` and `uc2ctl.md` — `auto_fetch`, refusal
  61, the auto audit actor.
- `docs/how-to/bound-journal-growth.md` — learner-only voters now purge by
  default; what turning the switch off costs.
- `docs/how-to/monitor-a-cluster.md` and `packaging/prometheus/uc2-alerts.yml`
  — the counter, the gauge, `Uc2SnapshotWontFit`.
- `docs/ops/uc2-runbook.md` — the start rule at restart, reading a refused
  fetch.
- `docs/reference/cnc-page.md` — the two slot words.
- The catalog spec's §12 and `docs/BACKLOG.md` project 2 entry point here;
  #48 closes with this work.

## 13. Risks

- **Fetch traffic beside live replication.** Every new agreed set is copied to
  every node that lacks it. Small sets make this cheap (D3); large ones put a
  link-saturating burst next to the commit path. The stagger spreads it; a
  rate limit is not designed here.
- **A wrong start set is a wrong state.** Eligibility leans entirely on the
  catalog's agreement; with one learner, agreement is over one reporter
  (catalog spec §4.6) and proves nothing. `uc2ctl` should warn about a
  single-learner standby cluster (already a catalog-spec recommendation).
- **The overrun path gains an install.** A lapped row now jumps on a local
  set; its correctness rests on the same artifact checks the gap path uses.

## 14. Open questions (Phase 3 and later)

- **Phase 3 (backup tier) — revisit distribution.** Whether copies should live
  only on producers and the backup service, with **on-demand fetch at start**
  and a **purge floor over K confirmed holders** (default K = 2) instead of
  every node keeping its own copy. The backup tier changes the availability
  argument that made D3 the safe default. Decided 2026-10-09: "go with every
  node keeping a copy for now, configurable; we might change this in Phase 3".
- A transfer rate limit for auto-fetch, if large sets become real.
- #66: the compatibility window that would let a binary start from a set
  another version built (D5).

## 15. The cost comparison behind D3 (recorded, not measured)

Arithmetic only, for 3 voters and 2 learners, a 32 GB set, 10 Gbit/s links
(~1.1 GB/s goodput), one instant per hour: every-node copies move 96 GB per
instant (≈ 45 s at line rate, ≈ 90 s at a 50 % cap, ≈ 2.3 TB per day) and
save ≈ 30 s per restart against fetching on demand. The maintainer noted real
sets are much smaller, which is what makes every-node copies the default.

## Review focus (for the plan)

1. A service restarting during the background fetch of the very set it would
   start from — it must start from the older held set or replay, never a
   half-written artifact.
2. A pinned row on a cluster whose newest agreed set is newer than the pin
   origin — the pin path must win.
3. `holders()` listing a node whose soft entry is stale — the fetch must fail
   over, not wait out the full timeout repeatedly on a dead holder.
4. Sizes on a set catalogued before this change (`0`) — the check must not
   block it, and the gauge must not alarm on it.
5. A learner-only cluster with one learner and the switch on — voters fetch
   and purge on a set agreed over one reporter; the alert and docs must make
   that visible.

#### Errata (as built, 2026-10-10)

Where the build differs from or refines this spec; numbered as in the plan
(`docs/superpowers/plans/2026-10-09-uc2-snapshot-lifecycle.md`, P) and the
execution ledger (PF = pre-flight, R = execution rulings).

- **P1 — fetch candidates on a follower.** The spec said to fetch from
  `holders()`. That table is filled only on the leader, so on any node the
  candidates are live holders, then the set's builders (the committed
  reports whose hash matches the catalog's), then every other member, each
  tier learners first then lowest id, never self. This is so a follower can
  fetch at all.
- **P2 — `refused` is local only.** The spec read a refusal as a holder's
  answer. A holder that cannot serve sends nothing, so `refused` counts only
  a fetch this node could not issue; a silent holder is `timeout`.
- **P3 — the start set's version rule is by line.** The spec wrote
  `start_set_version == S::VERSION`. The build uses `same_line`, as today's
  unpinned install does (D5), so a patch-level version difference still
  starts from the set.
- **P4 — install-error fail-stop.** The spec said any failure falls back to
  replay. A missing or unverifiable artifact does; an error from the state
  machine's own `install_snapshot` is a fail-stop, because replaying onto a
  half-mutated state would be silently wrong.
- **P5 — overrun jump guard.** The overrun-recovery jump to the start set is
  refused when the row now carries a pin or a version record above what the
  walk decided, so the jump cannot skip the exact stop of #33.
- **P6 — size is the file length.** The spec left `size` open. It is the
  artifact file's length on disk, envelope included, since that is what a
  fetch transfers.
- **P7 — majority size is the largest.** The recorded size is the largest
  reported with the majority hash; a reporter whose `stat` failed sends `0`
  and must not erase it.
- **P8, amended by PF7 — the build guard.** The plan said a node does not
  fetch `N` while an attached row has `snapshot_pos < N` and `applied < N`.
  Built: some attached declared row has `snapshot_pos < N` on a FULL instant
  (not a standby instant on a voter), and the guard holds at most 30 s from
  the first sighting of `N`, then the fetch proceeds. The `applied < N` clause
  read false during the build window and let a fetch race the local builder.
- **P9–P11 — waits.** Waiting re-checks every 100 ms; `no_holder` waits 30 s;
  `no_space` backs off on the same ladder as `refused` and `timeout`, naming
  the set once.
- **P12 — thin agreement.** A set reported by exactly one node logs
  `snapshot_fetch_single_reporter` once; this is the visibility Review focus 5
  asked for.
- **P13 — refusal 61 is door-only.** It is checked at the door, not at apply:
  the catalog can retire entries between append and apply, and a refusal at
  apply would fail an already-accepted request.
- **P14 — test seam.** `Node::set_free_bytes_for_test` is a hidden test hook
  that sets the free-bytes figure the holdings probe reports.
- **P15 — inherited straggler residual.** A new fetch still clears the
  receiver's single parked expired-fetch slot. Auto-fetch never issues one
  while a fetch is pending and waits at least 1 s after a timeout.
- **PF11 — one sampler, one extra gauge (addition to §7.4).** The node
  exports `uc2_snapshot_wont_fit` (0/1), computed from the same free-bytes
  figure and the same formula the fetch check uses (0 when the size is
  unknown). `Uc2SnapshotWontFit` keys on `uc2_snapshot_wont_fit > 0`, so the
  alert and the fetch cannot disagree.
- **PF15 — zero-first pair, log once per position.** The start-set cnc words
  are published zero-first (position zeroed, then version, then position) and
  read position-version-position; the start rule logs once per position, not
  once per call.
- **R2 — the report decoder infers the entry width.** The spec added a
  sized entry. The one decoder reads `n × 20` as sized and `n × 12` as
  pre-lifecycle (every size 0), from the record's length, so apply, replay
  and image install agree on bytes written before this change. The separate
  unsized decoders were removed (R3).
- **R5/R6 — no double install.** The replay gap guard measures from the
  follower's resume point rather than the state machine's cursor, so a
  below-floor joiner installs its start set once. On a forced pass the stall
  cursor is already the follower's own, so the change applies to unforced
  passes only.
- **R7 — a fetched set is held even if older.** A completed store-only fetch
  of `P <= snapshot_set_position` is still noted held (the files are on disk);
  `snapshot_set_position` itself stays monotone. Auto-fetch also keeps a
  per-target "fetched ok" flag so it never re-fetches the same set.
- **R8 — a 1 s floor after any fetch timeout.** The floor is set on every
  timeout, auto or operator's, and survives retargeting, so chasing a newer
  set cannot bypass the straggler-residual wait of P15.
- **R16 — a pinned row keeps starting from its pin's origin.** Rule 1 is
  read literally: pins never retire, so a row that has ever been pinned
  never takes a start set — the version rule the maintainer chose to keep;
  relaxing it (a start set above the origin on the pin's `to` line) is a
  `docs/BACKLOG.md` item under #66.
- **R15 — the auto-fetch audit record is written off the consensus agent.**
  The agent enqueues it into a bounded hand-off (16 records); the
  `uc2-holdings` thread writes and fsyncs it, same fields and format. A full
  queue drops and counts the record, named as `admin_audit_dropped` on the
  next drain.
