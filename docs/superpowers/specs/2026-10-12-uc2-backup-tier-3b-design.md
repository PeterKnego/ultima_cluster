# UC2 backup tier (3b: the tier as a holder) — design

**Status:** design approved in conversation on 2026-10-12; this document is
the written spec for review. **Project 3b** of the storage-service direction
(1 catalog, 2 node-local lifecycle, **3 backup tier**, 4 decision surface).
3a (`2026-10-10-uc2-backup-tier-design.md`, merged as PR #100) writes the
cluster's agreed history to an off-node tier, records how far it reaches and
restores from it. 3b makes the cluster **rely** on it. Read 3a's spec and its
errata first, then the catalog and lifecycle specs' errata.

## 1. Problem

After 3a the tier is a copy that only a person uses, by running a restore.
Three things are still missing:

- **A node cannot be built from the tier and join a live cluster.**
  `uc2ctl tier restore --new-node` refuses by name (3a ruling R12a): the
  restored node's term map starts at the set S as `[(t, S)]`, the live
  leader's map has term t at its real base below S, reconcile finds no
  alignment, truncates to 0, and the archive fail-stops on `PositionPurged`.
- **Node disks cannot shrink because the tier holds the history.** Purge is
  off by default because, once on, a node deletes the only copy of the log
  below its newest agreed set. Only the writer's own node holds its purge at
  the tier (3a §5.4); every other node purges regardless of the tier.
- **The tier is not a live source.** A node that needs a set it lacks (a
  restart after a long absence, a new learner, a learner-built set a voter
  never copied) gets it from a peer over the cluster network, loading the
  live nodes while they serve.

## 2. Scope

**In (3b), built in this order:**

- **A — lineage in the tier.** Each backed-up set records the cluster's term
  map up to S; restore writes it; `--new-node` is enabled (§4).
- **B — backup-gated purge.** A replicated setting K: every node's purge
  floor is held at or below the K-th highest confirmed tier position; the
  writer reads back what it wrote before it reports it (§5).
- **C — the tier as a source.** A node-local read-only tier source, tried
  before peers for store-only fetches and for below-floor catch-up (§6).

**Out:**

- **The tier's journal as a live source.** Peers always hold the log above
  their purge floor, and every install lands at or above that floor (§6.3),
  so no live node needs the tier's journal. It stays the offline restores'
  input.
- **Changing the D3 default.** `auto_fetch` stays on by default; 3b makes
  `auto_fetch = false` a supported configuration for clusters with a tier
  source (§6.5).
- **Object storage, a networked tier service, encryption at rest.** A later
  `BackupTarget` implementation; the reader in §6 goes through the same trait.
- **Retiring local sets below `retain_sets` because the tier holds them.**
  Sets are small by default (`retain_sets = 1`); the disk is the journal (§5).
- **A verify pass of the whole tier before any deletion.** Replaced by the
  writer's read-back (§5.3), which checks every object before it can count.

## 3. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | All three pieces in one spec, built A → B → C. | C's below-floor install is A's problem online (a node whose history starts at a set); B is independent and delivers the disk saving with 3a's watermark. |
| D2 | **Approach 1: the tier records lineage, nodes read the tier directly.** Not a consensus-side "floor-aware reconcile", not a networked tier peer. | The defect is missing data, so the fix is recording it; the consensus core and its proofs stay untouched. A tier peer is a new long-running service — the object-storage era's shape, deferred. |
| D3 | **B is off by default**; turning it on means setting `purge_requires_tiers` (K) to 1 or more. | Today's behaviour is unchanged until an operator opts in. |
| D4 | **The writer's read-back is always on.** | 3a §13: the leader trusts the writer's claim, and 3b deletes on it. Only the writer's host has the target mounted, so the writer is the one place a check can run. |
| D5 | **The gate releases on low space** (3a §5.4's rule, applied to every node). | A full disk stops the node; a gap in the tier's history is the lesser loss, and it is alerted. |
| D6 | **The tier is tried first, peers are the fallback**, in both fetch paths. | Keeps the load of a large set off live peers; a tier problem never blocks a restart. |
| D7 | **`auto_fetch` stays on by default.** | The maintainer's 2026-10-09 call stands; a cluster without a tier keeps its restarts local. |
| D8 | **The catalog, not the tier, is the authority** for every set a node installs from the tier. | The tier is shared storage under the operator's trust; the catalog's agreed hashes are the cluster's own record. |

## 4. A — lineage in the tier

### 4.1 What the writer records

`SET` (3a §4.3) gains one line:

```
lineage=<t1>:<b1>,<t2>:<b2>,…,<tn>:<bn>
```

the writer's term map entries with `base < S` (strictly) at the time it
writes the set, ascending, `term` and `base` in decimal. The last entry is the
term that covers the bytes just below S. Every entry starts strictly below
S ≤ the writer's commit, so it describes committed bytes: a data-stamped entry
below commit is the cluster's real history (the reconcile module's safety
contract), never a guess. An entry AT S is excluded on purpose: the bytes at S
are the first bytes of the chain after the set, not the set, and a term opened
exactly at S may not be committed when the writer copies the set (a new
leader's opening frame landing on the instant). Restore derives a term that
starts at S from the chain's frame headers (§4.2) like any later one. The writer's
map is its full map, not the 64-entry wire window, so the line can hold every
term in the cluster's life; there is no cap.

A set whose `SET` has no `lineage` line is refused by every 3b reader
(`set_lineage_missing`). 3a sets have none, but 3a is unreleased (the
0.11.0 flag day), so no released tier lacks it.

### 4.2 What restore writes

`materialize` (3a §8.1 step 5) writes `term_map.state` as the `SET`'s
lineage followed by the terms derived from the frame headers in `[S, Q)`
whose term is above the lineage's last — instead of 3a's map derived from
`[S, Q)` alone. The derivation is seeded with the lineage's last term, so the
term covering S is never re-stamped at S (3a's R12a shape); a term that opens
exactly at S is a higher term in the first frame and gets its entry `(t, S)`. Boot's `rederive_term_map` then has nothing to add. Every
mode uses it: whole-cluster restore, `--new-node`, fork and corpus export.

### 4.3 `--new-node`

`uc2ctl tier restore --new-node --node-id N` stops refusing. The 3a guard
stays (R11c): N must not appear in the membership the node will boot with —
the cluster artifact at S folded with every `Membership` frame in `[S, Q)`
(`restore_id_reused`). The operator then runs `uc2ctl add-learner N` and,
if wanted, `promote N`. On join, the leader's map aligns inside the node's
(they share every entry below S), reconcile keeps the node's log up to its
durable Q (≤ the tier's `through` ≤ commit), and replication continues from
Q. The new-node e2e 3a left `#[ignore]`d is un-ignored and is A's acceptance
test.

### 4.4 Tests

- Unit: `materialize`'s `term_map.state` equals a live node's map truncated
  to Q, for a history with several terms below S and one in `[S, Q)`.
- Unit: a `SET` without `lineage` is refused by name.
- Unit: a writer whose map has a term opening exactly at S records the
  lineage without it, and restore derives `(t, S)` from the chain; a `SET`
  whose lineage's last base is `≥ S` is refused (`set_lineage_malformed`).
- E2E (the un-ignored test): restore `--new-node` from a tier of a live
  3-node cluster under load, `add-learner`, assert the node keeps its log
  (no truncation below Q, its `journal_first` stays S) and reaches commit.
- Sim: a node seeded with a set's lineage and the log `[S, Q)` never
  truncates below Q against any leader of the same cluster (a new inv, run
  in the seeded fuzz).

## 5. B — backup-gated purge

### 5.1 The setting

`Settings` version 6 adds `purge_requires_tiers u8` at offset 46 (47 B; the
0.11.0 flag day, like v5). `0` = off (the default). `1..=4` = on, with K
tiers (4 = `MAX_BACKUP_TIERS`); above 4 is refused at the door with `47
settings_bounds`. Versions 1–5 decode with `0`. `[settings]` seeds genesis
only; `uc2ctl settings apply` changes it; `settings show` prints it.

### 5.2 The rule

With K ≥ 1, every node computes

```
gate(K) = the K-th highest `through` among the committed watermark's tier
          entries (§6.1 of 3a), or 0 when fewer than K are registered
```

and clamps its purge floor to `min(floor, gate(K))`, next to the existing
holds (`hold_floor_for_pins`, the writer's own hold). It only ever lowers the
floor: the gate holds purge back, it never causes one, and purge stays off
unless `[purge]` is on. `uc2ctl tier forget` can lower `gate(K)`; that only
holds purge back further.

### 5.3 The writer's read-back

Every object a writer creates is reopened through the target and checked
before it counts. An object a writer finds already in place — another
writer's, or its own from before a restart — is checked once too, unless the
cluster's committed watermark entry for the tier already covers it (a chunk
ending at or below `through`, a set at or below `newest_set`): those were
checked by the writer that created them. So a restart or a second writer never
re-reads the tier's history, while anything written just before a crash is
checked when the writer comes back. (Residual: a creator that crashes between
writing a set below `newest_set` and checking it leaves that one set
unchecked; `tier verify` covers it.) The check: every block's CRC and the header CRC over
the raw bytes; for a chunk, the frame walk over `[from, to)`; for an
artifact, its raw hash against the `SET`'s agreed hash. Only objects that
pass advance the reported chain. A failure leaves the report where it was,
increments `uc2_backup_errors_total{reason="readback"}`, logs
`backup_readback_failed` naming the object, and fires the existing
`Uc2BackupWriterError`. The writer retries the object at its next pass (the
object is write-once, so a retry re-reads; a persistently bad object is the
operator's, with `tier verify` naming it).

### 5.4 Release on low space

Each node applies 3a §5.4's release test to itself: while its free space is
below the auto-fetch headroom rule for one `chunk_bytes` (`auto_fetch::fits`),
the gate is not applied, the node purges as its other holds allow, and it logs
`backup_gate_released` once per episode with the new alert
`Uc2BackupGateReleased`. The tier's chain then has a gap there, which 3a's
writer already records.

### 5.5 What an operator must know

With the gate on, history older than a node's newest agreed set lives only in
the tier, so the tier's retention (`backup_retain_sets`,
`backup_retain_age`) decides how far back a point-in-time restore can go. The
how-to says so where it introduces the setting.

### 5.6 Visibility

`uc2_purge_gate_position` (the `gate(K)` each node applies; absent when
K = 0); `uc2ctl tier show --instance-dir` marks the tiers that count toward
K; `settings show` prints `purge_requires_tiers`.

### 5.7 Tests

- Unit: `gate(K)` for 0, 1, 2 and 4 registered tiers, K above the count, and
  a forgotten tier.
- Unit: `Settings` v6 round-trip; v1–v5 decode with `0`; K = 5 refused 47.
- E2E: purge on, K = 1, one writer: no node's `journal_first` passes the
  tier's `through`; stop the writer, simulate low space on one node — that
  node purges past `through`, logs the release once, the alert's metric
  moves; the others hold.
- Unit (writer): an injected read-back failure leaves the report unmoved and
  counts `readback`.

## 6. C — the tier as a source

### 6.1 Configuration

A new node-local, read-only section:

```toml
[tier_source]
target = "/mnt/uc-backup"
```

Any node may have it. A node with `[backup]` reads its own `target` as a
source without this section. At boot the node opens `TIER` and refuses a
foreign `app_id` by name (`tier_source_foreign`), as the writer does. A node
uses a source only while its `tier_id` is registered in the committed
watermark, and never reads beyond that entry's `through`.

### 6.2 Store-only fetch from the tier

The store-only fetch (auto-fetch, lifecycle spec §6; `uc2ctl snapshot fetch`)
tries the tier first. For an agreed set P in the catalog:

1. The tier has `sets/<P>/SET` (P ≤ the registered `through`).
2. Copy every artifact into `snapshots/<row>/` and `snapshots/cluster/`
   through a temp file and a rename, as a peer fetch does, so a service never
   sees a half-written artifact. The tier copy's temp names are its own
   (`incoming-<P>.tier.part`), so a peer session for the same P that the leader
   opens after giving up on the tier never shares a file with it; both are
   verified against the same catalog hash, so whichever renames last leaves
   the same bytes.
3. Verify each artifact's raw hash against the **catalog's** agreed hash for
   P (D8), and the `SET`'s versions against the catalog's.
4. Any failure — no such set, an unreadable object, a mismatch — deletes what
   step 2 wrote and falls back to the peer fetch at once, logging
   `tier_fetch_declined` with the reason, once per set position.

Success is the ordinary completeness path: the set is held, reported and
counted like a peer-fetched one.

### 6.3 Below-floor catch-up from the tier

Today a follower whose NAK falls below the leader's purge floor gets a
snapshot session pushed by the leader. 3b puts one exchange in front of it.

- **`TIER_INSTALL`**, new pairwise datagram kind **28**, leader → follower,
  body `floor u64 ‖ commit u64` (16 B): "install a set S with
  `floor ≤ S ≤ commit` from your tier".
- **`TIER_INSTALL_STATUS`**, kind **29**, follower → leader, body
  `state u8 ‖ reason u8 ‖ reserved [u8; 6] ‖ position u64 ‖ copied u64`
  (24 B): `0 = declined` (with a reason), `1 = installing` (position S,
  bytes copied so far), `2 = installed` (position S).

The leader, where it would open a snapshot session, sends `TIER_INSTALL`
instead and waits:

- **no answer in 2 s**, or `declined` → it opens the snapshot session as
  today;
- `installing` → it opens nothing while `copied` keeps rising (the follower
  answers every second); **10 s without progress** → it opens the session;
- `installed` → nothing to do; the follower's next report is above the floor
  and ordinary replication from S serves it.

The follower, on `TIER_INSTALL`, with a readable source:

1. Picks the newest tier set S with `floor ≤ S ≤ min(commit, through)` that
   is agreed in its catalog; none → `declined (no_set)`.
2. Copies and verifies it exactly as §6.2 steps 2–3 (mismatch → `declined
   (verify)`, its files removed).
3. Installs it exactly as a snapshot session's completion does: discards its
   local bytes, installs the artifacts, adopts the `SET`'s **lineage** with
   `adopt_snapshot_lineage` (A), adopts the membership in the cluster
   artifact at S with `adopt_snapshot_config`, and adopts S as its floor. Then
   answers `installed`.

A follower without a source answers `declined (no_source)` at once, so the
cost to a cluster with no tier is one round trip per below-floor episode.
`floor ≤ S` is the invariant that makes the leader's log cover everything
after S; `S ≤ commit` keeps the install committed.

A brand-new learner with a mounted source therefore catches up from the tier
on its first below-floor NAK, and the leader ships it nothing large. Offline
`--new-node` (§4.3) stays for building a node before it starts.

### 6.4 Services

Unchanged. A service starts from whatever is on disk — its start set, its
own state or a replay — and never waits for the network or the tier.

### 6.5 `auto_fetch = false` with a tier

Supported from 3b: voters stop copying learner-built (standby) sets; a voter
restarted below the floor catches up through §6.3. The how-to describes it as
the configuration for large standby sets with a tier source on every voter.

### 6.6 Visibility

`uc2_snapshot_fetch_total{source="tier"|"peer"}`,
`uc2_tier_install_total{outcome="installed"|"declined"|"timeout"}`, the
`tier_fetch_declined` and `tier_install_declined` log lines with their
reason.

### 6.7 Tests

- Unit: set choice for `TIER_INSTALL` (bounds, catalog agreement, the
  `through` cap); both bodies' wire round-trips and length refusals.
- E2E: a learner with a tier source joins a 3-node cluster whose leader has
  purged below its floor; it installs from the tier, and the leader opens no
  snapshot session (`snapshot_session` count unchanged).
- E2E: the same with one tier artifact corrupted: `declined (verify)`, the
  leader's session installs it.
- E2E: auto-fetch with a tier source takes the set from the tier
  (`source="tier"` counted), and from a peer when the tier lacks it.
- Sim: a tier install never lands below the leader's floor or above commit.

## 7. Wire, image and settings changes — inside the unreleased flag day

All inside wire `0.11.0`, unreleased: `SET` gains `lineage`; `Settings`
version 6 (47 B, `purge_requires_tiers`); pairwise kinds 28 `TIER_INSTALL`
and 29 `TIER_INSTALL_STATUS`. No cnc change. `node.toml` gains
`[tier_source]`.

## 8. Failure handling

| Failure | Behaviour |
|---|---|
| A tier set without `lineage` | Refused by restore and by both fetch paths (`set_lineage_missing`); the fetch paths fall back to peers. |
| The tier unreachable or foreign at boot | `[tier_source]` foreign → refuse to start by name; unreachable → start, log, and every fetch falls back to peers. |
| Read-back failure | The report does not advance; error counted and alerted (§5.3). |
| Fewer than K tiers registered | `gate(K) = 0`: no purge beyond the existing holds. |
| Low space with the gate on | The gate releases; alert; gap recorded (§5.4). |
| A tier artifact mismatches the catalog | Its copy is removed; fall back to peers (§6.2, §6.3). |
| The follower stops progressing mid-install | The leader opens the session after 10 s; the follower's partial copy is in temp files and is removed. |
| A crash mid-install | As a crash mid-session: the follower restarts below the floor and the exchange runs again. |

## 9. Proof

- Unit tests per §4.4, §5.7, §6.7.
- The un-ignored `--new-node` e2e (A's acceptance).
- The gate e2e (B) and the three source e2es (C).
- Sim: the seeded-lineage inv (§4.4) and the tier-install bounds inv (§6.7).
- `lin_backup_tier` gains a phase: a learner joins from the tier mid-load and
  the history stays linearizable.
- The whole proof stack as in 3a, MSRV and every feature-gated clippy line.

## 10. Documentation

- `docs/how-to/back-up-to-a-tier.md`: replacing a node from the tier (no
  longer refused), backup-gated purge (§5.5's warning), `[tier_source]`.
- `docs/how-to/bound-journal-growth.md`: purge with a tier.
- `docs/reference/configuration.md`, `wire-protocol.md`, `uc2ctl.md`,
  metrics and alerts reference.
- The backup-tier explainer: "the tier as a holder".
- `docs/BACKLOG.md`: close the 3b line; D3 recorded as revisited and kept.

## 11. Risks

- **The writer's own map at S.** §4.1 relies on the writer's map below its
  commit being the committed lineage. A writer that itself joined below a
  floor holds an adopted lineage (`adopt_snapshot_lineage`), which is the
  leader's — still committed history. The sim inv (§4.4) is the check.
- **Leader-side wait.** §6.3 delays a below-floor session by up to 2 s for a
  follower with no source. Below-floor episodes are rare and already take
  seconds; the e2e times the fallback.
- **A slow mount.** A tier on a slow mount progresses slowly but steadily;
  the 10 s rule is about stalls, not speed, so a slow tier still wins over a
  session. An operator who prefers the peer can remove `[tier_source]`.

## 12. Review focus (for the plan)

1. A follower installing from the tier while the leader's term changes
   mid-install: the new leader must not open a second install path in
   parallel with the first.
2. A tier set whose `lineage` disagrees with the catalog's position or with
   the cluster artifact's membership: refuse, never install.
3. K raised above the number of registered tiers on a cluster with purge on:
   purge stops cleanly (no release, no alert storm) and resumes when a tier
   registers.
4. A forgotten tier that a follower is mid-install from: the install
   finishes or declines; it never reads past the forgotten entry's last
   `through`.
5. `auto_fetch = false` and no tier source on a voter: today's behaviour
   (peer session), not a stall.

#### Errata (as built, 2026-10-13)

Where the build differs from or refines this spec; numbered as in the plan
(`docs/superpowers/plans/2026-10-12-uc2-backup-tier-3b.md`) and its execution
ledger (PF = pre-flight, RA = review amendments, T = execution rulings by
task). **RA1–RA7 are not repeated here:** the plan's "Review amendments"
section records them, and the spec text for RA1 (§4.1, lineage `base < S`),
RA2 (§5.3, read-back) and RA3 (§6.2, `.tier.part` temp names) was amended
**before** execution, not after — read those sections as written.

- **PF1 — lineage agreement between writers is suffix agreement (§4.1).** The
  spec said two writers' lineages for one set must agree. `NodeState::
  store_term_map` keeps only the newest 300 entries, so a restarted writer's
  map can be shorter than a never-restarted one's. `place_set` on
  `Put::Exists` compares every `SetLabel` field exactly except `lineage`,
  which agrees iff the shorter is a suffix of the longer; any other
  difference is still the `OverlapMismatch` stop. The line itself is uncapped.
- **PF2 (as amended by RA2), T8 — read-back gates the report, and the creator
  reads back its own objects (§5.3).** The watermark and `progress` are
  computed over the listing filtered to confirmed ∪ verified objects; every
  object this writer creates (`Put::Created`) is read back at once and enters
  the verified cache only if it passes. A found object counts without a
  re-read iff the committed watermark already covers it (its creator read it
  back). Placement (`copy_one_set`, `cut_one_chunk`, retention deletions) keeps
  the RAW listing, so a persistently bad object is never re-cut over — it is
  the operator's, named by `tier verify`. Reason `readback`; event
  `backup_readback_failed`.
- **PF3, T7 — the gate's chunk size on a non-writer node (§5.4).**
  `Consensus::backup_chunk_bytes` is 0 without a writer and `fits(free, 0)`
  means "free ≥ 1 GiB". The low-space test uses `backup.chunk_bytes` on a
  writer node and `tier::writer::DEFAULT_CHUNK_BYTES` (64 MiB) elsewhere.
- **PF4, T12 — the tier copy is a thread: `uc2-tier-fetch` and
  `SnapIntakeSignals` (§6.2, §6.3).** Copying a set is I/O the consensus
  agent must not do. One thread per fetch serves both store-only and install;
  on success it raises exactly the signals `Receiver::snap_complete` raises,
  factored into one `SnapIntakeSignals` struct both own, so
  `maybe_adopt_incoming_snapshot` and `check_set_completeness` serve a tier
  copy unchanged.
- **PF5, T13 — the install adopts the SET's lineage (§6.3).** The spec let the
  install adopt the leader's term-map tail (`last_leader_map`, 64 entries).
  A tier install sets `pending_install_lineage = Some(set.lineage)` first and
  the handler prefers it, so the leader's later `TermMap` datagrams reconcile
  against a map holding every committed entry ≤ S. The term-map line is
  parsed and range-checked but not hash-covered; rows and the cluster
  artifact are hash-verified against the leader's verdict.
- **PF6, T5, T13 — no sim floor model; `check_install_set` exhaustive test
  instead of the §6.7 sim inv.** `uc_sim` models no snapshot install and no
  purge floor, so "a tier install never lands below the floor or above commit"
  cannot be a sim invariant. It is pinned as an exhaustive test of the pure
  choice function (`check_install_set`). §4.4's seeded-lineage inv14 stands on
  a new seeding seam, `World::seed_from_set`. A floor model is not 3b's.
- **PF7, T14 — the sender owns the wait (§6.3).** The below-floor decision is
  `Sender::serve_nak` on the sender thread, timed with its own clock; kind 29
  reaches it as a `CtrlMsg` (the `SNAP_DONE` route) and kind 28 reaches the
  follower's node as a term-gated `NetEvent` (the `SNAP_REDIRECT` route). Floor
  = the journal's first archived base; commit = the counter the sender reads.
- **PF-R1 — the contract count moves with the task that adds a family.** The
  metric-contract count test and `docs/how-to/monitor-a-cluster.md` move with
  each task adding a family (T7 → 145, T12 → 146, T13 → 147); T16 asserts the
  final 147. Otherwise the count test is red from T7 to T13.
- **T8 — retention counts only verified sets (3a §7.2; this spec §5.3).**
  "Newest" and the keep window are computed over the verified ∪ confirmed
  listing; deletions still act on real names. Otherwise a corrupt newest set
  with `keep_sets = 1` would leave the tier with no restorable set while the
  gate holds nodes at the verified `through`. Cost: a persistently corrupt
  newest set keeps one extra set until the operator acts.
- **T11 — `NetEvent::TierInstall` is keyed by address (§6.3).** Shape
  `{ from: SocketAddr, floor, commit }`. The receiver's `peer_ids` map is
  populated only with wire crypto on (default off), so a NodeId would never be
  emitted on a default cluster. The receiver keeps the term gate; the leader
  identity check (`from == id_to_addr(leader_hint)`) moves to the node.
- **T12 — explicit vs implied foreign source (§6.1).** An explicit
  `[tier_source]` that is foreign refuses to start (`tier_source_foreign`). A
  source implied from `[backup]` that is foreign does not refuse: it logs
  `tier_source_unavailable{detail="foreign"}`, yields no handle, and the 3a
  writer's `backup_target_foreign` stop applies unchanged.
- **T12 — auto-fetch tries the tier first (§6.2).** `auto_fetch_step` issues a
  tier fetch whenever a registered source holds `sets/<P>/SET`, even with no
  peer holder; a tier fetch that times out falls back to the peer path like an
  `Err`. The log/audit `from` names the tier when it served.
- **T12 — the audit record's `fetch_source` (new, §8).** A tier-served
  auto-fetch writes the `snapshot_fetch` audit record (the node served itself,
  so the member field carries its own id); the record gains a 14th key,
  `fetch_source` = `"tier"` | `"peer"` on auto-fetch/op-9 records, `null`
  elsewhere.
- **T12 — a timed-out fetch is CANCELLED and drained (§6.2, §8).** The spec
  said a timed-out copy is abandoned. `TierFetch.cancelled` is set: the
  thread renames and publishes nothing more and removes only its own
  `.tier.part` files; the record stays in a draining slot until `done`, and no
  second tier copy of the same P starts while one drains.
- **T13, T14 — the leader-carried verdict: `TIER_INSTALL` is 200 B (§6.3,
  §7).** The spec's body was `floor ‖ commit` (16 B) and had the follower
  agree the set "in its catalog". A brand-new learner's catalog is EMPTY, so
  that agreement never holds (`NoSet` forever) and trusting the tier's SET
  alone breaks D8. The leader names S and carries its catalog verdict: floor
  @0, commit @8, s @16, row_hash [u64;8] @24, row_version [u32;8] @88,
  row_size [u64;8] @120, cluster_hash @184, cluster_size @192, LE, no padding
  (200 B; an early draft said 224, corrected). The follower verifies every
  object against the carried values; `choose_install_set` no longer reads the
  follower's catalog. Reader refuses other lengths, `floor > commit` and
  `s ∉ [floor, commit]`. Status reasons: `none=0 no_source=1 no_set=2
  verify=3 not_registered=4`, declines carrying `position = s`.
- **T14 — S is bounded by the tiers' max `newest_set` (§6.3).** The leader
  picks its newest agreed set in `[floor, commit]` that is also ≤ the maximum
  `newest_set` across registered tiers (it cannot tell which tier the follower
  reads); only if none exists does it fall back to the newest agreed set in
  range. `through` was the first candidate bound, rejected: it can reach P
  before the set at P is copied (cut-to-set).
- **T14 — `s <= durable` answers `Installed` (§6.3).** A `TIER_INSTALL` whose
  `s` is at or below the follower's durable position answers `Installed{s}` at
  once without copying, so a late publish cannot re-arm the receiver's
  `adopt_pending` below the adopted position.
- **T14 (F1) — installs need no registration (§6.1).** The follower does not
  consult `registered_through` for `TIER_INSTALL`: a new learner's committed
  view holds no registration, and the leader's verdict already proves the set.
  It requires only `TIER.app_id` = its app id and hash-verifies every object.
  `not_registered` therefore means "TIER unreadable or foreign". The
  registration / `through` cap remains the rule for store-only fetches.
- **T14 (F2) — a current-term kind 28 teaches the leader hint.** A restarted
  follower with no known leader learns it from a current-term `TIER_INSTALL`
  (the term gate admits only the current term; one leader exists per term).
- **T14 (F3) — e2e session counts are `>= 1`.** Where a session is expected
  the e2es assert `snap_sessions >= 1` (a joiner re-NAKs and several sessions
  open); "asked exactly once" is `tier_install_sent == 1`; "no session" cases
  keep `== 0`.
- **T14 — the `Installed` settling grace and hold backstop (§6.3).**
  `Installed{S}` from X gets a 10 s settling grace (`TIER_INSTALL_SETTLE_NS`)
  during which a below-floor NAK from X opens nothing (the joiner NAKs until
  its cluster-artifact install lands); from 10 s to 30 s such a NAK is held
  like a decline (session); after 30 s X is asked again. A first draft held
  from the first NAK and broke the healthy join.
- **T5 — scenario budgets, and the inv14 mutation is caught by inv2 (§4.4).**
  The plan's storm budgets (400/200) were below `STORM_INSTANT_STEPS` (500) and
  ran zero iterations; built as a `storm_steps` helper without the closing run,
  budgets 1000/500, `max_steps` 1M. The frames-only seed mutation is caught by
  inv2 (term-map prefix consistency, seed 14, step 5184) before any truncate,
  so inv14 never fires on that shape; it stays the truncate-time backstop for
  a prefix-consistent but cut seed, and is unproven reachable by any shape.
- **T3, RA6 — `tier_set_deferred` (detail).** After 10 consecutive passes
  deferring one set for an empty lineage (e.g. a restarted writer whose
  persisted map no longer reaches below an old S), `tier_set_deferred
  {position}` (Warn) fires once per position.
- **R10a stays open (3a §13).** 3b did not settle the origin-set edge.
  Settled after 3b, 2026-10-17: see the 3a errata's R10a entry
  ([#126](https://github.com/PeterKnego/ultima_cluster/issues/126)).
- **Task 15 (§9) — the lin capstone's tier-join test runs its body on a 16 MiB
  thread.** A debug build overflowed the 2 MiB test-thread stack (nightly runs
  the capstones in debug); the oversized frame was not isolated.
- **Final review I1 — install progress per block; the session wins (§6.3).**
  `copied` rose only after a whole artifact was copied, so the leader's 10 s
  no-progress rule fired on any one artifact slower than 10 s, opened a
  session, and the follower's copy kept running — two paths. `copy_artifact`
  now reports every block (≤ 1 MiB) it streams. And a running install is
  CANCELLED when the follower adopts a session-installed set at or above its
  S: the thread's publish and the node's cancel are one compare-exchange, so
  exactly one wins; a cancelled install is drained like a timed-out
  store-only copy and the session's lineage is adopted.
- **Final review I3 — purge holds until the node has joined (§5.4).** At boot
  the cluster view (K, `backups`) may be the recovered artifact's until the
  cluster agent's walk reaches commit, so a K raise or a `tier forget`
  committed after the newest artifact is invisible. With `[purge]` on, the
  gate is `0` (every purge held) until `declared_published` — the attach
  gate's own predicate — whatever K reads; `uc2_purge_gate_position` and the
  archive's effective gate read `0` meanwhile.
- **Final review I2 — no tier I/O on the consensus agent (§6.2, §6.3).** Every
  tier read (`TIER`, the registration check, the `SET`) runs on the
  `uc2-tier-fetch` thread; the node hands it in-memory inputs only. A
  `TIER_INSTALL` is answered `installing{S, 0}` at once and a plan-time
  decline follows as `declined(reason)`, which the leader handles as any
  decline after `installing` (session + 30 s hold). `TierSource::open` at
  boot is the one read left on the node's own thread.
- **Final review I4 — the adopted lineage is cross-checked (§6.3).** Before
  copying, the install thread requires every `SET` lineage entry whose term
  the leader's last map also holds to carry the leader's base, and the line's
  last term ≤ the node's current term; else `declined(verify)`,
  `detail = "lineage_mismatch"`, nothing written. A flipped digit in a
  well-formed line is otherwise adopted, and reconcile then cuts below the
  adopted floor (`PositionPurged`).
