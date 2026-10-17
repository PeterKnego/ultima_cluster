# The snapshot lifecycle, explained

*Written 2026-10-16 for the snapshot lifecycle (storage-service project 2,
PR #87), which ships in `2.14.0` inside the wire `0.11.0` / cnc `3.4` flag
day. Read the spec's "Errata (as built)" block before its body, because the
follower's fetch candidates, the start set's version rule and the whole of
pin completion are errata rather than spec text. This note carries the
argument in plain language.*

Spec:
[`2026-10-09-uc2-snapshot-lifecycle-design.md`](../superpowers/specs/2026-10-09-uc2-snapshot-lifecycle-design.md).

The lifecycle builds on the [snapshot catalog](uc2-snapshot-catalog-explained.md)
([`uc2ctl catalog`](../reference/uc2ctl.md#catalog)): the replicated list of snapshot
sets the cluster has taken, with every node's hash per row and a verdict on
whether they **agree**. The catalog knows which sets are good. This feature
is what uses that knowledge, in three places: where a restarting service
starts, which sets each node keeps a copy of, and when an upgrade pin is
finished.

## The problem

Before this change the catalog knew which sets were agreed, and nothing acted
on it:

- **A restart replayed history.** A service that attached or restarted
  resumed at its state machine's own `last_applied` — `0` for an in-memory
  state machine — and replayed the whole journal from there. It installed a
  snapshot only when the journal had a hole, or when an upgrade pin forced
  one. The node usually had a perfectly good, agreed set on its own disk; the
  service never asked.
- **On a learner-only cluster, voters never purged.** With
  `snapshot_target = "learners"` (or `uc2ctl snapshot --standby`) only
  learners freeze, so voters hold no set. A node's purge floor moves only on
  a set it holds, so every voter's journal grew without bound until an
  operator ran `uc2ctl snapshot fetch` on each one (#48).
- **A pin could start from anything, and never ended.** `uc2ctl upgrade pin`
  accepted any complete set as the origin, agreed or not — and a pin is a
  one-way door that makes every instance of the row install exactly that
  artifact. Once placed, the pin stayed in force forever: every attach
  re-installed the origin, retention kept the origin's set forever, and with
  purge on a restarted, already-upgraded service installed the old origin,
  met a purged journal, and then installed a newer set on top — two installs
  where one would do.

Rolling application upgrades (#66) need the first point most: a node
restarted with a new binary needs a known-good place to start, not a replay
of everything that ever happened.

## The start set: the node decides, the service installs

For each declared row the node computes a **start set**: the newest catalog
entry that

1. the catalog has **agreed** as a whole, with this row's own verdict
   `Agreed` (a set that did not report the row is not eligible for it);
2. this node **holds complete on its own disk**; and
3. sits at or below `min(commit, durable)` on this node.

No such entry — including an empty catalog, before the first set completes —
means no start set, and everything behaves as it did before.

Why the node and not the service? Because both inputs — the catalog and the
disk — live on the node. The alternative, having the service read the catalog
out of the newest cluster artifact, is one instant stale by construction: the
artifact at `P` was frozen before anyone had reported, let alone agreed, the
set at `P`.

The node publishes the answer in two words on the row's cnc service slot,
`start_set_pos` at `+264` and `start_set_version` at `+272` (the packed
version that built that row's artifact), on the same line as the service's
own `snapshot_pos`. Both writers on that line are rare — the service writes
`+256` once per instant, the node writes the pair only when the answer
changes — so the per-frame apply path never touches it. The pair is written
**zero-first** (position to `0`, then the version, then the new position) and
read position–version–position, so a reader never pairs a version with the
wrong position; if it cannot get a stable read it sees "none" and replays,
which is always safe. Each change logs `start_set_published`. The layout is
in [the cnc page reference § Service slots](../reference/cnc-page.md#service-slots).

The consensus agent recomputes the start set only when the catalog changes,
this node's held sets change, or the commit/durable frontier passes a newer
held set it was waiting on. A steady pass costs one flag test.

## The start rule

At **attach**, and again in **overrun recovery** (a follower that fell a lap
behind its log buffer and has to rebuild), the service reads the pair once and
applies one rule: install the start set if it is **strictly ahead** of where
the row would otherwise resume, at or below `min(commit, durable)`, and built
on **this binary's line** (major.minor — a patch difference still starts from
the set). Then resume at that position and replay only the tail. It prints:

```
uc_service: row 0 started from snap-73792 (start set; resume was 0)
```

There is deliberately no threshold and no cost model: "start from the newest
set whenever it moves the row forward" gives the most predictable restart
time, and a cost model would optimise a cost that is small either way while
adding a second code path to get wrong.

The cases where it does not install:

- **The state machine is already at or past the set** — a durable state
  machine is never rewound by this rule (the strict "ahead").
- **The set was built on another line.** The row replays instead, with one
  line naming both versions. Starting from another version's artifact is
  #66's question, not this one's.
- **The artifact is missing or fails its `ULTSNAP2` envelope check** (pruned
  between publish and attach, say). One line names why, and the row replays.
  These lines are printed once per position, so overrun recovery cannot spam
  them.
- **The state machine's own `install_snapshot` returns an error.** This one
  is a **fail-stop**, not a fallback: the install may have half-mutated the
  state, and replaying on top of a half-installed state would be silently
  wrong.
- **A version record sits above where the service decided to stop.** Overrun
  recovery will not jump over a pin or running-version record the service has
  not reached, because that jump could skip [the exact
  stop](uc2-row-running-version-explained.md#the-exact-stop) that keeps a row
  from running two versions.
- **The row is pinned and its pin is not complete** — see below.

A node too old to publish the pair leaves both words `0`, and a service too
old to read them ignores them; either way the row replays as before.

## Auto-fetch: every node keeps its own copy

A start set has to be on local disk, and a node purges only below a set it
holds. On a cluster where every node freezes, that is automatic. On a
learner-only cluster it never was — which is the #48 gap. So the node now
**fetches each new agreed set it does not hold, in the background**.

The replicated setting `auto_fetch` (Settings record v4, default `true`;
`[settings] auto_fetch` seeds genesis, `uc2ctl settings apply` changes it)
turns it on or off cluster-wide. When on, on every node, learners included:

- **Only the newest.** The target is the catalog's newest agreed set. When a
  newer one agrees, the chase moves to it; older unheld sets are never
  fetched. A set above this node's `durable` waits until the log reaches it.
- **One at a time,** through the same store-only fetch `uc2ctl snapshot
  fetch` uses: nothing is installed, and the set enters the node's holdings
  through the ordinary completeness path, which moves its floor and therefore
  its purge.
- **The tier first.** A node configured with a [backup tier as a
  source](uc2-backup-tier-explained.md#the-tier-as-a-holder) tries it before
  any peer.
- **Then peers, in a fixed order:** known holders (the leader's live table,
  then the nodes that built the set, read from the committed reports — which
  is how a follower knows them at all), then every other member; learners
  first within each group, lowest id first.
- **Staggered and backed off.** The first attempt for a set waits
  `node_id × 250 ms`, so the cluster does not stampede one holder. A silent
  holder costs the 60 s fetch timeout and moves on to the next; failures back
  off from 1 s, doubling to 30 s, and an exhausted list waits 30 s and starts
  over.
- **A local builder is not raced.** If this node is still freezing that very
  set itself, the fetch holds off for up to 30 s from first seeing the set,
  then proceeds — a wedged builder must not park auto-fetch forever.

**A service start never waits for the network.** It starts from whatever
agreed set is already on the node, or replays. The purge rule is unchanged:
a node purges only below an agreed set it holds. Auto-fetch just makes sure
it holds one.

The cost is traffic: every new agreed set is copied to every node that lacks
it, beside live replication. The maintainer's call was that real sets are
small, so per-instant copies are cheap and restarts stay local; the costed
alternative (copies only on builders, fetch on demand at start, purge over K
remote holders) is deferred to the backup-tier phase. There is no transfer
rate limit.

**One learner proves little.** A standby set is agreed over the nodes that
reported it. With one learner that is one reporter, and every voter then
fetches and purges on a set nobody cross-checked. The node logs
`snapshot_fetch_single_reporter` (warn) once per such set; run two learners
if the floor must rest on agreement.

**Turning it off** brings back the old trade: on a learner-only cluster,
voters hold no set and never purge until someone runs `uc2ctl snapshot fetch`
on each.

## The free-space check

UC fail-stops a node on a full disk — a named stop is better than a corrupt
journal ([M11](../benchmarks/uc2-m11-gate-2026-08-20.md)). A background
download must never be the thing that fills it. So reports and the catalog
now carry each artifact's **size** (the file length on disk, envelope
included — what a fetch transfers), and before every attempt the node checks

```
free_bytes >= total_size + max(total_size / 4, 1 GiB)
```

against the free-space figure its `uc2-holdings` probe already measures once
a second. A set that does not fit is skipped (`outcome="no_space"`,
`snapshot_fetch_skipped_no_space` once per set) and retried on the backoff
ladder. The headroom is a fixed default, not a setting.

Two unknowns are fetched without the check rather than blocked: a set
catalogued before sizes existed (size `0`, logged once as
`snapshot_fetch_size_unknown`), and a node whose free-space probe has never
succeeded (`snapshot_fetch_free_unknown`, warn, once per node start). An
unknown reading never counts as "no space": a boot-time race must not spend a
set's one warning on a false reading.

The early warning uses **the same formula on the same figure**:
`uc2_snapshot_wont_fit` reads `1` when the newest agreed set would fail the
check here, and the `Uc2SnapshotWontFit` alert keys on it — on every node,
including learners that build rather than fetch and nodes with `auto_fetch`
off — so the alert and the fetch cannot disagree.
`uc2_snapshot_newest_agreed_bytes` is that set's total size, and
`uc2_snapshot_auto_fetch_total{outcome}` counts `ok`, `refused` (this node
could not even issue the request), `timeout` (a holder stayed silent),
`no_space` and `no_holder`. Each auto-fetch also writes the usual
`snapshot_fetch` audit record with `actor = "auto"`, off the consensus agent
through a bounded queue.

## Pins: an agreed origin, and an end

### The origin must be agreed

`uc2ctl upgrade pin` now refuses `61 pin_origin_not_agreed` when the origin
is not an agreed catalog entry. A pin is the most consequential start point
the platform has — every instance of the row installs it, through a one-way
door — so an unverified or diverged origin would copy a possibly bad state to
every node. Reports take up to about 5 s after an instant, so the usual
answer is to wait and retry; a set that diverged never agrees, so take a new
instant and pin that. On an empty catalog the pin is allowed as before and
the node logs `upgrade_pin_agreement_unchecked`. The check runs at the admin
door only, never at apply: the catalog may retire entries between append and
apply, and failing an already-accepted request there would be wrong.

### Pin completion

The maintainer's rule: *the pinned snapshot and journal stay available as
long as not every FSM is upgraded; once upgraded, normal snapshot and journal
retention resume.* The platform cannot see "every FSM is upgraded" directly,
so it uses evidence it can check. A row's pin is **complete** once the
catalog lists an agreed set, with this row agreed, that

- sits **strictly above the pin record** (the frame end of the pin command
  itself), and
- was recorded as built on the pin's `to` **line**.

The record bound is the clause that matters. The catalog derives a row's
version from the pin history, so a set taken between the origin and the
moment the pin committed reads as `to` even though the old binary built it.
Only above the record is a `to` build guaranteed to have produced it, because
the old binary [stops at exactly that record](uc2-row-running-version-explained.md#the-exact-stop).
(For a `--patch` pin the old build is on the same line and is not stopped, so
a set it froze counts too — sound by the line contract alone.) One predicate,
`uc_node::catalog::pin_complete`, answers this for every reader, and since it
is a function of replicated state, every node reaches the same answer.

In practice: **after upgrading every instance, take one `uc2ctl snapshot`.**
Once it agrees, the pin is complete.

What changes when it is:

- **Start.** Before completion, a pinned row ignores start sets and takes the
  pinned install at the origin, as in `2.13.0`. For a pinned row the node
  publishes *only* a completion set, so once one is held here the attach
  takes it **instead of** the origin install — one install, not two. A
  durable state machine already at or past it on its own line installs
  nothing; a set it cannot use falls back to the pinned install. Because the
  pin view and the start-set pair are two publications by two writers, a
  pinned attach re-reads the view after reading the pair and refuses
  `PinUnreadable` (transient: retry) if it moved in between.
- **Retention and purge.** Until completion **every** node keeps the
  origin's set and holds its floor and journal purge at the origin
  (`snapshot_floor_held_for_pin`), because any instance not yet upgraded
  still has to install the origin and replay from it. After completion the
  catalog retires the origin like any other agreed set, and each node
  **releases** it — prunes the set, lets the floor move — once that node
  itself **holds a completion set** and no `to` instance attached there is
  still replaying below it. Having consumed the pin is not enough on its
  own: a node with no completion set can restart its row only through the
  pinned install, so it keeps the origin until it holds one (its own freeze
  or an auto-fetch). The floor stays increase-only throughout.

The accepted cost: a cluster that never takes an instant after an upgrade
never completes its pin, and its journal grows without bound at the origin.
The fix is the one `uc2ctl snapshot`, not waiting.

## What it costs, and what it does not do

- **No new wipe.** Every encoding change (32 B `SNAP_REPORT`, sized
  `SnapshotReport` entries, Settings v4, cluster image v5, the two slot
  words, refusal 61) is inside the `2.14.0` flag day; older images and
  reports decode with every size `0`, i.e. unknown.
- **Starting from another version's set** is not widened: only the pinned
  origin may cross lines (#66).
- **No on-demand fetch at start**, and no purge floor over remote holders —
  deferred to the backup-tier phase.
- **No rate limit** on background fetches; the stagger spreads them.
- **A wrong start set is a wrong state.** Eligibility rests on the catalog's
  agreement, and agreement over one reporter proves nothing — hence the
  single-reporter warning.

## Where to go next

- [The snapshot catalog, explained](uc2-snapshot-catalog-explained.md)
  — what "agreed" means and how a set gets there (read its errata first).
- [The cluster FSM, explained § Instants](uc2-cluster-fsm-explained.md#instants-one-position-one-set)
  — what a set is, and why it is committed by construction.
- [The row running version, explained](uc2-row-running-version-explained.md)
  — the pin record, the exact stop, and the line rule.
- [Upgrade an application](../how-to/upgrade-an-application.md) — the pinned
  procedure, ending in the instant that completes the pin.
- [Keep the journal from growing without bound § Learner-only
  clusters](../how-to/bound-journal-growth.md#learner-only-clusters-voters-fetch-and-purge-by-default).
- [Runbook § Changing a running cluster](../ops/uc2-runbook.md#changing-a-running-cluster)
  — the start rule at restart, completing a pin, reading a refused fetch.
- [`uc2ctl` § `upgrade pin`](../reference/uc2ctl.md#upgrade-pin) and
  [§ Refusal reasons](../reference/uc2ctl.md#refusal-reasons) — refusal 61.
- [Configuration § `[settings]`](../reference/configuration.md#settings) —
  `auto_fetch`.
- [Monitor a cluster § The snapshot families](../how-to/monitor-a-cluster.md#the-snapshot-families-2110)
  — the counter, the gauges and `Uc2SnapshotWontFit`.
- [The backup tier, explained](uc2-backup-tier-explained.md) — the tier as a
  holder auto-fetch tries first.
