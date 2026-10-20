# The committed cluster wire level, explained

*Written 2026-10-19 for the wire level (issue #31), which ships in
`2.15.0` with wire `0.12.0` and cnc `3.5`. Spec:
`docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md`; read its
errata block (items 1–12) before the body, because several behaviours below
are errata rather than body text. This note carries the argument in plain
language.*

The operator's procedure is
[Upgrade a cluster § Rolling upgrades since 2.15.0](../how-to/upgrade-a-cluster.md#rolling-upgrades-since-2150).
The assessment that started this is
[Rolling upgrades: the compatibility assessment](uc2-rolling-upgrade-compatibility-assessment.md),
and the issue is [#31](https://github.com/PeterKnego/ultima_cluster/issues/31).

## What a level is

Until `2.15.0` every change to the node-to-node wire was a flag day: stop
every node, swap every binary, start every node. A rolling upgrade needs
three promises UC kept none of: two wire versions on one UDP plane, two
writers in one log, and one cluster FSM read by two decoders. The third is
the dangerous one. A `0.11.0` node that meets a cluster record it cannot
decode drops it and moves on, so a mixed cluster does not stall, it
**diverges in silence**.

The wire level keeps all three promises with one committed number. The
level **is the wire version's minor**: `0.12.0` is level 12. It lives in the
replicated `Settings` record (`wire_level`, a v7 field), so every node agrees
on it and it survives in the cluster artifact. Every binary speaks a range,
`WIRE_MIN..WIRE_MAX` (`uc_protocol::version`), and the rule is:

- every node **encodes at the committed level**, never at its own max, so
  an old binary is never shown anything it cannot parse (promises 1 and 2);
- the level rises only when **every member** can follow, so no member is ever
  asked to decode a record above its max (promise 3);
- a node whose range does not contain the committed level **stops by name**
  rather than run.

From `2.15.0` on, a wire **minor** bump is a level and rolls; a wire
**major** bump stays a flag day.

## The one rule for senders

Every encoder writes at the committed level. A `0.13.0` leader on a level-12
cluster writes level-12 `Settings` records and no level-13 kinds; a
`0.13.0` follower sends level-12 reports. The `uc2-cluster` agent publishes
the committed level when it applies a `Settings` record: in-node consumers
(the leader's raise logic in the consensus agent) read it from the cluster
agent's published view, and outside the node `uc2ctl` and `/metrics` read
the cnc word (`wire_level` at offset 3992). The datagram byte is a different
thing: every sender writes its own `WIRE_MAX` there, not the level.

Each feature that changes the wire gets exactly one gate at its point of use
(`if level >= N`), deleted as always-true when the next level lands. The
first instance is `settings_version_for(level)` in
`uc_protocol::v2::settings`, which maps level 12 to `Settings` version 7.

Because the old binary is never shown anything new, the older tolerances
(drop an unknown kind, ignore a longer tail) are no longer the mechanism.
They become a backstop, and the backstop changed sign: a `CLUSTER` record
that does not decode at the committed level is now a fail-stop,
`ClusterRecordUndecodable`, in the cluster agent, because under the level a
correct peer could not have sent it.

## Reports and refusals

**The report is a byte every datagram already has.** Byte 13 of the 16-byte
datagram header was a `flags` byte, always written as zero and read by
nobody. Since `0.12.0` it is `wire`: every datagram a node sends (data, acks,
NAKs, durable reports, probes, snapshot sessions, handshakes) carries the
sender's `WIRE_MAX`. The byte sits inside the header the crypto plane
authenticates as AAD, so forging a report costs what forging any header
does. No new kind, no new timer.

**A receiver decides on the byte before reading anything else:**

| byte | what happens |
|---|---|
| in `[WIRE_MIN, WIRE_MAX + 1]` | accepted. One above my max is a peer rolling ahead of me, which is allowed |
| `0` | refused whole and counted as `prelevel`: a pre-level `0.11.0` peer |
| anything else | refused whole and counted as `out_of_range` |

Both refusals count in `uc2_wire_datagrams_refused_total{reason}` and are
named on stderr as `wire_refused`, throttled per source. On a crypto
cluster the stderr line is written only for a source that resolves to a
configured member; the counter is bumped before authentication, so traffic
from anyone can drive `prelevel` there. Do not page on it alone.

The leader keeps what it hears. The byte rides on each member's durable
report (`APPEND_POSITION`) to the consensus agent, which writes it into the
member's peer slot (`wire_max` at slot `+8`) when it changes. The table is
**leader-authoritative** and is cleared when a new leader takes office, so a
non-zero entry means "reported in this term".

**Two fail-stops guard the range, both in the cluster agent:**

- **`WireLevelAboveBinary`**: the committed level is above this binary's
  max. The binary is too old for the cluster. It is checked immediately,
  at every `Settings` apply, at every artifact install and at boot, because
  nothing after such a record is safe for this binary to read. Upgrade the
  node to a release whose wire minor is at least the level, then start it.
- **`WireLevelBelowBinary`**: the committed level is below this binary's
  min. The binary is too new. Install the release at level `wire_level + 1`
  first; the message names it. This one is judged only once the node has
  **JOINED** (leader known, commit learned, the cluster agent's walk at that
  commit), because a node replaying its own log or an old artifact walks
  through `Settings` records from before a raise, and stopping on history
  would stop a healthy node.

Both are process exits (the daemon exits 1 with the name on stderr) and both
write a `wire_level_fault` record naming the fault, the level and the
binary's range.
Neither is a wipe: nothing in the instance directory is wrong, only the
binary.

One case refuses without stopping. A lone joiner two or more levels above a
live cluster (a `[14,15]` binary on a level-12 cluster of `[12,13]` members)
refuses every datagram it receives, because 13 is below its min, and every
peer refuses its datagrams, because 15 is above their max plus one. It never
JOINs, so it never reaches the Below judgement. It waits in `NodeBooting`,
printing throttled `wire_refused` lines that name its range.

## The raise

**The predicate** is a pure function (`uc_node::wire_raise_target`), shared
by the leader's auto raise, the `wire raise` door and `uc2ctl wire show`:

```
target   = min(leader's own WIRE_MAX, every member's reported wire_max)
raisable = every member has reported this term  &&  target > wire_level
```

"Every member" is the whole committed membership, voters **and** learners,
latest report wins. A quorum rule is not enough here (the jumbo rung uses
one): a minority voter still on the old binary would be asked to decode
records it cannot read. Under one level back, a raise is always by exactly
one.

**Auto mode** (`wire_raise = auto`, the genesis default) has the leader's
consensus agent check every 100 ms and, when raisable, append a `Settings`
record copied from the committed one with the new level. It rides the same
single-in-flight lane as every other `CLUSTER` command. Nothing is retried:
the predicate is evaluated again on the next check, and a node restarted on
the old binary in the meantime simply makes it false.

**Manual mode** (`wire_raise = manual`) holds the level until an operator
runs `uc2ctl wire raise`. That gives a **rollback window**: until the raise
commits, any node can go back to the old binary, because nothing has been
written that the old binary cannot read. The raise is the point of no
return.

**Set manual before the first restart.** If auto mode is still on when the
last old node is replaced, the leader may raise within a check interval. A
switch to manual made after that can lose the race to a raise already
appended.

A node that rolled back after its last report is covered too. The raise
record is encoded at the old level, so the old binary decodes it, reads a
level above its max and stops with `WireLevelAboveBinary`. It never runs
silently beside a level it cannot speak.

**A newcomer can race an auto raise.** A `[13,14]` node joining an auto-mode
level-12 cluster of `[12,13]` members ends one of two ways: the leader raises
to 13 first (the newcomer's report of 14 makes the target `min = 13`) and the
newcomer runs in range, or the newcomer JOINs first and stops with
`WireLevelBelowBinary`. Both keep the invariant: before JOINED a node neither
serves nor appends anything level-dependent, and the end state has every
member in range.

## One level back, stop-all included

A binary runs one level back: `WIRE_MIN = WIRE_MAX - 1` by const arithmetic.
`0.12.0` is the one exception, with `WIRE_MIN = WIRE_MAX = 12`, because
nothing below it is level-aware. So a cluster at level *n* rolls to any
binary whose max is *n* or *n + 1*, and the test suite proves exactly that
pair. Two levels need a middle release.

That rule applies to **stop-all** upgrades too, from `0.13.0` on. A binary
that has dropped level *n − 1*'s encoders cannot run a cluster committed at
*n − 1*, whether its peers are running or not: it boots, JOINs and stops
with `WireLevelBelowBinary`, naming the middle release. This is etcd's and
CockroachDB's rule as well. Levels are rarer than releases, because a
release that adds no gated wire feature keeps its wire minor and rolls with
no step at all.

## Operator surface

- **`uc2ctl wire show`** prints `wire: level=L binary=MIN..MAX raise=<mode>`.
  On the live leader it adds one `member` line per other member and a
  verdict: `raisable to N`, `at level (nothing above L)`, or the holdout.
  The verdict names one holdout, this node first and then the peer slots in
  order. ([reference](../reference/uc2ctl.md#wire-show))
- **`uc2ctl wire raise`** is admin op 12, leader-only (a follower answers
  `retry`). It works in either mode. A cluster already at every member's
  level answers success and appends nothing, so a script that always runs it
  is harmless. It refuses `63 wire_holdout` only for a member with no report
  this term or one below the target, and writes one `wire_raise` audit
  record with `level=` and, on refusal, `holdout=` (on an accepted raise,
  `target=`).
  ([reference](../reference/uc2ctl.md#wire-raise))
- **`wire_raise = "auto" | "manual"`** is a key in the flat settings file
  for `uc2ctl settings apply`; absent means keep the committed mode. Put it
  in the cluster's full settings file: the apply replaces the whole record,
  and every other absent key reverts to its default.
  `wire_level` is leader-owned and never a file key. `settings show` prints
  both, and `uc2ctl status` prints a `wire:` line.
- **Metrics:** `uc2_wire_level`, `uc2_wire_binary_min`,
  `uc2_wire_binary_max`, `uc2_wire_member_max{peer}` (leader-authoritative,
  zeros on a follower) and `uc2_wire_datagrams_refused_total{reason}`.
- **Alert:** `Uc2WireLevelBelowMembers`, when the committed level stays below
  every binary's max for 30 minutes. In manual mode it means the raise is
  due; in auto mode a member is not reporting or the leader is not raising,
  and `wire show` on the leader says which.
  ([monitoring](../how-to/monitor-a-cluster.md))

## What is still a flag day

`2.15.0` itself is one, and it is the last for an additive wire change. The
`0.11.0` side carries byte 0 and is refused by name and counted (`prelevel`),
which makes it the first flag day where a mixed cluster is refused rather
than misparsed.

These stay flag days by choice: the wire **major** and the cnc **major**; the
16-byte datagram header's length and the DATA self-locating contract; the
32-byte frame header's length; turning crypto on or off; and cnc page growth
beyond the reserved bands. A cnc **minor** bump is not a cluster flag day
but a per-host one: a node and every process attached to its instance
directory restart together, because an older attacher refuses a newer page.
That fits inside a roll.

A future relayout of the frame header could become a level instead of a flag
day if each term stamps its layout. `NEW_TERM` carries an 8-byte body for
that since `0.12.0` (`wire_level u32 ‖ reserved u32`), written as zeros and
read by nobody yet.

## Precedents

- **etcd**: the leader computes the cluster version as the minimum over the
  members, raises it one minor at a time, and a member below it refuses to
  join. UC's auto mode and its one-level-back rule are this shape.
- **CockroachDB**: version gates consulted at the point of use, a one-way
  finalize, and `cluster.preserve_downgrade_option` to hold the old version
  open. UC's `if level >= N` gates and manual mode are this shape.
- **Kafka**: `metadata.version`, a floor record in the replicated metadata
  log that the operator raises once binaries are rolled. UC's level as a
  field in a replicated record is this shape.
- **Aeron Cluster** stamps the application's version into the log at every
  term boundary and checks it on read; UC reserves the same carrier in
  `NEW_TERM`. Between consensus peers Aeron checks no version at all and
  relies on additive SBE fields, where UC refuses on the header byte instead.
  ([the Aeron note](uc2-aeron-cluster-upgrade-model.md))

The first-party sources for etcd, CockroachDB and Kafka are cited in the
assessment's
[external precedents](uc2-rolling-upgrade-compatibility-assessment.md#external-precedents-first-party-sources).

## Where to go next

- [Upgrade a cluster](../how-to/upgrade-a-cluster.md): the rolling
  procedure and the `2.15.0` flag day.
- [Versioning and the semver promise](../reference/semver-policy.md): the
  level rule as policy.
- [Wire protocol](../reference/wire-protocol.md): the header byte, `Settings`
  v7 and the `NEW_TERM` body.
- [`docs/VERIFICATION.md`](../VERIFICATION.md): the two-binary rolling
  crashtest and the `uc_protocol_compat` fuzz target.
