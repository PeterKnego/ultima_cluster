# UC v2 — The committed cluster wire level: nodes upgrade one at a time

**Date:** 2026-10-19
**Status:** design brainstormed in chat 2026-10-18/19 — three clarifying
questions, three approaches, six design sections, each approved by the
maintainer as presented ("ok"). Next: the implementation plan.
**Baseline:** `origin/main` at `b361eb29` (after PR #170). `v2.14.0` is
tagged and published (wire `0.11.0`, cnc `3.4`). Nothing under
`uc_protocol` has moved since the tag, so `2.15.0` is not yet a flag day;
this design makes it one — and makes it the last of its kind for additive
wire changes.
**Requested by:** the maintainer, 2026-10-18 ("we will work next on rolling
upgrades"), as the first of two sub-projects: this one is
[#31](https://github.com/PeterKnego/ultima_cluster/issues/31), the cluster
wire level; the second is
[#66](https://github.com/PeterKnego/ultima_cluster/issues/66), the per-row
application level, which reuses the committed-level shape built here and
gets its own spec. Starting points:
`docs/notes/uc2-rolling-upgrade-compatibility-assessment.md` (the surface
inventory and the Level 0–3 ladder; written against `2.12.0`, re-checked
against today's tree in §1.2),
`docs/notes/uc2-aeron-cluster-upgrade-model.md` (the comparator), and the
FSM upgrade lifecycle spec's §9.2 Track 2.
**Release:** `2.15.0`. Wire `0.11.0` → `0.12.0`, cnc `3.4` → `3.5`.

#### Errata (plan, as discovered 2026-10-19) — read before the body

Exploration of `b361eb29` for the implementation plan corrected seven
details of the chat design. The plan implements these, not the body.

1. **§3.4, the unknown-kind drop is not in the receiver.** Replication stores
   frames as bytes; the silent `42 … applied advances` lives in
   `ClusterFsm::apply` (`uc_node/src/cluster_fsm.rs`). The fail-stop
   `ClusterRecordUndecodable` therefore sits in the **cluster agent**, which
   checks a `CLUSTER` record decodes at the committed level before handing
   it to the pure FSM. The FSM's own `42` path is untouched.
2. **§3.2 / §5.1, the receiver writes no peer slot.** The receiver knows a
   sender only by socket address when crypto is off, and `NetEvent` carries
   no header byte. The byte rides on `NetEvent::Report` (the
   `APPEND_POSITION` durable report every follower and learner already
   sends the leader), and the **consensus agent** keeps the table and writes
   the slot word, exactly as it does `reported_durable`.
3. **§5.1, the peer slot has no free word.** It is four 64-byte padded cells.
   `wire_max` goes at `+8` by widening the first cell's struct in
   `uc_log::cnc::PeerSlot`; same 256-byte stride, same writer as
   `id_and_role`.
4. **§4.1, nothing resets the band on a leadership change today.** "Reported
   in this term" is implemented as one map clear in `Action::BecomeLeader`.
5. **§4.3, `tier forget` is not forwarded.** A follower answers status 2 and
   `uc2ctl` re-sends to the leader hint. `wire raise` does the same.
6. **§2.2 / §5.2, the settings file is flat** (`uc_ctl/src/settings.rs`,
   `deny_unknown_fields`). The key is `wire_raise = "auto" | "manual"`, not
   a `[wire]` table.
7. **§5.1, the cnc compatibility sentence is backwards.** `version_compatible`
   accepts an OLDER page, so a 3.5 attacher reads a 3.4 page and a 3.4
   attacher refuses a 3.5 page. That is the standing "restart a host's
   processes together" rule for a cnc minor bump; it is per host, so it does
   not break a roll, and the how-to says so.

Also: `PathBelowCommittedMtu` is a fail-stop panic string in the consensus
agent, not an enum; the two level refusals use the same idiom in the cluster
agent (daemon exit 1, name on stderr), and the test-only range override
lives on `StartOpts` (which has `Default`), not `NodeConfig` (which has ~150
literal call sites).

## 1. Goal and locked decisions

### 1.1 Goal

Today every node↔node wire change is a cluster-wide flag day: stop every
node, swap every binary, start every node. The reason is structural, not
procedural — the log is one byte stream read by every node, and the only
way a `0.11.0` node copes with a record it cannot decode is to drop it and
advance `applied`, so a mixed cluster diverges in silence rather than
stalling. The assessment named three promises a rolling upgrade needs and
UC keeps none of: two wire versions on one UDP plane, two writers in one
log, one cluster FSM under two decoders.

This design gives the cluster one committed, monotone **wire level**, so
that a binary one level ahead can run beside a binary at the committed
level, send nothing the older one cannot parse, and the cluster raises the
level only once every member can follow. After `2.15.0`, a release that
changes the wire additively ships as a level, and a cluster upgrades by
restarting one node at a time under load.

### 1.2 What the assessment assumed, re-checked against `b361eb29`

- The datagram `flags` byte at offset 13 is still written as `0` and read
  by nobody (`uc_protocol/src/v2/datagram.rs`; the only `flags` readers in
  `uc_net`/`uc_node` are the FRAME header's `FLAG_SNAPSHOT_STANDBY`). It is
  a free carrier, authenticated under the crypto AAD. **Holds.**
- `NEW_TERM` (frame type 3) is still header-only
  (`uc_log/src/buffer.rs` `append_new_term`). **Holds.**
- `CURRENT = 0.11.0` and `MIN_COMPATIBLE = 0.1.0` are documentary: no
  receive path checks either (`uc_protocol/src/version.rs`). **Holds.**
- `CLUSTER kind = 4` is **taken** (`UpgradePin`, `2.13.0`); kinds run 1–7
  (`BackupWatermark` is 7). The level does not get a kind; it lives in the
  Settings record, which already carries the monotone-field precedent
  (`datagram_mtu`, kept `max` in `cluster_fsm.rs`). **Corrected.**
- `2.14.0` shipped read-your-writes and remote v2, so `main` is not inside
  a flag day today. **Corrected** (the assessment was written mid-`2.12.0`).

### 1.3 Locked in the brainstorm

1. **Wire level first, row level second.** Both need the same core — a
   committed, monotone level the leader raises only once every member
   supports it — so this spec builds the mechanism and #66 reuses it.
2. **Who raises: both, selectable by a replicated setting.** Automatic by
   the leader (the jumbo-rung / etcd shape, no step to forget) or
   operator-raised (`uc2ctl wire raise`, the Kafka / CockroachDB finalize
   shape, which keeps a rollback window open until the operator closes it).
   Default **auto**: a genesis cluster that never heard of the setting
   still gets rolling upgrades with no ceremony.
3. **"Member" means every node in the committed membership**, voters and
   learners, latest report wins; a non-reporter blocks the raise. A quorum
   rule (the rung's) is rejected: a minority voter still on the old binary
   would parse new-level records as unknown kinds and diverge in silence —
   the exact failure this feature exists to prevent. The rung could afford
   a quorum rule because a below-rung node only fails to raise; it never
   misparses.
4. **One level back.** `WIRE_MIN = WIRE_MAX - 1`. A cluster at level *n*
   rolls to any binary whose max is *n* or *n+1*; two levels need a middle
   binary. The code holds exactly one live gate at a time and the proof
   covers exactly the one pair the policy permits (etcd's rule). "One
   level proven, wider allowed" is rejected as a promise the gate never
   exercises.
5. **Approach 1** of three (§8): level as a Settings field, reports in the
   datagram header byte, and one rule for senders — encode at the
   committed level. A new `CLUSTER` kind plus an explicit report datagram
   (approach 2) buys nothing the free byte does not; per-connection
   negotiation (approach 3, Cassandra) is wrong for a log every node reads.

## 2. The level

### 2.1 Numbering

The level **is the wire version's minor**. `0.12.0` is level 12. From now
on a wire **minor** bump is a level and a wire **major** bump stays a flag
day. Each binary carries two constants in `uc_protocol::version`:

```
WIRE_MAX = CURRENT.minor()          // 12 in 0.12.0
WIRE_MIN = WIRE_MAX - 1             // by const arithmetic, never hand-edited
```

with the one exception that `0.12.0` has `WIRE_MIN = 12`, since nothing
below it is level-aware. `MIN_COMPATIBLE` stops being documentary: it
becomes `0.WIRE_MIN.0` and is what §2.3 enforces.

### 2.2 Where it lives: Settings v7

Settings v7 appends two bytes to the 47-byte v6 record:

| offset | field | type | meaning |
|---|---|---|---|
| 47 | `wire_level` | u8 | the committed level; `0` = keep |
| 48 | `wire_raise` | u8 | `0` = keep, `1` = auto, `2` = manual |

Both use the "0 = keep" idiom (the cnc "0 = absent" rule from the
assessment), so a settings file written before the roll — or a `settings
show` dump replayed later — cannot touch either. In the cluster FSM's
`Settings` arm:

```
wire_level = max(committed.wire_level, incoming.wire_level)   // 0 keeps, as for datagram_mtu
wire_raise = if incoming.wire_raise == 0 { committed.wire_raise } else { incoming.wire_raise }
```

Both are state-dependent, which is fine for the same reason `datagram_mtu`'s
`max` is: every replica applies the same record sequence from the same
committed state. Ruling R24's "derive from the record alone" applies to how
a **pre-level** record decodes, and it does: a v1–v6 record decodes as
`wire_level = 12`, `wire_raise = 1` (a new constant `WIRE_LEVEL_FIRST =
12`), so a `2.14.0` cluster artifact and a fresh genesis read the same
value. `genesis_default()` seeds `wire_level = WIRE_MAX` of the genesis
binary and auto.

`wire_level` never appears in a settings TOML file — leader-owned, like
`datagram_mtu`. The file gains one key, `[wire] raise = "auto" |
"manual"`; any other value is refused at the leader's door with the
existing `47 settings_bounds`. The cluster image version follows the
record (v6 → v7, decided in the plan).

### 2.3 The invariant and its two refusals

A node runs only while

```
WIRE_MIN <= wire_level <= WIRE_MAX
```

The check sits in **one** place: the `uc2-cluster` agent, immediately after
it applies a Settings record. That one place covers both moments it
matters:

- **at boot**, when the agent's walk reaches the committed record before
  the node is JOINED (and so before any service can attach — the
  `NodeBooting` gate already holds attach until the walk is consumed);
- **live**, when a raise commits under a node that was restarted on the
  old binary in the race after its last report (§4.1) — the raise record
  is encoded at the old level (§3.3), so the old binary decodes it, reads
  a level above its max, and stops.

The receiver needs no check: replication stores frames as bytes and
decodes `CLUSTER` payloads only at the cluster agent, so a too-old node
replicates correctly right up to the record that tells it to stop.

Two named startup/fail-stop refusals, the analogs of
`PathBelowCommittedMtu`:

| refusal | condition | meaning |
|---|---|---|
| `WireLevelAboveBinary` | `wire_level > WIRE_MAX` | the binary is too old for this cluster; upgrade it |
| `WireLevelBelowBinary` | `wire_level < WIRE_MIN` | the binary is too new; install the release at level `wire_level + 1` first (the message names it) |

### 2.4 A consequence stated plainly

The second refusal applies to **stop-all** upgrades too. Today any
stop-all upgrade works from any older version. From `0.13.0` on, a cluster
two levels behind must step through a middle binary even with every node
stopped, because the new binary no longer has the older level's encoders
(§7.2) and cannot tell at boot whether a peer is still old. This is etcd's
and CockroachDB's rule as well. Levels are rarer than releases (§7.2), so
this should bite rarely, and the refusal names the middle release.

## 3. Reports, and the one rule for senders

### 3.1 The report is the datagram header byte

Every node writes `WIRE_MAX` into the `flags` byte at offset 13 of every
datagram it sends — data, acks, NAKs, durable reports, probes, snapshot
sessions, handshakes. The byte is inside the 16-byte header the crypto
plane authenticates as AAD, so forging a report costs the same as forging
any header. No new kind, no new timer; the sender change is one constant.
The byte is renamed `wire` in `DatagramHeader`.

### 3.2 What a receiver does with it

Decided before anything else in the datagram is read:

| byte | action |
|---|---|
| in `[WIRE_MIN, WIRE_MAX + 1]` | accept. The upper bound is the one-level-back promise: a peer one above me is allowed; a peer further above should have refused to start |
| `0` | refuse the whole datagram by name and count it, `reason = prelevel`. A pre-level `0.11.0` peer — the first flag day's old side |
| anything else | refuse and count, `reason = out_of_range` |

This is the first time a mixed cluster is **refused** rather than
misparsed. The counting follows the wrong-length `STATUS` precedent
(`statuses_refused`).

On the leader, in addition: write the byte into the peer's `wire_max` word
in the peer-slot band (§5.1), **only when it changed**. The receiver
already resolves the peer slot for durable reports, so the hot-path cost is
one compare and a rare store.

### 3.3 The one rule for senders: encode at the committed level

**Every encoder writes at the committed level, never at the binary's
level.** A `0.13.0` leader at level 12 emits Settings v7, no level-13
kinds, no level-13 fields; a `0.13.0` follower at level 12 sends level-12
reports. The committed level is published by the cluster agent into a cnc
word at apply (§5.1), the way the live `payload_ceiling` word is, so the
sender, the consensus agent and `uc2ctl` read it without touching the FSM.

The gate is **exactly one per feature**, `if level >= 13 { ... }` at the
point of use (CockroachDB's version gates), and it is deleted as
always-true when level 14 lands (§7.2). Settings encoding is the first
instance: a `settings_version_for(level)` table maps level → record
version, trivially `12 → 7` in this release.

This rule is what makes the old binary safe: it is never shown anything it
cannot parse. The additive tolerances the assessment listed as Level 1
(tail growth, unknown-kind drop-and-count) are therefore **backstops, not
the mechanism**, and tail growth is not implemented.

### 3.4 The backstop changes sign

The three `_ => {}` arms that silently drop an unknown `CLUSTER` kind
today (`uc_net/src/receiver.rs`, three sites) become a **named fail-stop**
plus a counter. Under the level, a kind above the committed level cannot
arrive from a correct peer, so an arrival means corruption or a bug, and
the posture for a violated committed invariant is fail-stop by name, not a
silent drop. The same applies to a Settings record whose version is above
`settings_version_for(wire_level)`.

### 3.5 Reserved for later: eight bytes in `NEW_TERM`

`NEW_TERM` gets a body of `wire_level u32 ‖ reserved u32`, written as
zeros and ignored. This is the carrier the assessment's **Level 3**
(per-term frame layouts) needs: a future header relayout stamps the layout
at term boundaries, never per frame — the `2.11.0` relayout's 27 % lesson.
Reserving it now is what lets that relayout be a level instead of a flag
day. Nothing reads it in this release; the frame is 64 B instead of 32 B.

## 4. The raise

### 4.1 The predicate

Evaluated by the **leader's consensus agent once per duty cycle**, from
two things it already has: the committed membership in the cluster FSM's
published view, and the per-peer `wire_max` words (§3.2). For every member
id in the membership, voter or learner, the word must be non-zero. The
peer-slot band is leader-authoritative and starts from zero on the leader
flag's rising edge, so non-zero means "reported in this term" — no timer.

```
target   = min(WIRE_MAX of self, wire_max of every member)
raisable = every member reported  &&  target > wire_level
```

Under one-level-back, `target <= wire_level + 1`: a member two above would
have refused to start (§2.3). So a raise is always by exactly one.

### 4.2 Auto mode

When `raisable`, the consensus agent appends a Settings record copied from
the committed one with `wire_level = target` — the same path the jumbo
discovery uses to commit a rung. It rides the single-in-flight lane with
every other `CLUSTER` command: if one is in flight, try again next cycle.
Nothing is retried on failure because the predicate is re-evaluated from
scratch each cycle, and a node restarted on the old binary meanwhile makes
it false again. That is the whole rollback story inside the window: there
is nothing to undo, the raise simply stops being raisable.

### 4.3 Manual mode

Same predicate. `uc2ctl wire raise` is **admin op 12**, leader-only and
forwarded like the others, audited as `wire_raise`. It appends the
identical record when `raisable`, answers retry while a `CLUSTER` command
is in flight, and refuses with one new reason:

| refusal | meaning |
|---|---|
| `63 wire_holdout` | the first member below `target`, with what it reported, or `no report` if it has not reported this term |

In auto mode the verb still works and is idempotent ("already at level
N"), so a script that always runs it is harmless. Mode is flipped with
`[wire] raise = "manual"` in the settings file; the how-to says to set it
**before** the first node restarts, because a flip after the last node
came up can lose the race with an auto raise already appended.

### 4.4 Membership changes mid-roll

No special case. The predicate reads the committed membership at each
evaluation, so an added member blocks the raise until it reports, a
removed member stops counting, and a member added below the level is
refused at its own apply of the committed record by §2.3 — not by the
leader. A leader change mid-roll re-fills the new leader's table within a
duty cycle from the reports every member already sends it.

## 5. The operator surface

### 5.1 cnc page, 3.4 → 3.5, additive

| offset | word | writer | meaning |
|---|---|---|---|
| 3992 | `wire_level` | `uc2-cluster` agent, at apply | the committed level |
| 4000 | `wire_binary` | node, boot-once | `WIRE_MIN << 8 \| WIRE_MAX` of this node's binary |
| peer slot `+8` | `wire_max` | receiver, on change | the peer's reported max; leader-authoritative, zero on followers |

Both page-1 words sit on the 3968 line beside `payload_ceiling`, which
already mixes writers at a slow cadence. A 3.4 client reads a 3.5 page
unchanged: `version_compatible` accepts an older minor.

### 5.2 Two verbs and one settings key

- `uc2ctl wire show` — the committed level, the mode, this node's binary
  range, and on the leader the per-member table with the raise verdict:
  `raisable`, or the holdout and what it reported.
- `uc2ctl wire raise` — §4.3.
- `[wire] raise = "auto" | "manual"` in the settings file; `uc2ctl
  settings show` prints it with the rest. `uc2ctl status` prints
  `wire_level=` and `wire_binary=` from the two page words.

### 5.3 Metrics

| series | type | meaning |
|---|---|---|
| `uc2_wire_level` | gauge | the committed level; a fleet-wide constant once settled |
| `uc2_wire_binary_min` / `uc2_wire_binary_max` | gauge | this binary's range, so `min by ()` over the fleet gives the raise target |
| `uc2_wire_member_max{peer}` | gauge | the leader-authoritative band; zeros on followers |
| `uc2_wire_datagrams_refused_total{reason}` | counter | §3.2, `prelevel` or `out_of_range` |

### 5.4 One alert

`Uc2WireLevelBelowMembers`: `uc2_wire_level < min(uc2_wire_binary_max)`
across the fleet for 30 minutes. In manual mode it is the reminder that the
window is open and the raise is due. In auto mode it can only mean a member
is not reporting or the leader is not raising, and `wire show` names which.
It gets a `RULE_BUILDERS` entry in `scripts/m10_alert_fire.sh` so the M10
completeness cross-check still passes, and lives in
`packaging/prometheus/uc2-alerts.yml` with the rest.

### 5.5 Docs

- `docs/how-to/upgrade-a-cluster.md` gains the **rolling procedure**: set
  manual if you want the window; restart one node on the new binary; wait
  for `can_serve`; check `wire show`; repeat; raise (or watch auto do it).
  Its flag-day section gains the `2.15.0` entry (§7.1).
- `docs/reference/semver-policy.md` gets the level rule (§2.1, §2.4, §7.2).
- A new explainer, `docs/notes/uc2-wire-level-explained.md`.
- `docs/notes/uc2-rolling-upgrade-compatibility-assessment.md` gets a
  "taken up" note pointing here; `docs/BACKLOG.md` direction 3's
  version-negotiation line says the same.
- `RELEASES.md` / `docs/releases.md` at the cut, per the release rule.

## 6. Proof

### 6.1 Unit and property level (`uc_protocol`, `uc_node`)

- Settings v7 round trip; every v1–v6 fixture decodes to level 12 and
  auto; the FSM keeps `max` on the level and treats `0` as keep for both
  fields; `47` refuses a bad `[wire] raise`.
- The datagram byte: encode writes `WIRE_MAX`, decode returns it, and the
  three receiver cases of §3.2 are a table test, run on the crypto path too
  so the AAD provably covers the byte.
- The raise predicate is a pure function of (membership, reported table,
  own max, committed level) with a table test: missing report blocks, a
  holdout blocks, target is `level + 1` and never more.
- A **golden corpus**: checked-in bytes for every record and frame kind a
  level-12 binary emits, asserted equal to what the current encoders
  produce at level 12. From `0.13.0` on, the corpus is what pins "encode at
  the committed level": a release that changes a level-12 encoding breaks
  the test. Refreshing it is a release-checklist step (§7.2).
- One fuzz target, `uc_protocol_compat` (the 27th): arbitrary bytes decoded
  under an arbitrary level, asserting no panic and that nothing above the
  level is ever accepted.

### 6.2 In-process cluster tests

Nodes at different levels in one binary need a hook: `uc_node` gets a cargo
feature, `wire-level-override`, off by default, that lets a test set a
node's `[min, max]`. Scenarios: a holdout blocks the raise and the raise
lands once it restarts at the new max; a node at the old max fail-stops by
name when the raise commits; a node two above refuses to start; manual
holds and `wire raise` lands it; a byte-0 datagram is refused and counted;
a leader change mid-roll re-fills the table and raises.

### 6.3 The two-binary crashtest — the gate

In `uc_crashtest` behind `hard-crash-tests`: three nodes on the old binary
under load; restart one at a time on the new binary; SIGKILL one mid-roll;
assert linearizable throughout and the raise committed by the end; then
start an old binary and assert it refuses by name. For `2.15.0` the "new"
side is the **same tree with the override set to `[12, 13]`**, since no
real level 13 exists yet — the only way to prove the mechanism in the
release that introduces it. From `2.16.0` on the old side is the previous
release's tarball, which makes the gate prove the actual pair the policy
permits.

### 6.4 Fleet row

Via `m14_fleet_gate.py --base-tree`: the crashtest's roll on
4 × c6id.2xlarge, with the commit gap per restart recorded. **Functional
clauses only, no rate bar** — the standing bar question already has four
unadjudicable rate bars and a fifth would not help.

### 6.5 Untouched

The consensus kernel, `uc_sim` and the Lean model. The level is cluster
FSM state and never enters a vote or a commit decision.

## 7. Scope, the flag day, and what stays a flag day

### 7.1 `2.15.0` is a flag day, and the last of its kind

It ships stop-all: Settings grows, the datagram byte goes live, `NEW_TERM`
gets its body, cnc gains two words. A `0.11.0` peer's datagrams carry byte
`0` and are refused by name and counted — the first flag day a mixed
cluster notices instead of misparsing. No `snapshots/` wipe, no artifact
envelope change, cnc additive. The first real roll is whatever release first
bumps to `0.13.0`.

### 7.2 Release discipline from here

For `cut-a-release.md` and `semver-policy.md`:

- A release that adds no gated wire feature keeps `CURRENT` and rolls with
  **no step**.
- A release that adds one bumps the wire minor, writes exactly one
  `if level >= N` gate, deletes the `N - 1` gate as always-true, refreshes
  the golden corpus, and keeps the v1…v(N-1) **decoders** forever (as
  today) while dropping the `N - 1` **encoders**.
- `WIRE_MIN` is `WIRE_MAX - 1` by const arithmetic.
- A consensus safety fix defaults to a gated level like any feature — the
  fixed binary runs pre-fix until the raise — and the maintainer may
  declare it a flag day in the release notes instead when running pre-fix
  is unacceptable. A per-release judgment, not a mechanism.

### 7.3 Stays a flag day, by choice

The wire major and the cnc major. The 16-byte datagram header length and
the DATA self-locating contract. The 32-byte frame header length. Crypto on
or off per cluster. cnc page growth beyond the reserved bands.

### 7.4 Out of scope here

- #66, the per-row application level: reuses this shape with `ctx.level()`
  in `ApplyCtx`; next spec. The `SNAP_BEGIN` per-row `VERSION` equality
  stays as is until then.
- Level 3's per-term frame layouts, beyond the eight reserved bytes (§3.5).
- The client↔gateway remote protocol, which versions itself at `HELLO`.
- Leadership transfer (the other half of BACKLOG direction 3).

### 7.5 Residuals, recorded as known

- A dead learner left in the membership holds the level in auto mode until
  `uc2ctl remove` takes it out — by design; the alert and `wire show` name
  it.
- A mode flip to manual after the last node restarts can lose to an auto
  raise already appended; set manual before the roll.
- A stop-all upgrade across two levels must step through a middle binary
  (§2.4).
- The first roll's gate (§6.3) proves the mechanism against a synthetic
  old build, not a shipped one; the real-pair gate starts at `2.16.0`.

## 8. Approaches considered

1. **Level as a Settings field, reports in the datagram header byte,
   encode at the committed level** — chosen. One new rule, two bytes in a
   record with the monotone precedent, zero new kinds.
2. **A new `CLUSTER` kind `WireLevel` plus a pairwise `WIRE_REPORT`
   datagram on a timer.** Same rules, cleaner on paper; one more kind to
   snapshot, one more sender timer, one more report path to prove, and the
   mode still needs the Settings record. The free byte gives a report per
   packet already.
3. **Per-connection negotiation** (Cassandra). Rejected in the assessment
   and here: the log is one stream read by every node, so the level has to
   be a property of the cluster, not of a pair.

Precedents drawn on: etcd (leader-computed minimum cluster version, one
minor at a time), CockroachDB (version gates at the point of use, one-way
finalize, `preserve_downgrade_option`), Kafka (`metadata.version` as an
operator-raised floor), Aeron (a version byte dropped-and-counted; app
version stamped at term boundaries).
