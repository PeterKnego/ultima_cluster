# Wire protocol

The node-to-node UDP format: datagram kinds, log frame layout, and the version
gates. Defined in `uc_protocol::v2`.

This page states the wire surface. The rationale for byte positions and the
self-locating header is in [Architecture](../ARCHITECTURE.md).

## Version

| Constant | Value |
|---|---|
| `version::CURRENT` | `0.7.0` |
| cnc page version | 3.1 (FSM identity + log time, 2.11 pending: the name + hash line at boot, the version word at attach, `log_time_ns`, per-row `timers_pending`) |

The cnc page carries its own version gate, `CNC_V2_VERSION`, which is
independent of this one. cnc 3.1 changed the same-host shmem layout only
(the once-reserved slot line 7, plus two previously-unused words —
`log_time_ns` and the per-row `timers_pending`). The UDP datagram format
moved to 0.7.0 for **four features** shipping on the same unreleased `2.11.0`
flag day. FSM identity: `SNAP_BEGIN` swapped its `services_declared` bitmask
for a per-row identity-hash array plus a per-row version array. Log time and
timers: the log frame header was **relaid** to carry a leader-written
`time_ns` stamp, with a `TIMER` (5) frame type beside it. The replicated
schedule table, built on that. And **the cluster FSM**, which subsumed the
table's own carriage: frame type `4` becomes `CLUSTER`, a kind-dispatched
command carrying membership, the schedule table or the settings record;
`SCHEDULE_TABLE` (6) and `SNAP_TABLE` (21) are **retired before shipping**;
`SNAP_BEGIN` loses its trailing `config` tail and becomes fixed-length
(layout **V4**), because the snapshot session now carries the cluster FSM's
own artifact under the reserved `service_id = 255`. Every other datagram is
byte-identical to 0.6.0. `CURRENT` is
documentary and is not itself checked on any receive path (see
`version.rs`); the two version lines remain independent of each other.

None of `0.7.0`'s intermediate shapes ever shipped — the whole of `2.11.0` is
one unreleased flag day — but every retired number is **reserved, never
reassigned**: `FRAME_TYPE_SCHEDULE_TABLE_RETIRED = 6`,
`DGRAM_KIND_SNAP_TABLE_RETIRED = 21`, `SNAP_BEGIN_LAYOUT_V3 = 2`.

`app_id`, `instance_id`, and the protocol version are checked at every IPC
entry point. A mismatched `app_id` means the wrong cluster; a changed
`instance_id` means the node restarted since the attaching process last looked;
a protocol mismatch is refused.

## Datagram header

| | |
|---|---|
| `DATAGRAM_HEADER_LEN` | 16 B |
| `MTU_DEFAULT` | 1408 B |

The header is authenticated as AAD when wire crypto is enabled, and carries a
`key_epoch` field for the group key.

## Datagram kinds

### Replication and control

| Kind | Name | Scope |
|---|---|---|
| 1 | `DATA` | group |
| 2 | `HEARTBEAT` | group |
| 3 | `NAK` | pairwise |
| 4 | `STATUS` | pairwise |
| 5 | `APPEND_POSITION` | pairwise |
| 6 | `COMMIT_POSITION` | group |
| 7 | `REQUEST_VOTE` | pairwise |
| 8 | `VOTE` | pairwise |
| 9 | `TERM_MAP` | pairwise |
| 10 | `READ_PROBE` | group |
| 11 | `READ_PROBE_ACK` | pairwise |

### Snapshot sessions

| Kind | Name | Scope |
|---|---|---|
| 12 | `SNAP_BEGIN` | pairwise |
| 13 | `SNAP_CHUNK` | pairwise |
| 14 | `SNAP_NAK` | pairwise |
| 15 | `SNAP_DONE` | pairwise |
| 22 | `SNAP_REQUEST` | pairwise |
| 23 | `SNAP_REDIRECT` | pairwise |

Kind **21** was `SNAP_TABLE` — the schedule table carried beside a session —
and is **retired** (`DGRAM_KIND_SNAP_TABLE_RETIRED`). The table now rides the
session as part of the cluster artifact; the number is reserved so it is never
reassigned to something a pre-release build might misread.

A session is a **stream of artifacts**: one `SNAP_BEGIN` per declared FSM,
ascending by row, then one for the **cluster artifact** under the reserved
`service_id = 255` — always last, and deliberately outside the declared mask
so it is never mistaken for a row. Chunk offsets are stream-global, so
`SNAP_NAK` repair is byte-identical to `0.5.0`/`0.6.0`. The joiner installs the
cluster artifact **before** its purge floor advances, which is what makes
membership, the schedule table and the settings record present before it can
serve a read or win an election.

#### `SNAP_BEGIN` body (wire 0.7.0, layout V4)

`SNAP_BEGIN` opens (or extends) one artifact of a snapshot session; its body
is the only wire carrier of FSM identity (commands are broadcast, durable
reports are aggregates — neither names an FSM). Since the cluster FSM it is
**fixed-length**: `SNAP_BEGIN_FIXED_LEN = 120`, no tail. `SNAP_DONE` echoes the
same body as its ack, so it carries this layout too, with no separate change.

| bytes | field | width | meaning |
|---|---|---|---|
| 0..4 | `session` | u32 | session id |
| 4 | `layout` | u8 | body discriminator; `3` = `SNAP_BEGIN_LAYOUT_V4` (0.7.0 as shipped). `1` = `SNAP_BEGIN_LAYOUT_V2` (0.6.0) and `2` = `SNAP_BEGIN_LAYOUT_V3` (the intermediate 0.7.0 shape with a carried config, never released) are refused **by name** at the node layer |
| 5 | `service_id` | u8 | the row this artifact belongs to, or `255` = the **cluster artifact** |
| 6..8 | — | 2 B | zero (pads `snapshot_pos` to u64 alignment) |
| 8..16 | `snapshot_pos` | u64 | this artifact's snapshot position |
| 16..24 | `total_len` | u64 | this artifact's byte length |
| 24..88 | `identity` | `[u64; 8]` | the sender's per-row FSM identity hash (FNV-1a 64 of the declared name), in row order; `0` = row undeclared. Replaces 0.6.0's `services_declared` bitmask — the mask is now derived (`SnapBeginBody::declared_mask`) |
| 88..120 | `version` | `[u32; 8]` | the sender's per-row attached packed version, from the cnc slot; `0` = no service attached / unversioned |

The trailing `config_len` + `config` tail that the intermediate V3 shape
carried is **gone**. Membership was the last thing read live off the shipper at
ship time; it now rides the cluster artifact, tagged with the position it was
committed at, like every other piece of cluster state. `read_snap_begin_body`
**ignores** trailing bytes past the fixed part rather than refusing them, so a
peer speaking an older `0.7.0` shape is refused by its `layout` with a name
instead of silently by a length check.

The receiver compares `identity` **positionally**: for each row `r`,
`identity[r]` must equal the receiver's own hash for row `r` (both zero =
both undeclared). A mismatch refuses the session **by name** ("row 1:
ours=orders, theirs=kv" — a hash the receiver recognizes anywhere in its own
list prints as that name, an unknown one as its hash) and counts
`uc2_snapshot_refused_declared_set_total`; this subsumes the 0.6.0 declared-set
check (a set difference is a positional difference). `version` is compared
per row only when **both** sides are non-zero; a mismatch refuses by name
with both versions and counts the new `uc2_snapshot_refused_version_total`.
A 0.6.0 sender's body (34 B fixed) is shorter than 120 B, so the receiver
drops it by the same length check that drops a 0.5.0 body today — the
standing flag-day rule: a mixed cluster stalls a joiner rather than
installing a wrong or half-checked artifact. Artifacts still route by row,
unchanged.

#### One session, one position (wire 0.7.0)

Since coordinated snapshot instants (2.11 pending) a session ships **one
set**, and a set is the artifacts at **one** instant: every `SNAP_BEGIN` of a
session must carry the same `snapshot_pos`, and a session whose `BEGIN`s
disagree is refused outright and counted as
`uc2_snapshot_refused_position_total`. The sender enforces it at assembly —
`snapshot_set_at` looks each artifact up by its **file name** at the target
position (`snap-<pos>.ultsnap`, `snap-<pos>.ultcluster`), never by whatever a
row's live cnc `snapshot_pos` word happens to read, because that word runs
ahead the moment a later instant completes — and declines the session by name
(`floor 0` / `missing artifact` / `set does not cover declared`) rather than
shipping a set assembled from two different points of the log.

The artifact bytes are shipped **verbatim**, which includes the 16-byte
`ULTSNAP1` envelope every artifact file starts with. That is a file format,
not a wire format (see
[Instance directory § Snapshot artifacts](instance-directory.md#files)), but
it rides the session unchanged, so the receiver writes a file byte-identical
to the sender's and verifies the envelope at install.

#### `SNAP_REQUEST` body (wire 0.7.0)

A voter's pull: "send me your complete set at `position`" (spec §5.7).
`SNAP_REQUEST_BODY_LEN = 12`, and the reader is **exact-length**, not
minimum-length.

| bytes | field | width | meaning |
|---|---|---|---|
| 0..4 | `session` | u32 | scopes the resulting transfer, like any other snapshot session |
| 4..12 | `position` | u64 | the set to send; `0` = the sender's newest complete set |

The learner's sender opens an ordinary session for that set from its own
artifacts; the requesting voter's receiver takes it **store-only** — the
files are written and acked, nothing is installed by fiat, and the floor
moves through the ordinary completeness path instead. A voter above P
storing a set at P is not a joiner and must not be treated as one.

#### `SNAP_REDIRECT` body (wire 0.7.0)

The answer to a below-floor `SNAP_NAK` the receiving node cannot serve
itself: "ask learner `learner_id` for the set at `position`" (spec §5.7 item
6). `SNAP_REDIRECT_BODY_LEN = 16`, exact-length.

| bytes | field | width | meaning |
|---|---|---|---|
| 0..4 | `session` | u32 | the requester's original session |
| 4..8 | `learner_id` | u32 | the node that holds the set |
| 8..16 | `position` | u64 | the set's position |

It fires only when the local set is genuinely **missing** — with node-owned
retention the leader's set at its own floor normally exists, so in practice
this is a node restored from a backup taken before its current floor. The
joiner then sends its `SNAP_REQUEST` to the named learner. A redirect naming
a node the joiner does not know is dropped and recorded
(`snapshot_redirect_unknown`).

### Administration

| Kind | Name | Scope |
|---|---|---|
| 16 | `CONFIG_PROPOSAL` | pairwise |
| 17 | `CONFIG_REPLY` | pairwise |

### Crypto handshake

| Kind | Name |
|---|---|
| 18 | `HS_INIT` |
| 19 | `HS_RESP` |
| 20 | `HS_KEY` |

Scope determines which key seals a datagram. Group-scope kinds are sealed once
under the cluster group key and sent to N destinations. Pairwise-scope kinds are
sealed per destination under that peer's Noise session. Handshake kinds carry
their own protection.

## Log frames

| | |
|---|---|
| `HEADER_LEN` | 32 B |
| `FRAME_ALIGNMENT` | 32 B |

Frame lengths are aligned up to `FRAME_ALIGNMENT`. Frames never span the buffer
wrap; padding fills exactly to it.

### Header (wire 0.7.0, relaid for `time_ns`)

| Offset | Field | Width | Notes |
|---|---|---|---|
| 0 | `length` | u32 LE | the commit word: total frame length (header + payload), written LAST with a release store; `0` = not yet committed |
| 4 | `type` | u8 | see the type table below |
| 5 | `flags` | u8 | per-type. `FLAG_TIMER_TABLE = 0x01` on a `TIMER` frame — this tick came from the replicated schedule table, not from a state machine's own `schedule` call. `FLAG_SNAPSHOT_STANDBY = 0x01` on a `SNAPSHOT` frame — the same bit in the same byte, disjoint because the two types are; zero on every other type |
| 6 | reserved | u16 | written as zero |
| 8 | `leadership_term_id` | u32 LE | |
| 12 | `client_id` | u32 LE | the submitting client; `0` for node-originated frames |
| 16 | `seq` | u32 LE | that client's local sequence; `0` for node-originated frames |
| 20 | reserved | u32 | written as zero |
| 24 | `time_ns` | u64 LE | **the leader's stamp**: ns since the Unix epoch, non-decreasing along the log |

The header is still 32 bytes, and the payload ceiling is unchanged (1344 B
crypto-off / 1312 B crypto-on). `2.11.0` **relaid** it rather than growing it:
through `0.6.0` the two id fields were `session_id: u64` and
`correlation_id: u64`, of which the client only ever filled 32 bits each, so
narrowing them to `client_id: u32` + `seq: u32` freed exactly the 8 bytes
`time_ns` needed. A `0.6.0` peer's frames therefore *parse* on a `0.7.0` node
and mean something different, which is why the wire is a flag day: upgrade
every node together ([Upgrade a cluster](../how-to/upgrade-a-cluster.md)).

**The stamp rule.** The leader reads its wall clock **once per consensus
pass** and writes `max(now, last_stamp)` into every frame it appends, whatever
the type. The clamp lives in `uc_log::Appender`, so the log's time never goes
backwards, and equal stamps are allowed (position, not time, is the order). A
`TIMER` frame is stamped with its **deadline** instead, clamped the same way,
so a timer whose deadline has already been passed by the log's clock carries
`time_ns > deadline_ns` and is *late*. Followers, the archive and replay copy
headers verbatim and never re-stamp. The archive agent carries the highest
recorded stamp into the cnc page's `log_time_ns` word
([cnc page](cnc-page.md#counters-and-status)), which is what a new leader
seeds its clamp from.

| Type | Name | Notes |
|---|---|---|
| 1 | `MESSAGE` | an application command |
| 2 | `PADDING` | wrap padding; header-only on the wire, and its declared length is the full span it covers |
| 3 | `NEW_TERM` | written by a leader when it opens a term; header-only, 32 B |
| 4 | `CLUSTER` | **relabelled in 0.7.0** (was `CONFIG`): a cluster-FSM command. Kind-dispatched body, below |
| 5 | `TIMER` | **new in 0.7.0**: a scheduled timer the leader fired. 24-byte body, below |
| 6 | — | `SCHEDULE_TABLE` in an intermediate 0.7.0 shape; **retired before shipping** (`FRAME_TYPE_SCHEDULE_TABLE_RETIRED`) and reserved so the number is never reassigned |
| 7 | `SNAPSHOT` | **new in 0.7.0**: a coordinated snapshot instant. Header-only — the body is **empty**, because the frame's own END position is the instant. Header flag `FLAG_SNAPSHOT_STANDBY = 0x01` marks it standby (learners only). Below |

#### `CLUSTER` body (wire 0.7.0)

`FRAME_TYPE_CLUSTER = 4` carries every change to the cluster's own state —
membership, the replicated schedule table, and the replicated settings record.
It reuses `CONFIG`'s number: `2.11.0` is a flag day anyway, and no shipped node
ever emitted a frame `4` that was not a membership record.

The log is a **broadcast** log — it carries no service id and does no routing —
so the frame type is the only router there is. One type, one kind byte:

| bytes | field | meaning |
|---|---|---|
| 0 | `kind` | `1` = Membership, `2` = ScheduleTable, `3` = Settings; any other value is undecodable |
| 1..8 | reserved | written as zero, and a **non-zero** reserved byte makes the body undecodable — the bytes are claimable by a later kind without ambiguity |
| 8.. | `payload` | the kind's own encoding |

`CLUSTER_BODY_PREFIX_LEN = 8`, and `read_cluster_prefix` is total: it returns
`None` on a short body, an unknown kind, or a non-zero reserved byte, and
otherwise the kind plus the payload slice. Per kind:

| kind | payload | codec |
|---|---|---|
| `1` Membership | the `ClusterConfig` encoding `CONFIG` carried through `0.6.0`, unchanged | `uc_protocol::v2::config` |
| `2` ScheduleTable | the whole table — an 8-byte header plus `count × 33` bytes, at most `MAX_SCHEDULE_ENTRIES = 32`, so **≤ 1064 B**. Layout below | `uc_protocol::v2::schedule` |
| `3` Settings | `SETTINGS_LEN = 29` bytes exactly: `version u32 = 1 ‖ fsm_lag_bytes u64 ‖ admission_bytes u64 ‖ snapshot_interval_bytes u64 ‖ snapshot_target u8`. `0` in any u64 means "derive at use"; `fsm_lag_bytes = u64::MAX` (`FSM_LAG_LOCKSTEP`) means lockstep; `snapshot_target` is `0` = all, `1` = learners. No trailing bytes are tolerated | `uc_protocol::v2::settings` |

The largest of the three is the table at 1064 B, inside the 1312 B crypto-on
payload ceiling, so a `CLUSTER` frame always fits one datagram
([Limits](limits.md#hard-limits)).

**Two consumers, one frame.** Every FSM's apply loop yields a `CLUSTER` frame,
so it costs a user row nothing. The node's fifth polling agent, `uc2-cluster`,
acts on it: it applies the command at **commit** into the cluster FSM, which is
the snapshot authority for all three records. *Additionally*, for `kind = 1`
only, the archive's header walk reads the kind byte and feeds the membership
payload to the consensus kernel at **durability**, exactly as it fed `CONFIG`
— because Raft requires a node to use the newest configuration in its log
whether or not it is committed. Two readers, two time bases, one frame; the
reasoning is
[the cluster FSM explainer § Membership](../notes/uc2-cluster-fsm-explained.md#membership-one-frame-two-readers-and-why-that-is-safe).

Decode is fuzzed as `uc_protocol_cluster_frame` (the prefix plus all three
payload codecs) and `uc_protocol_settings` (the settings record alone, with a
re-encode round-trip).

#### `TIMER` body (wire 0.7.0)

`TIMER_BODY_LEN = 24`, three LE `u64`s, so the whole frame is 64 B after
alignment. `client_id` and `seq` are `0`.

| bytes | field | meaning |
|---|---|---|
| 0..8 | `identity_hash` | the FNV-1a 64 of the owning FSM's declared name (see [`SNAP_BEGIN`](#snap_begin-body-wire-070-layout-v4) for the same hash on the snapshot path) |
| 8..16 | `timer_id` | the FSM's own id for this timer |
| 16..24 | `deadline_ns` | what was asked for; compare against the header's `time_ns` for lateness |

This is the **first per-FSM frame** in a broadcast log. Every declared FSM
applies every `MESSAGE` frame; a `TIMER` frame is delivered only to the FSM
whose `identity_hash` it names, and every other row's apply loop skips it
while still counting it as a yielded frame for lag and lockstep accounting.
The body is id-only by design: there is no payload, and an FSM keeps whatever
context a timer needs in its own state, keyed by `timer_id`. Semantics,
delivery and the ordering guarantee:
[Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md).

#### `SNAPSHOT` frame (wire 0.7.0)

`FRAME_TYPE_SNAPSHOT = 7` is a **coordinated snapshot instant**: the leader
appends it, and its frame-END position **P** is the instant. The body is
empty and there is nothing to decode — the position *is* the payload, so the
whole frame is 32 B on the wire. `client_id` and `seq` are `0`. A `SNAPSHOT`
frame truncated by a leader change needs no revert record: any artifact
already built at that P is simply an orphan, unlinked by the node's retention
sweep once a later set completes, and the next leader's instant is a fresh P.

It is a **broadcast** frame: every declared row's apply loop acts on it, and
so does the cluster FSM, having applied everything below P. Each freezes and
publishes `snapshots/<row>/snap-<P>.ultsnap` (the cluster FSM,
`snapshots/cluster/snap-<P>.ultcluster`); a node whose declared rows have all
reached P holds the **complete set at P**, and that is its new purge floor.

| flag | meaning |
|---|---|
| `FLAG_SNAPSHOT_STANDBY = 0x01` | only a node with `NODE_FLAG_LEARNER` set in the cnc node-flags word acts on this instant. A voter's rows yield the frame like any other node-only frame, so no voter pays the freeze; the learner's set comes back to a voter by `SNAP_REQUEST` (above) |

Only the leader appends one, from either of two triggers:
[`uc2ctl snapshot`](uc2ctl.md#snapshot) (admin op 8) or the replicated
`snapshot_interval_bytes` cadence. Why the whole cluster freezing at one
position is worth the coordination — and what a standby instant buys — is
[the cluster FSM explainer § Instants](../notes/uc2-cluster-fsm-explained.md#instants-one-position-one-set).

#### `ScheduleTable` payload (`CLUSTER` kind 2, wire 0.7.0)

The `CLUSTER kind = 2` payload carries the whole replicated schedule table —
the recurrences an operator applied with
[`uc2ctl schedule apply`](uc2ctl.md#schedule-apply). Applying **replaces** the
table; there is no incremental edit and no delete verb. The codec is
`uc_protocol::v2::schedule` (`encode_schedule_table` / `decode_schedule_table`),
hand-laid and total in the same style as `v2::config`'s.

An 8-byte header followed by `count` fixed 33-byte entries:

| bytes | field | meaning |
|---|---|---|
| 0..4 | `version` | u32 LE, currently `1`; any other value decodes to `None` |
| 4..6 | `count` | u16 LE, `0..=MAX_SCHEDULE_ENTRIES` (**32**) |
| 6..8 | reserved | written as zero |

Each entry, `SCHEDULE_ENTRY_LEN = 33`:

| bytes | field | meaning |
|---|---|---|
| 0..8 | `identity_hash` | u64 LE — the owning FSM's name hash, the same one a `TIMER` frame names |
| 8..16 | `timer_id` | u64 LE — the id that FSM's `on_timer` will see |
| 16 | `kind` | u8: `1` = `every`, `2` = `at`, `3` = `once` |
| 17..25 | `a` | u64 LE — `every`: `period_ns` (must be > 0); `at`: `secs_of_day` (< 86 400, UTC); `once`: `at_ns` |
| 25..33 | `b` | u64 LE — `every`: `anchor_ns`; **must be zero** for `at` and `once` |

A full table is `8 + 32 × 33 = 1064` bytes; with the 8-byte `CLUSTER` prefix
that is 1072 B of payload, inside the 1312 B crypto-on ceiling, so the frame
always fits one datagram ([Limits](limits.md#hard-limits)).

The decoder refuses — returns `None`, never panics or allocates from a
peer-supplied length — on a short buffer, a version other than `1`, a `count`
above 32, a total length other than `8 + 33 × count`, an unknown `kind`, a
zero `period_ns`, a `secs_of_day >= 86_400`, a non-zero `b` on an `at`/`once`
entry, or a duplicate `(identity_hash, timer_id)` pair. It is fuzzed as
`uc_protocol_schedule_table` and its byte layout is frozen by
`table_codec_pins_bytes_and_is_total`.

The apply layer never sees this frame: every FSM's apply loop yields
`CLUSTER`. **Every** node adopts the table the same way — the cluster FSM
applies the command at commit and publishes it on the view; there is no
leader-at-append / follower-at-walk split any more, and no durable
`ScheduleRecord` with a predecessor to revert to, because a committed frame is
never truncated. What the table then does is
[Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#the-schedule-table).

Per-record framing uses an atomic-after-write length prefix: a reader that sees
length `0` has found a record that is not yet committed. A non-zero length below
`HEADER_LEN` is invalid.

## Positions

All wire positions are absolute byte offsets into the replicated log, not
indices. A position is stable for the life of the log and is the idempotency
key for `apply`.
