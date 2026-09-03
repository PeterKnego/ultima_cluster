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
moved to 0.7.0 for **three features** shipping on the same unreleased `2.11.0`
flag day. FSM identity: `SNAP_BEGIN` swapped its `services_declared` bitmask
for a per-row identity-hash array plus a per-row version array. Log time and
timers: the log frame header was **relaid** to carry a leader-written
`time_ns` stamp, with a `TIMER` (5) frame type beside it. The replicated
schedule table, built on that: a second new frame type, `SCHEDULE_TABLE`
(6), and one new datagram kind, `SNAP_TABLE` (21), which carries that table
on a snapshot session so a below-floor joiner gets it too. Every other
datagram is byte-identical to 0.6.0. `CURRENT` is
documentary and is not itself checked on any receive path (see
`version.rs`); the two version lines remain independent of each other.

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
| 21 | `SNAP_TABLE` | pairwise |

`SNAP_TABLE` belongs to this group but takes kind **21**, not 16: the
administration and crypto-handshake kinds below already own 16–20, and a
kind byte is never reused.

#### `SNAP_BEGIN` body (wire 0.7.0, FSM identity)

`SNAP_BEGIN` opens (or extends) one artifact of a snapshot session; its body
is the only wire carrier of FSM identity (commands are broadcast, durable
reports are aggregates — neither names an FSM). `SNAP_BEGIN_FIXED_LEN = 122`,
followed by a variable, length-prefixed `config` tail (M7's `ConfigRecord`
bytes). `SNAP_DONE` echoes the same body as its ack, so it carries this
layout too, with no separate change.

| bytes | field | width | meaning |
|---|---|---|---|
| 0..4 | `session` | u32 | session id |
| 4 | `layout` | u8 | body discriminator; `2` = `SNAP_BEGIN_LAYOUT_V3` (0.7.0). A shorter/older `layout` is refused as "peer wire ≤ 0.6.0" |
| 5 | `service_id` | u8 | the row this artifact belongs to |
| 6..8 | — | 2 B | zero (pads `snapshot_pos` to u64 alignment) |
| 8..16 | `snapshot_pos` | u64 | this artifact's snapshot position |
| 16..24 | `total_len` | u64 | this artifact's byte length |
| 24..88 | `identity` | `[u64; 8]` | the sender's per-row FSM identity hash (FNV-1a 64 of the declared name), in row order; `0` = row undeclared. Replaces 0.6.0's `services_declared` bitmask — the mask is now derived (`SnapBeginBody::declared_mask`) |
| 88..120 | `version` | `[u32; 8]` | the sender's per-row attached packed version, from the cnc slot; `0` = no service attached / unversioned |
| 120..122 | `config_len` | u16 | length of the `config` tail |
| 122.. | `config` | variable | the encoded `ConfigRecord`, identical on every `BEGIN` of a session |

The receiver compares `identity` **positionally**: for each row `r`,
`identity[r]` must equal the receiver's own hash for row `r` (both zero =
both undeclared). A mismatch refuses the session **by name** ("row 1:
ours=orders, theirs=kv" — a hash the receiver recognizes anywhere in its own
list prints as that name, an unknown one as its hash) and counts
`uc2_snapshot_refused_declared_set_total`; this subsumes the 0.6.0 declared-set
check (a set difference is a positional difference). `version` is compared
per row only when **both** sides are non-zero; a mismatch refuses by name
with both versions and counts the new `uc2_snapshot_refused_version_total`.
A 0.6.0 sender's body (34 B fixed) is shorter than 122 B, so the receiver
drops it by the same length check that drops a 0.5.0 body today — the
standing flag-day rule: a mixed cluster stalls a joiner rather than
installing a wrong or half-checked artifact. Artifacts still route by row,
unchanged.

#### `SNAP_TABLE` body (wire 0.7.0)

`SNAP_TABLE` carries the leader's adopted **replicated schedule table** to a
joiner that is below the purge floor and therefore cannot read the table's own
log frame. The leader sends it **immediately after every `SNAP_BEGIN` of a
session** — the initial one and each resend on the `SNAP_BEGIN_RESEND_NS`
(20 ms) cadence — so it needs no reliability machinery of its own: the same
resend that repairs a lost `BEGIN` repairs a lost `TABLE`.
`SNAP_TABLE_FIXED_LEN = 22`, followed by the encoded table.

| bytes | field | width | meaning |
|---|---|---|---|
| 0..4 | `session` | u32 | session id; must match the intake's, or the datagram is a stray |
| 4..12 | `position` | u64 | the adopted table frame's END position on the leader; `0` = the leader has none, or its table is unanchored (position 0 after a wipe) |
| 12..20 | `time_ns` | u64 | the adopting frame's log-time stamp, recorded on the joiner's record for diagnostics — **not** what the joiner arms from |
| 20..22 | `table_len` | u16 | length of the encoded table |
| 22.. | `table` | variable | `uc_protocol::v2::schedule::encode_schedule_table` bytes (a full 32-entry table is 1064 B) |

`read_snap_table_body` is **total** on any input and does not decode the
table itself (the node does, fail-stop, exactly as it does for a `CONFIG`
frame body). It enforces `(position == 0) ⇔ (table_len == 0)`, so "the leader
has none" has exactly one encoding on the wire, and a ceiling of
`SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES × SCHEDULE_ENTRY_LEN`. A full
table is `22 + 1064 = 1086 B` total, pinned below the crypto-on datagram
budget by a `const` assert beside the constant.

The receiver records the table on the session's intake, **withholds
`SNAP_DONE` until it has one**, ignores expected re-sends, and counts a
genuine stray (a refused or unknown session, a different peer, or a different
session id) once per episode in `uc2_snapshot_table_stray_total`. On
completion it publishes table → config → floor signal, in that order, so the
consensus agent installs the table before the floor moves. The install is by
**fiat** — a wholesale replace with `prev = None`, like the carried config —
because below the floor the joiner's own bytes are gone. Position `0` with an
empty table installs "no table" as a record rather than leaving the joiner's
stale one. A `0.6.0` peer sends no `SNAP_TABLE` at all, but withholding
`SNAP_DONE` is not how that is caught: its `SNAP_BEGIN` is already refused by
the `layout` check above, so the flag-day rule still bites at the same place
it did before.

A session whose `SNAP_TABLE` is systematically lost (never one that just
arrives late — the leader keeps resending it on the same 20 ms cadence as
`SNAP_BEGIN`) is not left withholding `SNAP_DONE` forever: the intake's
"no chunk" timeout (`SNAP_INTAKE_TIMEOUT_NS`, 60 s) fires against it exactly
as it would a lost chunk, since no further chunk arrives once every part has
already renamed — the intake is discarded and the joiner re-downloads on a
fresh session rather than wedging.

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
| 5 | `flags` | u8 | `FLAG_TIMER_TABLE = 0x01` on a `TIMER` frame — this tick came from the replicated schedule table, not from a state machine's own `schedule` call; zero on every other type today |
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
| 4 | `CONFIG` | a cluster configuration record |
| 5 | `TIMER` | **new in 0.7.0**: a scheduled timer the leader fired. 24-byte body, below |
| 6 | `SCHEDULE_TABLE` | **new in 0.7.0**: the replicated schedule table an operator applied. Variable body, below |

#### `TIMER` body (wire 0.7.0)

`TIMER_BODY_LEN = 24`, three LE `u64`s, so the whole frame is 64 B after
alignment. `client_id` and `seq` are `0`.

| bytes | field | meaning |
|---|---|---|
| 0..8 | `identity_hash` | the FNV-1a 64 of the owning FSM's declared name (see [`SNAP_BEGIN`](#snap_begin-body-wire-070-fsm-identity) for the same hash on the snapshot path) |
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

#### `SCHEDULE_TABLE` body (wire 0.7.0)

`FRAME_TYPE_SCHEDULE_TABLE = 6` carries the whole replicated schedule table —
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

A full table is `8 + 32 × 33 = 1064` bytes, inside the 1312 B crypto-on
payload ceiling, so the frame always fits one datagram
([Limits](limits.md#hard-limits)).

The decoder refuses — returns `None`, never panics or allocates from a
peer-supplied length — on a short buffer, a version other than `1`, a `count`
above 32, a total length other than `8 + 33 × count`, an unknown `kind`, a
zero `period_ns`, a `secs_of_day >= 86_400`, a non-zero `b` on an `at`/`once`
entry, or a duplicate `(identity_hash, timer_id)` pair. It is fuzzed as
`uc_protocol_schedule_table` and its byte layout is frozen by
`table_codec_pins_bytes_and_is_total`.

The apply layer never sees this frame: like `CONFIG`, it is skipped by every
FSM's apply loop. The leader adopts the table at append; every other node
adopts it from the archive's header walk, the same path `CONFIG` takes. What
the table then does is
[Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#the-schedule-table).

Per-record framing uses an atomic-after-write length prefix: a reader that sees
length `0` has found a record that is not yet committed. A non-zero length below
`HEADER_LEN` is invalid.

## Positions

All wire positions are absolute byte offsets into the replicated log, not
indices. A position is stable for the life of the log and is the idempotency
key for `apply`.
