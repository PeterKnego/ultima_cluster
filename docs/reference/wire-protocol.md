# Wire protocol

The node-to-node UDP format: datagram kinds, log frame layout, and the version
gates. Defined in `uc_protocol::v2`.

This page states the wire surface. The rationale for byte positions and the
self-locating header is in [Architecture](../ARCHITECTURE.md).

## Version

| Constant | Value |
|---|---|
| `version::CURRENT` | `0.7.0` |
| cnc page version | 3.1 (FSM identity, 2.11 pending: the name + hash line at boot, the version word at attach) |

The cnc page carries its own version gate, `CNC_V2_VERSION`, which is
independent of this one. cnc 3.1 changed the same-host shmem layout only
(the once-reserved slot line 7); the UDP datagram format moved to 0.7.0 in
the FSM identity work, when `SNAP_BEGIN` swapped its `services_declared`
bitmask for a per-row identity-hash array plus a per-row version array —
every other datagram is byte-identical to 0.6.0. `CURRENT` is documentary
and is not itself checked on any receive path (see `version.rs`); the two
version lines remain independent of each other.

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

| Type | Name | Notes |
|---|---|---|
| 1 | `MESSAGE` | an application command |
| 2 | `PADDING` | wrap padding; header-only on the wire, and its declared length is the full span it covers |
| 3 | `NEW_TERM` | written by a leader when it opens a term; header-only, 32 B |
| 4 | `CONFIG` | a cluster configuration record |

Per-record framing uses an atomic-after-write length prefix: a reader that sees
length `0` has found a record that is not yet committed. A non-zero length below
`HEADER_LEN` is invalid.

## Positions

All wire positions are absolute byte offsets into the replicated log, not
indices. A position is stable for the life of the log and is the idempotency
key for `apply`.
