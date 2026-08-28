# Wire protocol

The node-to-node UDP format: datagram kinds, log frame layout, and the version
gates. Defined in `uc_protocol::v2`.

This page states the wire surface. The rationale for byte positions and the
self-locating header is in [Architecture](../ARCHITECTURE.md).

## Version

| Constant | Value |
|---|---|
| `version::CURRENT` | `0.6.0` |
| cnc page version | 3.0 (M14: 8 KiB page) |

The cnc page carries its own version gate, `CNC_V2_VERSION`, which is
independent of this one. cnc 3.0 changed the same-host shmem layout only;
the UDP datagram format moved to 0.6.0 in M14c, when `SNAP_BEGIN` grew its
per-FSM fields — every other datagram is byte-identical to 0.5.0. `CURRENT`
is documentary and is not itself checked on any receive path (see
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
