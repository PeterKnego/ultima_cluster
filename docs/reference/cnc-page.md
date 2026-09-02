# The cnc control page

`cnc2.dat` is a fixed-layout **8 KiB** control page (two 4 KiB pages since cnc
3.0 / M14) in every instance directory. It carries the counters and status
fields the node, service, and client processes coordinate through.

Offsets are pinned in both `uc_protocol::v2::cnc` and `uc_log`, with
offset-assertion tests cross-checking them. They do not change within a wire
protocol major version. New fields are added in the reserved band.

To read a live page while diagnosing a node, see
[Diagnose a node](../how-to/diagnose-a-node.md).

## Page header

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 8 B | magic | `UC2CNC\0\0` |
| 8 | u32 LE | version | `CNC_V2_VERSION` = `(3 << 24) \| (1 << 16)` (cnc 3.1, FSM identity, 2.11 pending) |
| 12 | u32 LE | node id | |
| 16 | u64 LE | instance id, low | changes on every node restart |
| 24 | u64 LE | instance id, high | |
| 32 | 64 B | app id | UTF-8, NUL-padded |
| 96 | u64 LE | created, ns | |
| 104 | u64 LE | buffer bytes | log-buffer capacity; geometry for attaching processes |
| 112 | u32 LE | max payload | |
| 124 | u32 LE | header CRC | crc32 over bytes `[0, 124)`; written and checked by `uc_log` |

Page length is 8192 bytes: page 1 (`0..4096`) is the M1–M13 layout
byte-for-byte; page 2 (`4096..8192`) is the service-slot band.

## Counters and status

Each field occupies its own 64-byte stride. Every field has exactly one
writer.

| Offset | Field | Writer |
|---|---|---|
| 256 | `append` | leader appender, or follower receiver |
| 320 | `durable` | archive agent |
| 384 | `sent` | sender agent |
| 448 | `commit` | consensus agent |
| 512 | `service_applied` | node (consensus agent) — `min` over the declared FSMs' slots since 3.0 |
| 576 | `service_epoch` | retired at 0 since 3.0 (per-FSM epoch lives in the slot) |
| 640 | `output_completed` | node (consensus agent) — `min` over the declared FSMs' slots since 3.0 |
| 704 | `term` | consensus agent |
| 768 | `node_flags` | consensus agent |
| 832 | `leader_hint` | consensus agent; `u64::MAX` means unknown |
| 896 | `node_heartbeat_ns` | consensus agent |
| 960 | `service_heartbeat_ns` | node (consensus agent) — `min` over the declared FSMs' slots since 3.0 |
| 1024 | `output_progress` | consensus agent; mirror of the persisted marker |
| 1088 | `next_client_id` | clients, by `fetch_add`; initialised to 1 |
| 1152 | `service_snapshot_pos` | node (consensus agent) — `min` over the declared FSMs' slots since 3.0 |
| 1216 | `node_snapshot_floor` | consensus agent; initialised to 0 |
| 1280 | `incoming_snapshot_pos` | consensus agent; mirror of the receiver's completed inbound snapshot, `0` if none |
| 1344 | `archive_first_base` | consensus agent; mirrors the archive agent's first-base atomic |
| 3456 | `config_version` | |
| 3520 | `config_pending` | |
| 3584 | `admin_req` | admin request slot |
| 3648 | `admin_resp` | admin response slot |
| 3712 | `admission_bytes` | the node's configured admission window |
| 3776 | `seal_failures` | crypto seal failures |
| 3840 | `free_disk_bytes` | free bytes on the instance dir's filesystem; writer: the `uc2-node` daemon only, `0` = never published |
| 3904 | `admin_auth` | M12b: HMAC-SHA256 auth line for the admin request slot (tag ‖ `expiry_ns` ‖ key-name hash); all-zero = no auth attached |
| 3968 | `ingress_holes_skipped` | M13: dead-producer holes skipped on the client **ingress** MPSC ring; writer: the consensus agent, published on change only |
| 3976 | `query_holes_skipped` | M13: same counter for the **query** ring — deliberately the second u64 of the 3968 line (same writer, on-change only) |
| 4032 | `services_declared` | node, once at boot (bit *i* ⇔ id *i* declared). **Unchanged by cnc 3.1 / FSM identity**: this same-host bitmask is unrelated to the wire's per-row `SnapBeginBody.identity` array (`docs/reference/wire-protocol.md`) — the two are derived from the same `[services] names` config but serve different readers |
| 4040 | `fsm_lag_bytes` | node, once at boot (`0` ⇔ lockstep) — shares 4032's line |

Counters are absolute byte positions in the replicated log, not indices.

### `node_flags`

| Bit | Constant | Meaning |
|---|---|---|
| 0 | `NODE_FLAG_LEADER` | this node is leader |
| 1 | `NODE_FLAG_CAN_SERVE` | this node passes the serving gate |

## Peer slots

An 8-entry observability band, one slot per cluster member.

| | |
|---|---|
| Band offset | 1408 |
| Slot stride | 256 B |
| Slot count | 8 |

Fields within a slot:

| Slot offset | Field |
|---|---|
| 0 | `id_and_role` — id in bits 8 and above, role in the low byte |
| 64 | `reported_durable` |
| 128 | `advertised_limit` |
| 192 | `naks_plus_replay` |

Role values are `1` for voter and `2` for learner. A slot whose `id_and_role`
reads `0` is unoccupied.

The band has 8 slots and the cluster's member cap is 8.

## Service slots

An 8-entry band on page 2, one slot per declared FSM id (M14).

| | |
|---|---|
| Band offset | 4096 |
| Slot stride | 512 B |
| Slot count | 8 |

Fields within a slot (each its own 64 B line, one writer):

| Slot offset | Field | Writer |
|---|---|---|
| 0 | `status` (line 0, word 0) — `service_id` (bits 0..8) \| attached (bit 8) \| incarnation (bits 32..64) | service, at attach / clean detach |
| 8 | `version` (line 0, word 1) — packed FSM version (low 32 bits); `0` = unversioned/absent | service, at attach (cnc 3.1, FSM identity) |
| 64 | `applied` | service apply agent |
| 128 | `epoch` | service, `fetch_add` at attach |
| 192 | `output_completed` | service output agent |
| 256 | `snapshot_pos` | service builder agent |
| 320 | `heartbeat_ns` | service apply agent |
| 384 | `lag_waits` | service apply agent (one per wait episode at the lag barrier) |
| 448 | `name` (line 7) — `[u8; 32]`, NUL-padded FSM name | **node**, at `CncPage::init` (boot, once) — cnc 3.1, FSM identity |
| 480 | `identity_hash` (line 7) — u64, FNV-1a 64 of `name` | **node**, at `CncPage::init` (boot, once) — cnc 3.1, FSM identity |
| 488 | reserved (zero) | — |

A slot whose `status` reads `0` has never been attached this page generation.
The node re-creates the page at every boot, so incarnation and epoch restart
at 0 with the node. Line 7 (`name`/`identity_hash`) breaks the "one writer
per line, and it's the service" pattern the other seven lines follow: it is
written once by the **node** at boot, before any service attaches, from
`[services] names` — a service finds its row by scanning these eight name
lines for its own `S::NAME` rather than being told its row. `uc2ctl status`
prints `row= name= version= hash=` per row from these two lines plus the
status line's version word; `/metrics` exports them as
`uc2_service_identity_hash{service="<name>",row="<r>"}` and
`uc2_service_version{service="<name>",row="<r>"}`.
