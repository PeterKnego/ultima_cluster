# kv_store wire format — normative (v2)

Every byte a client sends to, and receives from, the kv store, from the TCP
socket inward. Enough to write a client in any language. Implemented once, in
`src/wire.rs`; `tests/sm_invariants.rs` pins it.

All integers are **little-endian**. Byte offsets are from the start of the
field group being described.

**v2** added two operations (`APPEND`, `LIST`), two reply statuses
(`WRONG_SHAPE`, `LIST_FULL`) and image version 2. Every v1 byte is unchanged;
a v1 client and a v1 image are still recognised. The v2-only pieces are
tagged **(v2)** below.

There are three layers. A client speaks the outer two to a `uc2-gateway`; the
innermost is the store's own.

```
┌──────────────────────────────────────────────────────────────┐
│ UC remote protocol v1 frame (24-byte header + payload)         │  ← layer 1, UC's
│   payload of SUBMIT / QUERY = the kv command / query (layer 3) │
│   payload of RESPONSE = 20-byte ResponseMeta + kv reply        │
│ [the gateway adds the 16-byte session envelope on SUBMIT, and  │  ← layer 2, UC's,
│  strips the 1-byte Sessioned tag from write replies — a client │    invisible to a
│  never sees either]                                            │    client
└──────────────────────────────────────────────────────────────┘
```

## Layer 1 — the UC remote protocol (what a client actually sends)

Normative source: `docs/reference/remote-protocol.md` in the UC documentation
(2.12.0). Restated here so this file is self-contained; if the two disagree,
UC's page wins.

**Header, 24 bytes, on every frame in both directions:**

| off | size | field | value |
|---:|---:|---|---|
| 0 | u32 | `len` | total frame length including this header |
| 4 | u8 | `type` | 1 HELLO, 2 HELLO_OK, 3 HELLO_REFUSED, 4 SUBMIT, 5 QUERY, 6 RESPONSE, 7 STATUS, 8 REDIRECT, 9 RETRY, 10 UNKNOWN, 11 LEADER_CHANGED, 12 PING, 13 PONG |
| 5 | u8 | `flags` | QUERY: `0x01` = linearizable. RESPONSE: `0x02` answers a QUERY, `0x04` replayed, `0x08` expired, `0x10` enveloped |
| 6 | u16 | `version` | `1` |
| 8 | u64 | `client_id` | the client's stable, self-chosen identity (random u64) |
| 16 | u64 | `seq` | per-client counter, starts at 1, +1 per SUBMIT/QUERY |

**Handshake.** Client sends `HELLO` with payload `u16 len ‖ app_id` (UTF-8;
the store's default `app_id` is `kv`). Edge answers `HELLO_OK`
(`u32 credits ‖ u32 leader_node_id ‖ u16 len ‖ leader_gateway_addr`) or
`HELLO_REFUSED` (`u8 reason ‖ u16 len ‖ detail`). If `HELLO_OK` names a
leader that is not this edge, connect there first.

**Requests.** `SUBMIT` (type 4) carries a kv **command** (§ 3.1) as its
payload, verbatim. `QUERY` (type 5) carries a kv **query** (§ 3.2); set flag
`0x01` for a linearizable read, clear it for a snapshot read served by
whichever replica the edge sits on.

**Flow control.** A client may have at most `credits` unanswered requests
(SUBMITs and QUERYs together) outstanding. `credits` arrives in `HELLO_OK`,
on every `RESPONSE` and on `STATUS`, is absolute, and may go down.

**Responses.** `RESPONSE` (type 6) payload:

| off | size | field |
|---:|---:|---|
| 0 | u32 | `credits` |
| 4 | u64 | `acked_seq` — highest SUBMIT seq answered for this client |
| 12 | u64 | `position` — log byte position the command applied at; `0` for a query |
| 20 | … | the kv **reply** (§ 3.3), verbatim |

Flag `0x04` (replayed) on a write's RESPONSE means: this `(client_id, seq)`
was already applied; the reply bytes are the cached original; nothing was
applied twice. Flag `0x08` (expired) means the store can no longer say what
happened to this seq (it fell out of the 4096-response per-client window);
no reply bytes follow — treat as unknown, do not resend.

**Everything else** (`REDIRECT`, `LEADER_CHANGED`, `RETRY`, `UNKNOWN`,
`STATUS`, `PING`/`PONG`) is failover and liveness machinery a conforming
client absorbs; see UC's page for the required behaviour (reconnect on
REDIRECT/LEADER_CHANGED to the named gateway, re-send unanswered seqs in
order, resend on UNKNOWN, honour `RETRY{PAYLOAD_TOO_LARGE}` as final, PING
when idle).

## Layer 2 — the session envelope (not a client concern)

The gateway prepends `client_id: u64 ‖ seq: u64` (16 B) to every SUBMIT
payload before the node sees it, and the service's `Sessioned` wrapper
prepends a 1-byte tag (`0` fresh, `1` replayed, `2` expired) to every write
reply, which the gateway lifts into the RESPONSE flags above. Queries carry
neither. A client never constructs or parses either — but they cost 16 B of
the command budget (§ 4).

## Layer 3 — the kv store's own frames

### 3.1 Commands (SUBMIT payload)

Common prefix:

| off | size | field | value |
|---:|---:|---|---|
| 0 | u8 | `format` | `1` |
| 1 | u8 | `op` | `1` PUT, `2` DELETE, `3` CAS |
| 2 | u16 | `key_len` | `1 ..= 256` |
| 4 | `key_len` | `key` | opaque bytes |

Then, by `op`:

| op | after the key | constraint |
|---|---|---|
| PUT (1) | `value` = every remaining byte | `0 ..= 1024` bytes |
| DELETE (2) | nothing | any trailing byte ⇒ BAD_REQUEST |
| CAS (3) | `expected_version: u64`, then `value` = every remaining byte | value `0 ..= 1024` bytes; `expected_version = 0` means "the key must be absent" |

Sizes: PUT = `4 + key_len + value_len`; DELETE = `4 + key_len`;
CAS = `12 + key_len + value_len`. Worst case CAS = 1292 B (§ 4).

### 3.2 Queries (QUERY payload)

| off | size | field | value |
|---:|---:|---|---|
| 0 | u8 | `format` | `1` |
| 1 | u8 | `op` | `1` GET, `2` DIGEST, **`3` LIST (v2)** |

GET / **LIST (v2)**: `u16 key_len ‖ key` follows (same bounds as above),
nothing after. DIGEST: nothing follows.

### 3.3 Replies (the bytes after `ResponseMeta` in a RESPONSE)

Byte 0 is always a **status**:

| status | name | meaning |
|---:|---|---|
| 0 | OK | the operation happened / the key was found |
| 1 | NOT_FOUND | DELETE or GET of an absent key |
| 2 | VERSION_MISMATCH | CAS: `expected_version` did not match |
| 3 | BAD_REQUEST | the frame did not parse; byte 1 says why |
| 4 | WRONG_SHAPE **(v2)** | the key holds the other shape (see § 3.6); nothing changed |
| 5 | LIST_FULL **(v2)** | APPEND would exceed a list cap; nothing changed |

Then, by request and status:

| request | status | bytes after the status |
|---|---:|---|
| PUT | 0 | `version: u64` — the new version of the key |
| DELETE | 0 | `version: u64` — the version that was removed |
| DELETE | 1 | nothing |
| CAS | 0 | `version: u64` — the new version |
| CAS | 2 | `current: u64` — the key's current version, `0` if absent |
| GET | 0 | `version: u64`, then `value` = every remaining byte |
| GET | 1 | nothing |
| DIGEST | 0 | `count: u64 ‖ digest: u64 ‖ last_applied: u64` |
| APPEND **(v2)** | 0 | `version: u64` (the list's new version), then `len: u32` (its new element count) |
| APPEND **(v2)** | 5 | `len: u32` — the list's unchanged length (a cap was hit) |
| LIST **(v2)** | 0 | `version: u64 ‖ count: u32`, then `count ×` (`len: u32 ‖ bytes`), oldest first |
| LIST **(v2)** | 1 | nothing (absent key) |
| any write/read on the wrong shape **(v2)** | 4 | nothing |
| any | 3 | `reason: u8` |

BAD_REQUEST reasons: `1` truncated, `2` unsupported format version, `3`
unknown op, `4` key length outside `1..=256`, `5` value longer than 1024,
`6` trailing bytes after a fixed-length frame.

A write reply is always preceded, before the gateway strips it, by the
Sessioned tag; a client reading the RESPONSE flags instead of the tag sees
the reply starting at the status byte.

### 3.4 Versions

A **version** is the absolute byte position in the replicated log of the
command that last wrote the key — the same number `ResponseMeta.position`
carries for that write. It is unique per write, strictly increasing over a
key's history, and never `0` for a real write (the log begins with the
leader's term frame). `0` is therefore the CAS sentinel for "absent".

### 3.5 Digest

`digest` is the XOR over all entries of `fnv1a64(key_len: u16 ‖ key ‖ version:
u64 ‖ value_len: u32 ‖ value)`, FNV-1a with offset basis
`0xcbf29ce484222325` and prime `0x100000001b3`, integers LE. It is
order-independent, so two replicas with the same entries report the same
digest regardless of insertion history. An empty store has digest `0`.

### 3.6 Shapes (v2)

A key holds one of two shapes:

- a **value** — created by PUT/CAS, read by GET;
- a **list** — created and grown by APPEND, read by LIST; the ordered
  sequence of everything appended, oldest first.

The shapes are **strict**. An operation of the wrong shape returns
`WRONG_SHAPE` (status 4) and changes nothing:

| on a … | PUT / CAS | GET | APPEND | LIST |
|---|---|---|---|---|
| value | overwrite / compare | read | `WRONG_SHAPE` | `WRONG_SHAPE` |
| list | `WRONG_SHAPE` | `WRONG_SHAPE` | append | read |
| absent | create value | `NOT_FOUND` | create list | `NOT_FOUND` |

**DELETE removes either shape** and reports the removed key's version; the
key may then be recreated as the other shape. A **list's version** is the
position of its most recent APPEND; a fresh list's version is its first
APPEND's position.

List caps: **`MAX_LIST_LEN = 4096`** elements and **`MAX_LIST_BYTES = 65536`**
bytes summed over elements (each element `0 ..= 1024` B, as a value). An
APPEND that would exceed either returns `LIST_FULL` with the unchanged
length. The largest LIST reply is
`1 + 8 + 4 + 4096×4 + 65536 = 81933` B — far under UC's 1 MiB remote frame
ceiling (`MAX_FRAME_LEN`) and its 4 MiB egress ring, so a list read is never
refused for size (unlike a command, which must fit one datagram).

## 4 — Size limits, and where they come from

UC refuses a command that does not fit one datagram (`docs/reference/limits.md`).
The ceiling is discovered per cluster and starts at 1344 B (crypto off) /
1312 B (crypto on); the value that holds on **every** cluster is 1312
(`uc_remote::engine::STANDARD_PAYLOAD`).

```
1312  standard ceiling
 -16  session envelope (layer 2)
1296  budget for a layer-3 command
1292  worst CAS  = 12 + 256 + 1024   ✓
1284  worst PUT  =  4 + 256 + 1024   ✓
```

Hence **`MAX_KEY = 256`, `MAX_VALUE = 1024`**. The store enforces both in
`apply` (BAD_REQUEST 4/5) so an oversize frame from any client is refused
without touching state; `kv` refuses them before sending. Queries (≤ 260 B)
and replies (≤ 1033 B) are far below every UC limit (1 MiB remote frame,
4 MiB egress ring).

## 5 — Snapshot image (not on the wire; on disk under `snapshots/0/`)

For upgrade planning. The framework wraps the artifact: UC writes its 24-byte
envelope (`ULTSNAP2 ‖ position: u64 ‖ version: u32 ‖ 4 reserved zero bytes`,
since 2.13.0 — a pre-2.13.0 `ULTSNAP1` 16-byte envelope, with no version
field, is refused by name) first, then — because the service runs wrapped in
`Sessioned` — a `u64` length-prefixed dedup table, and only then the store's
own image. The store owns the image bytes:

**Image version 2 (what a v2 binary writes):**

```
image_version: u32 = 2
cursor:        u64   last_applied at freeze, or 0xFFFF_FFFF_FFFF_FFFF if nothing applied
digest:        u64
count:         u64
count × entry, in key order:
    key_len: u16 ‖ key ‖ version: u64 ‖ shape: u8 ‖ body
    shape 0 (value): value_len: u32 ‖ value
    shape 1 (list):  n: u32 ‖ n × ( elem_len: u32 ‖ elem )
```

**Image version 1 (what a v1 binary wrote, still read by a v2 binary):**
identical, but with **no `shape` byte** — every entry is a value
(`key_len ‖ key ‖ version ‖ value_len ‖ value`). A v2 reader keys entirely
off the `image_version` word at offset 0: `1` ⇒ no shape byte, all values;
`2` ⇒ a shape byte per entry.

**Digest continuity.** A value entry hashes exactly as in v1
(`fnv1a64(key_len ‖ key ‖ version ‖ value_len ‖ value)`), so a v1 image's
stored digest recomputes bit-for-bit under v2 — that is what lets the v2
reader *verify* an old image rather than merely parse it. A list entry is
domain-separated by a leading `b"L"` and hashes
`fnv1a64("L" ‖ key_len ‖ key ‖ version ‖ n ‖ (elem_len ‖ elem)…)`.

`install_snapshot` refuses (never half-applies) an image version other than
1 or 2, a cursor at or above the artifact's position, an unknown shape byte,
out-of-range lengths, a duplicate key, trailing bytes, or a digest that does
not recompute.

**Going backwards.** A **v1** binary's `install_snapshot` only knows image
version 1, so handed a v2 artifact it returns `Codec("unknown kv image
version 2")` and the service fail-stops — refused, never misread. So a
rollback from v2 to v1 is only possible to a snapshot set taken *before* the
first v2 instant (or by clearing `snapshots/` and replaying an unpurged
journal). This is safe (refusal, not corruption) but one-way in practice.
