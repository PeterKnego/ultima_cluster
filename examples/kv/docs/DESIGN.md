# kv_store — design note (v1)

Written per `docs/reference/application-sdlc.md` § 1 before implementation.
Sources: the UC 2.12.0 documentation snapshot and rustdoc only; every
assumption is numbered in `../../LEDGER.md`.

## 1. Determinism boundary

| Inside the replicated state machine (hot path) | Outside |
|---|---|
| `KvSm::apply` — decode a command frame, mutate the map, encode a response | `kv` CLI: argument parsing, hex/UTF-8 conversion, printing |
| `KvSm::query` — decode a query frame, read the map, encode a response | `kv-service` process wiring: attach, signals, `is_alive` supervision |
| `KvSm::{freeze, stream_snapshot, install_snapshot}` — image codec | The gateway's session envelope and `Sessioned<KvSm>`'s dedup table (UC-owned, deterministic by its own contract) |

Inside the boundary: no clock, no randomness, no I/O, no `HashMap`, no
floats, no panics on foreign input. The only ordered collection is a
`std::collections::BTreeMap` behind an `Arc` (ordered by key bytes). Malformed input
produces a `BAD_REQUEST` response, never a panic — the raw tier was chosen
partly because the typed tier fail-stops on a malformed `QUERY`
(`limits.md` § residuals).

## 2. State shape

```
KvSm {
    map:          Arc<BTreeMap<Bytes, Entry { version: u64, value: Bytes }>>,
    last_applied: Option<u64>,   // the cursor the framework resumes from
    digest:       u64,           // XOR of fnv1a64(key ‖ version ‖ value) over all entries
}
```

**Version** = the absolute log byte position (`ctx.position`) of the command
that last wrote the key. It is unique per write, strictly increasing over a
key's history, returned by every write and by `Get`, and is what `CAS`
compares. `0` means "absent" (a user frame never applies at position 0 —
L11).

**Digest** is an order-independent, incrementally maintained hash of the
whole map: O(1) per apply, and the `DIGEST` query returns
`(count, digest, last_applied)` so replicas can be compared cheaply through
snapshot reads on each gateway (SDLC § 3 "cross-node divergence checks").

**Why `Arc<BTreeMap>`.** `freeze()` runs on the apply thread and must be O(1)
(`state-machine-contract.md` § Snapshots), and that contract names the shape:
"clone an `Arc`". `freeze` clones the `Arc`; the first `insert`/`remove`
after a freeze pays one O(n) copy through `Arc::make_mut`, once per instant,
and every later write is an ordinary O(log n) B-tree operation. The store
originally used `im::OrdMap` (a persistent B-tree, O(log n) path-copying on
every write instead) — dropped 2026-09-20 because `im` is unmaintained
(archived 2026-05-03, RUSTSEC-2026-0248) with an open unsoundness advisory on
its shared node-insertion path (RUSTSEC-2023-0126), and a worked example must
not carry that. The trade is one O(n) copy per instant against a dependency;
`docs/benchmarks/` holds the measured OrdMap-vs-HashMap numbers if that trade
is ever revisited.

**Memory.** Limits (§ 5) allow 256 B keys and 1024 B values, so a few hundred
thousand keys is at most ~300k × (256 + 1024 + ~100 B of node/`Bytes`
overhead) ≈ 400 MiB worst case, well inside this host.

## 3. Wire formats (the application's contract with itself)

Exact bytes in `WIRE-FORMAT.md`. Summary: a 1-byte format version, a 1-byte
op, a `u16` key length, the key, then op-specific fields; every integer LE.
Responses start with a 1-byte status. Every frame is self-delimiting from
the front, so a later version can append fields (SDLC § 2.2: append only).

The UC layers around it: the gateway prepends the 16-byte session envelope
to every `SUBMIT` (`remote-protocol.md`); `Sessioned` prepends a 1-byte tag
to every write response, which the gateway lifts into `RESPONSE` flags.
Queries are neither enveloped nor tagged.

## 4. Failure semantics

| Event | What happens |
|---|---|
| Leader election / failover | `kv` (via `uc_remote::RemoteClient`) follows `REDIRECT`/`LEADER_CHANGED`, re-sends unanswered requests in `seq` order; `Sessioned` answers a re-send of an applied write `REPLAYED` — applied once. |
| Node crash-restart | The node replays its journal; `kv-service` reattaches (supervised restart: `is_alive` → exit non-zero), reconstructs from journal or snapshot + tail, resumes at `last_applied`. Acknowledged writes were quorum-`fsync`'d before the ack. |
| Service crash | Same reconstruction; the dedup table is rebuilt by replay (it is a pure function of the applied prefix) or restored from the snapshot image. |
| Network partition | Minority side cannot commit; `kv` times out (`request_timeout`) or fails over to a reachable gateway. Linearizable reads on the minority fail the read barrier → `RETRY` → client fails over. |
| Mixed-version cluster | Not supported by the platform at 2.12.0 (SDLC § 5 status): an application upgrade is a flag day. See § 7. |
| Client process restart with a reused `client_id` | Starts at `seq = 1` again — its old cached answers are replayed (L9). `kv` defaults to a fresh random id per process. |

## 5. Limits and the arithmetic

The platform's command ceiling is discovered per cluster
(`limits.md` § Hard limits): 1344 B (crypto off) / 1312 B (crypto on) at the
1408 B baseline rung every cluster starts from, up to 8896 / 8864 B on a
proven jumbo path. The one value that holds on **every** cluster is
`STANDARD_PAYLOAD = 1312` (`uc_remote::engine`). From there:

```
1312   standard ceiling (baseline rung, crypto on)
 -16   session envelope the gateway prepends (client_id u64 ‖ seq u64)
= 1296 application command budget

PUT frame  = 1 (fmt) + 1 (op) + 2 (key_len) + key + value            = 4 + key + value
CAS frame  = 1 + 1 + 2 + key + 8 (expected_version) + value          = 12 + key + value

MAX_KEY   = 256 B
MAX_VALUE = 1024 B
worst PUT = 4  + 256 + 1024 = 1284 ≤ 1296  ✓
worst CAS = 12 + 256 + 1024 = 1292 ≤ 1296  ✓  (4 B headroom)
GET query = 4 + 256 = 260 B
GET reply = 1 (status) + 8 (version) + 1024 = 1033 B
```

The limits are enforced in the state machine (a violating frame gets
`BAD_REQUEST`, never applied) and in `kv` before sending.

## 6. Invariants (test targets)

1. **Determinism / replay.** Applying the same command sequence to two fresh
   `KvSm`s yields identical `(map, digest, last_applied)`; a golden sequence
   pins a digest constant.
2. **Version = position.** After `Put`/`CAS` at position P, `Get` returns
   version P; versions of a key are strictly increasing.
3. **CAS.** Succeeds iff `expected == current` (0 ⇔ absent); on failure the
   map is unchanged and the reply carries the current version.
4. **Digest.** `digest` equals a from-scratch recomputation after any
   sequence of ops, including through snapshot install.
5. **Snapshot round-trip.** `freeze → stream → install` on a fresh SM
   reproduces map, digest and `last_applied`; `install_snapshot(P, …)` returns
   `P` and leaves `last_applied < P`.
6. **Idempotent skip.** `last_applied` never regresses; re-applying a
   position ≤ `last_applied` is the framework's job, but the SM never
   depends on it.
7. **No panic on any input** to `apply`/`query` (fuzz-style proptest).
8. **Exactly-once through `Sessioned`.** The same `(client_id, seq)` frame
   applied twice yields `TAG_FRESH` then `TAG_REPLAYED` with the identical
   inner response, and the map is unchanged by the second.

## 7. Versioning and upgrade plan

- Command/query/response formats carry a format-version byte (`1`).
  Additions are new op codes or appended fields; nothing is redefined.
- The snapshot image carries `image_version: u32 = 1`. A new binary must
  read every older image version it has ever written.
- `KvSm::VERSION` is packed `1.0.0`, checked for equality on the snapshot
  path by the platform.
- **Rolling upgrade is not available at 2.12.0** (`application-sdlc.md` § 5
  "Status"). Upgrading the store is a flag day: stop traffic, stop every
  `kv-service`, install the new binary, start them all. If the new version
  changes the image, it must also read v1 images (old snapshot + new binary
  ⇒ correct state). Rollback: the old binary must refuse an image version it
  does not know (it does: `install_snapshot` returns `Codec`), so roll back
  only to a set at a position before the upgrade, or wipe `snapshots/` and
  replay the journal if purge has not removed the prefix.

## 8. Observability

The platform exports per-FSM apply/lag/snapshot metrics on the node's
`[metrics]` endpoint (`monitor-a-cluster.md`); the store adds nothing there.
`DIGEST` is the application-level divergence check; `kv-service` logs
attach, stop, and fail-stop to stderr. Runbook: `README.md` § Operating.

---

# v2 — Append and List-read (design note, second pass)

Per `docs/reference/application-sdlc.md` § 1, for the version change and the
snapshot-format change. Ledger entries L17–L22 are the material-gap record.

## 9.1 Determinism boundary — unchanged

Append and List run inside the state machine, deterministically, like the v1
ops. No new clock, randomness or I/O. Lists are `Vec<Bytes>` inside the
`Arc<BTreeMap>`, so `freeze()` stays O(1) (the `Arc` clone) and a list's
elements are copied with the map on the first write after a freeze. The
digest stays incremental.

## 9.2 State shape

An entry is now `{ version: u64, shape: Value(Bytes) | List(Vector<Bytes>) }`.
A key is a value or a list, never both.

- **Version of a list** = the position of its most recent Append (a fresh
  list's = its first Append's). Same rule as a value: the position of the
  write that last changed the key.
- **Shapes are strict.** Put/CAS/Get require a value; Append/List require a
  list; the wrong shape returns `WRONG_SHAPE` and changes nothing. **Delete**
  removes either shape (reporting its version); the key may then be recreated
  as the other shape. This keeps every edge case total and observable rather
  than overloading an op (e.g. "Get on a list" is an explicit error, not a
  guess).
- **Caps:** `MAX_LIST_LEN = 4096`, `MAX_LIST_BYTES = 65536`, element ≤ 1024 B.
  Chosen so the largest List reply (~82 KB) stays far under UC's 1 MiB remote
  frame and 4 MiB egress ring (L6 is still open on the exact response ceiling;
  this stays well clear of every candidate). Append still fits one datagram —
  it is a single-element command, framed exactly like Put.
- **Digest continuity** (the load-bearing choice for the old-image test): a
  value entry hashes byte-for-byte as in v1, so a v1 image's stored digest
  recomputes under v2 and can be *verified*, not just parsed. List entries are
  domain-separated (`b"L"` prefix) so a value and a single-element list of the
  same bytes never collide.

## 9.3 Versioning the state machine (L17)

`KV_VERSION` moves `1.0.0` → `2.0.0` (`pack_version(2,0,0)`), computed in-crate
because `uc_protocol::identity::pack_version` is in an unpromised crate. Major,
because a v1 binary can neither apply Append nor read a v2 image — there is no
compatibility relation the platform understands, so the digit is a signal to
operators and to the snapshot-path equality check, nothing more. `NAME` stays
`"kv"` (the row identity must not move).

## 9.4 Snapshot format change (L18)

Image version 2 adds a `shape` byte per entry; the reader dispatches on the
`image_version` word and accepts 1 (no shape byte, all values) and 2. Forward
compatibility (old image, new binary) is the SDLC requirement and is met and
tested. Backward (new image, old binary) is **refused by name** — a v1
`install_snapshot` fail-stops on version 2 — which is safe (never a misread)
but makes rollback one-way past the first v2 snapshot. Documented, not fixed:
the platform offers no migration hook, so this is the application's to own.

## 9.5 Rollout (L19, L20, L22) — a flag day, because the platform has no rolling upgrade

`application-sdlc.md` § 5 states plainly that a rolling application upgrade is
not supported at 2.12.0. So: stop every `kv-service`, install v2, start every
`kv-service`; nodes and gateways stay up. Acknowledged writes are not lost
(quorum-durable in the log; a v2 service reconstructs from the floor + tail).
Mixed operation is unsafe and is demonstrated as such, never used: a v2 leader
commits an Append v1 followers cannot apply, and a failover to a v1 successor
makes that acknowledged write invisible until every service is v2 and replays.
The platform makes this *visible* (`Uc2ServiceVersionDrift`,
`uc2_service_version`) but does not prevent it. Rollback: only to a set before
the first v2 snapshot.

## 9.6 Invariants added (test targets)

9. Shapes are strict and Delete crosses them (unit).
10. A list is ordered and List reads it oldest-first (unit + model proptest).
11. Caps enforced in `apply`, not just the client (unit).
12. Image v2 round-trips both shapes; **a v1 image installs into a v2 binary
    and reproduces the v1 digest** (the old-image test, against a real v1
    artifact fixture); a v1 binary refuses a v2 image (cluster).
13. The whole upgrade path on a real cluster: v1 data survives, mixed-version
    divergence appears and heals, rollback is refused (cluster).
