# Task 03 — M3: Shmem Ring Buffers + Service Process Split

**Status:** In progress. **Phase 1 complete** (uc_protocol primitives, M3 Tasks 1-7 + tail-wrap OOB fix). Phase 2 (uc_service runtime, uc_node IPC wiring, multi-node shmem tests) is pending — Tasks 8-22 in `docs/superpowers/plans/2026-05-11-uc-m3-shmem-service-split.md`.
**Branch:** `main`, commits `01b53fa..bd70a7a` (9 commits, +5,795 lines).
**Workspace:** `ultima_cluster/`.

## Goal (full M3)

Split the user's state machine into a separate **`uc_service`** process, with shared-memory IPC between `uc_node` (Raft engine) and `uc_service` (deterministic apply / query / output). Same-host clients also reach `uc_node` over shmem via the future **`uc_client`** crate (full client wiring lands in M4).

```
[client process]     ──shmem──▶  [uc_node]  ◀──QUIC──▶  [uc_node on peer host]
                                     ▲
                                     │ shmem
                                     ▼
                                [uc_service]
```

After M3, `cargo run -p uc_node` + `cargo run -p uc_service` in two terminals on the same host should form a single replicated state-machine instance; M2's "embedded SM in `uc_node`" path becomes a special-case `IpcMode::Embedded`.

## Goal (Phase 1 — this task doc)

Land the on-the-wire primitives that the rest of M3 builds on: lock-free ring buffers, the `cnc.dat` control file, per-RPC frame types, and the heartbeat/handshake helpers. Everything lives in the `no_std`-leaning `uc_protocol` crate — language-gate so a future Python/Java/JS SDK can implement the same wire format without depending on `uc_node` or `uc_service`.

## Scope

**Phase 1 — shipped:**
- Workspace deps for ring/cnc layer (`memmap2`, `parking_lot`, `page_size`, `crc32fast`, `tempfile` dev-dep).
- `uc_protocol::ring::common` — `RingHeader` (192 bytes, cache-padded), per-record `FrameHeader`, length-last record framing, `write_record_at` / `try_read_record_at` / `write_padding_marker_at`, `RingError`.
- `uc_protocol::ring::spsc` — single-producer single-consumer ring (Task 2).
- `uc_protocol::ring::mpsc` — multi-producer single-consumer ring with CAS-claim (Task 3).
- `uc_protocol::ring::broadcast` — single-producer many-consumer ring (Task 4).
- `uc_protocol::cnc` — `cnc.dat` layout, `CncHeader` (256-byte header with CRC), `NodeStatus` / `ServiceStatus` (64-byte cache-aligned status blocks), `sub::*` sub-buffer index constants, `init_cnc` / `validate_cnc` (Task 5).
- `uc_protocol::frames::{apply, query, snapshot}` — `msg_type` discriminants + `header_extra` codecs for the apply ring, query/response, and snapshot build/install handshake (Task 6).
- `uc_protocol::handshake` — `ServiceReady` / `RoleChanged` frame helpers (Task 7).
- `uc_protocol::liveness` — `tick_node` / `tick_service`, `HeartbeatWatcher` for monotonic-seq + wall-time stall detection (Task 7).
- Tail-wrap padding-marker OOB fix (see "Padding alignment" below).

**Phase 2 — pending (still in `docs/superpowers/plans/`):**
- `uc_service::ultima_db` adapter module (Task 8).
- `uc_service` runtime + `ServiceBuilder` (Task 9).
- Service-side attach + handshake (Task 10).
- Service apply/query/heartbeat loops (Task 11).
- `uc_node::ipc` instance-directory owner (Task 12).
- `ipc::service_link` (apply/query ring lifecycle, owned by node) (Task 13).
- Node-side heartbeat + service-handshake watcher (Task 14).
- Shmem-mode `AdaptedStateMachine` (Task 15).
- `NodeConfig::ipc_mode` + dispatch in `NodeBuilder` (Task 16).
- 5 integration tests: in-process shmem, query roundtrip, 3-node shmem cluster, service-crash + leadership-transfer, ultima_db adapter e2e (Tasks 17-22).

## Architecture (relevant to Phase 1)

### `uc_protocol` posture

`uc_protocol` was M1-only `no_std`-friendly. M3 relaxes that for the new modules (`ring`, `cnc`, `frames`, `liveness`, `handshake`) which need `memmap2` and `std::sync::atomic`. The three pure-data modules — `version.rs`, `magic.rs`, `error_codes.rs` — stay `core`-only-compatible. Multi-language SDKs reimplement the wire format directly; they don't link `uc_protocol`.

### Ring buffer record layout

```
length_inclusive_header  u32   total record size (header + payload + crc)
msg_type                 u16
flags                    u16
header_extra             [u8; 8]   per-msg-type metadata
payload                  variable
crc32                    u32   over (msg_type..end-of-payload)
```

`FRAME_HEADER_LEN = 16`, `FRAME_TRAILER_LEN = 4`. Records are length-prefixed with the length **written last** (Release fence + non-atomic store on `*mut u8`). Consumers Acquire-load `producer_position` and treat `length == 0` as "claimed but not yet committed."

**Padding alignment (post-M3-Task-7 fix).** All position advancements (`producer_position`, `consumer_position`, broadcast `head`) round up to `RECORD_ALIGN = 8`. The on-wire length field still stores the unaligned record size — `align_record_size()` is applied only when bumping positions. Two properties fall out:

1. `producer_position & (capacity - 1)` is always a multiple of `RECORD_ALIGN`, so `bytes_to_tail = capacity - slot_offset` is also a multiple of `RECORD_ALIGN` and ≥ 8 whenever a wrap is needed.
2. The padding marker's 6-byte write (4-byte length + 2-byte msg_type = `PADDING_MSG_TYPE = 0xffff`) fits unconditionally inside that 8-byte minimum.

Without this, tiny payloads (e.g. 1-byte payload → 21-byte record) on a small capacity could leave `bytes_to_tail < 6` and `write_padding_marker_at` would scribble past the slot region. The fix is structural — no special-case at the OOB callsite — and the SPSC regression test `tiny_payload_tail_wrap_no_oob` drives 200 wraps against a 64-byte ring to lock it in.

`init_ring_header` additionally rejects `capacity_bytes < RECORD_ALIGN`.

### Three ring shapes

| Ring | Producers | Consumers | Producer pos update | Consumer pos update | Use case |
|---|---|---|---|---|---|
| `SpscRing` | 1 (`&mut self`) | 1 (`&mut self`) | Release store after record bytes | Release after consume | service↔node apply / apply_resp / output (M3 Task 13) |
| `MpscRing` | N (`MpscProducer: Clone`, `&self`) | 1 (`&mut self`) | AcqRel CAS *before* record bytes | Release after consume | clients→node submit ring (M4), cnc control rings (M3) |
| `BroadcastRing` | 1 (`&mut self`) | N (each holds own `head: u64`) | Release after record bytes | n/a (in-memory `head`) | node→clients response broadcast (M4) |

All three share the same `RingHeader` + per-record framing, differ only in the producer/consumer half. The producer/consumer split is enforced structurally (separate types, `&mut self` for single-writer/single-reader contracts).

### Known limitation — MPSC/Broadcast post-wrap torn-record race

For MPSC and Broadcast, the producer advances `producer_position` **before** writing the record. The `length == 0 → not yet committed` check works on the **first generation** (mmap is zero-initialized), but **not after wrap-around** — stale length bytes from the previous generation at the same offset can spoof a commit. Documented prominently in `ring::mpsc` and `ring::broadcast` module headers. Fix tracked for M4:
- (a) a separate "published-up-to" position that producers advance in claim order (LMAX-Disruptor-style), or
- (b) per-slot generation counters.

In M3 this is acceptable because MPSC is only used for the cnc control rings (handshake + role-change — tiny traffic, no wrap expected) and Broadcast is not yet wired. **MPSC/Broadcast must not be used for high-traffic rings until M4 lands the fix.**

### `cnc.dat` layout

```
offset   contents                                                  size
──────   ─────────────────────────────────────────────────────     ────
   0     CncHeader (magic, protocol_version, app_id, instance_id,
         node_id, sub_buffer_offsets[8], sub_buffer_sizes[8], CRC)  256
 256     NodeStatus (heartbeat_seq, heartbeat_at_ns, role,
         current_term, last_applied, leader_node_id, …)              64
 320     ServiceStatus (heartbeat_seq, heartbeat_at_ns, state,
         last_applied, last_handshake_at_ns, …)                      64
 384     control_to_service ring (MPSC: RingHeader + 16 KiB slots)
   …     control_to_node    ring (MPSC: RingHeader + 16 KiB slots)
```

`sub::*` indices are stable across protocol-version-minor bumps; additions go to higher indices. Indices 4-7 are reserved for M4/M5 (`CONTROL_TO_CLIENTS`, `COUNTERS_METADATA`, `COUNTERS_VALUES`, `ERROR_LOG`).

`CncHeader::header_crc32` (offset 248) protects the whole header except the CRC field itself. `validate_cnc` rejects bad magic / bad CRC. Per-IPC-entry checks for `app_id` / `instance_id` / `protocol_version` are **not yet implemented** in `validate_cnc` — they belong at the `uc_node` / `uc_service` attach paths and will land in Phase 2 Tasks 10 / 12.

### Frame types

| `msg_type` | Frame | Ring | Direction |
|---|---|---|---|
| `1` | `ApplyFrame { log_index → header_extra, cmd → payload }` | `service/apply.ring` (SPSC) | node → service |
| `2` | `ApplyRespFrame { log_index → header_extra, resp → payload }` | `service/apply_resp.ring` (SPSC) | service → node |
| `50` | `QueryFrame { kind ∈ {Linearizable, Snapshot}, request_id, query → payload }` | `service/query.ring` (MPSC) | clients → service via node |
| `51` | `QueryRespFrame { request_id, response → payload }` | `service/query_resp.ring` (SPSC) | service → node |
| `100` | `BuildSnapshot { /* empty header_extra */ }` | cnc control rings | node → service |
| `101` | `SnapshotBuilt { log_index → header_extra }` | cnc control rings | service → node |
| `200` | `ServiceReady { last_applied → header_extra }` | `control_to_node` (MPSC) | service → node |
| `201` | `RoleChanged { role → header_extra }` | `control_to_service` (MPSC) | node → service |

All `header_extra` fields are little-endian — pinned by a unit test (`snapshot_built_is_little_endian`) to lock the contract for cross-language SDKs.

### Liveness

Each side increments its own `heartbeat_seq` (relaxed `fetch_add`) and bumps `heartbeat_at_ns` on a fixed tick (typically 100ms). `HeartbeatWatcher` records the last observed `seq` + wall-time. Both signals are useful:
- `seq` change → peer is alive and ticking;
- wall-time → guards against the watcher itself waking from a long pause and falsely declaring death.

Liveness ops are all `Relaxed` because the seq doesn't protect any data — it's a free-running counter, and the `HeartbeatWatcher` only needs eventual consistency.

## Files added / changed in Phase 1

```
Cargo.toml                              # +6  workspace deps
Cargo.lock                              # +96 transitive
uc_protocol/Cargo.toml                  # +11 mmap2/parking_lot/page_size/crc32fast/tempfile
uc_protocol/src/lib.rs                  # +18 module wiring
uc_protocol/src/ring/{mod,common,spsc,mpsc,broadcast}.rs    # +1,450
uc_protocol/src/cnc.rs                  # +352
uc_protocol/src/frames/{mod,apply,query,snapshot}.rs        # +151 +34 tests
uc_protocol/src/handshake.rs            # +88
uc_protocol/src/liveness.rs             # +143
```

## Tests

`uc_protocol` went from 4 tests at M1-merge to **38** at end-of-Phase-1.

| Module | Tests | Coverage |
|---|---|---|
| `version::tests` | 4 | (M1) pack roundtrip, compat cases |
| `ring::common::tests` | 4 | init/validate round-trip; reject bad magic, undersized buffer, non-power-of-two capacity |
| `ring::spsc::tests` | 6 | single-record round-trip, empty returns None, full returns Err, wrap-around steady drain, cross-thread send, **tiny_payload_tail_wrap_no_oob** |
| `ring::mpsc::tests` | 2 | single-producer round-trip, 8 producers × 50 msgs no-wrap fan-in |
| `ring::broadcast::tests` | 2 | one producer + two consumers same records; slow consumer gets `Overwritten` |
| `cnc::tests` | 4 | init+validate round-trip, reject bad magic, reject bad CRC, sub-buffer offsets populated |
| `frames::apply::tests` | 1 | log_index round-trip |
| `frames::query::tests` | 3 | kind round-trip, unknown discriminant rejected, reserved bytes zero |
| `frames::snapshot::tests` | 3 | discriminant stability (100/101), log_index round-trip, little-endian byte-order pin |
| `handshake::tests` | 2 | service_ready round-trip, role_changed round-trip per-role |
| `liveness::tests` | 3 | watcher dead after timeout; node tick; service tick |

All ring/cnc tests use `tempfile::NamedTempFile` + real `MmapMut::map_mut` (mmap is page-aligned; a `Vec<u8>` would only be byte-aligned and would UB when cast to `*const RingHeader`).

## Notable design decisions

- **`RECORD_ALIGN = 8` chosen as the minimum that fits the 6-byte padding marker.** Could have been larger (e.g. 16 for cache-line-quarter alignment) but 8 minimizes wasted space for small records. The invariant is documented at the `RECORD_ALIGN` const and re-stated at each `bytes_to_tail` site.
- **Length field stores unaligned size; advance uses aligned size.** Keeps the wire format unchanged from a cross-SDK perspective (length still equals `FRAME_HEADER_LEN + payload + FRAME_TRAILER_LEN`); only the position arithmetic carries the alignment.
- **`RingError` covers all of ring + cnc + frame decode** (single error type via `Corrupt(String)` for non-ring paths). May split later if the call sites need exhaustive matching by category.
- **MPSC/Broadcast wrap race is shipped as a documented limitation** rather than blocked on the fix, because M3's cnc control rings stay within first generation and Broadcast is unused in M3. Fix scheduled for M4 alongside `clients/submit.ring`.
- **`unsafe impl Send for FooInner` on each ring's Inner** is intentional per CLAUDE.md — the mmap is owned, atomics provide the synchronization, and the SAFETY comments name the invariants.
- **`tempfile = { workspace = true }` for tests, not hand-rolled tempdirs** — keeps test cleanup tied to the test's `_t` binding lifetime.

## Follow-ups tracked for Phase 2 / later milestones

- **Phase 2 wiring** — Tasks 8-22 in the plan (uc_service runtime, uc_node IPC owner, 5 integration tests). Plan file stays under `docs/superpowers/plans/` until Phase 2 ships.
- **Hard Rule 11 enforcement** — `validate_cnc` currently only checks magic + CRC. Phase 2 Task 10/12 will add the `(app_id, instance_id, protocol_version)` trio check at the uc_node/uc_service attach paths. Could also be lifted into `validate_cnc(expected_trio)` directly.
- **MPSC/Broadcast post-wrap fix** (M4): published-up-to position or per-slot generation counters. Tracked in `ring::mpsc` and `ring::broadcast` module headers.
- **`CncHeader::app_id_str`** returns `""` on bad UTF-8, which weakens app_id matching to "two empty-strings match." Tighten to `Option<&str>` or compare raw bytes (Phase 2 polish).
- **`HeartbeatWatcher` monotonicity** — `seq != self.last_seq` currently accepts a regression as "alive." Tighten to `seq > self.last_seq` and surface a typed `SeqRewound` if the peer rewinds (Phase 2 polish).
- **`BroadcastRing::producer(&self)`** can be called repeatedly, breaking the single-producer invariant structurally. Change to `into_producer(self)` or `Option`-take the handle before M4 wires high-traffic broadcast traffic.
- **Doc clean-up** — several "M5" vs "M4" callouts in `cnc.rs` and forward-looking comments in `frames/snapshot.rs` are rot-prone; revisit at M3 ship time.
- **`fix(uc_protocol)` commit `8203ad1` carried two cosmetic fmt drifts** (`cnc.rs`, `handshake.rs`) — not strictly part of the OOB fix but folded in so `cargo fmt --check` stayed green.

## Build/test/lint

```bash
cargo build --workspace
cargo test  --workspace          # 38 in uc_protocol, plus M1+M2 tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

All green at Phase 1 close (commit `bd70a7a`).

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (Section 4: shmem layer; Section 5: cnc; Section 6: rings).
- Phase 2 implementation plan: `docs/superpowers/plans/2026-05-11-uc-m3-shmem-service-split.md` (Tasks 8-22).
- M1 record: `docs/tasks/task01_m1_embedded_single_node.md`.
- M2 record: `docs/tasks/task02_m2_multi_node_quic.md`.
- `memmap2` 0.9 / `crc32fast` 1 / `parking_lot` 0.12 / `page_size` 0.6.
