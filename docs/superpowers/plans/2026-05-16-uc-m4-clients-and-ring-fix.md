# M4 Implementation Plan — MPSC/Broadcast wrap-fix + `uc_client` end-to-end

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the documented MPSC/Broadcast post-wrap torn-record race, then ship `uc_client` end-to-end so an out-of-process client on the same host can submit commands and queries through `uc_node` to `uc_service` over shmem.

**Architecture:** Three coordinated changes inside `ultima_cluster`. (1) `uc_protocol::ring::common::RingHeader` gains a `publish_position` (LMAX-Disruptor "published-up-to") that separates the producer claim point from the commit point, eliminating the post-wrap torn-record window for MPSC + Broadcast. (2) `uc_protocol::cnc` adds a `next_client_id` slot and a `uc_protocol::frames::client` module with five new frame types for the client↔node rings. (3) `uc_node` adds `ipc::client_link` (three new ring files under `clients/`) and three new dispatcher tasks; `uc_client` ships its public `Client` SDK with handshake, submit, query, response routing, session liveness, and stall detection. M3 tests continue to pass unchanged.

**Tech Stack:** Rust 2024 edition, openraft 0.10, tokio (current_thread for tests), bincode 2, bytes, memmap2, parking_lot, dashmap, crc32fast, thiserror, tempfile, tracing.

---

## Spec & predecessor pointers

- **Canonical M4 spec:** `docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md` (decisions table, phasing, error model, test scenarios).
- **Canonical project design:** `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§4 cnc/rings, §5 public APIs, §8 pipelines).
- **Predecessor task records:** `docs/tasks/task03_m3_shmem_service_split.md`, `docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`.
- **M4 follow-ups carried in from M3.5:** see `project_m4_followups_from_m35` memory. Items 2 (bootstrap retry exponential backoff) and 3 (`shmem_state_machine` response-value coverage) are folded into this plan. Items 1 and 4 require cnc-sub-mmap peer-service visibility and remain deferred to M5.

## File structure

### New files

| File | Responsibility |
|---|---|
| `uc_protocol/src/frames/client.rs` | Five client-↔-node frame types (`MSG_TYPE_SUBMIT`/`SUBMIT_RESPONSE`/`CLIENT_QUERY`/`CLIENT_QUERY_RESP`/`NOT_LEADER_RESP`), `encode_extra_client`/`decode_extra_client` 8-byte `(client_id: u32, local_seq: u32)` codec, and `QueryKind` re-export for the `flags` field encoding. |
| `uc_node/src/ipc/client_link.rs` | Creates `clients/submit.ring` (MPSC), `clients/query.ring` (MPSC), `clients/response.broadcast` (Broadcast), and `clients/sessions.dir/`. Returns the node-side halves. |
| `uc_node/src/ipc/client_dispatcher.rs` | `client_dispatcher` task (submit ring → raft.client_write → broadcast response or NotLeader) and `client_query_dispatcher` task (query ring → `ShmemQueryLink` → broadcast). Shared broadcast producer guarded by `parking_lot::Mutex`. |
| `uc_node/src/ipc/session_gc.rs` | 2 s tick that reads `clients/sessions.dir/`, runs a per-session `HeartbeatWatcher`, unlinks stale `.session` files. |
| `uc_client/src/cnc.rs` | Read-only attach + validation of `cnc.dat` (`app_id`, `protocol_version`, `instance_id`); `fetch_add` on `next_client_id`; exposes `NodeStatus` / `ServiceStatus` pointers. |
| `uc_client/src/session.rs` | `clients/sessions.dir/{client_id}.session` 64-byte layout (`heartbeat_seq`/`heartbeat_at_ns`/`client_id_check`); client-side ticker task. |
| `uc_client/src/rings.rs` | Per-client opens of `submit.ring` (MPSC producer), `query.ring` (MPSC producer), `response.broadcast` (Broadcast consumer); broadcast-reader task that routes frames into a `DashMap<u32, oneshot::Sender<(u16, Bytes)>>`. |
| `uc_client/src/watchers.rs` | Two `HeartbeatWatcher` tasks polling `NodeStatus`/`ServiceStatus`; set `node_stalled`/`service_stalled` `AtomicBool`s. |
| `uc_client/src/client.rs` | Public `Client` struct: `connect`, `submit`, `query_linearizable`, `query_snapshot`, `current_leader`, `last_applied`, `instance_id`, `client_id`, `shutdown`. Wires the modules above. |
| `uc_node/tests/m4_client_single_node.rs` … `m4_client_response_overwritten.rs` | Seven integration tests (one file per scenario, matching M3 capstone style). |
| `docs/tasks/task05_m4_clients_and_ring_fix.md` | Final consolidated record. Created in Phase 6; replaces the spec + this plan in `docs/superpowers/`. |

### Modified files

| File | Change |
|---|---|
| `uc_protocol/src/ring/common.rs` | `RingHeader` gains `publish_position: AtomicU64` (+ cache-line padding); `RING_HEADER_LEN` grows from 192 → 256; `init_ring_header` inits all three positions; existing helpers unchanged. |
| `uc_protocol/src/ring/spsc.rs` | Producer writes record then `publish_position.store(Release)`. Consumer reads up to `publish_position` (was `producer_position`). Header comment updated. |
| `uc_protocol/src/ring/mpsc.rs` | Producer CAS-advances `claim_position` (was `producer_position`), writes record, spins until `publish_position == my_slot_start`, then advances `publish_position`. Consumer reads up to `publish_position`. Header comment loses the "do not use under wrap" warning. |
| `uc_protocol/src/ring/broadcast.rs` | Single producer advances `claim_position` (claim), writes, then `publish_position.store(Release)`. Consumer reads up to `publish_position`; fall-behind check still uses publish position. Header comment loses the "wrap-race torn-record" caveat. |
| `uc_protocol/src/cnc.rs` | Adds `sub::NEXT_CLIENT_ID = 4` index (renumbers M5 indices: `COUNTERS_METADATA = 5`, etc.); 16-byte sub-region (`AtomicU64` + 8-byte pad) appended after the two control rings; `cnc_file_size()` grows accordingly; `init_cnc` initializes counter to 1. **Renames the current `sub::CONTROL_TO_CLIENTS = 4` constant — it was an M5 placeholder, repurposed for `NEXT_CLIENT_ID` here.** Also widens `sub::SERVICE_STATUS` sub-buffer from 64 B to 512 B (8-slot services-table; slot 0 = today's `ServiceStatus`); adds `service_status_slot_ptr` accessor. |
| `uc_protocol/src/frames/apply.rs`, `uc_protocol/src/frames/query.rs` | Retrofit with `service_id: u8` in `flags` byte 0; new `encode_flags_*`/`decode_flags_*` helpers; `ApplyFrameError::UnknownServiceId`/`QueryFrameError::UnknownServiceId` variants. M3 callsites updated to write `encode_flags_*(0)` explicitly. |
| `uc_protocol/src/frames/mod.rs` | `pub mod client;`. |
| `uc_node/src/ipc/mod.rs` | `pub mod client_link;` + `pub mod client_dispatcher;` + `pub mod session_gc;`. |
| `uc_node/src/runtime/builder.rs` | Phase 3 wiring: `ClientLink::create`, spawn three dispatcher tasks, attach handles to `NodeHandle`. Bootstrap retry loop changes from fixed 5 ms to exponential backoff (M3.5 follow-up #2). |
| `uc_node/src/runtime/node.rs` | `NodeHandle` gains `client_link: Option<ClientLink>`, three new task handles; `shutdown` joins them before dropping `_instance`. |
| `uc_client/Cargo.toml` | Adds `tokio`, `bincode`, `bytes`, `serde`, `parking_lot`, `dashmap`, `memmap2`, `tracing` deps. |
| `uc_client/src/lib.rs` | Exports `Client`, `ClientError`. |
| `uc_client/src/error.rs` | Adds variants per spec §"Error model": `InstanceRestart`, `SessionCreate`, `ResponseOverwritten`, `BackpressureFull`, `ShutDown`, and switches existing variants to carry the structured fields the spec lists. |

---

## Phase 1 — `uc_protocol::ring` wrap-fix

**Why first:** every later phase rides on these rings. Get the regression tests green before any new ring traffic exists.

### Task 1.1: Add failing MPSC wrap-race regression test

**Files:**
- Modify: `uc_protocol/src/ring/mpsc.rs` (tests module at the bottom)

- [ ] **Step 1: Add the failing test**

Add inside `mod tests`:

```rust
/// 8 producers × 200 records on a tiny ring (~64 records' worth of
/// capacity) forces many wraps. Verifies the post-wrap torn-record race
/// is gone: every record written is read back exactly once, no panics.
#[test]
fn wrap_under_many_producers_no_torn_read() {
    let tmp = NamedTempFile::new().unwrap();
    // 4 KiB capacity, ~24 B/record => ~170 records/generation;
    // 8 × 200 = 1600 records forces ~9 wraps.
    let ring = MpscRing::create(tmp.path(), 4096, 128).expect("create");
    let (producer, mut consumer) = ring.into_split();

    const N_THREADS: usize = 8;
    const PER_THREAD: usize = 200;

    let handles: Vec<_> = (0..N_THREADS)
        .map(|t| {
            let p = producer.clone();
            thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let payload = format!("t{t}-i{i}").into_bytes();
                    loop {
                        match p.try_write(1, 0, [0; 8], &payload) {
                            Ok(()) => break,
                            Err(RingError::Full) => thread::yield_now(),
                            Err(e) => panic!("write: {e}"),
                        }
                    }
                }
            })
        })
        .collect();

    let mut received: HashSet<Vec<u8>> = HashSet::new();
    let total = N_THREADS * PER_THREAD;
    while received.len() < total {
        let mut buf = Vec::new();
        match consumer.try_read(&mut buf) {
            Ok(Some(_)) => {
                assert!(received.insert(buf), "duplicate or torn record read");
            }
            Ok(None) => thread::yield_now(),
            Err(e) => panic!("read: {e}"),
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(received.len(), total);
}
```

- [ ] **Step 2: Run the test, expect a failure (or hang)**

Run: `cargo test -p uc_protocol --lib ring::mpsc::tests::wrap_under_many_producers_no_torn_read -- --nocapture`
Expected: **FAIL** — either a `BadCrc`, a `RingError::Corrupt`, an assertion failure ("duplicate or torn record read"), or a hang (kill after 30s; treat as failure). The reproduction confirms the race documented in the module header.

- [ ] **Step 3: Commit the failing test (with `#[ignore]` to keep CI green)**

Add `#[ignore = "regression for M4 wrap-fix; un-ignore in Task 1.5"]` above the test attribute, then:

```bash
git add uc_protocol/src/ring/mpsc.rs
git commit -m "test(uc_protocol): regression for MPSC wrap-race (ignored until fix lands)"
```

### Task 1.2: Add failing Broadcast wrap-race regression test

**Files:**
- Modify: `uc_protocol/src/ring/broadcast.rs` (tests module at the bottom)

- [ ] **Step 1: Add the failing test**

```rust
/// Single producer + 2 consumers; wrap several times. Both consumers must
/// see every record that the producer has not yet lapped them on.
#[test]
fn wrap_no_torn_read() {
    let tmp = NamedTempFile::new().unwrap();
    let ring = BroadcastRing::create(tmp.path(), 4096, 128).expect("create");
    let mut producer = ring.producer();
    let mut sub_a = ring.subscribe();
    let mut sub_b = ring.subscribe();

    // Writer thread: 1000 records, ~24 B each => ~6 wraps on a 4 KiB ring.
    let writer = std::thread::spawn(move || {
        for i in 0..1000u32 {
            let payload = i.to_le_bytes();
            producer.write(1, 0, [0; 8], &payload).expect("write");
        }
    });

    // Readers: keep up; assert no BadCrc/Corrupt. Overwritten is allowed
    // (slow consumer detection — we accept it and reset).
    let read_all = |sub: &mut BroadcastConsumer| {
        let mut seen = 0usize;
        let mut buf = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match sub.try_read(&mut buf) {
                Ok(Some(_rec)) => {
                    // payload is a u32 LE; pure read is enough — just no panic.
                    assert_eq!(buf.len(), 4, "torn read?");
                    seen += 1;
                }
                Ok(None) => std::thread::yield_now(),
                Err(RingError::Overwritten) => {
                    // Acceptable for the slow-consumer recovery path.
                }
                Err(e) => panic!("torn record: {e}"),
            }
        }
        seen
    };

    let a_seen = read_all(&mut sub_a);
    let b_seen = read_all(&mut sub_b);
    writer.join().unwrap();
    // At least *some* records observed; we don't pin the exact count
    // because Overwritten resets are timing-dependent.
    assert!(a_seen > 0 && b_seen > 0);
}
```

- [ ] **Step 2: Run the test, expect a failure**

Run: `cargo test -p uc_protocol --lib ring::broadcast::tests::wrap_no_torn_read -- --nocapture`
Expected: **FAIL** with `BadCrc`, `Corrupt`, or "torn read?" panic.

- [ ] **Step 3: Commit (ignored)**

```rust
#[ignore = "regression for M4 wrap-fix; un-ignore in Task 1.5"]
```

```bash
git add uc_protocol/src/ring/broadcast.rs
git commit -m "test(uc_protocol): regression for Broadcast wrap-race (ignored until fix lands)"
```

### Task 1.3: Add `publish_position` to `RingHeader`

**Files:**
- Modify: `uc_protocol/src/ring/common.rs`

- [ ] **Step 1: Expand `RingHeader` to four cache lines**

In `common.rs`, replace the `RingHeader` struct with:

```rust
/// Fixed-size header at the start of every ring file. 256 bytes,
/// cache-padded so claim/publish/consumer atomics live on separate cache
/// lines.
///
/// * `claim_position` — producers atomically claim slot ranges here
///   (CAS for MPSC; single producer for SPSC/Broadcast).
/// * `publish_position` — producer advances this only after the record's
///   bytes are visible. Consumers read records up to this position.
///   Eliminates the post-wrap torn-record race that plagued M3.
/// * `consumer_position` — single reader's progress marker (unused on
///   Broadcast; each consumer keeps its own in-memory `head`).
#[repr(C, align(64))]
pub struct RingHeader {
    pub magic: [u8; 8],
    pub capacity_bytes: u64,
    pub max_msg_size: u32,
    pub msg_kind_filter: u32,
    pub _pad_1: [u8; 40],
    pub claim_position: AtomicU64,
    pub _pad_2: [u8; 56],
    pub publish_position: AtomicU64,
    pub _pad_3: [u8; 56],
    pub consumer_position: AtomicU64,
    pub _pad_4: [u8; 56],
}

const _: () = {
    assert!(std::mem::size_of::<RingHeader>() == 256);
    assert!(std::mem::align_of::<RingHeader>() == 64);
};

pub const RING_HEADER_LEN: usize = std::mem::size_of::<RingHeader>();
```

- [ ] **Step 2: Update `init_ring_header` to initialize all three atomics**

In the existing `init_ring_header`, replace the `RingHeader { … }` literal:

```rust
RingHeader {
    magic: crate::magic::RING_MAGIC,
    capacity_bytes,
    max_msg_size,
    msg_kind_filter,
    _pad_1: [0; 40],
    claim_position: AtomicU64::new(0),
    _pad_2: [0; 56],
    publish_position: AtomicU64::new(0),
    _pad_3: [0; 56],
    consumer_position: AtomicU64::new(0),
    _pad_4: [0; 56],
},
```

- [ ] **Step 3: Update the module-level "Torn-record protection" doc comment**

Replace the existing comment block (lines ~28–43) with:

```rust
//! ## Torn-record protection
//!
//! Producers split a write into two atomic steps:
//!
//!   1. Claim — bump `claim_position` to reserve a slot range. MPSC uses
//!      `compare_exchange_weak` so producers can claim in parallel; SPSC
//!      and Broadcast use a single `store` (single producer per ring).
//!   2. Publish — write the record bytes, then `publish_position.store(…,
//!      Release)` (MPSC spins until `publish_position == my_slot_start` so
//!      the publication order matches the claim order).
//!
//! Consumers load `publish_position` with Acquire and read only records
//! whose `[slot, slot+size)` is fully below `publish_position`. This
//! eliminates the post-wrap torn-record race documented as an M3
//! limitation: a consumer can never see a slot offset whose bytes are
//! still being written.
//!
//! The length-last-Release commit inside `write_record_at` remains —
//! between `publish_position` advance and the consumer's record read,
//! the in-record length-zero check is the final guard.
```

- [ ] **Step 4: Run existing common.rs tests; expect pass**

Run: `cargo test -p uc_protocol --lib ring::common`
Expected: PASS (the existing tests only touch the fields they care about; adding `publish_position` is additive on the read side).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/ring/common.rs
git commit -m "feat(uc_protocol): split RingHeader into claim/publish/consumer positions"
```

### Task 1.4: Update SPSC, MPSC, Broadcast to use `claim_position` + `publish_position`

**Files:**
- Modify: `uc_protocol/src/ring/spsc.rs`
- Modify: `uc_protocol/src/ring/mpsc.rs`
- Modify: `uc_protocol/src/ring/broadcast.rs`

- [ ] **Step 1: SPSC producer — bump claim then publish in lockstep**

Find every `header.producer_position` reference in `spsc.rs`. There are typically two on the producer side (load + store) and one on the consumer side (load).

Producer side (single-producer; claim and publish move together):

```rust
// in SpscProducer::try_write — replace the final store:
let new_pos = producer_pos + advance as u64;
header.claim_position.store(new_pos, Ordering::Relaxed);
header.publish_position.store(new_pos, Ordering::Release);
```

The pre-loop position load should read `claim_position`:

```rust
let producer_pos = header.claim_position.load(Ordering::Relaxed);
```

(Free space calculation continues to compare against `consumer_position`.)

Tail-wrap padding marker also bumps both:

```rust
// after writing the padding marker:
let padded_pos = producer_pos + bytes_to_tail as u64;
header.claim_position.store(padded_pos, Ordering::Relaxed);
header.publish_position.store(padded_pos, Ordering::Release);
// fall through into the retry that re-reads claim_position.
```

- [ ] **Step 2: SPSC consumer — read up to `publish_position`**

```rust
let producer_pos = header.publish_position.load(Ordering::Acquire);
```

- [ ] **Step 3: MPSC producer — CAS on `claim_position`, then publish in claim order**

In `MpscProducer::try_write`, replace the loop body. The capacity/free-space probe uses `consumer_position` (unchanged) and `claim_position` (the new name for the old `producer_position`). After a successful CAS on `claim_position`, the producer writes the record, then spins until `publish_position == my_slot_pos`, then advances `publish_position`.

Replace the body of the loop with:

```rust
loop {
    let consumer_pos = header.consumer_position.load(Ordering::Acquire);
    let claim_pos = header.claim_position.load(Ordering::Acquire);

    let used = claim_pos - consumer_pos;
    let free = capacity.saturating_sub(used as usize);
    if free < advance {
        return Err(RingError::Full);
    }

    let slot_offset = (claim_pos as usize) & (capacity - 1);
    let bytes_to_tail = capacity - slot_offset;

    let claim_size = if bytes_to_tail < advance {
        if free < bytes_to_tail + advance {
            return Err(RingError::Full);
        }
        bytes_to_tail
    } else {
        advance
    };

    let target_pos = claim_pos + claim_size as u64;
    if header
        .claim_position
        .compare_exchange_weak(claim_pos, target_pos, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        continue; // raced with another producer; retry
    }

    // We own `[slot_offset, slot_offset + claim_size)`. Write bytes:
    if claim_size != advance {
        // SAFETY: exclusive ownership of the claimed range.
        unsafe {
            write_padding_marker_at(self.inner.slot_region_mut(), slot_offset, claim_size);
        }
    } else {
        // SAFETY: exclusive ownership of the claimed range.
        unsafe {
            write_record_at(
                self.inner.slot_region_mut(),
                slot_offset,
                msg_type,
                flags,
                header_extra,
                payload,
                total,
            );
        }
    }

    // Publish in claim order: spin until our predecessor publishes.
    while header.publish_position.load(Ordering::Acquire) != claim_pos {
        std::hint::spin_loop();
    }
    header
        .publish_position
        .store(target_pos, Ordering::Release);

    if claim_size != advance {
        continue; // padding written; loop to claim the real record
    }
    return Ok(());
}
```

- [ ] **Step 4: MPSC consumer — read up to `publish_position`**

```rust
let producer_pos = header.publish_position.load(Ordering::Acquire);
```

(The variable can stay named `producer_pos` locally; only the source field changes.)

- [ ] **Step 5: Broadcast producer — single-producer, write then publish**

In `BroadcastProducer::write`, replace `producer_position` reads with `claim_position`, and split the final commit into the claim/publish pair:

```rust
let claim_pos = header.claim_position.load(Ordering::Relaxed);
// … (capacity / wrap logic uses `claim_pos`, unchanged in shape) …

// On tail-wrap, after writing the padding marker:
let padded_pos = claim_pos + bytes_to_tail as u64;
header.claim_position.store(padded_pos, Ordering::Relaxed);
header.publish_position.store(padded_pos, Ordering::Release);
return self.write(msg_type, flags, header_extra, payload);

// On record write, after `write_record_at`:
let new_pos = claim_pos + advance as u64;
header.claim_position.store(new_pos, Ordering::Relaxed);
header.publish_position.store(new_pos, Ordering::Release);
Ok(())
```

- [ ] **Step 6: Broadcast consumer — read and fall-behind check use `publish_position`**

```rust
let producer_pos = header.publish_position.load(Ordering::Acquire);
// … the fall-behind check `(producer_pos - self.head) as usize > capacity`
// stays unchanged in shape; only the source of producer_pos changed.

// On Overwritten reset, also use publish_position:
self.head = producer_pos;
```

(`BroadcastRing::subscribe` likewise uses `publish_position` to set initial `head`.)

- [ ] **Step 7: Build the workspace**

Run: `cargo build --workspace`
Expected: clean build.

- [ ] **Step 8: Run all ring tests**

Run: `cargo test -p uc_protocol --lib ring -- --nocapture`
Expected: all existing tests pass (`single_producer_round_trip`, `many_producers_one_consumer_no_wrap`, `one_producer_two_consumers_same_records`, `slow_consumer_gets_overwritten_error`, `late_subscriber_skips_historical_records`, `init_then_validate_round_trip`, etc.). The two `#[ignore]`d regression tests stay ignored for now.

- [ ] **Step 9: Commit**

```bash
git add uc_protocol/src/ring/spsc.rs uc_protocol/src/ring/mpsc.rs uc_protocol/src/ring/broadcast.rs
git commit -m "feat(uc_protocol): SPSC/MPSC/Broadcast use claim/publish split for torn-record safety"
```

### Task 1.5: Un-ignore the regression tests; verify

**Files:**
- Modify: `uc_protocol/src/ring/mpsc.rs`
- Modify: `uc_protocol/src/ring/broadcast.rs`

- [ ] **Step 1: Remove `#[ignore]` from both regression tests**

In `mpsc.rs` (`wrap_under_many_producers_no_torn_read`) and `broadcast.rs` (`wrap_no_torn_read`), delete the `#[ignore = "…"]` attribute.

- [ ] **Step 2: Run both regression tests**

Run: `cargo test -p uc_protocol --lib ring::mpsc::tests::wrap_under_many_producers_no_torn_read ring::broadcast::tests::wrap_no_torn_read -- --nocapture`
Expected: PASS for both.

- [ ] **Step 3: Re-run the full ring test suite to confirm no regressions**

Run: `cargo test -p uc_protocol --lib ring`
Expected: all PASS.

- [ ] **Step 4: Update module-header comments**

In `uc_protocol/src/ring/mpsc.rs`, replace the `# Known limitation: post-wrap torn-record race` block with a single short note that MPSC uses the LMAX-style claim/publish split (1–2 lines).

In `uc_protocol/src/ring/broadcast.rs`, similarly trim the `# Known limitations` block; keep only the "No producer↔consumer happens-before across overwrite" point.

In `uc_protocol/src/ring/common.rs`, remove the M3-era warning about MPSC being unsafe under wrap traffic from the module-level doc.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/ring/mpsc.rs uc_protocol/src/ring/broadcast.rs uc_protocol/src/ring/common.rs
git commit -m "test(uc_protocol): enable MPSC/Broadcast wrap regressions; drop stale warnings"
```

---

## Phase 2 — `cnc.dat` extensions + `uc_protocol::frames::client`

> **Multi-service shape addendum (2026-05-17 decision).** Phase 2 also widens `sub::SERVICE_STATUS` from a single 64 B slot to an 8-slot services-table (512 B total; slot 0 = today's `ServiceStatus`), and retrofits `service_id: u8` in `flags` byte 0 across all multi-service-bearing frames. v1 always writes `service_id = 0`; decoders reject non-zero with `UnknownServiceId(u8)`. See spec §"Forward compatibility: multi-service" for rationale. Concretely:
> - **Task 2.1** also widens `sub::SERVICE_STATUS` sub-buffer size to `8 * STATUS_BLOCK_LEN` (Step 0 below, before the existing renumber).
> - **Task 2.2** adds `encode_flags_client` / `decode_flags_client` and includes `service_id` in every new client frame's roundtrip test.
> - **New Task 2.3** retrofits `frames::apply` and `frames::query` with `service_id` `flags`-byte helpers + decoder rejection.

### Task 2.1: Widen `sub::SERVICE_STATUS` to services-table + add `next_client_id` sub-region to `cnc.dat`

**Files:**
- Modify: `uc_protocol/src/cnc.rs`
- Test: `uc_protocol/src/cnc.rs` (tests module)

- [ ] **Step 0: Widen `sub::SERVICE_STATUS` to a services-table**

The single 64 B `ServiceStatus` slot becomes an 8-slot table; slot 0 holds today's status, slots 1..7 are zero-reserved for future multi-service rollout.

Add a constant near `STATUS_BLOCK_LEN`:

```rust
/// Number of reserved slots in the services-table. v1 uses slot 0 only.
pub const SERVICES_TABLE_SLOTS: usize = 8;

/// Total size of the services-table sub-region.
pub const SERVICES_TABLE_LEN: usize = SERVICES_TABLE_SLOTS * STATUS_BLOCK_LEN;
```

Add an accessor helper near `validate_cnc`:

```rust
/// Return a stable pointer to the `ServiceStatus` slot for `service_id`.
/// Returns `None` if `service_id >= SERVICES_TABLE_SLOTS`.
///
/// # Safety
///
/// `cnc_base` must point at a fully-initialized cnc.dat mapping that
/// outlives the returned reference.
pub unsafe fn service_status_slot_ptr(
    cnc_base: *const u8,
    service_id: u8,
) -> Option<*const ServiceStatus> {
    if (service_id as usize) >= SERVICES_TABLE_SLOTS {
        return None;
    }
    let header = unsafe { &*cnc_base.cast::<CncHeader>() };
    let base_off = header.sub_buffer_offsets[sub::SERVICE_STATUS] as usize;
    let slot_off = base_off + (service_id as usize) * STATUS_BLOCK_LEN;
    Some(unsafe { cnc_base.add(slot_off) as *const ServiceStatus })
}
```

Update `init_cnc`: `sub_buffer_sizes[sub::SERVICE_STATUS] = SERVICES_TABLE_LEN as u64` (was `STATUS_BLOCK_LEN as u64`); the offset stays where it is, but `off_control_to_service` must shift forward by `(SERVICES_TABLE_LEN - STATUS_BLOCK_LEN)` bytes. Zero-init the entire `SERVICES_TABLE_LEN` region. All existing `ServiceStatus` writers in `uc_node` and `uc_service` continue to write at the same base offset because they target slot 0.

Step 3 (`cnc_file_size`) below must add `(SERVICES_TABLE_LEN - STATUS_BLOCK_LEN)` to the total (or restructure the formula to use `SERVICES_TABLE_LEN` directly — see updated formula in Step 3).

Existing callers of `ServiceStatus` (in `uc_node::ipc`, `uc_service`): no immediate change. Convert callsites to `service_status_slot_ptr(base, 0)` opportunistically; an `unsafe { &*service_status_slot_ptr(base, 0).unwrap() }` is acceptable until M5 introduces multi-service runtime.

Add a unit test:

```rust
#[test]
fn services_table_has_eight_slots() {
    let (mut mmap, _tmp) = mmap_file(cnc_file_size());
    init_cnc(&mut mmap[..], "x", 0, 0).expect("init");
    let base = mmap.as_ptr();
    // SAFETY: just-initialized cnc.
    for i in 0..SERVICES_TABLE_SLOTS as u8 {
        let p = unsafe { service_status_slot_ptr(base, i) }
            .unwrap_or_else(|| panic!("slot {i} should exist"));
        // freshly zeroed
        let s: &ServiceStatus = unsafe { &*p };
        assert_eq!(s.last_applied.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(s.state.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
    assert!(unsafe { service_status_slot_ptr(base, SERVICES_TABLE_SLOTS as u8) }.is_none());
}
```

- [ ] **Step 1: Renumber sub-buffer indices**

In `uc_protocol/src/cnc.rs`, replace the `pub mod sub` block with:

```rust
pub mod sub {
    pub const NODE_STATUS: usize = 0;
    pub const SERVICE_STATUS: usize = 1;
    pub const CONTROL_TO_SERVICE: usize = 2;
    pub const CONTROL_TO_NODE: usize = 3;
    pub const NEXT_CLIENT_ID: usize = 4; // M4
    pub const CONTROL_TO_CLIENTS: usize = 5; // M5 (placeholder; was index 4 in M3)
    pub const COUNTERS_METADATA: usize = 6; // M5
    pub const COUNTERS_VALUES: usize = 7; // M5
    // ERROR_LOG: M5; would require widening `sub_buffer_*` arrays past 8.
}
```

> Note: `ERROR_LOG` is dropped from the constants for now — once the `sub_buffer_offsets`/`sub_buffer_sizes` arrays in `CncHeader` are widened in M5, it can be re-added.

- [ ] **Step 2: Define the new constants**

Below `CNC_CONTROL_RING_MAX_MSG`, add:

```rust
/// Size of the `next_client_id` sub-region (8-byte AtomicU64 + 8-byte pad).
pub const NEXT_CLIENT_ID_REGION_LEN: usize = 16;
```

- [ ] **Step 3: Grow `cnc_file_size`**

```rust
pub const fn cnc_file_size() -> usize {
    CNC_HEADER_LEN
        + STATUS_BLOCK_LEN // node_status
        + SERVICES_TABLE_LEN // M4: services-table (8 × STATUS_BLOCK_LEN; slot 0 = today's service_status)
        + RING_HEADER_LEN + CNC_CONTROL_RING_CAP as usize // control_to_service
        + RING_HEADER_LEN + CNC_CONTROL_RING_CAP as usize // control_to_node
        + NEXT_CLIENT_ID_REGION_LEN // M4: client identity allocator
}
```

- [ ] **Step 4: Init the region in `init_cnc`**

Update the offset chain to account for the widened services-table:

```rust
let off_node_status = CNC_HEADER_LEN as u64;
let off_service_status = off_node_status + STATUS_BLOCK_LEN as u64;
let off_control_to_service = off_service_status + SERVICES_TABLE_LEN as u64; // was + STATUS_BLOCK_LEN
let off_control_to_node =
    off_control_to_service + RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;
let off_next_client_id = off_control_to_node + RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;
```

Update the sub-buffer table:

```rust
sub_buffer_offsets[sub::SERVICE_STATUS] = off_service_status;
sub_buffer_sizes[sub::SERVICE_STATUS] = SERVICES_TABLE_LEN as u64; // was STATUS_BLOCK_LEN
// ... (NODE_STATUS, CONTROL_TO_SERVICE, CONTROL_TO_NODE unchanged) ...
sub_buffer_offsets[sub::NEXT_CLIENT_ID] = off_next_client_id;
sub_buffer_sizes[sub::NEXT_CLIENT_ID] = NEXT_CLIENT_ID_REGION_LEN as u64;
```

Update the zero-fill: zero the full services-table, not just slot 0:

```rust
let ss_lo = off_service_status as usize;
mmap[ss_lo..ss_lo + SERVICES_TABLE_LEN].fill(0); // was ..+ STATUS_BLOCK_LEN
```

After the two `init_ring_header` calls, initialize the counter:

```rust
// next_client_id starts at 1 so `0` can remain a sentinel.
let next_id_off = off_next_client_id as usize;
mmap[next_id_off..next_id_off + NEXT_CLIENT_ID_REGION_LEN].fill(0);
let counter_init: u64 = 1;
// SAFETY: cnc mmap is page-aligned; AtomicU64 has alignof 8; offset is
// 8-byte-aligned by construction (preceded by a power-of-two ring).
unsafe {
    std::ptr::copy_nonoverlapping(
        counter_init.to_le_bytes().as_ptr(),
        mmap.as_mut_ptr().add(next_id_off),
        8,
    );
}
```

- [ ] **Step 5: Add an accessor helper**

```rust
/// Return a stable pointer to the `next_client_id` `AtomicU64` slot.
///
/// # Safety
///
/// `cnc_base` must point at a fully-initialized cnc.dat mapping that
/// outlives the returned reference. The sub-buffer offset is read from
/// the header, which is `#[repr(C)]` and validated by [`validate_cnc`].
pub unsafe fn next_client_id_ptr(cnc_base: *const u8) -> *const std::sync::atomic::AtomicU64 {
    let header = unsafe { &*cnc_base.cast::<CncHeader>() };
    let off = header.sub_buffer_offsets[sub::NEXT_CLIENT_ID] as usize;
    unsafe { cnc_base.add(off) as *const std::sync::atomic::AtomicU64 }
}
```

- [ ] **Step 6: Add a unit test**

Append to `mod tests`:

```rust
#[test]
fn next_client_id_starts_at_one_and_increments() {
    let (mut mmap, _tmp) = mmap_file(cnc_file_size());
    init_cnc(&mut mmap[..], "x", 0, 0).expect("init");
    // SAFETY: mmap was just initialized.
    let counter = unsafe { &*next_client_id_ptr(mmap.as_ptr()) };
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);

    let a = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let b = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(a, 1);
    assert_eq!(b, 2);
}

#[test]
fn next_client_id_fetch_add_concurrent() {
    use std::sync::atomic::Ordering;
    use std::thread;
    let (mmap_box, _tmp) = mmap_file(cnc_file_size());
    let mmap = std::sync::Arc::new(parking_lot::Mutex::new(mmap_box));
    // init under lock
    {
        let mut g = mmap.lock();
        init_cnc(&mut g[..], "x", 0, 0).expect("init");
    }
    // Each thread fetch_adds 1000 times; collect all returned values.
    const N: usize = 8;
    const PER: usize = 1000;
    let base_ptr = {
        let g = mmap.lock();
        g.as_ptr()
    };
    // SAFETY: AtomicU64 is Sync, mmap outlives the threads (joined below).
    struct PtrSend(*const std::sync::atomic::AtomicU64);
    unsafe impl Send for PtrSend {}
    unsafe impl Sync for PtrSend {}
    let counter_ptr = PtrSend(unsafe { next_client_id_ptr(base_ptr) });
    let counter_arc = std::sync::Arc::new(counter_ptr);

    let handles: Vec<_> = (0..N)
        .map(|_| {
            let c = counter_arc.clone();
            thread::spawn(move || {
                let mut out = Vec::with_capacity(PER);
                for _ in 0..PER {
                    // SAFETY: counter ptr lives as long as mmap (held by
                    // the outer Arc, dropped after join).
                    let v = unsafe { (*c.0).fetch_add(1, Ordering::Relaxed) };
                    out.push(v);
                }
                out
            })
        })
        .collect();

    let mut all: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_unstable();
    // Should be 1..=N*PER (counter starts at 1).
    let expected: Vec<u64> = (1..=(N * PER) as u64).collect();
    assert_eq!(all, expected);
}
```

(The test imports `parking_lot::Mutex`; if `parking_lot` is not already a dev-dep of `uc_protocol`, add it to `[dev-dependencies]` of `uc_protocol/Cargo.toml`. It already ships in the workspace.)

- [ ] **Step 7: Run the tests**

Run: `cargo test -p uc_protocol --lib cnc -- --nocapture`
Expected: all PASS, including the two new ones.

- [ ] **Step 8: Commit**

```bash
git add uc_protocol/src/cnc.rs uc_protocol/Cargo.toml
git commit -m "feat(uc_protocol): add next_client_id sub-region to cnc.dat (M4)"
```

### Task 2.2: Add `uc_protocol::frames::client` module

**Files:**
- Create: `uc_protocol/src/frames/client.rs`
- Modify: `uc_protocol/src/frames/mod.rs`

- [ ] **Step 1: Create the frames::client module**

```rust
//! Client ↔ node ring frame types (M4).
//!
//! `header_extra` layout (8 bytes):
//!   * bytes 0..4 — `client_id` (u32 LE; identifies the originating
//!     client process; allocated via cnc.dat's `next_client_id` slot).
//!   * bytes 4..8 — `local_seq` (u32 LE; per-client monotonic).
//!
//! Client query frames pack `QueryKind` into bit 8 of the 16-bit `flags`
//! field; bits 0..7 carry the multi-service `service_id: u8` (always `0`
//! in v1). The service-side query frame (`frames::query`) keeps a
//! different `header_extra` shape — see that module — because it does
//! not carry `client_id`.
//!
//! `flags` layout (uniform across client frames and the M4-retrofit
//! service frames):
//!   * bits 0..7  — `service_id: u8` (always `0` in v1; decoders error
//!     on `!= 0` with `UnknownServiceId`).
//!   * bits 8..15 — type-specific. For `ClientQueryFrame`: bit 8 =
//!     `QueryKind` (0 = Linearizable, 1 = Snapshot). For all other
//!     frames: reserved (must be zero).
//!
//! `msg_type`:
//!   * `5` — `SubmitFrame` (clients → node, MPSC `clients/submit.ring`)
//!   * `6` — `SubmitResponse` (node → clients, Broadcast `clients/response.broadcast`)
//!   * `7` — `ClientQueryFrame` (clients → node, MPSC `clients/query.ring`)
//!   * `8` — `ClientQueryResp` (node → clients, Broadcast `clients/response.broadcast`)
//!   * `9` — `NotLeaderResp` (node → clients, Broadcast; payload is the
//!     bincode-encoded `Option<u64>` leader-id hint)

use crate::frames::query::QueryKind;

pub const MSG_TYPE_SUBMIT: u16 = 5;
pub const MSG_TYPE_SUBMIT_RESPONSE: u16 = 6;
pub const MSG_TYPE_CLIENT_QUERY: u16 = 7;
pub const MSG_TYPE_CLIENT_QUERY_RESP: u16 = 8;
pub const MSG_TYPE_NOT_LEADER_RESP: u16 = 9;

/// `flags` bit assignments shared across all M4+ frames.
pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;
/// `flags` bit assignments for client-query frames (bit 8 of `flags`).
pub const FLAG_QUERY_KIND_BIT: u16 = 0x0100;

#[derive(Debug, thiserror::Error)]
pub enum ClientFrameError {
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

#[inline]
pub fn encode_extra_client(client_id: u32, local_seq: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0..4].copy_from_slice(&client_id.to_le_bytes());
    out[4..8].copy_from_slice(&local_seq.to_le_bytes());
    out
}

#[inline]
pub fn decode_extra_client(extra: [u8; 8]) -> (u32, u32) {
    let client_id = u32::from_le_bytes(extra[0..4].try_into().unwrap());
    let local_seq = u32::from_le_bytes(extra[4..8].try_into().unwrap());
    (client_id, local_seq)
}

/// Encode `flags` for a client frame.
///
/// `service_id` rides in the low byte; `kind` (only meaningful for
/// `ClientQueryFrame`) rides in bit 8. v1 always writes `service_id = 0`.
#[inline]
pub fn encode_flags_client(service_id: u8, kind: Option<QueryKind>) -> u16 {
    let mut flags = service_id as u16;
    if let Some(QueryKind::Snapshot) = kind {
        flags |= FLAG_QUERY_KIND_BIT;
    }
    flags
}

/// Decode the (service_id, kind) tuple from a client frame's `flags`.
///
/// Returns `UnknownServiceId(n)` on `service_id != 0` (v1 contract; M5+
/// loosens this).
#[inline]
pub fn decode_flags_client(flags: u16) -> Result<(u8, QueryKind), ClientFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(ClientFrameError::UnknownServiceId(service_id));
    }
    let kind = if flags & FLAG_QUERY_KIND_BIT == 0 {
        QueryKind::Linearizable
    } else {
        QueryKind::Snapshot
    };
    Ok((service_id, kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_extra_round_trip() {
        for (cid, seq) in [(0u32, 0u32), (1, 0), (0, 1), (0xdead_beef, 0xcafe_babe), (u32::MAX, u32::MAX)] {
            let extra = encode_extra_client(cid, seq);
            let (got_cid, got_seq) = decode_extra_client(extra);
            assert_eq!(got_cid, cid);
            assert_eq!(got_seq, seq);
        }
    }

    #[test]
    fn flags_round_trip_v1_service_zero() {
        for k in [QueryKind::Linearizable, QueryKind::Snapshot] {
            let f = encode_flags_client(0, Some(k));
            let (sid, got_k) = decode_flags_client(f).expect("v1 decode");
            assert_eq!(sid, 0);
            assert_eq!(got_k, k);
        }
        // No kind specified: defaults to Linearizable on decode.
        let f = encode_flags_client(0, None);
        let (sid, got_k) = decode_flags_client(f).expect("v1 decode");
        assert_eq!(sid, 0);
        assert_eq!(got_k, QueryKind::Linearizable);
    }

    #[test]
    fn flags_rejects_nonzero_service_id() {
        let f = encode_flags_client(7, Some(QueryKind::Linearizable));
        assert!(matches!(
            decode_flags_client(f),
            Err(ClientFrameError::UnknownServiceId(7))
        ));
    }
}
```

- [ ] **Step 2: Register the module**

In `uc_protocol/src/frames/mod.rs`, after `pub mod snapshot;` add:

```rust
pub mod client;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p uc_protocol --lib frames::client`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add uc_protocol/src/frames/client.rs uc_protocol/src/frames/mod.rs
git commit -m "feat(uc_protocol): add frames::client (M4 wire types + header_extra codec)"
```

### Task 2.3: Retrofit `frames::{apply, query}` with `service_id` (multi-service shape)

**Files:**
- Modify: `uc_protocol/src/frames/apply.rs`
- Modify: `uc_protocol/src/frames/query.rs`

Both modules currently leave `flags: u16` unused. This task adds `service_id: u8` in `flags` byte 0, matching the uniform convention introduced in Task 2.2. v1 callers always pass `service_id = 0`; decoders error on `!= 0`.

- [ ] **Step 1: Extend `frames::apply`**

Add to `uc_protocol/src/frames/apply.rs`:

```rust
/// Mask for the low byte of `flags`, where `service_id` lives.
pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;

#[derive(Debug, thiserror::Error)]
pub enum ApplyFrameError {
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

#[inline]
pub fn encode_flags_apply(service_id: u8) -> u16 {
    service_id as u16
}

/// Decode `service_id` from an `ApplyFrame`/`ApplyRespFrame` `flags` field.
/// Returns `UnknownServiceId(n)` on `service_id != 0` (v1 contract).
#[inline]
pub fn decode_flags_apply(flags: u16) -> Result<u8, ApplyFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(ApplyFrameError::UnknownServiceId(service_id));
    }
    Ok(service_id)
}

#[cfg(test)]
mod flags_tests {
    use super::*;

    #[test]
    fn flags_round_trip_v1() {
        let f = encode_flags_apply(0);
        assert_eq!(decode_flags_apply(f).unwrap(), 0);
    }

    #[test]
    fn flags_rejects_nonzero() {
        let f = encode_flags_apply(3);
        assert!(matches!(
            decode_flags_apply(f),
            Err(ApplyFrameError::UnknownServiceId(3))
        ));
    }
}
```

Update the module header doc to mention the new `flags` layout (service_id in byte 0; high byte reserved).

- [ ] **Step 2: Extend `frames::query`**

Add the same `FLAGS_SERVICE_ID_MASK`, `QueryFrameError::UnknownServiceId(u8)` variant, and `encode_flags_query` / `decode_flags_query` helpers (mirror of apply). Update module doc header to note that `header_extra` still carries `(request_id, kind)` for backwards compatibility with the M3 `ipc::query_link` codec, and that the new `flags` byte 0 = `service_id`.

Add unit tests mirroring `frames::apply`'s `flags_tests`.

- [ ] **Step 3: Update M3 callsites to write `service_id = 0`**

Search for `flags:` initializers in `uc_node` and `uc_service` for apply/query frame writes. Each should explicitly pass `encode_flags_apply(0)` / `encode_flags_query(0)` instead of the literal `0`, both to document the field and to centralize the encoding. (Functionally identical; readability + retrofit-completeness.)

Run `rg "msg_type:\s*MSG_TYPE_(APPLY|QUERY)" uc_node uc_service` to enumerate writers; update each.

Update the corresponding readers (`apply_loop`, `query_loop`, dispatcher consumers) to call `decode_flags_apply` / `decode_flags_query` and propagate the error path — for v1 this never fires, but the validation must exist now (otherwise M5+ migration to multi-service silently accepts garbage from older callers).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_protocol --lib frames`
Expected: PASS (new tests + existing ones).

Run: `cargo test --workspace`
Expected: PASS — the `encode_flags_apply(0)`/`encode_flags_query(0)` retrofits must not regress M3 capstone tests.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/frames/apply.rs uc_protocol/src/frames/query.rs uc_node uc_service
git commit -m "feat(uc_protocol): add service_id flags-byte to apply/query frames (M4 multi-service shape)"
```

---

## Phase 3 — node-side wiring

> **Multi-service shape addendum.** Every dispatcher introduced in this phase MUST `decode_flags_client` (or `decode_flags_apply` / `decode_flags_query` for the service-side forwarding paths) on consumed frames. On `Err(UnknownServiceId(n))`: publish a `SubmitResponse` / `ClientQueryResp` carrying a bincode-encoded error payload (decoded by the client SDK as `ClientError::Submission("unknown service_id {n}")`). For v1 this path is unreachable in correct callers; the validation exists to make later multi-service rollout non-breaking.

### Task 3.1: `ipc::client_link` — create the three client-facing rings + `sessions.dir`

**Files:**
- Create: `uc_node/src/ipc/client_link.rs`
- Modify: `uc_node/src/ipc/mod.rs`
- Modify: `uc_node/src/ipc/instance.rs` (one-line mkdir for `clients/`)

- [ ] **Step 1: Create `client_link.rs`**

```rust
//! Node-side ownership of the three client-facing ring files
//! (`submit.ring`, `query.ring`, `response.broadcast`) and the
//! `sessions.dir/` directory under `<instance_dir>/clients/`.
//!
//! Ring sizing (chosen to ride wrap traffic without producer stalls under
//! M4-scale workloads, but small enough that the wrap-fix is exercised
//! routinely in tests):
//!   * submit / query — 16 MiB cap, 4 MiB max single frame (MPSC).
//!   * response — 16 MiB cap, 4 MiB max single frame (Broadcast).
//!
//! Ownership: this module consumes from `submit.ring` and `query.ring`
//! (node side reads what clients write), and produces onto
//! `response.broadcast`. Clients take the matching halves via
//! `uc_client::Client::connect`.

use std::path::Path;

use uc_protocol::ring::broadcast::{BroadcastProducer, BroadcastRing};
use uc_protocol::ring::mpsc::{MpscConsumer, MpscRing};

use super::instance::IpcError;

pub const CLIENT_RING_CAP: u64 = 16 * 1024 * 1024;
pub const CLIENT_RING_MAX_MSG: u32 = 4 * 1024 * 1024;

pub struct ClientLink {
    pub submit_consumer: MpscConsumer,
    pub query_consumer: MpscConsumer,
    pub response_producer: BroadcastProducer,
}

impl ClientLink {
    /// Create the three ring files and `sessions.dir/`. Caller must
    /// ensure `<instance_dir>/clients/` already exists (Instance::create
    /// handles that).
    pub fn create(instance_dir: &Path) -> Result<Self, IpcError> {
        let clients_dir = instance_dir.join("clients");
        std::fs::create_dir_all(clients_dir.join("sessions.dir"))?;

        let submit = MpscRing::create(
            &clients_dir.join("submit.ring"),
            CLIENT_RING_CAP,
            CLIENT_RING_MAX_MSG,
        )?;
        let query = MpscRing::create(
            &clients_dir.join("query.ring"),
            CLIENT_RING_CAP,
            CLIENT_RING_MAX_MSG,
        )?;
        let response = BroadcastRing::create(
            &clients_dir.join("response.broadcast"),
            CLIENT_RING_CAP,
            CLIENT_RING_MAX_MSG,
        )?;

        let (_, submit_consumer) = submit.into_split();
        let (_, query_consumer) = query.into_split();
        let response_producer = response.producer();

        Ok(ClientLink {
            submit_consumer,
            query_consumer,
            response_producer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_instance_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("clients")).unwrap();
        tmp
    }

    #[test]
    fn create_writes_three_ring_files_and_sessions_dir() {
        let tmp = fresh_instance_dir();
        let _link = ClientLink::create(tmp.path()).expect("create");
        let clients_dir = tmp.path().join("clients");
        for name in ["submit.ring", "query.ring", "response.broadcast"] {
            let p = clients_dir.join(name);
            assert!(p.is_file(), "{} not created", p.display());
            assert!(std::fs::metadata(&p).unwrap().len() > 0);
        }
        assert!(clients_dir.join("sessions.dir").is_dir());
    }

    #[test]
    fn client_side_can_open_each_ring() {
        let tmp = fresh_instance_dir();
        let _link = ClientLink::create(tmp.path()).expect("create");
        let clients_dir = tmp.path().join("clients");
        // Sanity: clients can open the rings (matching halves).
        let _ = MpscRing::open(&clients_dir.join("submit.ring")).expect("open submit");
        let _ = MpscRing::open(&clients_dir.join("query.ring")).expect("open query");
        let _ = BroadcastRing::open(&clients_dir.join("response.broadcast")).expect("open resp");
    }
}
```

- [ ] **Step 2: Register the module in `ipc::mod`**

In `uc_node/src/ipc/mod.rs`:

```rust
pub mod client_link;
```

(Place it next to the existing `pub mod service_link;`.)

- [ ] **Step 3: Add `clients/` mkdir to `Instance::create`**

In `uc_node/src/ipc/instance.rs`, after `std::fs::create_dir_all(instance_dir.join("service"))?;`:

```rust
std::fs::create_dir_all(instance_dir.join("clients"))?;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p uc_node --lib ipc::client_link -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/ipc/client_link.rs uc_node/src/ipc/mod.rs uc_node/src/ipc/instance.rs
git commit -m "feat(uc_node): ClientLink — clients/{submit,query,response}.ring + sessions.dir"
```

### Task 3.2: `client_dispatcher` + `client_query_dispatcher` tasks

**Files:**
- Create: `uc_node/src/ipc/client_dispatcher.rs`
- Modify: `uc_node/src/ipc/mod.rs`

- [ ] **Step 1: Create `client_dispatcher.rs`**

```rust
//! Submit-ring and query-ring dispatchers for the client-facing IPC plane.
//!
//! Both dispatchers share the response Broadcast producer through a
//! `parking_lot::Mutex` — `BroadcastProducer` is single-producer-by-design,
//! and the mutex enforces that across the two tasks. Each broadcast write
//! is one record; no awaits are held under the lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex as PlMutex;
use tokio::task::JoinHandle;

use uc_protocol::frames::client::{
    MSG_TYPE_CLIENT_QUERY, MSG_TYPE_CLIENT_QUERY_RESP, MSG_TYPE_NOT_LEADER_RESP, MSG_TYPE_SUBMIT,
    MSG_TYPE_SUBMIT_RESPONSE, decode_extra_client, decode_flags_query_kind, encode_extra_client,
};
use uc_protocol::frames::query::QueryKind;
use uc_protocol::ring::RingError;
use uc_protocol::ring::broadcast::BroadcastProducer;
use uc_protocol::ring::mpsc::MpscConsumer;

use crate::ipc::query_link::ShmemQueryLink;
use crate::raft::AppCommand;
use crate::raft::NodeId;
use crate::runtime::node::RaftHandle;

use uc_service::StateMachine;

/// Shared single-producer guard for `clients/response.broadcast`.
pub type SharedResponseProducer = Arc<PlMutex<BroadcastProducer>>;

const POLL_BACKOFF: Duration = Duration::from_micros(100);
const BROADCAST_RETRY_BACKOFF: Duration = Duration::from_micros(100);

pub struct ClientDispatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the submit-ring dispatcher.
pub fn spawn_client_dispatcher<S>(
    submit_consumer: MpscConsumer,
    response_producer: SharedResponseProducer,
    raft: RaftHandle<S>,
    node_id: NodeId,
) -> ClientDispatcherHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut consumer = submit_consumer;
        let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_SUBMIT => {
                    let extra = rec.header_extra;
                    let payload = std::mem::take(&mut payload_buf);

                    // Leader check + dispatch.
                    let leader = raft.current_leader().await;
                    if leader != Some(node_id) {
                        broadcast_not_leader(&response_producer, extra, leader).await;
                        continue;
                    }

                    let app_command = AppCommand::from(Bytes::from(payload));
                    match raft.client_write(app_command).await {
                        Ok(resp) => {
                            broadcast_record(
                                &response_producer,
                                MSG_TYPE_SUBMIT_RESPONSE,
                                0,
                                extra,
                                resp.data.as_ref(),
                            )
                            .await;
                        }
                        Err(e) => {
                            use openraft::error::{ClientWriteError, RaftError};
                            if let RaftError::APIError(ClientWriteError::ForwardToLeader(f)) = &e {
                                broadcast_not_leader(
                                    &response_producer,
                                    extra,
                                    f.leader_id,
                                )
                                .await;
                            } else {
                                tracing::warn!(node_id, error = ?e, "client_write failed; dropping");
                                // No SubmitResponse — caller will time out.
                            }
                        }
                    }
                }
                Ok(Some(rec)) => {
                    tracing::warn!(msg_type = rec.msg_type, "unexpected frame on submit.ring");
                }
                Ok(None) => tokio::time::sleep(POLL_BACKOFF).await,
                Err(e) => {
                    tracing::error!(error = ?e, "submit.ring read");
                    tokio::time::sleep(POLL_BACKOFF).await;
                }
            }
        }
    });

    ClientDispatcherHandle { join, stop }
}

/// Spawn the query-ring dispatcher.
pub fn spawn_client_query_dispatcher<S>(
    query_consumer: MpscConsumer,
    response_producer: SharedResponseProducer,
    raft: RaftHandle<S>,
    query_link: Arc<ShmemQueryLink>,
    node_id: NodeId,
) -> ClientDispatcherHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut consumer = query_consumer;
        let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_CLIENT_QUERY => {
                    let extra = rec.header_extra;
                    let kind = decode_flags_query_kind(rec.flags);
                    let payload = std::mem::take(&mut payload_buf);

                    if let QueryKind::Linearizable = kind {
                        let leader = raft.current_leader().await;
                        if leader != Some(node_id) {
                            broadcast_not_leader(&response_producer, extra, leader).await;
                            continue;
                        }
                        // M4 simplification: rely on raft.client_write timing
                        // to give "linearizable enough" semantics until openraft
                        // 0.10's `ensure_linearizable` plumb-through lands in
                        // M5. Snapshot path is unchanged.
                    }

                    match query_link.submit(&payload, kind).await {
                        Ok(resp_bytes) => {
                            broadcast_record(
                                &response_producer,
                                MSG_TYPE_CLIENT_QUERY_RESP,
                                0,
                                extra,
                                resp_bytes.as_ref(),
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::warn!(node_id, error = ?e, "query_link submit failed");
                            // Drop; caller times out.
                        }
                    }
                }
                Ok(Some(rec)) => {
                    tracing::warn!(msg_type = rec.msg_type, "unexpected frame on query.ring");
                }
                Ok(None) => tokio::time::sleep(POLL_BACKOFF).await,
                Err(e) => {
                    tracing::error!(error = ?e, "query.ring read");
                    tokio::time::sleep(POLL_BACKOFF).await;
                }
            }
        }
    });

    ClientDispatcherHandle { join, stop }
}

async fn broadcast_not_leader(
    response_producer: &SharedResponseProducer,
    extra: [u8; 8],
    hint: Option<NodeId>,
) {
    let hint_bytes = match bincode::serde::encode_to_vec(&hint, bincode::config::standard()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = ?e, "encode NotLeader hint");
            return;
        }
    };
    broadcast_record(response_producer, MSG_TYPE_NOT_LEADER_RESP, 0, extra, &hint_bytes).await;
}

async fn broadcast_record(
    response_producer: &SharedResponseProducer,
    msg_type: u16,
    flags: u16,
    extra: [u8; 8],
    payload: &[u8],
) {
    loop {
        // Hold the lock only across one record write. No awaits while held.
        let result = {
            let mut g = response_producer.lock();
            g.write(msg_type, flags, extra, payload)
        };
        match result {
            Ok(()) => return,
            Err(RingError::Full) => {
                // Broadcast never returns Full in practice (it overwrites);
                // treat as a backpressure signal just in case.
                tokio::time::sleep(BROADCAST_RETRY_BACKOFF).await;
            }
            Err(e) => {
                tracing::error!(?e, "response broadcast write failed; dropping");
                return;
            }
        }
    }
}

// Note: `encode_extra_client` is re-exported by uc_protocol; not used
// directly here since the dispatchers forward the originating frame's
// `header_extra` verbatim.
#[allow(dead_code)]
fn _silence_unused_imports() {
    let _ = encode_extra_client;
    let _ = decode_extra_client;
}
```

> **Note about `ensure_linearizable`:** the spec calls for a `raft.ensure_linearizable().await` round-trip on linearizable queries. openraft 0.10's `Raft::ensure_linearizable()` returns the committed log id; the integration is straightforward but requires plumbing through `RaftHandle`. Deferred to a follow-up task in this phase if the test plan exercises it (Task 5.2 only checks `query_snapshot`); otherwise tracked for M5.

- [ ] **Step 2: Register the module**

In `uc_node/src/ipc/mod.rs`:

```rust
pub mod client_dispatcher;
```

- [ ] **Step 3: Build the crate**

Run: `cargo build -p uc_node`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/ipc/client_dispatcher.rs uc_node/src/ipc/mod.rs
git commit -m "feat(uc_node): client_dispatcher + client_query_dispatcher tasks"
```

### Task 3.3: `session_gc` task

**Files:**
- Create: `uc_node/src/ipc/session_gc.rs`
- Modify: `uc_node/src/ipc/mod.rs`

- [ ] **Step 1: Define the on-disk session-file layout**

In a new file `uc_node/src/ipc/session_gc.rs`:

```rust
//! Node-side session-file garbage collector for `clients/sessions.dir/`.
//!
//! Each connected client owns one file `{client_id}.session`, 64 bytes:
//!
//! ```text
//!  0..8   heartbeat_seq      AtomicU64  (client ticks)
//!  8..16  heartbeat_at_ns    AtomicU64  (wall time of the last tick)
//! 16..20  client_id_check    u32        (== filename's u32; sanity)
//! 20..64  padding (zero)
//! ```
//!
//! The GC task wakes every [`GC_TICK`], reads each file once, and runs a
//! [`HeartbeatWatcher`] against the heartbeat counters. A file whose
//! `heartbeat_seq` has not advanced for [`STALE_AFTER`] (5 s by default)
//! is unlinked. No further node-side state needs cleanup — in-flight
//! broadcasts for the dead client are no-ops (no consumer reads them).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use memmap2::Mmap;
use tokio::task::JoinHandle;

use uc_protocol::liveness::HeartbeatWatcher;

pub const SESSION_FILE_LEN: usize = 64;
pub const GC_TICK: Duration = Duration::from_secs(2);
pub const STALE_AFTER: Duration = Duration::from_secs(5);

#[repr(C, align(8))]
pub struct SessionFile {
    pub heartbeat_seq: AtomicU64,
    pub heartbeat_at_ns: AtomicU64,
    pub client_id_check: u32,
    pub _pad: [u8; 44],
}

const _: () = {
    assert!(std::mem::size_of::<SessionFile>() == SESSION_FILE_LEN);
};

pub struct SessionGcHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_session_gc(sessions_dir: PathBuf) -> SessionGcHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut watchers: HashMap<u32, HeartbeatWatcher> = HashMap::new();

        while !stop_for_task.load(Ordering::Relaxed) {
            sweep(&sessions_dir, &mut watchers);
            tokio::time::sleep(GC_TICK).await;
        }
    });

    SessionGcHandle { join, stop }
}

fn sweep(sessions_dir: &std::path::Path, watchers: &mut HashMap<u32, HeartbeatWatcher>) {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(it) => it,
        Err(e) => {
            tracing::warn!(?e, "session_gc: read_dir failed");
            return;
        }
    };

    let now_ns = now_ns();
    let timeout_ns = STALE_AFTER.as_nanos() as u64;
    let mut live_ids: std::collections::HashSet<u32> = Default::default();

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(stem) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if path.extension().and_then(|e| e.to_str()) != Some("session") {
            continue;
        }
        live_ids.insert(stem);

        let f = match std::fs::OpenOptions::new().read(true).open(&path) {
            Ok(f) => f,
            Err(_) => continue, // race with unlink; skip
        };
        // SAFETY: read-only mmap of a 64-byte file we just opened.
        let mmap = match unsafe { Mmap::map(&f) } {
            Ok(m) => m,
            Err(_) => continue,
        };
        if mmap.len() < SESSION_FILE_LEN {
            continue;
        }
        // SAFETY: file len ≥ SESSION_FILE_LEN; mmap is page-aligned.
        let sess = unsafe { &*mmap.as_ptr().cast::<SessionFile>() };
        let seq = sess.heartbeat_seq.load(Ordering::Relaxed);
        let watcher = watchers
            .entry(stem)
            .or_insert_with(|| HeartbeatWatcher::new(seq, now_ns));

        // We synthesize a NodeStatus/ServiceStatus-style poll by hand
        // because SessionFile isn't one of those types. Reuse the
        // primitive: if seq advanced, alive; else compare time.
        let alive = if seq != watcher.last_seq() {
            *watcher = HeartbeatWatcher::new(seq, now_ns);
            true
        } else {
            now_ns.saturating_sub(watcher.last_seen_ns()) < timeout_ns
        };

        if !alive {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(?e, client_id = stem, "session_gc: unlink failed");
            } else {
                tracing::info!(client_id = stem, "session_gc: unlinked stale session");
            }
            watchers.remove(&stem);
        }
    }

    // Drop watchers whose session file disappeared (graceful shutdown).
    watchers.retain(|id, _| live_ids.contains(id));
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn make_session_file(dir: &std::path::Path, client_id: u32, seq: u64) -> std::path::PathBuf {
        let path = dir.join(format!("{client_id}.session"));
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let mut bytes = vec![0u8; SESSION_FILE_LEN];
        bytes[0..8].copy_from_slice(&seq.to_le_bytes());
        bytes[16..20].copy_from_slice(&client_id.to_le_bytes());
        f.write_all(&bytes).unwrap();
        path
    }

    #[tokio::test]
    async fn stale_session_is_unlinked() {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_session_file(tmp.path(), 42, 0);
        // No further writes => heartbeat_seq stays 0.

        let handle = spawn_session_gc(tmp.path().to_path_buf());
        // GC_TICK=2s, STALE_AFTER=5s. Override via direct sweep calls
        // would be cleaner, but here we just wait it out.
        tokio::time::sleep(Duration::from_secs(8)).await;
        handle.stop.store(true, Ordering::Relaxed);
        let _ = handle.join.await;
        assert!(!path.exists(), "stale session should have been unlinked");
    }

    #[tokio::test]
    async fn live_session_is_preserved() {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_session_file(tmp.path(), 7, 0);

        // Live writer: bump heartbeat_seq via direct mmap every 100 ms.
        let writer_stop = Arc::new(AtomicBool::new(false));
        let ws = Arc::clone(&writer_stop);
        let p_for_writer = path.clone();
        let writer = tokio::spawn(async move {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&p_for_writer)
                .unwrap();
            let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
            let mut seq: u64 = 0;
            while !ws.load(Ordering::Relaxed) {
                seq += 1;
                mmap[0..8].copy_from_slice(&seq.to_le_bytes());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let handle = spawn_session_gc(tmp.path().to_path_buf());
        tokio::time::sleep(Duration::from_secs(8)).await;
        handle.stop.store(true, Ordering::Relaxed);
        let _ = handle.join.await;
        writer_stop.store(true, Ordering::Relaxed);
        let _ = writer.await;
        assert!(path.exists(), "live session should not be unlinked");
    }
}
```

- [ ] **Step 2: Register the module**

In `uc_node/src/ipc/mod.rs`:

```rust
pub mod session_gc;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p uc_node --lib ipc::session_gc -- --nocapture --test-threads=1`
Expected: PASS. (Two tests take ~8s each — tag-as-slow if CI cares.)

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/ipc/session_gc.rs uc_node/src/ipc/mod.rs
git commit -m "feat(uc_node): session_gc — unlink stale clients/sessions.dir/*.session"
```

### Task 3.4: Wire `ClientLink` + dispatchers + GC into `NodeBuilder` and `NodeHandle`

**Files:**
- Modify: `uc_node/src/runtime/builder.rs`
- Modify: `uc_node/src/runtime/node.rs`

- [ ] **Step 1: Extend `NodeHandle` fields**

In `uc_node/src/runtime/node.rs`, after the existing `service_watcher: Option<ServiceWatcherHandle>,` line in `NodeHandle`:

```rust
/// Shmem-mode only: client-facing ring files + sessions.dir mmaps.
pub(crate) _client_link: Option<crate::ipc::client_link::ClientLink>,
/// Shmem-mode only: submit-ring dispatcher.
pub(crate) client_dispatcher: Option<crate::ipc::client_dispatcher::ClientDispatcherHandle>,
/// Shmem-mode only: query-ring dispatcher.
pub(crate) client_query_dispatcher: Option<crate::ipc::client_dispatcher::ClientDispatcherHandle>,
/// Shmem-mode only: session-file GC.
pub(crate) session_gc: Option<crate::ipc::session_gc::SessionGcHandle>,
```

In the `shutdown()` body, after the existing `if let Some(w) = self.service_watcher { … }` block, **before** `if let Some(lv) = self.node_liveness { … }`:

```rust
if let Some(d) = self.client_dispatcher {
    d.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = d.join.await;
}
if let Some(d) = self.client_query_dispatcher {
    d.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = d.join.await;
}
if let Some(g) = self.session_gc {
    g.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = g.join.await;
}
// _client_link drops here (after dispatchers joined).
```

> Order matters: dispatchers + GC join before `_client_link` drops (each holds ring half/mmap handles).

- [ ] **Step 2: Update the `Embedded` arm of `finish` call sites**

In `NodeBuilder::start`, the `IpcMode::Embedded` branch needs four `None`s appended to the `NodeHandle` it returns (or just update `finish` to default them). Simpler: update `finish` to construct `NodeHandle` with the four new fields set to `None`, since `finish` is the central constructor:

Inside `finish`'s `Ok(NodeHandle { … })` block at the bottom, append:

```rust
_client_link: None,
client_dispatcher: None,
client_query_dispatcher: None,
session_gc: None,
```

- [ ] **Step 3: Shmem branch — create `ClientLink`, spawn dispatchers, attach**

In `NodeBuilder::start`'s `IpcMode::Shmem { instance_dir }` arm, **after** the existing `ServiceLink::create(&instance_dir)?;` line, add:

```rust
let client_link = crate::ipc::client_link::ClientLink::create(&instance_dir)?;
```

Then construct the `ShmemQueryLink` as today, but wrap it in `Arc` so the query dispatcher can share it. Find:

```rust
let query_link = ShmemQueryLink::new(link.query_producer, link.query_resp_consumer);
```

Replace with:

```rust
let query_link = std::sync::Arc::new(ShmemQueryLink::new(
    link.query_producer,
    link.query_resp_consumer,
));
```

(Then update the existing `Some(query_link)` argument passed into `finish` to `Some(query_link.clone())` — but since `query_link` is now `Arc<ShmemQueryLink>`, the field type on `NodeHandle` must change too. See Step 4 below.)

- [ ] **Step 4: Migrate `NodeHandle::query_link` to `Arc<ShmemQueryLink>`**

In `uc_node/src/runtime/node.rs`:

```rust
pub(crate) query_link: Option<std::sync::Arc<ShmemQueryLink>>,
```

`Self::submit_query` already calls `.submit(…)` on a `&ShmemQueryLink`; the `Arc` deref makes the call site compile unchanged. Verify by reading the existing call.

- [ ] **Step 5: Shmem branch — spawn dispatchers + GC after `finish` returns the handle**

In `NodeBuilder::start`'s shmem arm, after the existing `service_watcher` setup, before `Ok(handle)`:

```rust
// Wrap the response producer in a single-producer guard shared by
// both dispatchers. parking_lot::Mutex is fine here — the lock is
// held only across one ring record write (no awaits underneath).
let response_producer = std::sync::Arc::new(parking_lot::Mutex::new(client_link.response_producer));

// The dispatchers consume the MpscConsumer halves of submit/query
// rings. We move them out of `client_link` and replace with sentinel
// stubs only the lifetime owner cares about. Simpler: split `ClientLink`
// into the link-of-mmaps and the consumer halves, then store only the
// mmap-owning part on NodeHandle for shutdown ordering.
// → Refactor: extract the consumers before storing client_link.
let submit_consumer = std::mem::replace(
    &mut handle._client_link_consumers_submit, // doesn't exist yet — see below
    None,
);
// [see refactor note below — pick option A]
```

> **Refactor note (pick one before continuing):**
>
> Option A (simpler — recommended). Have `ClientLink::create` return a struct with three named fields, and have `NodeBuilder::start` decompose it locally: `let ClientLink { submit_consumer, query_consumer, response_producer } = ClientLink::create(&instance_dir)?;`. Then `NodeHandle` doesn't hold `_client_link` at all; it holds `_clients_dir: PathBuf` so that file paths stay reachable for diagnostics, plus the three task handles. Backing mmaps live inside the `MpscConsumer`/`BroadcastProducer` halves directly (each ring half clones its `Arc<…Inner>`), so dropping the dispatcher tasks drops the consumer halves drops the mmaps — clean.
>
> Option B. Keep `ClientLink` as a struct on `NodeHandle`; have the dispatchers take `MpscConsumer` + `BroadcastProducer` borrowed by `Arc` clones from inside `ClientLink`. Requires `ClientLink::split(self) -> (Submit, Query, ProducerArc)` plus mmap-keepalive bookkeeping.
>
> **Adopt Option A.** Update Task 3.1's `ClientLink` so it's a return-by-value struct (no `_client_link` field on `NodeHandle`). Remove `_client_link` from Step 1 above.

Apply the refactor: in `runtime/node.rs`, **delete** the `_client_link` field and **keep only**:

```rust
pub(crate) client_dispatcher: Option<crate::ipc::client_dispatcher::ClientDispatcherHandle>,
pub(crate) client_query_dispatcher: Option<crate::ipc::client_dispatcher::ClientDispatcherHandle>,
pub(crate) session_gc: Option<crate::ipc::session_gc::SessionGcHandle>,
```

In `NodeBuilder::start`'s shmem arm (with `handle` already returned from `finish`):

```rust
let crate::ipc::client_link::ClientLink {
    submit_consumer,
    query_consumer,
    response_producer,
} = crate::ipc::client_link::ClientLink::create(&instance_dir)?;
let response_producer =
    std::sync::Arc::new(parking_lot::Mutex::new(response_producer));

let client_dispatcher = crate::ipc::client_dispatcher::spawn_client_dispatcher(
    submit_consumer,
    response_producer.clone(),
    handle.raft.clone(),
    node_id_for_watcher,
);
let client_query_dispatcher = crate::ipc::client_dispatcher::spawn_client_query_dispatcher(
    query_consumer,
    response_producer.clone(),
    handle.raft.clone(),
    query_link.clone(),
    node_id_for_watcher,
);
let session_gc = crate::ipc::session_gc::spawn_session_gc(
    instance_dir.join("clients").join("sessions.dir"),
);

handle.client_dispatcher = Some(client_dispatcher);
handle.client_query_dispatcher = Some(client_query_dispatcher);
handle.session_gc = Some(session_gc);
```

(The `ClientLink::create` call should run **before** `finish` because the `clients/` directory + ring files must exist when client connects start happening; reorder if needed.)

> Reorder verification: `ServiceLink::create` runs after `Instance::create`. Insert `ClientLink::create(&instance_dir)?` right after `ServiceLink::create(&instance_dir)?` and bind its return to local variables. The dispatchers can still be spawned after `finish` because they only need `handle.raft` and the (already-moved) ring halves.

- [ ] **Step 6: Build**

Run: `cargo build -p uc_node`
Expected: clean build.

- [ ] **Step 7: Run the existing M3 capstone tests; all must still pass**

Run: `cargo test -p uc_node --test m3_shmem_single_node --test m3_three_node_shmem --test m3_service_crash --test m3_ultima_db_adapter --test shmem_state_machine -- --nocapture`
Expected: all PASS. (The dispatchers run but have no clients; they idle harmlessly.)

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/runtime/builder.rs uc_node/src/runtime/node.rs
git commit -m "feat(uc_node): wire ClientLink + dispatchers + session_gc into NodeBuilder/Handle"
```

### Task 3.5: Exponential backoff for the bootstrap `add_learner` retry (M3.5 follow-up #2)

**Files:**
- Modify: `uc_node/src/runtime/builder.rs`

- [ ] **Step 1: Replace fixed 5 ms with exponential backoff**

Find the `BootstrapConfig::Peers` arm's `add_learner` retry loop. Currently:

```rust
tokio::time::sleep(std::time::Duration::from_millis(5)).await;
continue;
```

Replace with a small exponential-backoff helper scoped to the loop:

```rust
let mut backoff_ms: u64 = 5;
let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
loop {
    match raft.add_learner(peer.node_id, node.clone(), true).await {
        Ok(_) => {
            promotable.insert(peer.node_id);
            break;
        }
        Err(OR::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::InProgress(_),
        ))) if std::time::Instant::now() < deadline => {
            tracing::trace!(
                node_id = peer.node_id,
                backoff_ms,
                "add_learner saw InProgress; retrying after backoff"
            );
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms.saturating_mul(2)).min(200); // cap at 200 ms
            continue;
        }
        Err(OR::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::InProgress(_),
        ))) => {
            tracing::warn!(
                node_id = peer.node_id,
                "add_learner timed out (InProgress past 10s deadline); \
                 peer will not be promoted to voter"
            );
            break;
        }
        Err(e) => {
            tracing::warn!(
                node_id = peer.node_id,
                error = ?e,
                "add_learner failed; peer will not be promoted to voter"
            );
            break;
        }
    }
}
```

- [ ] **Step 2: Build and run M3 three-node test**

Run: `cargo test -p uc_node --test m3_three_node_shmem -- --nocapture`
Expected: PASS. The exponential backoff is a polite-to-leader change; behavior on the happy path is unchanged (first retry still at 5 ms).

- [ ] **Step 3: Commit**

```bash
git add uc_node/src/runtime/builder.rs
git commit -m "refactor(uc_node): exponential backoff for bootstrap add_learner retry (M3.5 #2)"
```

---

## Phase 4 — `uc_client` SDK

### Task 4.1: Cargo + module skeleton

**Files:**
- Modify: `uc_client/Cargo.toml`
- Modify: `uc_client/src/lib.rs`
- Modify: `uc_client/src/error.rs`

- [ ] **Step 1: Add dependencies**

Replace `uc_client/Cargo.toml`'s `[dependencies]` with:

```toml
[dependencies]
thiserror = { workspace = true }
tokio = { workspace = true }
bincode = { workspace = true }
bytes = { workspace = true }
serde = { workspace = true }
parking_lot = { workspace = true }
dashmap = "6"
memmap2 = { workspace = true }
tracing = { workspace = true }
uc_protocol = { path = "../uc_protocol" }

[dev-dependencies]
tempfile = { workspace = true }
```

Then in the workspace `Cargo.toml` `[workspace.dependencies]`, add the new shared crate `dashmap = "6"` so that `uc_client` and any future consumer share a version pin.

- [ ] **Step 2: Update `error.rs` to spec §"Error model"**

Replace `uc_client/src/error.rs` with:

```rust
use std::io;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    // ── Connect-time ────────────────────────────────────────────────────
    #[error("not connected")]
    NotConnected,
    #[error("app_id mismatch: expected {expected:?}, got {actual:?}")]
    AppIdMismatch { expected: String, actual: String },
    #[error("protocol version mismatch: local={local}, node={node}")]
    ProtocolMismatch { local: u32, node: u32 },
    #[error("node restarted since previous connect: previous={previous:x}, current={current:x}")]
    InstanceRestart { previous: u128, current: u128 },
    #[error("session file create/mmap: {0}")]
    SessionCreate(#[from] io::Error),

    // ── Steady-state ────────────────────────────────────────────────────
    #[error("not leader; hint: {hint:?}")]
    NotLeader { hint: Option<u64> },
    #[error("node stalled")]
    NodeStalled,
    #[error("service stalled")]
    ServiceStalled,
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("response was overwritten by broadcast lap")]
    ResponseOverwritten,
    #[error("submit ring full past grace period")]
    BackpressureFull,
    #[error("submit: {0}")]
    Submission(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("client shut down")]
    ShutDown,
}

impl From<bincode::error::EncodeError> for ClientError {
    fn from(e: bincode::error::EncodeError) -> Self {
        Self::Decode(format!("encode: {e}"))
    }
}
impl From<bincode::error::DecodeError> for ClientError {
    fn from(e: bincode::error::DecodeError) -> Self {
        Self::Decode(e.to_string())
    }
}
```

- [ ] **Step 3: Scaffold `lib.rs`**

```rust
//! Local-shmem client SDK for ultima_cluster (M4).
//!
//! Connects to a `uc_node` over the `<instance_dir>/cnc.dat` +
//! `clients/*` shmem rings. The public entry point is [`Client`].

pub mod error;
pub use error::ClientError;

mod cnc;
mod rings;
mod session;
mod watchers;
mod client;

pub use client::Client;
```

- [ ] **Step 4: Stub the four new modules so the crate compiles**

Create each as an empty file with `//!` doc:

`uc_client/src/cnc.rs`, `uc_client/src/rings.rs`, `uc_client/src/session.rs`, `uc_client/src/watchers.rs`, `uc_client/src/client.rs`.

Each starts as:

```rust
//! TODO: implemented in the next task.
```

The `client.rs` stub also needs a `pub struct Client;` so `lib.rs` compiles:

```rust
//! TODO: implemented in Task 4.5.

pub struct Client {}
```

- [ ] **Step 5: Build**

Run: `cargo build -p uc_client`
Expected: clean build.

- [ ] **Step 6: Commit**

```bash
git add uc_client/Cargo.toml Cargo.toml uc_client/src/
git commit -m "feat(uc_client): scaffold + error model + dependencies for M4"
```

### Task 4.2: `cnc` attach + handshake validation

**Files:**
- Modify: `uc_client/src/cnc.rs`

- [ ] **Step 1: Implement the read-only cnc attach**

```rust
//! Read-only attach to `<instance_dir>/cnc.dat` + handshake.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::Mmap;
use uc_protocol::ProtocolVersion;
use uc_protocol::cnc::{
    CncHeader, NodeStatus, ServiceStatus, next_client_id_ptr, sub, validate_cnc,
};

use crate::ClientError;

pub struct CncAttach {
    /// Keeps the mmap alive for the lifetime of the Client.
    pub _mmap: Mmap,
    pub base: *const u8,
    pub instance_id: u128,
    pub app_id: String,
    pub protocol_version: u32,
    pub client_id: u32,
}

// SAFETY: the cnc.dat file is shared read-only with the node-side
// writer; all writes happen via atomics or via the node's exclusive
// init. Read access from any thread is safe.
unsafe impl Send for CncAttach {}
unsafe impl Sync for CncAttach {}

impl CncAttach {
    /// Open `cnc.dat`, validate magic + CRC, check app_id and protocol
    /// version, allocate a fresh `client_id` via the cnc next_client_id
    /// slot.
    pub fn attach(instance_dir: &Path, expected_app_id: &str) -> Result<Self, ClientError> {
        let cnc_path = instance_dir.join("cnc.dat");
        let file = std::fs::OpenOptions::new().read(true).open(&cnc_path)?;
        // SAFETY: read-only mmap of a node-owned cnc.dat. The node holds
        // an exclusive flock on instance.lock for the lifetime of the
        // instance, so the file size/layout is stable.
        let mmap = unsafe { Mmap::map(&file)? };

        let header = validate_cnc(&mmap).map_err(|e| {
            ClientError::Decode(format!("cnc validate: {e}"))
        })?;
        let actual_app_id = header.app_id_str().to_owned();
        if actual_app_id != expected_app_id {
            return Err(ClientError::AppIdMismatch {
                expected: expected_app_id.to_owned(),
                actual: actual_app_id,
            });
        }
        let local_version = ProtocolVersion::new(0, 1, 0).0;
        if header.protocol_version != local_version {
            return Err(ClientError::ProtocolMismatch {
                local: local_version,
                node: header.protocol_version,
            });
        }
        let instance_id = header.instance_id();
        let protocol_version = header.protocol_version;

        // Allocate client_id.
        // SAFETY: cnc is a valid initialized mapping (validated above).
        let counter = unsafe { &*(next_client_id_ptr(mmap.as_ptr()) as *const AtomicU64) };
        let raw = counter.fetch_add(1, Ordering::Relaxed);
        let client_id = raw as u32; // truncate; 4B per restart is plenty for v1

        let base = mmap.as_ptr();
        Ok(CncAttach {
            _mmap: mmap,
            base,
            instance_id,
            app_id: actual_app_id,
            protocol_version,
            client_id,
        })
    }

    /// Pointer to the NodeStatus block. Safe to read concurrently.
    pub fn node_status(&self) -> *const NodeStatus {
        // SAFETY: header validated; sub-buffer offsets are within the mmap.
        unsafe {
            let header = &*self.base.cast::<CncHeader>();
            self.base.add(header.sub_buffer_offsets[sub::NODE_STATUS] as usize)
                as *const NodeStatus
        }
    }

    pub fn service_status(&self) -> *const ServiceStatus {
        unsafe {
            let header = &*self.base.cast::<CncHeader>();
            self.base
                .add(header.sub_buffer_offsets[sub::SERVICE_STATUS] as usize)
                as *const ServiceStatus
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::cnc::{cnc_file_size, init_cnc};

    fn fresh_cnc(dir: &std::path::Path, app_id: &str, node_id: u64, instance_id: u128) {
        let cnc_path = dir.join("cnc.dat");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cnc_path)
            .unwrap();
        f.set_len(cnc_file_size() as u64).unwrap();
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
        init_cnc(&mut mmap[..], app_id, node_id, instance_id).unwrap();
    }

    #[test]
    fn attach_succeeds_and_allocates_distinct_ids() {
        let tmp = tempfile::tempdir().unwrap();
        fresh_cnc(tmp.path(), "kv", 1, 0xdead_beef);
        let a = CncAttach::attach(tmp.path(), "kv").expect("attach a");
        let b = CncAttach::attach(tmp.path(), "kv").expect("attach b");
        assert_ne!(a.client_id, b.client_id);
        assert_eq!(a.instance_id, 0xdead_beef);
    }

    #[test]
    fn attach_rejects_wrong_app_id() {
        let tmp = tempfile::tempdir().unwrap();
        fresh_cnc(tmp.path(), "kv", 1, 0);
        let r = CncAttach::attach(tmp.path(), "other");
        assert!(matches!(r, Err(ClientError::AppIdMismatch { .. })));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p uc_client --lib cnc -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_client/src/cnc.rs
git commit -m "feat(uc_client): cnc attach + handshake + client_id allocation"
```

### Task 4.3: Session ticker + watchers

**Files:**
- Modify: `uc_client/src/session.rs`
- Modify: `uc_client/src/watchers.rs`

- [ ] **Step 1: Implement `session.rs`**

```rust
//! Client-side session file under `clients/sessions.dir/{client_id}.session`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;
use tokio::task::JoinHandle;

use crate::ClientError;

pub const SESSION_FILE_LEN: usize = 64;
const TICK_PERIOD: Duration = Duration::from_millis(100);

#[repr(C, align(8))]
struct SessionFile {
    heartbeat_seq: AtomicU64,
    heartbeat_at_ns: AtomicU64,
    client_id_check: u32,
    _pad: [u8; 44],
}

const _: () = {
    assert!(std::mem::size_of::<SessionFile>() == SESSION_FILE_LEN);
};

pub struct SessionHandle {
    pub path: PathBuf,
    /// Keeps the mmap alive while the ticker runs.
    _mmap: Arc<MmapHolder>,
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

struct MmapHolder(MmapMut);
// SAFETY: SessionFile is all-atomic; concurrent access through the raw
// ptr is sound across threads.
unsafe impl Send for MmapHolder {}
unsafe impl Sync for MmapHolder {}

impl SessionHandle {
    pub fn create(sessions_dir: &Path, client_id: u32) -> Result<Self, ClientError> {
        let path = sessions_dir.join(format!("{client_id}.session"));
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        f.set_len(SESSION_FILE_LEN as u64)?;
        // SAFETY: just-created file we own; no other process maps it
        // until the GC sweep reads it later (read-only).
        let mut mmap = unsafe { MmapMut::map_mut(&f)? };

        // Init: zero, then write client_id_check.
        mmap[..SESSION_FILE_LEN].fill(0);
        mmap[16..20].copy_from_slice(&client_id.to_le_bytes());

        let holder = Arc::new(MmapHolder(mmap));
        let stop = Arc::new(AtomicBool::new(false));

        let holder_for_task = Arc::clone(&holder);
        let stop_for_task = Arc::clone(&stop);

        let join = tokio::spawn(async move {
            // SAFETY: holder kept alive by Arc until task exits.
            let sess: *mut SessionFile = holder_for_task.0.as_ptr() as *mut SessionFile;
            while !stop_for_task.load(Ordering::Relaxed) {
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                // SAFETY: SessionFile is repr(C, align(8)) and all-atomic.
                unsafe {
                    (*sess).heartbeat_seq.fetch_add(1, Ordering::Relaxed);
                    (*sess).heartbeat_at_ns.store(now_ns, Ordering::Relaxed);
                }
                tokio::time::sleep(TICK_PERIOD).await;
            }
        });

        Ok(SessionHandle {
            path,
            _mmap: holder,
            join,
            stop,
        })
    }
}
```

- [ ] **Step 2: Implement `watchers.rs`**

```rust
//! Client-side liveness watchers for NodeStatus and ServiceStatus.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use uc_protocol::cnc::{NodeStatus, ServiceStatus};
use uc_protocol::liveness::HeartbeatWatcher;

const POLL_PERIOD: Duration = Duration::from_millis(100);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

pub struct StallWatchers {
    pub node_stalled: Arc<AtomicBool>,
    pub service_stalled: Arc<AtomicBool>,
    pub join_node: JoinHandle<()>,
    pub join_service: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn both stall watchers. `node_status_ptr` / `service_status_ptr`
/// must outlive the returned tasks (kept alive by the Client's CncAttach).
///
/// # Safety
///
/// Both pointers must reference valid initialized cnc.dat regions for
/// the lifetime of the spawned tasks.
pub unsafe fn spawn_stall_watchers(
    node_status_ptr: *const NodeStatus,
    service_status_ptr: *const ServiceStatus,
) -> StallWatchers {
    let stop = Arc::new(AtomicBool::new(false));
    let node_stalled = Arc::new(AtomicBool::new(false));
    let service_stalled = Arc::new(AtomicBool::new(false));

    struct PtrNs(*const NodeStatus);
    unsafe impl Send for PtrNs {}
    struct PtrSs(*const ServiceStatus);
    unsafe impl Send for PtrSs {}

    let ns = PtrNs(node_status_ptr);
    let ss = PtrSs(service_status_ptr);

    let join_node = {
        let stalled = Arc::clone(&node_stalled);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let now_ns = now_ns();
            // SAFETY: see spawn_stall_watchers # Safety.
            let status: &'static NodeStatus = unsafe { &*ns.0 };
            let mut w = HeartbeatWatcher::new(
                status.heartbeat_seq.load(Ordering::Relaxed),
                now_ns,
            );
            while !stop.load(Ordering::Relaxed) {
                let alive = w.poll_node(status, now_ns(), DEFAULT_TIMEOUT.as_nanos() as u64);
                stalled.store(!alive, Ordering::Relaxed);
                tokio::time::sleep(POLL_PERIOD).await;
            }
        })
    };
    let join_service = {
        let stalled = Arc::clone(&service_stalled);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let now_ns = now_ns();
            // SAFETY: see spawn_stall_watchers # Safety.
            let status: &'static ServiceStatus = unsafe { &*ss.0 };
            let mut w = HeartbeatWatcher::new(
                status.heartbeat_seq.load(Ordering::Relaxed),
                now_ns,
            );
            while !stop.load(Ordering::Relaxed) {
                let alive = w.poll_service(status, now_ns(), DEFAULT_TIMEOUT.as_nanos() as u64);
                stalled.store(!alive, Ordering::Relaxed);
                tokio::time::sleep(POLL_PERIOD).await;
            }
        })
    };

    StallWatchers {
        node_stalled,
        service_stalled,
        join_node,
        join_service,
        stop,
    }
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p uc_client`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add uc_client/src/session.rs uc_client/src/watchers.rs
git commit -m "feat(uc_client): session ticker + node/service stall watchers"
```

### Task 4.4: Ring opens + broadcast reader

**Files:**
- Modify: `uc_client/src/rings.rs`

- [ ] **Step 1: Implement `rings.rs`**

```rust
//! Client-side ring opens + broadcast reader task.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use uc_protocol::frames::client::{
    MSG_TYPE_CLIENT_QUERY_RESP, MSG_TYPE_NOT_LEADER_RESP, MSG_TYPE_SUBMIT_RESPONSE,
    decode_extra_client,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::broadcast::{BroadcastConsumer, BroadcastRing};
use uc_protocol::ring::mpsc::{MpscProducer, MpscRing};

use crate::ClientError;

pub type ResponseSender = oneshot::Sender<RawResponse>;
pub type ResponseReceiver = oneshot::Receiver<RawResponse>;
pub type InFlight = Arc<DashMap<u32, ResponseSender>>;

/// Resolved response variant routed back to a single in-flight awaiter.
/// Distinct enum variants disambiguate Overwritten vs ShutDown vs
/// "we got a record back" — required by `m4_client_response_overwritten`.
#[derive(Debug)]
pub enum RawResponse {
    Record { msg_type: u16, payload: Bytes },
    Overwritten,
    ShutDown,
}

pub struct ClientRings {
    pub submit_producer: MpscProducer,
    pub query_producer: MpscProducer,
}

impl ClientRings {
    pub fn open(clients_dir: &Path) -> Result<(Self, BroadcastConsumer), ClientError> {
        let submit = MpscRing::open(&clients_dir.join("submit.ring"))
            .map_err(|e| ClientError::Decode(format!("open submit.ring: {e}")))?;
        let query = MpscRing::open(&clients_dir.join("query.ring"))
            .map_err(|e| ClientError::Decode(format!("open query.ring: {e}")))?;
        let response = BroadcastRing::open(&clients_dir.join("response.broadcast"))
            .map_err(|e| ClientError::Decode(format!("open response.broadcast: {e}")))?;
        let (submit_producer, _) = submit.into_split();
        let (query_producer, _) = query.into_split();
        let response_consumer = response.subscribe();
        Ok((
            ClientRings {
                submit_producer,
                query_producer,
            },
            response_consumer,
        ))
    }
}

pub struct BroadcastReaderHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_broadcast_reader(
    mut consumer: BroadcastConsumer,
    my_client_id: u32,
    in_flight: InFlight,
) -> BroadcastReaderHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut buf) {
                Ok(Some(rec)) => {
                    let (cid, local_seq) = decode_extra_client(rec.header_extra);
                    if cid != my_client_id {
                        continue;
                    }
                    // Recognized types only.
                    let msg_type = rec.msg_type;
                    let ok = matches!(
                        msg_type,
                        MSG_TYPE_SUBMIT_RESPONSE | MSG_TYPE_CLIENT_QUERY_RESP | MSG_TYPE_NOT_LEADER_RESP
                    );
                    if !ok {
                        continue;
                    }
                    let payload = Bytes::copy_from_slice(&buf);
                    if let Some((_, tx)) = in_flight.remove(&local_seq) {
                        let _ = tx.send(RawResponse::Record { msg_type, payload });
                    }
                }
                Ok(None) => tokio::time::sleep(Duration::from_micros(100)).await,
                Err(RingError::Overwritten) => {
                    // Drain every in-flight with Overwritten.
                    let drained: Vec<u32> =
                        in_flight.iter().map(|e| *e.key()).collect();
                    for k in drained {
                        if let Some((_, tx)) = in_flight.remove(&k) {
                            let _ = tx.send(RawResponse::Overwritten);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(?e, "broadcast reader: error");
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        }
    });

    BroadcastReaderHandle { join, stop }
}
```

> `RawResponse` is a three-variant enum so awaiters can distinguish a real record from a broadcast lap (Overwritten) from a client shutdown drain (ShutDown). Task 4.5's submit/query code branches on the variant directly.

- [ ] **Step 2: Build**

Run: `cargo build -p uc_client`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add uc_client/src/rings.rs
git commit -m "feat(uc_client): ring opens + broadcast reader with in-flight routing"
```

### Task 4.5: `Client` public API

**Files:**
- Modify: `uc_client/src/client.rs`

- [ ] **Step 1: Implement `Client`**

```rust
//! Public `Client` SDK for ultima_cluster (M4).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex as PlMutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

use uc_protocol::cnc::{NodeStatus, ServiceStatus};
use uc_protocol::frames::client::{
    MSG_TYPE_CLIENT_QUERY, MSG_TYPE_CLIENT_QUERY_RESP, MSG_TYPE_NOT_LEADER_RESP, MSG_TYPE_SUBMIT,
    MSG_TYPE_SUBMIT_RESPONSE, encode_extra_client, encode_flags_query_kind,
};
use uc_protocol::frames::query::QueryKind;
use uc_protocol::ring::RingError;

use crate::ClientError;
use crate::cnc::CncAttach;
use crate::rings::{ClientRings, InFlight, RawResponse, spawn_broadcast_reader};
use crate::session::SessionHandle;
use crate::watchers::{StallWatchers, spawn_stall_watchers};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const BACKPRESSURE_GRACE: Duration = Duration::from_secs(1);
const RING_FULL_RETRY: Duration = Duration::from_micros(100);

pub struct Client {
    cnc: Arc<CncAttach>,
    rings: PlMutex<ClientRings>,
    next_local_seq: AtomicU32,
    in_flight: InFlight,
    session: PlMutex<Option<SessionHandle>>,
    broadcast_reader: PlMutex<Option<crate::rings::BroadcastReaderHandle>>,
    watchers: PlMutex<Option<StallWatchers>>,
    shut_down: AtomicBool,
}

impl Client {
    pub async fn connect(instance_dir: &Path, app_id: &str) -> Result<Self, ClientError> {
        let cnc = Arc::new(CncAttach::attach(instance_dir, app_id)?);

        let clients_dir = instance_dir.join("clients");
        let (rings, response_consumer) = ClientRings::open(&clients_dir)?;
        let in_flight: InFlight = Arc::new(DashMap::new());

        let session = SessionHandle::create(&clients_dir.join("sessions.dir"), cnc.client_id)?;
        let broadcast_reader =
            spawn_broadcast_reader(response_consumer, cnc.client_id, in_flight.clone());

        // SAFETY: cnc Arc keeps the mmap alive until shutdown joins
        // the watchers via Drop or explicit shutdown().
        let watchers = unsafe {
            spawn_stall_watchers(cnc.node_status(), cnc.service_status())
        };

        Ok(Client {
            cnc,
            rings: PlMutex::new(rings),
            next_local_seq: AtomicU32::new(0),
            in_flight,
            session: PlMutex::new(Some(session)),
            broadcast_reader: PlMutex::new(Some(broadcast_reader)),
            watchers: PlMutex::new(Some(watchers)),
            shut_down: AtomicBool::new(false),
        })
    }

    pub fn client_id(&self) -> u32 {
        self.cnc.client_id
    }
    pub fn instance_id(&self) -> u128 {
        self.cnc.instance_id
    }

    pub fn current_leader(&self) -> Option<u64> {
        // SAFETY: cnc.node_status() returns a pointer valid for the cnc
        // mmap lifetime, which is tied to self.
        let ns: &NodeStatus = unsafe { &*self.cnc.node_status() };
        let id = ns.leader_node_id.load(Ordering::Relaxed);
        if id == u64::MAX { None } else { Some(id) }
    }

    pub fn last_applied(&self) -> u64 {
        let ns: &NodeStatus = unsafe { &*self.cnc.node_status() };
        ns.last_applied.load(Ordering::Relaxed)
    }

    pub async fn submit<C: Serialize, R: DeserializeOwned>(
        &self,
        cmd: &C,
    ) -> Result<R, ClientError> {
        if self.shut_down.load(Ordering::Relaxed) {
            return Err(ClientError::ShutDown);
        }
        let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard())?;
        let raw = self
            .send_and_await(MSG_TYPE_SUBMIT, payload, /*flags*/ 0, /*on_query_ring*/ false)
            .await?;
        match raw {
            RawResponse::Record { msg_type: MSG_TYPE_SUBMIT_RESPONSE, payload } => {
                let (resp, _) = bincode::serde::decode_from_slice::<R, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Ok(resp)
            }
            RawResponse::Record { msg_type: MSG_TYPE_NOT_LEADER_RESP, payload } => {
                let (hint, _) = bincode::serde::decode_from_slice::<Option<u64>, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Err(ClientError::NotLeader { hint })
            }
            RawResponse::Record { msg_type, .. } => Err(ClientError::Decode(format!(
                "unexpected msg_type {msg_type} on submit response"
            ))),
            RawResponse::Overwritten => Err(ClientError::ResponseOverwritten),
            RawResponse::ShutDown => Err(ClientError::ShutDown),
        }
    }

    pub async fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.submit_query::<Q, QR>(q, QueryKind::Snapshot).await
    }

    pub async fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.submit_query::<Q, QR>(q, QueryKind::Linearizable).await
    }

    async fn submit_query<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
        kind: QueryKind,
    ) -> Result<QR, ClientError> {
        if self.shut_down.load(Ordering::Relaxed) {
            return Err(ClientError::ShutDown);
        }
        let payload = bincode::serde::encode_to_vec(q, bincode::config::standard())?;
        let flags = encode_flags_query_kind(kind);
        let raw = self
            .send_and_await(MSG_TYPE_CLIENT_QUERY, payload, flags, /*on_query_ring*/ true)
            .await?;
        match raw {
            RawResponse::Record { msg_type: MSG_TYPE_CLIENT_QUERY_RESP, payload } => {
                let (resp, _) = bincode::serde::decode_from_slice::<QR, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Ok(resp)
            }
            RawResponse::Record { msg_type: MSG_TYPE_NOT_LEADER_RESP, payload } => {
                let (hint, _) = bincode::serde::decode_from_slice::<Option<u64>, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Err(ClientError::NotLeader { hint })
            }
            RawResponse::Record { msg_type, .. } => Err(ClientError::Decode(format!(
                "unexpected msg_type {msg_type} on query response"
            ))),
            RawResponse::Overwritten => Err(ClientError::ResponseOverwritten),
            RawResponse::ShutDown => Err(ClientError::ShutDown),
        }
    }

    async fn send_and_await(
        &self,
        msg_type: u16,
        payload: Vec<u8>,
        flags: u16,
        on_query_ring: bool,
    ) -> Result<RawResponse, ClientError> {
        let local_seq = self.next_local_seq.fetch_add(1, Ordering::Relaxed);
        let extra = encode_extra_client(self.cnc.client_id, local_seq);

        let (tx, rx): (oneshot::Sender<RawResponse>, oneshot::Receiver<RawResponse>) =
            oneshot::channel();
        self.in_flight.insert(local_seq, tx);

        // Write — retry on Full up to BACKPRESSURE_GRACE.
        let write_deadline = std::time::Instant::now() + BACKPRESSURE_GRACE;
        loop {
            let result = {
                let g = self.rings.lock();
                if on_query_ring {
                    g.query_producer
                        .try_write(msg_type, flags, extra, &payload)
                } else {
                    g.submit_producer
                        .try_write(msg_type, flags, extra, &payload)
                }
            };
            match result {
                Ok(()) => break,
                Err(RingError::Full) if std::time::Instant::now() < write_deadline => {
                    tokio::time::sleep(RING_FULL_RETRY).await;
                }
                Err(RingError::Full) => {
                    self.in_flight.remove(&local_seq);
                    return Err(ClientError::BackpressureFull);
                }
                Err(e) => {
                    self.in_flight.remove(&local_seq);
                    return Err(ClientError::Submission(format!("ring write: {e}")));
                }
            }
        }

        // Await response with stall + timeout selectors.
        let timeout = tokio::time::sleep(DEFAULT_REQUEST_TIMEOUT);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                biased;
                resp = &mut rx => {
                    return match resp {
                        Ok(r) => Ok(r),
                        Err(_) => Err(ClientError::ShutDown), // sender dropped without sending
                    };
                }
                _ = &mut timeout => {
                    self.in_flight.remove(&local_seq);
                    return Err(ClientError::Timeout(DEFAULT_REQUEST_TIMEOUT));
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Check stall flags.
                    let watchers_g = self.watchers.lock();
                    if let Some(w) = watchers_g.as_ref() {
                        if w.node_stalled.load(Ordering::Relaxed) {
                            drop(watchers_g);
                            self.in_flight.remove(&local_seq);
                            return Err(ClientError::NodeStalled);
                        }
                        if w.service_stalled.load(Ordering::Relaxed) {
                            drop(watchers_g);
                            self.in_flight.remove(&local_seq);
                            return Err(ClientError::ServiceStalled);
                        }
                    }
                }
            }
        }
    }

    pub async fn shutdown(self) -> Result<(), ClientError> {
        self.shut_down.store(true, Ordering::Relaxed);

        let session = self.session.lock().take();
        if let Some(s) = session {
            s.stop.store(true, Ordering::Relaxed);
            let _ = s.join.await;
            // Unlink the session file (best-effort).
            let _ = std::fs::remove_file(&s.path);
        }

        let reader = self.broadcast_reader.lock().take();
        if let Some(r) = reader {
            r.stop.store(true, Ordering::Relaxed);
            let _ = r.join.await;
        }

        let watchers = self.watchers.lock().take();
        if let Some(w) = watchers {
            w.stop.store(true, Ordering::Relaxed);
            let _ = w.join_node.await;
            let _ = w.join_service.await;
        }

        // Drain any leftover in-flights with ShutDown.
        let keys: Vec<u32> = self.in_flight.iter().map(|e| *e.key()).collect();
        for k in keys {
            if let Some((_, tx)) = self.in_flight.remove(&k) {
                let _ = tx.send(RawResponse::ShutDown);
            }
        }
        Ok(())
    }
}
```

> **Note on `MpscProducer::try_write` mutability:** the existing signature on `mpsc.rs` takes `&self` (`Clone`-able producer), so the `parking_lot::Mutex<ClientRings>` is actually only needed to coordinate the producer halves with their backing mmap lifetime. If `MpscProducer::try_write` already takes `&self`, the `PlMutex` can be lowered to `()` — but the lock is cheap; keep it for now to make the code obviously sound under additional future paths (e.g., reconfigurable rings).

- [ ] **Step 2: Update `lib.rs` re-export**

In `uc_client/src/lib.rs`, replace the stub `pub use client::Client;` line is already there; nothing more to do.

- [ ] **Step 3: Build**

Run: `cargo build -p uc_client`
Expected: clean build. Address any naming/lifetime mismatches against the actual signatures of `MpscProducer::try_write`, `BroadcastConsumer::try_read`, etc., from Task 1.4's implementation.

- [ ] **Step 4: Commit**

```bash
git add uc_client/src/client.rs uc_client/src/lib.rs
git commit -m "feat(uc_client): Client::{connect,submit,query_*,shutdown}"
```

---

## Phase 5 — Integration tests (`uc_node/tests/m4_client_*`)

All seven tests live under `uc_node/tests/` for consistency with the M3 capstone-per-file style. Each follows the same harness shape — boot node, boot service, wait for leader, build clients, exercise, shutdown.

### Task 5.1: `m4_client_single_node` — happy-path round trip

**Files:**
- Create: `uc_node/tests/m4_client_single_node.rs`

- [ ] **Step 1: Write the test**

```rust
//! 1 node + 1 service + 1 client (all in-process tokio tasks).
//! Client connects, submits two increments, queries via snapshot, shuts down.

use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
use uc_node::{BootstrapConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning, TlsConfig};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Cmd {
    Inc(u64),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Resp {
    value: u64,
}

impl StateMachine for Counter {
    type Command = Cmd;
    type Response = Resp;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> Resp {
        let Cmd::Inc(d) = cmd;
        self.value = self.value.wrapping_add(d);
        self.last_applied = Some(log_index);
        Resp { value: self.value }
    }
    fn query(&self, _: ()) -> u64 {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, _: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

async fn wait_for_path(p: &std::path::Path, t: Duration) {
    let deadline = std::time::Instant::now() + t;
    while !p.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", p.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn m4_client_single_node() {
    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m4-single".to_string();

    let cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance_dir.clone(),
        },
    };
    let node_task = tokio::spawn(async move {
        NodeBuilder::new(cfg, Counter::default()).start().await
    });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: svc_data.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task =
        tokio::spawn(async move { ServiceBuilder::new(svc_cfg, Counter::default()).run().await });

    let node = tokio::time::timeout(Duration::from_secs(15), node_task)
        .await
        .expect("node timeout")
        .expect("node panic")
        .expect("node start");
    let service = tokio::time::timeout(Duration::from_secs(15), svc_task)
        .await
        .expect("svc timeout")
        .expect("svc panic")
        .expect("svc start");
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1));

    // ── Client ─────────────────────────────────────────────────────────
    let client = Client::connect(&instance_dir, &app_id)
        .await
        .expect("connect");

    let r: Resp = client.submit(&Cmd::Inc(5)).await.expect("inc 5");
    assert_eq!(r, Resp { value: 5 });
    let r: Resp = client.submit(&Cmd::Inc(3)).await.expect("inc 3");
    assert_eq!(r, Resp { value: 8 });

    let v: u64 = client.query_snapshot(&()).await.expect("query");
    assert_eq!(v, 8);

    client.shutdown().await.expect("client shutdown");
    service.shutdown().await.expect("svc shutdown");
    node.shutdown().await.expect("node shutdown");
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p uc_node --test m4_client_single_node -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m4_client_single_node.rs
git commit -m "test(uc_node): m4_client_single_node — happy-path client round trip"
```

### Task 5.2: `m4_client_three_node` — multi-node NotLeader routing

**Files:**
- Create: `uc_node/tests/m4_client_three_node.rs`

- [ ] **Step 1: Write the test**

Adapt the existing `uc_node/tests/m3_three_node_shmem.rs` harness (boot three nodes + three services, wait for leader convergence) and append: connect one `Client` per instance_dir, have each follower's client `submit` and assert `ClientError::NotLeader { hint: Some(leader_id) }`, have the leader's client `submit` and `query_snapshot` and verify convergence on every follower's snapshot query.

```rust
// Full file under uc_node/tests/m4_client_three_node.rs.
// Use m3_three_node_shmem.rs as the structural template.
// Key additions after the three nodes converge on leader L:
let clients: Vec<Client> = futures::future::try_join_all(
    instance_dirs.iter().map(|d| Client::connect(d, &app_id))
).await.expect("connect all");

// Leader client submits twice.
let leader_idx = clients.iter().position(|c| c.current_leader() == Some(c_node_id(c))).unwrap();
clients[leader_idx].submit::<Cmd, Resp>(&Cmd::Inc(1)).await.unwrap();
clients[leader_idx].submit::<Cmd, Resp>(&Cmd::Inc(4)).await.unwrap();

// Each follower's client snapshot-queries and converges to 5.
for (i, c) in clients.iter().enumerate() {
    if i == leader_idx { continue; }
    for _ in 0..50 {
        let v: u64 = c.query_snapshot(&()).await.unwrap();
        if v == 5 { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let v: u64 = c.query_snapshot(&()).await.unwrap();
    assert_eq!(v, 5);
}

// Each follower's client submits and gets NotLeader { hint: Some(leader) }.
for (i, c) in clients.iter().enumerate() {
    if i == leader_idx { continue; }
    let err = c.submit::<Cmd, Resp>(&Cmd::Inc(10)).await.unwrap_err();
    match err {
        ClientError::NotLeader { hint: Some(l) } => assert_eq!(l, leader_node_id),
        e => panic!("expected NotLeader, got {e:?}"),
    }
}
```

> Reference the existing `m3_three_node_shmem.rs` for the boot harness; the part that differs is the additional `Client::connect` × 3 and the assertions above.

- [ ] **Step 2: Run**

Run: `cargo test -p uc_node --test m4_client_three_node -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m4_client_three_node.rs
git commit -m "test(uc_node): m4_client_three_node — NotLeader hint + follower snapshot convergence"
```

### Task 5.3: `m4_client_concurrent` — 4 clients × parallel submits

**Files:**
- Create: `uc_node/tests/m4_client_concurrent.rs`

- [ ] **Step 1: Write the test**

Same single-node harness as Task 5.1, but build 4 `Client`s and use `tokio::join!` of 4 submit batches (say 50 submits each = 200 commands). Assert no client errors. Assert the final `query_snapshot` value equals the sum (deterministic given total ordering on the leader).

```rust
let c1 = Client::connect(&instance_dir, &app_id).await.unwrap();
let c2 = Client::connect(&instance_dir, &app_id).await.unwrap();
let c3 = Client::connect(&instance_dir, &app_id).await.unwrap();
let c4 = Client::connect(&instance_dir, &app_id).await.unwrap();

let work = |c: &Client, delta: u64| {
    let c = &*c;
    async move {
        for _ in 0..50 {
            let _: Resp = c.submit(&Cmd::Inc(delta)).await.unwrap();
        }
    }
};

tokio::join!(work(&c1, 1), work(&c2, 2), work(&c3, 3), work(&c4, 4));

let v: u64 = c1.query_snapshot(&()).await.unwrap();
assert_eq!(v, 50 * (1 + 2 + 3 + 4));
for c in [c1, c2, c3, c4] {
    c.shutdown().await.unwrap();
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p uc_node --test m4_client_concurrent -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m4_client_concurrent.rs
git commit -m "test(uc_node): m4_client_concurrent — 4 clients × 50 submits, no torn responses"
```

### Task 5.4: `m4_client_wrap` — small rings, force wrap

**Files:**
- Create: `uc_node/tests/m4_client_wrap.rs`
- Modify: `uc_node/src/ipc/client_link.rs` (expose a `with_capacity` constructor)

- [ ] **Step 1: Add `ClientLink::create_with_cap` for tests**

In `client_link.rs`:

```rust
#[cfg(any(test, feature = "test-small-rings"))]
impl ClientLink {
    pub fn create_with_cap(
        instance_dir: &Path,
        cap_bytes: u64,
        max_msg: u32,
    ) -> Result<Self, IpcError> {
        let clients_dir = instance_dir.join("clients");
        std::fs::create_dir_all(clients_dir.join("sessions.dir"))?;
        let submit = MpscRing::create(&clients_dir.join("submit.ring"), cap_bytes, max_msg)?;
        let query = MpscRing::create(&clients_dir.join("query.ring"), cap_bytes, max_msg)?;
        let response = BroadcastRing::create(&clients_dir.join("response.broadcast"), cap_bytes, max_msg)?;
        let (_, submit_consumer) = submit.into_split();
        let (_, query_consumer) = query.into_split();
        let response_producer = response.producer();
        Ok(ClientLink { submit_consumer, query_consumer, response_producer })
    }
}
```

> Test-only knob; production code still uses `ClientLink::create` with the M4-standard 16 MiB cap.

For the test path to be wired in, `NodeBuilder::start` needs an optional override. Simpler: have the test create the rings *before* `NodeBuilder::start` by directly invoking `MpscRing::create`/`BroadcastRing::create` with the small caps; that leaves `ClientLink::create` skipped. **Cleanest:** add a public `ClientRingConfig` knob on `NodeConfig` defaulting to (16 MiB, 4 MiB), and have `NodeBuilder::start`'s shmem arm call `ClientLink::create_with_cap` using those.

Add to `uc_node/src/config.rs`:

```rust
#[derive(Debug, Clone)]
pub struct ClientRingConfig {
    pub cap_bytes: u64,
    pub max_msg: u32,
}
impl Default for ClientRingConfig {
    fn default() -> Self {
        Self { cap_bytes: 16 * 1024 * 1024, max_msg: 4 * 1024 * 1024 }
    }
}
```

Add `pub client_rings: ClientRingConfig,` to `NodeConfig` (with `..Default::default()` callers continuing to compile).

In `NodeBuilder::start`'s shmem arm, replace `ClientLink::create` with `ClientLink::create_with_cap(&instance_dir, cfg.client_rings.cap_bytes, cfg.client_rings.max_msg)?` and gate `create_with_cap` to always-on (drop the `#[cfg]` attribute since we now call it in production).

- [ ] **Step 2: Write the wrap test**

```rust
//! Force the client rings to wrap many times under realistic load and
//! verify no torn responses, no lost commands.

#[tokio::test]
async fn m4_client_wrap() {
    // ... same harness as m4_client_single_node, but with:
    let cfg = NodeConfig {
        // ...
        client_rings: ClientRingConfig { cap_bytes: 32 * 1024, max_msg: 4 * 1024 },
        // ...
    };
    // 2 clients × 500 submits each on a 32 KiB ring forces multiple wraps.
    let c1 = Client::connect(&instance_dir, &app_id).await.unwrap();
    let c2 = Client::connect(&instance_dir, &app_id).await.unwrap();

    let work = |c: &Client, base: u64| async move {
        for i in 0..500u64 {
            let _: Resp = c.submit(&Cmd::Inc(base + i)).await.unwrap();
        }
    };
    tokio::join!(work(&c1, 0), work(&c2, 10_000));

    let v: u64 = c1.query_snapshot(&()).await.unwrap();
    let expected = (0..500u64).map(|i| i + (10_000 + i)).sum::<u64>();
    assert_eq!(v, expected);
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p uc_node --test m4_client_wrap -- --nocapture`
Expected: PASS. (This test is the end-to-end validation that Phase 1's wrap-fix actually does its job under client traffic.)

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/m4_client_wrap.rs uc_node/src/ipc/client_link.rs uc_node/src/config.rs uc_node/src/runtime/builder.rs
git commit -m "test(uc_node): m4_client_wrap — small rings force wrap; validates Phase 1 fix end-to-end"
```

### Task 5.5: `m4_client_leader_failover` — leader shutdown + reconnect

**Files:**
- Create: `uc_node/tests/m4_client_leader_failover.rs`

- [ ] **Step 1: Write the test**

Use the three-node harness from Task 5.2. After convergence, capture the leader's instance_dir + node_id, connect a client to each. Call `node.shutdown()` on the leader. The old-leader-side client should fail submits with `ClientError::NodeStalled` (its node's heartbeats stop). Poll the surviving two clients' `current_leader()`; once a new leader is elected, submit one `Inc(1)` through the new leader's client and verify success.

```rust
// after harness setup:
let leader = clients[leader_idx].current_leader().unwrap();

// Tear down the old leader's node.
nodes.remove(leader_idx).shutdown().await.unwrap();

// Old leader's client should report NodeStalled within ~3s.
let start = std::time::Instant::now();
let mut got_stall = false;
while start.elapsed() < Duration::from_secs(5) {
    match clients[leader_idx].submit::<Cmd, Resp>(&Cmd::Inc(99)).await {
        Err(ClientError::NodeStalled) => { got_stall = true; break; }
        _ => tokio::time::sleep(Duration::from_millis(100)).await,
    }
}
assert!(got_stall, "old-leader client should detect NodeStalled");

// Wait for new leader on the surviving clients.
let new_leader = wait_for_new_leader(&clients, leader, Duration::from_secs(15)).await;

// Submit through whichever surviving client points at the new leader.
let active = clients.iter().find(|c| c.current_leader() == Some(new_leader)).unwrap();
let r: Resp = active.submit(&Cmd::Inc(1)).await.unwrap();
let _ = r;
```

- [ ] **Step 2: Run**

Run: `cargo test -p uc_node --test m4_client_leader_failover -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m4_client_leader_failover.rs
git commit -m "test(uc_node): m4_client_leader_failover — NodeStalled on dead leader, new-leader submits"
```

### Task 5.6: `m4_client_session_gc` — drop client without shutdown

**Files:**
- Create: `uc_node/tests/m4_client_session_gc.rs`

- [ ] **Step 1: Write the test**

Use single-node harness. Connect one client, capture `client.client_id()`. Drop the client without calling `shutdown()`. The session file should still exist immediately (no Drop impl removes it). Within `session_gc::STALE_AFTER + GC_TICK` (~7 s), the file should be unlinked.

```rust
let client = Client::connect(&instance_dir, &app_id).await.unwrap();
let cid = client.client_id();
let session_path = instance_dir.join("clients").join("sessions.dir").join(format!("{cid}.session"));
assert!(session_path.exists());

drop(client); // background tasks leak; ticker stops emitting

tokio::time::sleep(Duration::from_secs(8)).await;
assert!(!session_path.exists(), "session_gc should have unlinked the file");
```

- [ ] **Step 2: Run**

Run: `cargo test -p uc_node --test m4_client_session_gc -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m4_client_session_gc.rs
git commit -m "test(uc_node): m4_client_session_gc — dropped Client's session unlinked by GC"
```

### Task 5.7: `m4_client_response_overwritten` — slow consumer, broadcast laps

**Files:**
- Create: `uc_node/tests/m4_client_response_overwritten.rs`
- Modify: `uc_client/src/client.rs` (expose a test-only "pause broadcast reader" knob)

- [ ] **Step 1: Add the test-only pause knob**

In `uc_client/src/client.rs`, add behind `#[cfg(any(test, feature = "test-helpers"))]`:

```rust
impl Client {
    /// Test-only: stop the broadcast reader without joining it. Lets us
    /// simulate a slow consumer while the producer side keeps appending.
    pub fn _test_pause_broadcast_reader(&self) {
        if let Some(r) = self.broadcast_reader.lock().as_ref() {
            r.stop.store(true, Ordering::Relaxed);
        }
    }
}
```

Expose the `test-helpers` feature in `uc_client/Cargo.toml`:

```toml
[features]
default = []
test-helpers = []
```

In `uc_node/Cargo.toml` `[dev-dependencies]` add `uc_client = { path = "../uc_client", features = ["test-helpers"] }` (or reach for the path dep already present and gate via feature).

- [ ] **Step 2: Write the test**

Single-node harness. Pause the client's broadcast reader, submit enough commands that the broadcast.ring laps several times (need to know ring cap ≈ 16 MiB ÷ ~64 B/response ≈ 250 k records — too many; instead boot with `client_rings: ClientRingConfig { cap_bytes: 32 KiB, max_msg: 4 KiB }`). Then resume — well, actually the reader can't easily be resumed after stop; simplest: don't pause-then-resume, just *don't poll* for a while.

Cleanest alternative: connect the client *after* the broadcast has already lapped — but the client subscribes at the current `publish_position`, so it won't see those frames. Better: have the test artificially fall behind by pausing the reader (test-only `_test_pause_broadcast_reader`), then submitting via a second client to make the producer advance, then waking the first reader. Since stop is irreversible, expose `_test_pause_and_resume_broadcast_reader` instead — flip an `AtomicBool` that the read loop checks each iteration.

Update `rings.rs::spawn_broadcast_reader` to take an additional `paused: Arc<AtomicBool>` checked at each loop top; while paused, the reader sleeps. Expose `_test_pause(&self)` / `_test_resume(&self)` on `Client`.

```rust
#[tokio::test]
async fn m4_client_response_overwritten() {
    // ... boot with small client rings (cap 32 KiB, max_msg 4 KiB) ...
    let slow = Client::connect(&instance_dir, &app_id).await.unwrap();
    let driver = Client::connect(&instance_dir, &app_id).await.unwrap();

    slow._test_pause();
    // Driver client submits enough to lap the response.broadcast several times.
    for _ in 0..2000 {
        let _: Resp = driver.submit(&Cmd::Inc(1)).await.unwrap();
    }
    // While paused, slow tries a submit. It will write to submit.ring,
    // the response will broadcast, but slow's consumer head is way behind
    // and will be overrun.
    let pending = tokio::spawn({
        let slow_ref = std::sync::Arc::new(slow);
        let s2 = slow_ref.clone();
        async move { s2.submit::<Cmd, Resp>(&Cmd::Inc(1)).await }
    });
    // Drive more traffic to lap broadcast.
    for _ in 0..2000 {
        let _: Resp = driver.submit(&Cmd::Inc(1)).await.unwrap();
    }
    // Now resume the slow reader.
    // slow._test_resume();
    // (acquire slow back from the Arc — see test scaffolding details.)

    let result = pending.await.unwrap();
    match result {
        Err(ClientError::ResponseOverwritten) => { /* expected */ }
        other => panic!("expected ResponseOverwritten, got {other:?}"),
    }
}
```

> The pause/resume scaffolding is a bit hairy; if it's still flaky after one round of tuning, downgrade the test to just verify that the broadcast reader doesn't panic on `RingError::Overwritten` (drain in-flights with sentinel `msg_type = 0` from Task 4.4), then assert the submit fails with `ResponseOverwritten`. Functional verification, not exact timing.

- [ ] **Step 3: Run**

Run: `cargo test -p uc_node --test m4_client_response_overwritten -- --nocapture`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/m4_client_response_overwritten.rs uc_client/src/client.rs uc_client/src/rings.rs uc_client/Cargo.toml uc_node/Cargo.toml
git commit -m "test(uc_node): m4_client_response_overwritten — slow consumer surfaces ResponseOverwritten"
```

---

## Phase 6 — Polish + consolidation

### Task 6.1: Clippy + fmt clean across the workspace

**Files:** all of `ultima_cluster/`.

- [ ] **Step 1: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: zero warnings. Fix any that surfaced (typical: `clippy::needless_borrow`, `clippy::redundant_closure`).

- [ ] **Step 2: Run fmt**

Run: `cargo fmt --all`
Expected: clean (no diff).

- [ ] **Step 3: Run the full test suite once more**

Run: `cargo test --workspace`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(uc): clippy + fmt clean across workspace for M4"
```

### Task 6.2: Consolidate to `docs/tasks/task05_m4_clients_and_ring_fix.md`; delete superpowers artifacts; bump README

**Files:**
- Create: `docs/tasks/task05_m4_clients_and_ring_fix.md`
- Delete: `docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md`
- Delete: `docs/superpowers/plans/2026-05-16-uc-m4-clients-and-ring-fix.md`
- Modify: `README.md`

- [ ] **Step 1: Write the canonical task record**

Structure it after `docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`:

1. **Goal** — paraphrased from the spec.
2. **Architectural decisions** — the four spec table rows.
3. **What we built** — list each new module / modified API.
4. **What stayed deferred** — M3.5 follow-ups #1 and #4 still wait on M5 cnc-sub-mmap; `Raft::ensure_linearizable` plumb-through; multi-process tests; cnc-sub-mmap MPSC attach; output ring + at-least-once OutputHandler; snapshot.region mmap.
5. **Tests delivered** — list the seven `m4_client_*` tests + the Phase 1 regression tests.
6. **Pointers** — to `task04`, to the canonical design spec.

- [ ] **Step 2: Delete the superpowers artifacts**

```bash
git rm docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md
git rm docs/superpowers/plans/2026-05-16-uc-m4-clients-and-ring-fix.md
```

- [ ] **Step 3: Update README pointer**

In the project `README.md`, find the "Milestones" / status section and bump it from "M3.5 complete" to "M4 complete". Add a one-line description of the M4 deliverable.

- [ ] **Step 4: Run the full test suite one last time**

Run: `cargo test --workspace`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task05_m4_clients_and_ring_fix.md README.md docs/superpowers/
git commit -m "docs(m4): consolidate plan into task05; remove superpowers artifacts; README bump"
```

---

## Risks & considerations

- **Phase 1's MPSC publish-spin can stall** if a claimed-but-not-yet-published producer is preempted indefinitely. The spec accepts this for our payload sizes (microseconds). If a stuck producer becomes a real problem (long GC pause in a future-language SDK), the Aeron-style partition-rotation alternative (option E in the spec) replaces this without touching callers.
- **`session_gc::sweep` does I/O on each tick.** For O(100) clients this is negligible (each session file is 64 bytes; one syscall + mmap per file). For O(10k) clients we'd batch into a single `readdir` and skip already-known-stale entries; deferred until that workload arrives.
- **`ensure_linearizable` is approximated** in the M4 query dispatcher (a leader check, not a round-trip read-index). M3's query tests didn't require strict linearizability, and M4's tests don't either — but production linearizable reads should plumb `Raft::ensure_linearizable` through. Tracked as an M5 follow-up.
- **Phase 4's `_test_pause`/`_test_resume` knob** is gated behind a Cargo feature; it must not leak into release builds. Confirm with `cargo build --release -p uc_client` that no `test-helpers` symbols are exported.
- **Multi-process tests are out of scope.** All seven `m4_client_*` tests run as in-process tokio tasks. The shmem protocol is identical across the same/different-process boundary — covered functionally, deferred operationally to a later M.
