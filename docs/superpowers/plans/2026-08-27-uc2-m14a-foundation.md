# M14a — Multi-service foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let N state-machine processes (ids 0..7) attach to one `uc2_node` over one replicated log, each with its own cnc progress slot, its own query/egress rings and its own snapshot directory, paced by a bounded lag barrier and a quorum-gated durable report — with FSM 0 answering clients exactly as the single service does today.

**Architecture:** The cnc page grows to 8 KiB (version 3.0, a same-host flag day): page 2 is `ServiceSlot[8]` (512 B stride, one writer per line), page 1's singular service fields become node-written `min` aggregates over the declared set, and two boot-once fields at 4032/4040 publish the declared set and the lag policy. The node creates `svc_query.<id>.ring` / `egress_service.<id>.broadcast` / `snapshots/<id>/` per declared id and computes the aggregates once per consensus cycle; the service writes only its slot and, before each batch, caps its apply target at `min(slot.applied) + fsm_lag` (or one frame, in lockstep); the leader's admission door and every node's published validated frontier take `min_applied + fsm_lag` as a second ceiling so a lagging quorum stalls commit instead of running the FSMs off the ring. M11 backup/verify/restore learn the per-id snapshot layout.

**Tech Stack:** Rust 2024 (workspace edition), stable 1.96.0 pinned / MSRV 1.89, `memmap2` shared memory, `fs2` flock (workspace dep, new to `uc2_service`), `serde`/`toml` config, `cargo-fuzz` on nightly for the corpus regen.

**Spec:** `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` — this plan implements §3 (control page + config), §4.1–§4.2, §4.4 (the service side minus queries/responses fan-out), §5.1–§5.3, §5.5, §7.1–§7.2, §7.4–§7.5. **Not in this plan:** §5.4 + §6 (per-id query routing, client `submit_to`/`submit_all`, `MSG_V2_BAD_SERVICE`) and the `uc2_sim` scenario → plan **M14b**; §7.3 (N artifacts per snapshot session, wire 0.6.0), §9 (labelled metrics, alerts, `uc2ctl status` table), the capstones (§12: `lin_v2 two_fsm`, crashtest, elle), the fleet gate and the release writeup → plans **M14c/M14d**. Those plans are written after this one lands, against the tree it produces.

## Deviations from the spec, for the reviewer

Each is a plan-level decision found while mapping the spec onto the tree; the spec is not amended here. Veto any of them before execution.

1. **The lag barrier is a target cap, not a spin.** Spec §4.2 says the wait "is the same spin/yield the follower already uses for `Batch::NotCommitted`". No such variant exists: `uc2_log::reader::Batch` is `Frames | CaughtUp | Overrun` (`uc2_log/src/reader.rs:49-58`), the apply loop `break`s on `CaughtUp` and the agent's `IdleStrategy::Sleep(50 µs)` idles it. But `LogFollower::next_batch(target)` yields **only frames whose end ≤ `target`** (`reader.rs:64-84`, `FrameIter::next`'s target guard), so the bounded predicate `p + len − floor ≤ fsm_lag` is exactly `target = min(commit, durable, floor + fsm_lag)`, and lockstep ("floor ≥ p") is "if `cursor == floor` apply exactly one frame, else `CaughtUp`". A capped batch simply returns `CaughtUp` and the existing idle strategy waits. Zero new wait machinery; the invariant in §4.2 holds verbatim because `floor` is sampled once per cycle and only ever increases (a stale sample is conservative).
2. **`output_progress.state` stays one file, = `min(slot.output_completed)` over declared ids**, not `state/output_progress.<id>.state` (spec §4.4, §7.2, §7.5). The marker is a *resume hint* under an at-least-once contract: a faster FSM resuming from the min re-delivers a window it already delivered, which the contract permits, and it keeps the M11 `STATE_FILES` set, the MANIFEST and `BackupReport` untouched. Per-FSM `output_completed` on the slot (line 3) is kept, so `/metrics` and `uc2ctl` can still show each FSM's own progress in M14c. If the reviewer wants the per-id file anyway, it is a M14c item (N `StableValue`s in `NodeState` + a per-slot node-written mirror on the reserved line 7).
3. **This plan ships snapshot transfer for FSM 0 only.** The sender's `SnapshotSource` and the receiver's intake dir move from `snapshots/` to `snapshots/0/`; a learner joining a cluster with N > 1 declared ids gets FSM 0's artifact and FSM 1..N-1 stay below the floor until M14c lands §7.3. Stated in the docs task as a M14a limitation, not hidden.
4. **`fsm_lag_eff` under lockstep is `align_frame_len(HEADER_LEN + max_payload)`.** Spec §5.2 names `max_claim`, which does not exist in the tree (`uc2_node/src` has no such identifier); `FrameHeader.length` is the *total* frame length (`uc_protocol/src/v2/frame.rs:15`), so the largest frame the appender can produce is that expression, and "at most one frame past the FSMs" is one such frame.
5. **`min` over declared ids includes unattached ids reading 0** (spec §5.1 says so, and §7.1 says a never-snapshotted declared id holds the purge floor at 0). Kept exactly as specified — with one consequence pinned by a test: the purge floor's increase-only guard (`maybe_persist_snapshot_floor`, `node.rs:2901-2967`) means a *newly declared* id can never regress the floor, only freeze it, which is the §8 "declared set grew after purge ran" row.

## Global Constraints

- MSRV is **1.89**; CI's `msrv` job runs `cargo clippy --workspace --all-targets --locked -- -D warnings` on 1.89.0. Nothing here needs an API newer than that (`std::mem::offset_of!` is 1.77, `is_none_or` is 1.82 and already used in the tree).
- `cargo clippy --workspace --all-targets -- -D warnings` must be clean after **every** task.
- **Never write scratch or test artifacts to `/tmp`** — RAM-backed tmpfs, no swap. Tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`; the local smoke's instance dirs go under `/home/claude/`.
- **Use a private `CARGO_TARGET_DIR`** for any measurement or the final proof-stack run from this worktree (`~/.cache/cargo-target` is shared with the main checkout and other worktrees).
- cnc page offsets are pinned in **both** `uc_protocol::v2::cnc` and `uc2_log::cnc`, each with its own offset-assertion test; every new offset lands in both, with both tests grown.
- **`version::CURRENT` stays 0.5.0 in this plan.** The UDP wire is untouched; the only wire change (SNAP_BEGIN) is M14c. `CNC_V2_VERSION` goes 2.0 → 3.0 here (same-host flag day; every attaching party already refuses a page whose major differs, via `CncPage::validate`, `uc2_log/src/cnc.rs:321`).
- `uc2_consensus`, `uc2_net`'s receiver, `uc2_crypto`, the log frame, the datagram header and the M13 ring formats (`ULTRNG2`) are not touched. `publish_validated_frontier` changes the *value* the receiver already reads, not the receiver.
- Command payload ceiling, `PurgePolicy::Disabled` default, `[crypto]`/`[admin]` explicit-choice refusals: unchanged.
- The public `uc2_service` API changes shape in exactly three places, all additive except one: `ServiceConfig` gains `service_id` (+ builder `.service_id(u8)`), `ServiceError` gains two variants, and `SnapshotStore::open(instance_dir)` becomes `open(instance_dir, service_id: u8)` (the one breaking change; no external users exist — the standing "design freely" rule).
- Commit after every task with a conventional message. One task, one commit.

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `uc_protocol/src/v2/cnc.rs` | Modify | `CNC_PAGE_LEN` 8192, `CNC_V2_VERSION` 3.0, the page-2 `ServiceSlot` band constants (`CNC_OFF_SERVICE_SLOTS`, `CNC_SERVICE_SLOT_STRIDE`, `CNC_MAX_SERVICES`, `CNC_SVC_OFF_*`), the 4032/4040 pair (`CNC_OFF_SERVICES_DECLARED`, `CNC_OFF_FSM_LAG_BYTES`), module-doc layout map, `offsets_do_not_overlap` growth, the literal-byte version pin. |
| `uc_protocol/src/version.rs` | Modify | The NB paragraph that says the cnc page is "stuck at major=2" — corrected. |
| `fuzz/corpus/uc_protocol_cnc/*` | Regenerate | Seeds are built from `CNC_PAGE_LEN`; regenerated, not hand-edited. |
| `uc2_log/src/cnc.rs` | Modify | `ServiceSlot` (`#[repr(C)]`, 8 × `PaddedAtomicU64`), `CncPage::service_slot(i)`, `services_declared`/`store_services_declared`, `fsm_lag_bytes`/`store_fsm_lag_bytes`, the length gates (`new`, `create_file`, `open_file`, `heap`, `page`/`page_mut`), SAFETY comments, `cnc_offsets_match_protocol_constants` growth, new round-trip tests. |
| `uc2_node/src/services.rs` | Create | `ServicesConfig`/`FsmLag`/`parse_fsm_lag` (Task 3), `service_mins` (Task 5), `fsm_lag_eff`/`report_ceiling` (Task 8) — the pure, unit-tested half of the node's multi-service logic. |
| `uc2_node/src/config_file.rs` | Modify | `[services]` section (`ServicesSection`), `parse_byte_size`, `FsmLag`, the named refusals, tests. |
| `uc2_node/src/node.rs` | Modify | `ServicesConfig` on `NodeConfig`; per-id ring/dir creation; boot-once cnc fields; `Consensus::publish_service_mins` (top of `do_work`); `admission_open` second predicate; `publish_validated_frontier` ceiling; `PendingRead.service_id` + the per-slot ready bracket; `svc_query` producer table; `SnapshotSource`/intake at `snapshots/0/`. |
| `uc2_node/src/ipc.rs` | Modify | `svc_query_ring_for(id)`, `egress_service_for(id)`, `snapshot_dir_for(id)`, `service_lock_for(id)` accessors + test. |
| `uc2_node/src/obs/metrics.rs` | Modify | `uc2_service_epoch` reads slot 0 (page-1 `service_epoch` is retired). |
| `uc2_node/src/backup.rs` | Modify | Recursive `snapshots/<id>/` copy, per-id coverage (`BackupError::Hole { service }`), `BackupReport.newest_snapshots: [Option<u64>; 8]`, MANIFEST v2. |
| `uc2ctl/src/main.rs` | Modify | `print_backup_report` prints the per-id list. |
| `uc2_service/Cargo.toml` | Modify | `fs2 = { workspace = true }`. |
| `uc2_service/src/config.rs` | Modify | `ServiceConfig.service_id` + `.service_id()`, `ServiceError::{ServiceNotDeclared, AlreadyAttached}`. |
| `uc2_service/src/attach.rs` | Modify | Declared-set check, `service.<id>.lock`, per-id ring names, slot writes. |
| `uc2_service/src/lag.rs` | Create | The lag barrier as a pure plan: `LagMode`, `Plan`, `plan()`, `mode_from_page`, `floor` (Task 7). |
| `uc2_service/src/apply.rs` | Modify | Slot-based `applied`/`heartbeat_ns`, the lag barrier (`apply_target`), `lag_waits`. |
| `uc2_service/src/lib.rs` | Modify | `Service::service_id()`, the lock handle kept alive, `SnapshotStore::open(dir, id)`, slot-based snapshot/output reads. |
| `uc2_service/src/output.rs`, `builder_agent.rs`, `snapshots.rs` | Modify | Slot-based `output_completed` / `snapshot_pos`; `snapshots/<id>/`. |
| `uc2_client/src/engine.rs` + 6 test fixtures | Modify | `EGRESS_SERVICE` → `egress_service.0.broadcast` (FSM 0 only in M14a). |
| `examples/counter/src/bin/counter-service.rs`, `examples/uc2-crashtest/src/bin/uc2-crashtest-service.rs` | Modify | `--service-id`. |
| `uc2_gateway/examples/hop_bench/dummy_node.rs`, `uc2_node/examples/read_profile.rs` | Modify | Ring names. |
| 42 `NodeConfig` literal sites (listed in Task 3) | Modify | `services: ServicesConfig::default()`. |
| `uc2_node/tests/services.rs` | Create | The M14a integration tests: refusals, two FSMs apply, the lag bound, the door, the report ceiling. |
| `uc2_node/tests/backup.rs` | Modify | The two-id round trip + per-id hole test. |
| `packaging/node.example.toml`, `docs/reference/{cnc-page,instance-directory,wire-protocol}.md`, `docs/how-to/{upgrade-a-cluster,run-a-cluster,back-up-a-cluster}.md`, `docs/ops/uc2-runbook.md`, `docs/VERIFICATION.md` | Modify | Task 10. |

---

### Task 1: `uc_protocol` — page 2, the 4032 line, cnc version 3.0

Constants only (`uc_protocol::v2::cnc` is `core`-only and holds no accessors). After this task `uc2_log` fails to compile its tests? No — `uc2_log` imports constants by name and asserts `CNC_OFF_INGRESS_HOLES_SKIPPED + 64 == 4032` etc., all still true; but `CncPage::new` asserts `region.len() == CNC_PAGE_LEN` and `create_file` allocates `CNC_PAGE_LEN`, so `uc2_log` and everything above it simply start using 8 KiB pages. `CNC_V2_VERSION` 3.0 makes every party refuse a 2.0 page — the intended flag day.

**Files:**
- Modify `uc_protocol/src/v2/cnc.rs` (module doc lines 4–31; constants at 44–49 and after 227; `offsets_do_not_overlap` at 493–569; `header_write_pins_literal_bytes_0_16` at 383)
- Modify `uc_protocol/src/version.rs` (the NB block at 32–45)
- Regenerate `fuzz/corpus/uc_protocol_cnc/`

**Interfaces:**
- Produces (all `pub const`, `uc_protocol::v2::cnc`): `CNC_PAGE_LEN: usize = 8192`; `CNC_V2_VERSION: u32 = (3 << 24) | (0 << 16)`; `CNC_OFF_SERVICES_DECLARED: usize = 4032`; `CNC_OFF_FSM_LAG_BYTES: usize = 4040`; `CNC_OFF_SERVICE_SLOTS: usize = 4096`; `CNC_SERVICE_SLOT_STRIDE: usize = 512`; `CNC_MAX_SERVICES: usize = 8`; `CNC_SVC_OFF_STATUS: usize = 0`, `CNC_SVC_OFF_APPLIED = 64`, `CNC_SVC_OFF_EPOCH = 128`, `CNC_SVC_OFF_OUTPUT_COMPLETED = 192`, `CNC_SVC_OFF_SNAPSHOT_POS = 256`, `CNC_SVC_OFF_HEARTBEAT_NS = 320`, `CNC_SVC_OFF_LAG_WAITS = 384`, `CNC_SVC_OFF_RESERVED = 448`; `CNC_SVC_STATUS_ATTACHED: u64 = 1 << 8` (bit 8 of the status word; bits 0..8 = `service_id`, bits 32..64 = `incarnation`).

- [ ] **Step 1: Write the failing offset tests**

Append to `offsets_do_not_overlap` in `uc_protocol/src/v2/cnc.rs` (after the existing `assert_eq!(CNC_OFF_INGRESS_HOLES_SKIPPED + 64, 4032);` line, replacing its "still reserved and free" comment):

```rust
        // M14a: the last page-1 line is the boot-once pair the node writes at
        // startup — services_declared and fsm_lag_bytes share it exactly as the
        // M13 holes pair shares 3968 (one writer, two plain AtomicU64s).
        assert_eq!(CNC_OFF_SERVICES_DECLARED, 4032);
        assert_eq!(CNC_OFF_SERVICES_DECLARED % 64, 0);
        assert_eq!(CNC_OFF_FSM_LAG_BYTES, 4040);
        assert_eq!(CNC_OFF_FSM_LAG_BYTES - CNC_OFF_SERVICES_DECLARED, 8);
        const { assert!(CNC_OFF_FSM_LAG_BYTES + 8 <= CNC_OFF_SERVICES_DECLARED + 64) };
        // Page 1 is now FULL: the pair's line ends exactly where page 2 starts.
        assert_eq!(CNC_OFF_SERVICES_DECLARED + 64, 4096);
        // M14a: page 2 = ServiceSlot[8], 512 B stride, eight 64 B lines each.
        assert_eq!(CNC_OFF_SERVICE_SLOTS, 4096);
        assert_eq!(CNC_SERVICE_SLOT_STRIDE, 512);
        assert_eq!(CNC_MAX_SERVICES, 8);
        assert_eq!(CNC_SVC_OFF_STATUS, 0);
        assert_eq!(CNC_SVC_OFF_APPLIED, 64);
        assert_eq!(CNC_SVC_OFF_EPOCH, 128);
        assert_eq!(CNC_SVC_OFF_OUTPUT_COMPLETED, 192);
        assert_eq!(CNC_SVC_OFF_SNAPSHOT_POS, 256);
        assert_eq!(CNC_SVC_OFF_HEARTBEAT_NS, 320);
        assert_eq!(CNC_SVC_OFF_LAG_WAITS, 384);
        assert_eq!(CNC_SVC_OFF_RESERVED, 448);
        assert_eq!(CNC_OFF_SERVICE_SLOTS + CNC_MAX_SERVICES * CNC_SERVICE_SLOT_STRIDE, 8192);
        assert_eq!(CNC_PAGE_LEN, 8192);
        assert_eq!(CNC_SVC_STATUS_ATTACHED, 1 << 8);
```

And change the literal pin in `header_write_pins_literal_bytes_0_16` from `&[0x00, 0x00, 0x00, 0x02]` to `&[0x00, 0x00, 0x00, 0x03]` (the version bytes at 8..12; the comment beside it names "major 2" — change it to "major 3").

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_protocol --lib cnc`
Expected: compile error — `CNC_OFF_SERVICES_DECLARED` (and the other new names) not found.

- [ ] **Step 3: Add the constants and the version bump**

In `uc_protocol/src/v2/cnc.rs`, replace lines 46–49:

```rust
/// Two 4 KiB pages since M14a (cnc 3.0): page 1 is the M1–M13 layout
/// unchanged, page 2 is the per-service slot band (`CNC_OFF_SERVICE_SLOTS`).
pub const CNC_PAGE_LEN: usize = 8192;
/// Packed like `uc_protocol::ProtocolVersion`: `(major << 24) | (minor << 16) | patch`.
/// 3.0 (M14a): the page grew to two 4 KiB pages, the singular service band on
/// page 1 became node-written aggregates, and `MSG_V2_QUERY` gained a
/// service-id prefix (M14b) — a 2.0 party would misread all three, so it is a
/// major bump: every same-host attacher refuses the other side's page.
#[allow(clippy::identity_op)] // (0 << 16) spells out the packing explicitly (minor = 0)
pub const CNC_V2_VERSION: u32 = (3 << 24) | (0 << 16);
```

After the `CNC_OFF_QUERY_HOLES_SKIPPED` block (line 227), append:

```rust
// M14a: the boot-once pair on page 1's last line. Both are written ONCE by
// the node at startup (`Node::start_with`, before any agent runs) and read
// by every attaching service and client, which take the declared set and the
// lag policy from the PAGE, not from the config file. Two plain `AtomicU64`s
// sharing one 64-byte line, for the same reason as 3968/3976: one writer,
// and `PaddedAtomicU64` cannot sit at +8.
/// Bit `i` set ⇔ service id `i` is declared in this node's `[services] ids`.
pub const CNC_OFF_SERVICES_DECLARED: usize = 4032;
const _: () = assert!(CNC_OFF_SERVICES_DECLARED % 64 == 0);
/// The lag bound in bytes; `0` ⇔ lockstep.
pub const CNC_OFF_FSM_LAG_BYTES: usize = 4040;
const _: () = assert!(CNC_OFF_FSM_LAG_BYTES == CNC_OFF_SERVICES_DECLARED + 8);
const _: () = assert!(CNC_OFF_SERVICES_DECLARED + 64 == 4096, "page 1 is exactly full");

// M14a: page 2 — `ServiceSlot[CNC_MAX_SERVICES]`. One slot per service id,
// fixed 512 B stride (eight 64 B lines), ONE WRITER PER LINE — every line is
// written by one agent of the service process that owns the slot, except the
// reserved line 7. Per-slot layout (offsets relative to the slot base):
//   +0   status          u64 = service_id (bits 0..8) | attached (bit 8)
//                              | incarnation (bits 32..64)    writer: service (attach/detach)
//   +64  applied         u64 position                          writer: service apply agent
//   +128 epoch           u64 (attach-time fetch_add, AcqRel)   writer: service (attach)
//   +192 output_completed u64 position                         writer: service output agent
//   +256 snapshot_pos    u64 position                          writer: service builder agent
//   +320 heartbeat_ns    u64 unix ns                           writer: service apply agent
//   +384 lag_waits       u64 count                             writer: service apply agent
//   +448 reserved (zero)
pub const CNC_OFF_SERVICE_SLOTS: usize = 4096;
pub const CNC_SERVICE_SLOT_STRIDE: usize = 512;
pub const CNC_MAX_SERVICES: usize = 8;
pub const CNC_SVC_OFF_STATUS: usize = 0;
pub const CNC_SVC_OFF_APPLIED: usize = 64;
pub const CNC_SVC_OFF_EPOCH: usize = 128;
pub const CNC_SVC_OFF_OUTPUT_COMPLETED: usize = 192;
pub const CNC_SVC_OFF_SNAPSHOT_POS: usize = 256;
pub const CNC_SVC_OFF_HEARTBEAT_NS: usize = 320;
pub const CNC_SVC_OFF_LAG_WAITS: usize = 384;
pub const CNC_SVC_OFF_RESERVED: usize = 448;
/// `status` bit 8: the owning service process is attached (cleared on a clean
/// detach; a crashed service leaves it set and its heartbeat ages instead).
pub const CNC_SVC_STATUS_ATTACHED: u64 = 1 << 8;
/// `status` bits 32..64: the attach count of this id on this page (bumped
/// per attach, so a restart is visible even before the epoch is read).
pub const CNC_SVC_STATUS_INCARNATION_SHIFT: u32 = 32;
const _: () = assert!(
    CNC_OFF_SERVICE_SLOTS + CNC_MAX_SERVICES * CNC_SERVICE_SLOT_STRIDE <= CNC_PAGE_LEN,
    "service-slot band overruns the cnc page"
);
```

Update the module-doc layout map (lines 10–31): change the "One fixed-size 4 KiB page" sentence to "Two 4 KiB pages (8 KiB) since cnc 3.0", replace the `4032..4096  reserved (zero)` line with `4032  services_declared / fsm_lag_bytes (node, boot-once)` and add `4096  ServiceSlot[8] (per service: status, applied, epoch, output_completed, snapshot_pos, heartbeat_ns, lag_waits, reserved) 8 × 512 B`. Update the `512 ServiceProgress` line's comment to say "(node-written aggregates since 3.0: min over declared ids; service_epoch retired at 0)".

In `uc_protocol/src/version.rs`, in the NB block (lines 32–45), replace the sentence that says `CNC_V2_VERSION` has been "stuck at major=2/minor=0 since M5" with: "`CNC_V2_VERSION` moved 2.0 → 3.0 in M14a (page grew to 8 KiB); the two version lines remain independent — that bump did not touch this constant."

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_protocol`
Expected: PASS, including `offsets_do_not_overlap` and `header_write_pins_literal_bytes_0_16`. (`uc_protocol` had 82 tests after M13a's plan; expect 82 still — no test was added, two grew.)

- [ ] **Step 5: Regenerate the cnc fuzz corpus and smoke the target**

The seed generator builds pages from `CNC_PAGE_LEN` (`fuzz/src/seeds.rs:597`), so it now emits 8 KiB pages:

```bash
(cd fuzz && cargo +nightly run --bin seed-corpus)
git status --short fuzz/corpus/uc_protocol_cnc/
scripts/fuzz_smoke.sh 30 uc_protocol_cnc
```

Expected: the `01-valid-page`, `02-future-version`, `03-bad-magic` seeds change size (4096 → 8192 bytes; `04`/`05`/`06` are prefixes and do not); the smoke prints its per-target PASS line for `uc_protocol_cnc`. Only `uc_protocol_cnc/` files change.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p uc_protocol --all-targets -- -D warnings
git add uc_protocol/src/v2/cnc.rs uc_protocol/src/version.rs fuzz/corpus/uc_protocol_cnc
git commit -m "feat(protocol): cnc 3.0 — 8 KiB page, ServiceSlot[8] band on page 2, services_declared/fsm_lag_bytes at 4032"
```

---

### Task 2: `uc2_log` — `ServiceSlot`, `service_slot(i)`, the boot-once accessors, the length gates

`uc2_log::cnc::CncPage` is the only typed view of the page; every attacher goes through `open_file` → `validate`. This task mirrors Task 1's constants with `#[repr(C)]` structs + `offset_of!` pins and grows the length gates from 4096 to `CNC_PAGE_LEN`. The `create_shared_backing_file` path (`uc_protocol/src/ring/common.rs:474-514`) grows a leftover 4 KiB `cnc2.dat` to 8 KiB and zero-fills it; a leftover 8 KiB file under an old binary fails the old `open_file`'s length gate with `BadHeader` — both directions refuse rather than misread.

**Files:**
- Modify `uc2_log/src/cnc.rs` (imports 23–30; structs after `PeerSlot` at 126–150; `CncPage` accessors after `peer_slot` at 446; SAFETY comments at 255, 411, 420, 426, 432, 440, 449; test `cnc_offsets_match_protocol_constants` at 834; new tests after `query_holes_skipped_roundtrip_and_offset_pin` at 1190)

**Interfaces:**
- Consumes: Task 1's constants.
- Produces (`uc2_log::cnc`):
  ```rust
  #[repr(C)]
  pub struct ServiceSlot {
      pub status: PaddedAtomicU64,
      pub applied: PaddedAtomicU64,
      pub epoch: PaddedAtomicU64,
      pub output_completed: PaddedAtomicU64,
      pub snapshot_pos: PaddedAtomicU64,
      pub heartbeat_ns: PaddedAtomicU64,
      pub lag_waits: PaddedAtomicU64,
      pub reserved: PaddedAtomicU64,
  }
  impl CncPage {
      pub fn service_slot(&self, i: usize) -> &ServiceSlot;   // panics if i >= CNC_MAX_SERVICES
      pub fn services_declared(&self) -> u64;
      pub fn store_services_declared(&self, v: u64);
      pub fn fsm_lag_bytes(&self) -> u64;
      pub fn store_fsm_lag_bytes(&self, v: u64);
  }
  pub fn pack_service_status(service_id: u8, attached: bool, incarnation: u32) -> u64;
  pub fn unpack_service_status(v: u64) -> (u8, bool, u32);
  ```

- [ ] **Step 1: Write the failing tests**

Append to `cnc_offsets_match_protocol_constants` (after the `CNC_OFF_INGRESS_HOLES_SKIPPED + 64 == 4032` assertion):

```rust
        // M14a: the boot-once pair and page 2.
        assert_eq!(cnc::CNC_OFF_SERVICES_DECLARED, 4032);
        assert_eq!(cnc::CNC_OFF_FSM_LAG_BYTES, 4040);
        assert_eq!(std::mem::size_of::<ServiceSlot>(), 512);
        assert_eq!(std::mem::size_of::<ServiceSlot>(), cnc::CNC_SERVICE_SLOT_STRIDE);
        for i in 0..cnc::CNC_MAX_SERVICES {
            let slot = page.service_slot(i);
            let expect = cnc::CNC_OFF_SERVICE_SLOTS + i * cnc::CNC_SERVICE_SLOT_STRIDE;
            assert_eq!(slot as *const _ as usize - base, expect, "service slot {i}");
        }
        let s0 = page.service_slot(0);
        let s0_base = s0 as *const _ as usize;
        assert_eq!(&s0.status as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_STATUS);
        assert_eq!(&s0.applied as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_APPLIED);
        assert_eq!(&s0.epoch as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_EPOCH);
        assert_eq!(
            &s0.output_completed as *const _ as usize - s0_base,
            cnc::CNC_SVC_OFF_OUTPUT_COMPLETED
        );
        assert_eq!(&s0.snapshot_pos as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_SNAPSHOT_POS);
        assert_eq!(&s0.heartbeat_ns as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_HEARTBEAT_NS);
        assert_eq!(&s0.lag_waits as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_LAG_WAITS);
        assert_eq!(&s0.reserved as *const _ as usize - s0_base, cnc::CNC_SVC_OFF_RESERVED);
        assert_eq!(page.page().len(), 8192);
```

Add three new tests after `query_holes_skipped_roundtrip_and_offset_pin`:

```rust
    #[test]
    fn services_declared_and_fsm_lag_roundtrip_and_offset_pin() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.services_declared(), 0, "fresh page: nothing declared");
        assert_eq!(page.fsm_lag_bytes(), 0);
        page.store_services_declared(0b101);
        page.store_fsm_lag_bytes(16 << 20);
        assert_eq!(page.services_declared(), 0b101);
        assert_eq!(page.fsm_lag_bytes(), 16 << 20);
        let raw = page.page();
        assert_eq!(
            u64::from_le_bytes(raw[4032..4040].try_into().unwrap()),
            0b101,
            "offset pin: services_declared lives at 4032"
        );
        assert_eq!(
            u64::from_le_bytes(raw[4040..4048].try_into().unwrap()),
            16 << 20,
            "offset pin: fsm_lag_bytes lives at 4040"
        );
        assert_eq!(page.query_holes_skipped(), 0, "the 3968 line is untouched");
    }

    #[test]
    fn service_slots_init_zero_and_are_independent() {
        let page = CncPage::heap(&test_meta());
        for i in 0..cnc::CNC_MAX_SERVICES {
            let s = page.service_slot(i);
            assert_eq!(s.status.load_acquire(), 0, "slot {i} dormant");
            assert_eq!(s.applied.load_acquire(), 0);
            assert_eq!(s.epoch.load_acquire(), 0);
        }
        let s3 = page.service_slot(3);
        s3.status.store_release(pack_service_status(3, true, 1));
        s3.applied.store_release(4096);
        assert_eq!(s3.epoch.fetch_add(1) + 1, 1);
        assert_eq!(unpack_service_status(s3.status.load_acquire()), (3, true, 1));
        assert_eq!(page.service_slot(2).applied.load_acquire(), 0, "neighbour below untouched");
        assert_eq!(page.service_slot(4).applied.load_acquire(), 0, "neighbour above untouched");
        // Byte pin: slot 3's `applied` line is at 4096 + 3*512 + 64.
        let raw = page.page();
        let off = 4096 + 3 * 512 + 64;
        assert_eq!(u64::from_le_bytes(raw[off..off + 8].try_into().unwrap()), 4096);
    }

    #[test]
    #[should_panic(expected = "service slot index 8 out of range")]
    fn service_slot_index_is_bounds_checked() {
        let page = CncPage::heap(&test_meta());
        let _ = page.service_slot(8);
    }

    #[test]
    fn service_status_pack_roundtrips_every_field() {
        assert_eq!(unpack_service_status(pack_service_status(0, false, 0)), (0, false, 0));
        assert_eq!(unpack_service_status(pack_service_status(7, true, u32::MAX)), (7, true, u32::MAX));
        assert_eq!(pack_service_status(5, true, 2), 5 | (1 << 8) | (2u64 << 32));
    }
```

Also change `open_file_rejects_wrong_length_file_without_panicking` (line 975) so its "one too long" case is explicit about the new length: keep `CNC_PAGE_LEN + 1`, and **add** a case writing exactly 4096 bytes — a 2.0-era file — asserting `Err(CncError::BadHeader)` with the message "a 4 KiB (cnc 2.0) file is refused by length before the version is even read".

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc2_log --lib cnc`
Expected: compile error — `ServiceSlot`, `service_slot`, `pack_service_status` not found.

- [ ] **Step 3: Implement**

Extend the import block (lines 23–30) with `CNC_MAX_SERVICES, CNC_OFF_FSM_LAG_BYTES, CNC_OFF_SERVICES_DECLARED, CNC_OFF_SERVICE_SLOTS, CNC_SERVICE_SLOT_STRIDE, CNC_SVC_STATUS_ATTACHED, CNC_SVC_STATUS_INCARNATION_SHIFT`.

After `pack_naks_plus_replay` (line 162), add:

```rust
/// M14a: one per-service slot on page 2 — see `uc_protocol::v2::cnc`'s
/// `CNC_OFF_SERVICE_SLOTS` doc for the writer-per-line table. Same shape as
/// [`PeerSlot`]: every field its own cache line, `#[repr(C)]`, stride pinned.
#[repr(C)]
pub struct ServiceSlot {
    pub status: PaddedAtomicU64,
    pub applied: PaddedAtomicU64,
    pub epoch: PaddedAtomicU64,
    pub output_completed: PaddedAtomicU64,
    pub snapshot_pos: PaddedAtomicU64,
    pub heartbeat_ns: PaddedAtomicU64,
    pub lag_waits: PaddedAtomicU64,
    pub reserved: PaddedAtomicU64,
}

const _: () = assert!(std::mem::size_of::<ServiceSlot>() == 512);
const _: () = assert!(std::mem::size_of::<ServiceSlot>() == CNC_SERVICE_SLOT_STRIDE);
const _: () = assert!(std::mem::offset_of!(ServiceSlot, status) == cnc::CNC_SVC_OFF_STATUS);
const _: () = assert!(std::mem::offset_of!(ServiceSlot, applied) == cnc::CNC_SVC_OFF_APPLIED);
const _: () = assert!(std::mem::offset_of!(ServiceSlot, epoch) == cnc::CNC_SVC_OFF_EPOCH);
const _: () = assert!(
    std::mem::offset_of!(ServiceSlot, output_completed) == cnc::CNC_SVC_OFF_OUTPUT_COMPLETED
);
const _: () =
    assert!(std::mem::offset_of!(ServiceSlot, snapshot_pos) == cnc::CNC_SVC_OFF_SNAPSHOT_POS);
const _: () =
    assert!(std::mem::offset_of!(ServiceSlot, heartbeat_ns) == cnc::CNC_SVC_OFF_HEARTBEAT_NS);
const _: () = assert!(std::mem::offset_of!(ServiceSlot, lag_waits) == cnc::CNC_SVC_OFF_LAG_WAITS);
const _: () = assert!(std::mem::offset_of!(ServiceSlot, reserved) == cnc::CNC_SVC_OFF_RESERVED);

/// Pack a slot's `status` word: `service_id` (bits 0..8) | attached (bit 8)
/// | `incarnation` (bits 32..64).
pub fn pack_service_status(service_id: u8, attached: bool, incarnation: u32) -> u64 {
    (service_id as u64)
        | if attached { CNC_SVC_STATUS_ATTACHED } else { 0 }
        | ((incarnation as u64) << CNC_SVC_STATUS_INCARNATION_SHIFT)
}

/// Inverse of [`pack_service_status`]: `(service_id, attached, incarnation)`.
pub fn unpack_service_status(v: u64) -> (u8, bool, u32) {
    (
        (v & 0xFF) as u8,
        v & CNC_SVC_STATUS_ATTACHED != 0,
        (v >> CNC_SVC_STATUS_INCARNATION_SHIFT) as u32,
    )
}
```

After `peer_slot` (line 446–453) add:

```rust
    /// M14a: the per-service slot for id `i` on page 2 (panics on `i >= 8`,
    /// like `peer_slot`). Every attaching party reads all declared slots; a
    /// service writes ONLY its own.
    pub fn service_slot(&self, i: usize) -> &ServiceSlot {
        assert!(i < CNC_MAX_SERVICES, "service slot index {i} out of range");
        let off = CNC_OFF_SERVICE_SLOTS + i * CNC_SERVICE_SLOT_STRIDE;
        // SAFETY: as `peer_slot` — off is 64-aligned (4096 + i*512), the slot
        // is 512 bytes, and 4096 + 8*512 = 8192 = CNC_PAGE_LEN.
        unsafe { &*(self.region.ptr_at(off) as *const ServiceSlot) }
    }

    /// M14a: bit `i` set ⇔ service id `i` is declared. Boot-once, node-written.
    /// A bare `AtomicU64` (not `PaddedAtomicU64`) because it shares its line
    /// with `fsm_lag_bytes` — see the 3968/3976 pair's doc on why.
    pub fn services_declared(&self) -> u64 {
        // SAFETY: 4032 is 8-aligned and 4032 + 8 <= CNC_PAGE_LEN.
        unsafe { (*(self.region.ptr_at(CNC_OFF_SERVICES_DECLARED) as *const AtomicU64)).load(Ordering::Acquire) }
    }
    pub fn store_services_declared(&self, v: u64) {
        // SAFETY: as `services_declared`.
        unsafe { (*(self.region.ptr_at(CNC_OFF_SERVICES_DECLARED) as *const AtomicU64)).store(v, Ordering::Release) }
    }
    /// M14a: the lag bound in bytes, `0` ⇔ lockstep. Boot-once, node-written.
    pub fn fsm_lag_bytes(&self) -> u64 {
        // SAFETY: 4040 is 8-aligned and 4040 + 8 <= CNC_PAGE_LEN.
        unsafe { (*(self.region.ptr_at(CNC_OFF_FSM_LAG_BYTES) as *const AtomicU64)).load(Ordering::Acquire) }
    }
    pub fn store_fsm_lag_bytes(&self, v: u64) {
        // SAFETY: as `fsm_lag_bytes`.
        unsafe { (*(self.region.ptr_at(CNC_OFF_FSM_LAG_BYTES) as *const AtomicU64)).store(v, Ordering::Release) }
    }
```

Length gates: nothing to change in code — `new` (257), `page`/`page_mut` (267/277), `create_file` (366), `open_file` (387) and `heap` (401) all name `CNC_PAGE_LEN`. Fix the **SAFETY comments** that spell `4096` (lines 255, 411, 420, 426, 432, 440, 449): replace each `<= 4096` with `<= 4096 (page 1)` — every page-1 band still ends at or before 4096, and the comment is now precise rather than wrong. The `CncPage::new` assert message already interpolates `CNC_PAGE_LEN`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc2_log`
Expected: PASS; `cnc` module goes 25 → 29 tests (the four added above). `open_file_rejects_wrong_length_file_without_panicking` passes with its new 4096-byte case.

- [ ] **Step 5: Build the workspace — the flag day shows up as nothing**

```bash
cargo build --workspace --all-targets
cargo test -p uc2_client -p uc2_service -p uc2_node --test smoke 2>&1 | tail -5
```

Expected: builds clean; `uc2_node --test smoke` passes (every process in a test is the same binary, so every party writes and reads 8 KiB pages). The `uc2_client` `torn_header.rs` comment mentioning `set_len(4096)` is a comment; leave it.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p uc2_log --all-targets -- -D warnings
git add uc2_log/src/cnc.rs
git commit -m "feat(log): cnc 3.0 view — ServiceSlot band accessors, services_declared/fsm_lag_bytes, 8 KiB length gates"
```

---

### Task 3: `[services]` config — `ServicesConfig`, `FsmLag`, the named refusals, and the `NodeConfig` sweep

A new module `uc2_node/src/services.rs` owns the typed value (it will also own the aggregate/`fsm_lag_eff` helpers in Tasks 5 and 8, keeping `node.rs` from growing). `config_file.rs` deserialises the raw section and calls `ServicesConfig::from_ids`, following the `[admin]` precedent (typed value + cross-field rules in `parse_str`, `ConfigError::Invalid { field, detail }` with `detail` naming the field). There is no byte-size string parser in the workspace today (grepped `MiB|parse_bytes|byte_size` — comments and `1 << 26` literals only), so `parse_fsm_lag` is new.

One refusal beyond spec §3.3, stated as **deviation 6**: **id 0 must be declared.** FSM 0 is the default responder (`submit`, spec §6.2) and the only FSM the remote path reaches (§6.4); a declared set without it makes every default client call unanswerable. Cheap to refuse by name at boot, expensive to diagnose at runtime.

**Files:**
- Create `uc2_node/src/services.rs`
- Modify `uc2_node/src/lib.rs` (add `pub mod services;` after `pub mod recovery;` at line 47; extend the `pub use node::{…}` list at 51–56 with `ServicesConfig, FsmLag` re-exported from `services`)
- Modify `uc2_node/src/config_file.rs` (struct at 175–220, `parse_str` construction at 410–432, tests)
- Modify `uc2_node/src/node.rs` (`NodeConfig` at 152–211)
- Modify `packaging/node.example.toml`
- Modify the 42 `NodeConfig` literal sites (list in Step 6)

**Interfaces:**
- Produces (`uc2_node::services`, re-exported at the crate root):
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum FsmLag { Lockstep, Bounded(u64) }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct ServicesConfig { /* private */ }
  impl Default for ServicesConfig;                         // {0}, fsm_lag unset (⇒ buffer_bytes / 4)
  impl ServicesConfig {
      pub fn from_ids(ids: &[u8], fsm_lag: Option<FsmLag>) -> Result<Self, String>;
      #[doc(hidden)] pub fn none_for_tests() -> Self;      // declared = 0: no FSM pacing — harness only
      pub fn declared(&self) -> u64;                       // bitmask
      pub fn ids(&self) -> impl Iterator<Item = u8> + '_;
      pub fn is_declared(&self, id: u8) -> bool;
      pub fn ring_ids(&self) -> impl Iterator<Item = u8> + '_;   // declared, or {0} when none
      pub fn resolve_lag(&self, buffer_bytes: u64) -> FsmLag;    // unset ⇒ Bounded(buffer_bytes / 4)
      pub fn validate(&self, buffer_bytes: u64) -> Result<(), String>;
      pub fn page_lag_value(&self, buffer_bytes: u64) -> u64;    // 0 ⇔ lockstep (the cnc 4040 encoding)
  }
  pub fn parse_fsm_lag(s: &str) -> Result<FsmLag, String>;
  ```
- `NodeConfig` gains `pub services: ServicesConfig`.

- [ ] **Step 1: Write the failing unit tests for the typed value**

Create `uc2_node/src/services.rs` with only the test module first:

```rust
//! M14a: the declared service set and the FSM lag policy (`[services]` in
//! `node.toml`, `NodeConfig::services` programmatically). See the design spec
//! §3.3 and §5.1–§5.2.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_fsm_zero_with_unset_lag_resolving_to_a_quarter_buffer() {
        let s = ServicesConfig::default();
        assert_eq!(s.declared(), 0b1);
        assert!(s.is_declared(0));
        assert!(!s.is_declared(1));
        assert_eq!(s.ids().collect::<Vec<_>>(), vec![0]);
        assert_eq!(s.resolve_lag(4 << 20), FsmLag::Bounded(1 << 20));
        assert_eq!(s.page_lag_value(4 << 20), 1 << 20);
        s.validate(4 << 20).unwrap();
    }

    #[test]
    fn from_ids_builds_the_bitmask_in_any_order() {
        let s = ServicesConfig::from_ids(&[2, 0, 5], None).unwrap();
        assert_eq!(s.declared(), 0b10_0101);
        assert_eq!(s.ids().collect::<Vec<_>>(), vec![0, 2, 5]);
        assert_eq!(s.ring_ids().collect::<Vec<_>>(), vec![0, 2, 5]);
    }

    #[test]
    fn from_ids_refusals_are_named() {
        let e = ServicesConfig::from_ids(&[], None).unwrap_err();
        assert!(e.contains("services.ids must not be empty"), "{e}");
        let e = ServicesConfig::from_ids(&[0, 1, 1], None).unwrap_err();
        assert!(e.contains("duplicate service id 1"), "{e}");
        let e = ServicesConfig::from_ids(&[0, 8], None).unwrap_err();
        assert!(e.contains("service id 8 is out of range (0..8)"), "{e}");
        let e = ServicesConfig::from_ids(&[1, 2], None).unwrap_err();
        assert!(e.contains("service id 0 must be declared"), "{e}");
    }

    #[test]
    fn lag_validation_refuses_half_the_ring_and_zero() {
        let buf = 4u64 << 20;
        ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded((buf / 2) - 1))).unwrap().validate(buf).unwrap();
        let e = ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded(buf / 2))).unwrap().validate(buf).unwrap_err();
        assert!(e.contains("services.fsm_lag must be below buffer_bytes / 2"), "{e}");
        let e = ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded(0))).unwrap().validate(buf).unwrap_err();
        assert!(e.contains("services.fsm_lag = 0 is not a bound; write \"lockstep\""), "{e}");
        ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap().validate(buf).unwrap();
        assert_eq!(ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap().page_lag_value(buf), 0);
    }

    #[test]
    fn none_for_tests_declares_nothing_but_still_rings_fsm_zero() {
        let s = ServicesConfig::none_for_tests();
        assert_eq!(s.declared(), 0);
        assert_eq!(s.ids().count(), 0);
        assert_eq!(s.ring_ids().collect::<Vec<_>>(), vec![0]);
        s.validate(4 << 20).unwrap();
    }

    #[test]
    fn parse_fsm_lag_table() {
        assert_eq!(parse_fsm_lag("lockstep"), Ok(FsmLag::Lockstep));
        assert_eq!(parse_fsm_lag("65536"), Ok(FsmLag::Bounded(65536)));
        assert_eq!(parse_fsm_lag("64KiB"), Ok(FsmLag::Bounded(64 << 10)));
        assert_eq!(parse_fsm_lag("16MiB"), Ok(FsmLag::Bounded(16 << 20)));
        assert_eq!(parse_fsm_lag("1GiB"), Ok(FsmLag::Bounded(1 << 30)));
        for bad in ["", "16 MiB", "16mb", "MiB", "1.5MiB", "-1", "99999999999GiB", "Lockstep"] {
            let e = parse_fsm_lag(bad).unwrap_err();
            assert!(e.contains("services.fsm_lag"), "{bad:?}: {e}");
        }
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Add `pub mod services;` to `uc2_node/src/lib.rs` (after line 47). Run: `cargo test -p uc2_node --lib services`
Expected: compile errors — `ServicesConfig`, `FsmLag`, `parse_fsm_lag` not found.

- [ ] **Step 3: Implement the module**

Above the test module in `uc2_node/src/services.rs`:

```rust
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

/// The FSM pacing policy (spec §1, "FSM pacing"). There is deliberately no
/// unbounded variant: an FSM slower than the log's sustained rate can never
/// catch up from journal replay, so "unbounded" is a silent death spiral.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsmLag {
    /// No FSM starts frame k+1 until every FSM finished frame k.
    Lockstep,
    /// `applied_a - applied_b <= bytes` for any two declared FSMs.
    Bounded(u64),
}

/// The declared service set + lag policy. Static per node; must match
/// cluster-wide (checked on the snapshot path in M14c, exported for alerting
/// in M14c). Absent `[services]` ⇒ `{0}` with the default bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServicesConfig {
    /// Bit `i` set ⇔ id `i` declared. `0` only via `none_for_tests`.
    declared: u64,
    /// `None` ⇒ `Bounded(buffer_bytes / 4)`, resolved once `buffer_bytes` is known.
    fsm_lag: Option<FsmLag>,
}

impl Default for ServicesConfig {
    fn default() -> Self {
        Self { declared: 0b1, fsm_lag: None }
    }
}

impl ServicesConfig {
    /// Build from an explicit id list. Refusals (each names the field, M9
    /// style): empty list, duplicate id, id ≥ 8, id 0 missing (FSM 0 is the
    /// default responder and the only FSM the remote path reaches).
    pub fn from_ids(ids: &[u8], fsm_lag: Option<FsmLag>) -> Result<Self, String> {
        if ids.is_empty() {
            return Err("services.ids must not be empty (omit the [services] section for the default [0])".into());
        }
        let mut declared = 0u64;
        for &id in ids {
            if id as usize >= CNC_MAX_SERVICES {
                return Err(format!("services.ids: service id {id} is out of range (0..{CNC_MAX_SERVICES})"));
            }
            if declared & (1 << id) != 0 {
                return Err(format!("services.ids: duplicate service id {id}"));
            }
            declared |= 1 << id;
        }
        if declared & 1 == 0 {
            return Err("services.ids: service id 0 must be declared (it is the default responder)".into());
        }
        Ok(Self { declared, fsm_lag })
    }

    /// HARNESS ONLY: a node with no FSMs declared. The aggregates are not
    /// published, the admission door's FSM term and the report ceiling are
    /// inert, and page 1's service band behaves as it did on cnc 2.0 (a test
    /// may poke it). Unreachable from `node.toml` (`from_ids` refuses an
    /// empty list); exists so node-only tests are not silently stalled by a
    /// service that was never going to attach.
    #[doc(hidden)]
    pub fn none_for_tests() -> Self {
        Self { declared: 0, fsm_lag: None }
    }

    pub fn declared(&self) -> u64 {
        self.declared
    }

    pub fn is_declared(&self, id: u8) -> bool {
        (id as usize) < CNC_MAX_SERVICES && self.declared & (1 << id) != 0
    }

    /// Declared ids, ascending.
    pub fn ids(&self) -> impl Iterator<Item = u8> + '_ {
        (0..CNC_MAX_SERVICES as u8).filter(move |&i| self.is_declared(i))
    }

    /// The ids the node creates rings/dirs for: the declared set, or `{0}`
    /// for a `none_for_tests` node (clients still need FSM 0's rings to
    /// attach).
    pub fn ring_ids(&self) -> impl Iterator<Item = u8> + '_ {
        let mask = if self.declared == 0 { 1 } else { self.declared };
        (0..CNC_MAX_SERVICES as u8).filter(move |&i| mask & (1 << i) != 0)
    }

    pub fn resolve_lag(&self, buffer_bytes: u64) -> FsmLag {
        self.fsm_lag.unwrap_or(FsmLag::Bounded(buffer_bytes / 4))
    }

    /// The cnc 4040 encoding: the byte bound, or `0` for lockstep.
    pub fn page_lag_value(&self, buffer_bytes: u64) -> u64 {
        match self.resolve_lag(buffer_bytes) {
            FsmLag::Lockstep => 0,
            FsmLag::Bounded(b) => b,
        }
    }

    /// The bound must provably keep every FSM on the ring: below half the
    /// buffer (the other half is the appender's overrun margin plus the
    /// leader's admission window). `0` is refused because it is the page's
    /// lockstep sentinel — a config that means lockstep must say so.
    pub fn validate(&self, buffer_bytes: u64) -> Result<(), String> {
        match self.resolve_lag(buffer_bytes) {
            FsmLag::Lockstep => Ok(()),
            FsmLag::Bounded(0) => {
                Err("services.fsm_lag = 0 is not a bound; write \"lockstep\" for lockstep".into())
            }
            FsmLag::Bounded(b) if b >= buffer_bytes / 2 => Err(format!(
                "services.fsm_lag must be below buffer_bytes / 2 ({} < {}); got {b}",
                b,
                buffer_bytes / 2
            )),
            FsmLag::Bounded(_) => Ok(()),
        }
    }
}

/// `"lockstep"`, or a byte count as `<digits>` with an optional `KiB`/`MiB`/
/// `GiB` suffix (no spaces, no fractions, binary units only — the same
/// vocabulary the spec uses). Errors name the field.
pub fn parse_fsm_lag(s: &str) -> Result<FsmLag, String> {
    if s == "lockstep" {
        return Ok(FsmLag::Lockstep);
    }
    let (digits, shift) = if let Some(d) = s.strip_suffix("GiB") {
        (d, 30)
    } else if let Some(d) = s.strip_suffix("MiB") {
        (d, 20)
    } else if let Some(d) = s.strip_suffix("KiB") {
        (d, 10)
    } else {
        (s, 0)
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "services.fsm_lag must be \"lockstep\" or <digits>[KiB|MiB|GiB], got {s:?}"
        ));
    }
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("services.fsm_lag: {digits:?} does not fit in u64"))?;
    n.checked_shl(shift)
        .filter(|v| shift == 0 || *v >> shift == n)
        .map(FsmLag::Bounded)
        .ok_or_else(|| format!("services.fsm_lag: {s:?} overflows u64"))
}
```

Re-export in `uc2_node/src/lib.rs`: `pub use services::{FsmLag, ServicesConfig};` next to the `node::{…}` re-export.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p uc2_node --lib services`
Expected: 6 passed.

- [ ] **Step 5: Write the failing config-file tests**

In `uc2_node/src/config_file.rs`'s test module, after `admin_hmac_duplicate_key_names_are_refused`:

```rust
    #[test]
    fn services_section_absent_means_fsm_zero_and_the_default_bound() {
        let (cfg, _) = load_str(MINIMAL).unwrap();
        assert_eq!(cfg.services, ServicesConfig::default());
    }

    #[test]
    fn services_section_parses_ids_and_a_byte_size_lag() {
        let body = format!("{MINIMAL}\n[services]\nids = [0, 1, 2]\nfsm_lag = \"16MiB\"\n");
        let (cfg, _) = load_str(&body).unwrap();
        assert_eq!(cfg.services, ServicesConfig::from_ids(&[0, 1, 2], Some(FsmLag::Bounded(16 << 20))).unwrap());
    }

    #[test]
    fn services_section_parses_lockstep() {
        let body = format!("{MINIMAL}\n[services]\nids = [0, 3]\nfsm_lag = \"lockstep\"\n");
        let (cfg, _) = load_str(&body).unwrap();
        assert_eq!(cfg.services.resolve_lag(1 << 26), FsmLag::Lockstep);
    }

    #[test]
    fn services_refusals_name_the_field() {
        for (tail, needle, field) in [
            ("ids = []", "services.ids must not be empty", "services.ids"),
            ("ids = [0, 0]", "duplicate service id 0", "services.ids"),
            ("ids = [0, 9]", "out of range", "services.ids"),
            ("ids = [1]", "service id 0 must be declared", "services.ids"),
            ("ids = [0]\nfsm_lag = \"16 MiB\"", "services.fsm_lag must be", "services.fsm_lag"),
            ("ids = [0]\nfsm_lag = \"0\"", "not a bound", "services.fsm_lag"),
            // default buffer_bytes is 64 MiB; half is 32 MiB.
            ("ids = [0]\nfsm_lag = \"32MiB\"", "below buffer_bytes / 2", "services.fsm_lag"),
        ] {
            let body = format!("{MINIMAL}\n[services]\n{tail}\n");
            let err = load_str(&body).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(needle), "{tail}: expected {needle:?} in {msg:?}");
            match err {
                ConfigError::Invalid { field: f, .. } => assert_eq!(f, field, "{tail}"),
                other => panic!("{tail}: expected Invalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_typo_inside_services_is_refused() {
        let body = format!("{MINIMAL}\n[services]\nids = [0]\nfsm_lagg = \"1MiB\"\n");
        let err = load_str(&body).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }), "{err:?}");
    }
```

Also extend `minimal_config_maps_to_node_config_with_defaults` (line 531) with `assert_eq!(cfg.services, ServicesConfig::default());`.

- [ ] **Step 6: Run to verify they fail, then implement the section and the `NodeConfig` field**

Run: `cargo test -p uc2_node --lib config_file` — expected: compile error (`cfg.services` has no such field).

In `config_file.rs`:

```rust
/// M14a: `[services]` — the declared FSM set and the lag policy (spec §3.3).
/// `fsm_lag` is a STRING (`"16MiB"`, `"lockstep"`), parsed by
/// `services::parse_fsm_lag` so the refusal can name the field.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServicesSection {
    ids: Vec<u8>,
    #[serde(default)]
    fsm_lag: Option<String>,
}
```

Add to `NodeConfigFile`: `#[serde(default)] services: Option<ServicesSection>,` with the doc comment "Absent means `[0]` with the default bound (`buffer_bytes / 4`)."

In `parse_str`, before the final `Ok((NodeConfig { … }))`:

```rust
    let services = match f.services {
        None => ServicesConfig::default(),
        Some(s) => {
            let fsm_lag = match s.fsm_lag.as_deref() {
                None => None,
                Some(raw) => Some(
                    crate::services::parse_fsm_lag(raw)
                        .map_err(|detail| ConfigError::Invalid { field: "services.fsm_lag", detail })?,
                ),
            };
            let cfg = ServicesConfig::from_ids(&s.ids, fsm_lag)
                .map_err(|detail| ConfigError::Invalid { field: "services.ids", detail })?;
            cfg.validate(f.buffer_bytes as u64)
                .map_err(|detail| ConfigError::Invalid { field: "services.fsm_lag", detail })?;
            cfg
        }
    };
```

and `services,` in the `NodeConfig { … }` literal. Import `crate::services::{FsmLag, ServicesConfig}` at the top (and in the test module).

In `node.rs`, add to `NodeConfig` after `crypto`:

```rust
    /// M14a: the declared service set + FSM lag policy (`[services]`). Default
    /// `{0}` with `fsm_lag = buffer_bytes / 4`. Validated at `Node::start`
    /// (`ServicesConfig::validate`) — a bad bound is a named startup refusal
    /// before any file is created.
    pub services: ServicesConfig,
```

Now the sweep — every `NodeConfig { … }` literal needs the field. Run `cargo build --workspace --all-targets 2>&1 | grep -c E0063` to count, then add one line per site. The files (from `grep -rln "journal_segment_bytes:" --include=*.rs .`, 42 files; several have more than one literal):

- Use **`services: ServicesConfig::none_for_tests(),`** in the node-only harnesses — files that call `Node::start*` and never build a `ServiceBuilder`: `uc2_node/tests/{smoke,force_config,failover,reconfig,lifecycle,purge_safety,admin_auth,obs_log,learner}.rs`, `uc2_node/examples/m4_gate.rs`, `uc2ctl/tests/admin_auth_bin.rs`, `uc2_client/tests/timeout_and_restart.rs`. (These drive `Node::submit` with no FSM ever attaching; with `{0}` declared the Task 8 door would close at `fsm_lag` bytes.)
- Use **`services: ServicesConfig::default(),`** everywhere else (the tests and examples that attach a real service, `config_file.rs`'s own construction, `obs/mod.rs::for_tests`, `preflight.rs`, `metrics.rs` tests, `node.rs`'s two test literals at ~5789/7412, the counter and crashtest node bins, `lincheck_v2/mod.rs::make_config`, `m5/m6/m7/m10_gate`, `m10_alerts`, `read_profile`, `m12_gate`, `uc2_gateway/tests/common`, `uc2_client/tests/{roundtrip,pipelined}.rs`, `uc2_service/tests/*`, `elle_v2.rs`, `lin_v2.rs`, `backup.rs`, `crypto_*.rs`, `obs_http.rs`, `query_barrier.rs`).

`crate::services::ServicesConfig` inside `uc2_node`; `uc2_node::ServicesConfig` outside.

Add to `packaging/node.example.toml`, after the `[metrics]` block:

```toml
# M14a: the state-machine processes this node hosts (one process per id, all
# applying the same log). Absent ⇒ ids = [0] with fsm_lag = buffer_bytes / 4.
# The set is static and must match on every node; changing it is a restart,
# and an id may only be ADDED while the journal is intact from position 0
# (purge disabled or never fired) — a new FSM rebuilds from genesis.
# fsm_lag bounds how far any FSM may run ahead of the slowest ("lockstep" =
# one frame); it must be below buffer_bytes / 2. Every id beyond 0 reserves a
# further 5 MiB of ring files at boot (see instance-directory.md).
#
# [services]
# ids = [0]
# fsm_lag = "16MiB"
```

- [ ] **Step 7: Run the tests, clippy, commit**

```bash
cargo build --workspace --all-targets
cargo test -p uc2_node --lib
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(node): [services] config — ServicesConfig/FsmLag, named refusals, NodeConfig.services"
```

Expected: `config_file` tests pass including `the_packaged_example_config_is_valid` (the new block is commented out) and `an_unreserved_unknown_section_is_still_refused` (`[telemetry]` still refused; `[services]` is now known).

---

### Task 4: Per-id rings and directories, end to end — the file-name flag day

The node creates `svc_query.<id>.ring`, `egress_service.<id>.broadcast` and `snapshots/<id>/` for every `ring_ids()` entry (5 MiB per id: 1 MiB SPSC + 4 MiB broadcast, the sizes at `node.rs:5015,5019`) and publishes the two boot-once fields; the service opens the `<id>` names for its `service_id` (plumbed through `ServiceConfig` here, **not enforced** until Task 6); the client opens `egress_service.0.broadcast`. Renaming is atomic across node/service/client/fixtures in this one task because a half-renamed tree cannot attach.

**Files:**
- Modify `uc2_node/src/ipc.rs` (accessors at 63–88; test `path_accessors_are_rooted` at 116)
- Modify `uc2_node/src/node.rs` (`Rings` at 389–401; `create_rings` at 4981–5027; the call at 650; `Consensus.svc_query` field + `forward_svc_query` at 3288–3305; step 3 of `start_with` after `cnc.counters().prime(durable)` at 620; `snap_dir` at 878)
- Modify `uc2_service/src/config.rs`, `uc2_service/src/attach.rs` (lines 60–65), `uc2_service/src/lib.rs` (`Service` gains `service_id`)
- Modify `uc2_client/src/engine.rs:58`
- Modify fixtures: `uc2_client/src/pipelined.rs:478`, `uc2_client/tests/{engine_synthetic.rs:41,187, pipelined.rs:112, synthetic.rs:49,98, timeout_and_restart.rs:28, torn_header.rs:31}`, `uc2_service/tests/{apply.rs:141,187, query.rs:167,180}`, `uc2_node/tests/smoke.rs:96-97`, `uc2_node/tests/learner.rs:444-446`, `uc2_gateway/examples/hop_bench/dummy_node.rs:147-148,171,194`, `uc2_node/examples/read_profile.rs:197`, `uc_protocol/src/v2/ipc.rs` module doc (ring names)

**Interfaces:**
- Produces (`uc2_node::ipc::InstanceDir`): `pub fn svc_query_ring_for(&self, id: u8) -> PathBuf` (`svc_query.<id>.ring`), `pub fn egress_service_for(&self, id: u8) -> PathBuf` (`egress_service.<id>.broadcast`), `pub fn snapshot_dir_for(&self, id: u8) -> PathBuf` (`snapshots/<id>`), `pub fn service_lock_for(&self, id: u8) -> PathBuf` (`service.<id>.lock`). The singular `svc_query_ring()` / `egress_service()` accessors are **deleted**.
- Produces (`uc2_service`): `ServiceConfig.service_id: u8` (default 0), `ServiceConfig::service_id(self, id: u8) -> Self`, `Service::service_id(&self) -> u8`.
- Produces (`uc2_node::Consensus`, crate-private): `svc_query: Vec<Option<SpscProducer>>` (len 8, `Some` for every ring id); `fn forward_svc_query(&mut self, service_id: u8, client_id: u32, local_seq: u32, expected_epoch: u64, query: &[u8]) -> bool`.
- Produces (cnc): `services_declared` and `fsm_lag_bytes` stored at boot.

- [ ] **Step 1: Write the failing tests**

`uc2_node/src/ipc.rs`, extend `path_accessors_are_rooted`:

```rust
        assert_eq!(d.svc_query_ring_for(0), dir.path().join("svc_query.0.ring"));
        assert_eq!(d.svc_query_ring_for(7), dir.path().join("svc_query.7.ring"));
        assert_eq!(d.egress_service_for(3), dir.path().join("egress_service.3.broadcast"));
        assert_eq!(d.snapshot_dir_for(1), dir.path().join("snapshots").join("1"));
        assert_eq!(d.service_lock_for(2), dir.path().join("service.2.lock"));
```

(and delete the two assertions on the singular names).

`uc2_node/tests/smoke.rs:96-97` → assert `svc_query.0.ring` and `egress_service.0.broadcast` exist, plus `!dir.path().join("svc_query.ring").exists()` ("the legacy singular name is not created").

Create `uc2_node/tests/services.rs` — the M14a integration file; this task adds its harness and first test:

```rust
//! M14a multi-service integration tests: per-id rings, the declared set on
//! the page, attach refusals (Task 6), the lag bound (Task 7), the door and
//! the report ceiling (Task 8). Single node unless stated; every instance dir
//! is on the ext4 target volume, never /tmp.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc2_log::cnc::CncPage;
use uc2_node::{CryptoConfig, FsmLag, Node, NodeConfig, PurgePolicy, ServicesConfig};

pub const APP: &str = "m14-services";

static TEST_LOCK: Mutex<()> = Mutex::new(());
pub fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-m14-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

pub fn config(dir: &Path, services: ServicesConfig) -> NodeConfig {
    let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        learners: Vec::new(),
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 1,
        faults: uc2_net::fault::FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: 64 * 1024,
        crypto: CryptoConfig::Disabled,
        services,
    }
}

pub fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "timeout waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub fn open_cnc(dir: &Path) -> std::sync::Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP).expect("open cnc")
}

pub fn ids(ids: &[u8], lag: Option<FsmLag>) -> ServicesConfig {
    ServicesConfig::from_ids(ids, lag).unwrap()
}

#[test]
fn node_creates_per_id_rings_dirs_and_publishes_the_declared_set() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 2], Some(FsmLag::Bounded(64 << 10))))).unwrap();
    wait_until("serving", || node.can_serve());
    for id in [0u8, 2] {
        assert!(dir.path().join(format!("svc_query.{id}.ring")).is_file(), "svc_query.{id}.ring");
        assert!(dir.path().join(format!("egress_service.{id}.broadcast")).is_file(), "egress {id}");
        assert!(dir.path().join("snapshots").join(id.to_string()).is_dir(), "snapshots/{id}");
    }
    assert!(!dir.path().join("svc_query.1.ring").exists(), "undeclared id gets no ring");
    assert!(!dir.path().join("svc_query.ring").exists(), "legacy singular name is not created");
    let cnc = open_cnc(dir.path());
    assert_eq!(cnc.services_declared(), 0b101);
    assert_eq!(cnc.fsm_lag_bytes(), 64 << 10);
    node.stop();
}

#[test]
fn lockstep_publishes_zero_and_none_for_tests_still_rings_fsm_zero() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0], Some(FsmLag::Lockstep)))).unwrap();
    wait_until("serving", || node.can_serve());
    assert_eq!(open_cnc(dir.path()).fsm_lag_bytes(), 0, "0 ⇔ lockstep");
    node.stop();

    let dir2 = tempdir();
    let node = Node::start(config(dir2.path(), ServicesConfig::none_for_tests())).unwrap();
    wait_until("serving", || node.can_serve());
    assert!(dir2.path().join("egress_service.0.broadcast").is_file());
    assert_eq!(open_cnc(dir2.path()).services_declared(), 0);
    node.stop();
}

#[test]
fn a_bad_lag_bound_is_a_named_startup_refusal_before_any_file_exists() {
    let _g = serialize();
    let dir = tempdir();
    let cfg = config(dir.path(), ids(&[0], Some(FsmLag::Bounded(2 << 20)))); // == buffer/2
    let err = Node::start(cfg).err().expect("must refuse");
    assert!(err.to_string().contains("services.fsm_lag must be below buffer_bytes / 2"), "{err}");
    assert!(!dir.path().join("cnc2.dat").exists(), "refused before creating the page");
    assert!(!dir.path().join("instance.lock").exists(), "refused before taking the lock");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_node --test services` — expected: compile error (`svc_query_ring_for` etc. missing; `fsm_lag_bytes` exists but the `services_declared` assertion would read 0).

- [ ] **Step 3: Implement — node side**

`ipc.rs`: replace `svc_query_ring()` and `egress_service()` with:

```rust
    /// M14a: the node→service query ring for service `id`.
    pub fn svc_query_ring_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("svc_query.{id}.ring"))
    }
    /// M14a: service `id`'s response broadcast (service → clients).
    pub fn egress_service_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("egress_service.{id}.broadcast"))
    }
    /// M14a: service `id`'s snapshot directory (`snapshots/<id>/`).
    pub fn snapshot_dir_for(&self, id: u8) -> PathBuf {
        self.root.join("snapshots").join(id.to_string())
    }
    /// M14a: the exclusive flock a service process takes for its id.
    pub fn service_lock_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("service.{id}.lock"))
    }
```

`node.rs`:

At the top of `Node::start_with` (before `InstanceDir::acquire`):

```rust
        // M14a: the lag bound is validated BEFORE any file is created — a
        // named startup refusal, like a bad crypto key.
        cfg.services
            .validate(cfg.buffer_bytes as u64)
            .map_err(|d| io::Error::new(io::ErrorKind::InvalidInput, d))?;
```

`Rings` becomes `struct Rings { egress_services: Vec<BroadcastRing> }`. `create_rings` takes `services: &ServicesConfig` and returns `(Rings, MpscConsumer, BroadcastProducer, MpscConsumer, Vec<Option<SpscProducer>>)`:

```rust
fn create_rings(
    dir: &InstanceDir,
    services: &ServicesConfig,
) -> io::Result<(Rings, MpscConsumer, BroadcastProducer, MpscConsumer, Vec<Option<SpscProducer>>)> {
    const MIB: u64 = 1 << 20;
    const MAX_MSG: u32 = 64 << 10;
    // Unlink every ring this or a PREVIOUS layout could have left (the
    // cnc-2.0 singular names included): a stale file's attachment is
    // invalidated by the new instance_id anyway.
    let mut stale = vec![
        dir.ingress_ring(),
        dir.query_ring(),
        dir.egress_node(),
        dir.root.join("svc_query.ring"),
        dir.root.join("egress_service.broadcast"),
    ];
    for id in 0..CNC_MAX_SERVICES as u8 {
        stale.push(dir.svc_query_ring_for(id));
        stale.push(dir.egress_service_for(id));
    }
    for p in stale {
        let _ = std::fs::remove_file(&p);
    }
    let ingress = MpscRing::create(&dir.ingress_ring(), 4 * MIB, MAX_MSG).map_err(to_io)?;
    let (_ingress_producer, ingress_consumer) = ingress.into_split();
    let egress_node = BroadcastRing::create(&dir.egress_node(), 4 * MIB, MAX_MSG).map_err(to_io)?;
    let egress_node_producer = egress_node.producer();
    let query = MpscRing::create(&dir.query_ring(), MIB, MAX_MSG).map_err(to_io)?;
    let (_query_producer, query_consumer) = query.into_split();
    // M14a: one svc_query (SPSC, 1 MiB) + one egress_service (broadcast,
    // 4 MiB) + one snapshots/<id>/ per ring id. 5 MiB per id, fallocated —
    // part of the boot reservation (instance-directory.md).
    let mut svc_query: Vec<Option<SpscProducer>> = (0..CNC_MAX_SERVICES).map(|_| None).collect();
    let mut egress_services = Vec::new();
    for id in services.ring_ids() {
        let ring = SpscRing::create(&dir.svc_query_ring_for(id), MIB, MAX_MSG).map_err(to_io)?;
        let (producer, _consumer) = ring.into_split();
        svc_query[id as usize] = Some(producer);
        egress_services.push(
            BroadcastRing::create(&dir.egress_service_for(id), 4 * MIB, MAX_MSG).map_err(to_io)?,
        );
        std::fs::create_dir_all(dir.snapshot_dir_for(id))?;
    }
    Ok((Rings { egress_services }, ingress_consumer, egress_node_producer, query_consumer, svc_query))
}
```

Call site (650): `let (rings, ingress_ring, egress_node, query_ring, svc_query) = create_rings(&instance, &cfg.services)?;`.

Boot-once fields, right after `cnc.counters().prime(durable);` (line 620):

```rust
        // M14a: the declared set and the lag policy, published ONCE, before
        // any agent runs; services and clients read them from the page.
        cnc.store_services_declared(cfg.services.declared());
        cnc.store_fsm_lag_bytes(cfg.services.page_lag_value(cfg.buffer_bytes as u64));
```

`snap_dir` (878): `let snap_dir = instance.snapshot_dir_for(0);` — **M14a ships FSM 0's artifact only** (deviation 3); the comment must say so and point at M14c. Keep `create_dir_all(&snap_dir)`.

`Consensus.svc_query: SpscProducer` → `Vec<Option<SpscProducer>>`; `forward_svc_query` gains a leading `service_id: u8` and becomes:

```rust
        let Some(producer) = self.svc_query.get_mut(service_id as usize).and_then(|p| p.as_mut()) else {
            return false; // not a ring id — M14b answers MSG_V2_BAD_SERVICE before reaching here
        };
        producer.try_write(MSG_V2_SVC_QUERY, 0, extra, &payload).is_ok()
```

Both callers (`drain_query_ring` snapshot path at 3447, `advance_pending_reads` at 3616) pass `0` for now; Task 5 replaces that with `read.service_id`.

`Consensus` needs `services: ServicesConfig` (copy of `cfg.services`) — add the field now; Task 5 uses it.

- [ ] **Step 4: Implement — service, client, fixtures**

`uc2_service/src/config.rs`: add `pub service_id: u8` to `ServiceConfig` (doc: "M14a: which declared FSM slot this process is; default 0. Refused at attach if not declared on the node's page."), initialise `service_id: 0` in `new`, add:

```rust
    pub fn service_id(mut self, id: u8) -> Self {
        self.service_id = id;
        self
    }
```

`attach.rs:60-65`: open `dir.join(format!("egress_service.{}.broadcast", cfg.service_id))` and `dir.join(format!("svc_query.{}.ring", cfg.service_id))`. `Attached` gains `service_id: u8`; `Service` gains `service_id: u8` + `pub fn service_id(&self) -> u8`; `start`/`start_with_snapshots` copy it through.

`uc2_client/src/engine.rs:58`: `pub(crate) const EGRESS_SERVICE: &str = "egress_service.0.broadcast";` with the comment "M14a: FSM 0's ring — the default responder. M14b opens every declared id's ring."

Every fixture listed in **Files** above: `"egress_service.broadcast"` → `"egress_service.0.broadcast"`, `"svc_query.ring"` → `"svc_query.0.ring"`. `learner.rs:444-446`: `let snap_dir = v_dir.join("snapshots").join("0");`. `uc_protocol/src/v2/ipc.rs` module doc lines 15–17: name the rings `svc_query.<id>.ring` / `egress_service.<id>.broadcast`.

- [ ] **Step 5: Run everything that attaches**

```bash
cargo test -p uc2_node --test services --test smoke --test learner
cargo test -p uc2_service -p uc2_client
cargo test -p uc2_gateway
cargo build --release -p uc2_gateway --example hop_bench
```

Expected: all PASS. `learner.rs::fresh_learner_joins_a_purged_leader_via_snapshot_session` passes with the fake artifact under `snapshots/0/`.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(ipc): per-id rings, snapshots/<id>/, boot-once declared set + lag on the page; FSM 0 names everywhere"
```

---

### Task 5: The progress band moves to page 2 — the service writes its slot, the node publishes the mins

After this task no service process writes page 1. The service writes `slot[service_id]` (status/applied/epoch/output_completed/snapshot_pos/heartbeat_ns), and the consensus agent publishes `min` over declared ids into the five page-1 fields once per cycle — so `maybe_persist_output_progress`, `maybe_persist_snapshot_floor`, `/readyz`'s heartbeat check, `uc2ctl status` and the unlabelled `/metrics` families keep reading one number that now means "the slowest FSM". `service_epoch` (576) is retired at 0; its two readers move to the slot. This is one task because a tree where the service writes slots and the node still reads page 1 (or vice-versa) has no working reads.

**Ordering constraint inside `do_work`** (`node.rs:2020-2249`): the mins must be published **before** step 0's `refresh_durable` (which calls `publish_validated_frontier`, `node.rs:4732`), before step 3b's `drain_ingress_ring` (the door, Task 8) and before steps 7/8 (the persisters) — i.e. as the first statement after the `halt_removed` check.

**Files:**
- Modify `uc2_node/src/services.rs` (add `ServiceMins` + `service_mins`)
- Modify `uc2_node/src/node.rs` (`Consensus` fields; `do_work` top; `PendingRead` + `drain_query_ring` + `advance_pending_reads` bracket at 3581–3609; `Node::service_applied` doc at 1340)
- Modify `uc2_node/src/obs/metrics.rs:277-282`
- Modify `uc2_service/src/attach.rs:94-106`, `apply.rs:293,362,396,404,440`, `output.rs:158,299-305,339`, `builder_agent.rs:43-49,64`, `lib.rs:214,244,269,311-378`
- Modify `uc2_node/tests/services.rs`

**Interfaces:**
- Produces (`uc2_node::services`):
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct ServiceMins { pub applied: u64, pub snapshot_pos: u64, pub output_completed: u64, pub heartbeat_ns: u64 }
  /// `None` when nothing is declared (harness node): page 1 is left alone.
  pub fn service_mins(cnc: &CncPage, services: &ServicesConfig) -> Option<ServiceMins>;
  ```
- Produces (`Consensus`, crate-private): `min_applied: u64` (`u64::MAX` when nothing is declared), refreshed by `publish_service_mins()` at the top of every cycle. `PendingRead.service_id: u8`.
- Service side: `ApplyState.service_id`, `OutputState.service_id`, `BuilderState.service_id`; `fn slot(cnc: &CncPage, id: u8) -> &ServiceSlot` helper in `attach.rs` (`cnc.service_slot(id as usize)`).

- [ ] **Step 1: Write the failing tests**

`uc2_node/src/services.rs` tests:

```rust
    #[test]
    fn service_mins_is_the_min_over_declared_ids_and_ignores_undeclared_slots() {
        let page = uc2_log::cnc::CncPage::heap(&uc2_log::cnc::CncMeta {
            node_id: 1, instance_id: 7, app_id: "t".into(), buffer_bytes: 1 << 20, max_payload: 256,
        });
        let s = ServicesConfig::from_ids(&[0, 2], None).unwrap();
        page.service_slot(0).applied.store_release(500);
        page.service_slot(0).snapshot_pos.store_release(400);
        page.service_slot(0).output_completed.store_release(300);
        page.service_slot(0).heartbeat_ns.store_release(1_000);
        page.service_slot(2).applied.store_release(200);
        page.service_slot(2).snapshot_pos.store_release(900);
        page.service_slot(2).output_completed.store_release(50);
        page.service_slot(2).heartbeat_ns.store_release(2_000);
        page.service_slot(1).applied.store_release(1); // undeclared: must not count
        let m = service_mins(&page, &s).unwrap();
        assert_eq!(m, ServiceMins { applied: 200, snapshot_pos: 400, output_completed: 50, heartbeat_ns: 1_000 });
        // A declared-but-dormant id (slot 2 zeroed) drags every min to 0 — spec §5.1, intentional.
        page.service_slot(2).applied.store_release(0);
        page.service_slot(2).snapshot_pos.store_release(0);
        assert_eq!(service_mins(&page, &s).unwrap().applied, 0);
        assert_eq!(service_mins(&page, &s).unwrap().snapshot_pos, 0);
        assert!(service_mins(&page, &ServicesConfig::none_for_tests()).is_none());
    }
```

`uc2_node/tests/services.rs` — add a state machine and the first end-to-end test:

```rust
use serde::{Deserialize, Serialize};
use uc2_client::Client;
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

#[derive(Serialize, Deserialize)]
pub enum Cmd { Add(u64) }

#[derive(Default)]
pub struct CountSm { total: u64, last: Option<u64> }
impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last = Some(position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 { self.total }
    fn last_applied(&self) -> Option<u64> { self.last }
}

pub fn start_service(dir: &Path, id: u8) -> uc2_service::Service<CountSm> {
    ServiceBuilder::new(ServiceConfig::new(dir, APP).service_id(id), CountSm::default())
        .start()
        .expect("service start")
}

#[test]
fn page_one_service_band_is_the_min_over_declared_ids() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let cnc = open_cnc(dir.path());
    let s0 = cnc.service_slot(0);
    wait_until("slot 0 attached", || {
        uc2_log::cnc::unpack_service_status(s0.status.load_acquire()) == (0, true, 1)
    });
    assert_eq!(s0.epoch.load_acquire(), 1);
    assert_eq!(cnc.service().service_epoch.load_acquire(), 0, "page-1 epoch is retired");

    let client = Client::connect(dir.path(), APP).unwrap();
    for _ in 0..20 {
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap();
    }
    let applied0 = s0.applied.load_acquire();
    assert!(applied0 > 0, "FSM 0 applied {applied0}");
    // FSM 1 is declared and absent: every page-1 aggregate is held at 0.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(cnc.service().service_applied.load_acquire(), 0);
    assert_eq!(cnc.status().service_heartbeat_ns.load_acquire(), 0);
    assert_eq!(cnc.snapshots().service_snapshot_pos.load_acquire(), 0);
    assert!(s0.heartbeat_ns.load_acquire() > 0, "the slot's own heartbeat ticks");

    client.shutdown();
    svc0.stop();
    wait_until("slot 0 detached", || !uc2_log::cnc::unpack_service_status(s0.status.load_acquire()).1);
    assert_eq!(s0.epoch.load_acquire(), 1, "detach does not bump the epoch");
    node.stop();
}
```

(`serde`, `uc2_client`, `uc2_service` are already dev-dependencies of `uc2_node` — `lin_v2` uses all three.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_node --lib services::tests::service_mins` — compile error. `cargo test -p uc2_node --test services page_one` — fails at `slot 0 attached` (the service still writes page 1).

- [ ] **Step 3: Implement — node side**

`services.rs`:

```rust
use uc2_log::cnc::CncPage;

/// The page-1 aggregates the node publishes each cycle (spec §3.2): the
/// slowest FSM's numbers. Every reader that used to read "the service" now
/// reads "the slowest service" — the purge floor, the output marker, the
/// readiness heartbeat, `uc2ctl status`, the unlabelled `/metrics` families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceMins {
    pub applied: u64,
    pub snapshot_pos: u64,
    pub output_completed: u64,
    pub heartbeat_ns: u64,
}

/// N acquire loads, no stores. `None` for a `none_for_tests` node.
pub fn service_mins(cnc: &CncPage, services: &ServicesConfig) -> Option<ServiceMins> {
    let mut m = ServiceMins {
        applied: u64::MAX,
        snapshot_pos: u64::MAX,
        output_completed: u64::MAX,
        heartbeat_ns: u64::MAX,
    };
    let mut any = false;
    for id in services.ids() {
        let s = cnc.service_slot(id as usize);
        m.applied = m.applied.min(s.applied.load_acquire());
        m.snapshot_pos = m.snapshot_pos.min(s.snapshot_pos.load_acquire());
        m.output_completed = m.output_completed.min(s.output_completed.load_acquire());
        m.heartbeat_ns = m.heartbeat_ns.min(s.heartbeat_ns.load_acquire());
        any = true;
    }
    any.then_some(m)
}
```

`node.rs` — `Consensus` gains `services: ServicesConfig` (Task 4) and `min_applied: u64` (init `u64::MAX`). New method, placed next to `publish_peer_band`:

```rust
    /// M14a (spec §3.2/§5.1): publish `min` over the declared FSMs' slots into
    /// page 1's singular service fields, once per cycle, store-on-change.
    /// Runs FIRST in `do_work`: `refresh_durable` (the report ceiling), the
    /// ingress door and the two persisters all read this cycle's value.
    fn publish_service_mins(&mut self) {
        let Some(m) = crate::services::service_mins(&self.cnc, &self.services) else {
            self.min_applied = u64::MAX; // nothing declared: no FSM pacing
            return;
        };
        self.min_applied = m.applied;
        let sp = self.cnc.service();
        if sp.service_applied.load_acquire() != m.applied {
            sp.service_applied.store_release(m.applied);
        }
        if sp.output_completed.load_acquire() != m.output_completed {
            sp.output_completed.store_release(m.output_completed);
        }
        let sn = self.cnc.snapshots();
        if sn.service_snapshot_pos.load_acquire() != m.snapshot_pos {
            sn.service_snapshot_pos.store_release(m.snapshot_pos);
        }
        let st = self.cnc.status();
        if st.service_heartbeat_ns.load_acquire() != m.heartbeat_ns {
            st.service_heartbeat_ns.store_release(m.heartbeat_ns);
        }
    }
```

In `do_work`, immediately after the `halt_removed` early return:

```rust
        // M14a: the FSM aggregates first — everything below reads them.
        self.publish_service_mins();
```

`PendingRead` gains `service_id: u8` (doc: "which FSM's slot certifies this read; M14a always 0, M14b takes it from the query record"). `drain_query_ring` sets `service_id: 0` and passes `0` to `forward_svc_query`. The ready bracket in `advance_pending_reads` becomes:

```rust
                let ready = {
                    let slot = self.cnc.service_slot(self.pending_reads[i].service_id as usize);
                    let e = slot.epoch.load_acquire();
                    let applied = slot.applied.load_acquire();
                    // Same sentinel + capture-recheck bracket as before, now on
                    // THIS FSM's slot: `e >= 1` (an unattached slot is never
                    // ready), applied through the read index, epoch unchanged.
                    if e >= 1 && applied >= commit_at && slot.epoch.load_acquire() == e {
                        Some(e)
                    } else {
                        None
                    }
                };
```

(keep the long M5 comment above it, reworded from "service_epoch" to "the slot's epoch"), and the forward call passes `self.pending_reads[i].service_id`.

`Node::service_applied` (1340–1344): doc becomes "the slowest declared FSM's applied position (page-1 aggregate)". `metrics.rs:277-282`: `s.cnc.service_slot(0).epoch.load_acquire()` with the help string "FSM 0's incarnation counter, bumped each attach (per-FSM families: M14c)."

- [ ] **Step 4: Implement — service side**

`attach.rs`: add `pub(crate) fn slot(cnc: &CncPage, id: u8) -> &uc2_log::cnc::ServiceSlot { cnc.service_slot(id as usize) }`. Replace steps 4–5 (lines 94–106):

```rust
    let start_pos = last_applied.unwrap_or(0);
    let s = slot(&cnc, cfg.service_id);
    s.applied.store_release(start_pos);
    // Status: attached, incarnation += 1 (the prior life's value survives a
    // crash on the same page; a node restart zeroes it with the page).
    let (_, _, incarnation) = unpack_service_status(s.status.load_acquire());
    s.status.store_release(pack_service_status(cfg.service_id, true, incarnation.wrapping_add(1)));
    // 5. Bump the epoch AFTER applied, AcqRel — the discipline the node's
    //    capture-recheck bracket relies on (unchanged, now per slot).
    let epoch = s.epoch.fetch_add(1) + 1;
```

`ApplyState` gains `pub(crate) service_id: u8` (set from `cfg.service_id`). `apply.rs`: lines 293 and 404 → `slot(&st.cnc, st.service_id).heartbeat_ns.store_release(unix_ns())`; 362 and 396 → `slot(&st.cnc, st.service_id).applied.store_release(...)`; 440 (`maybe_build_snapshot`) → the slot's `applied`.

`output.rs`: `OutputState` gains `service_id: u8`; 158 and 339 read `slot(cnc, id).applied` (own progress, never the min); `store_output_completed(cnc, id, frame_end)` uses `slot(cnc, id).output_completed`. Line 152 (`status().output_progress`, the node's persisted mirror) is unchanged — deviation 2.

`builder_agent.rs`: `BuilderState` gains `service_id: u8`; line 64 → `slot(&st.cnc, st.service_id).snapshot_pos.store_release(pos)`; the four unit tests there build `BuilderState` — add `service_id: 0`.

`lib.rs`: 244 → `slot(&cnc, cfg.service_id).snapshot_pos.load_acquire()`; 269 → `service_id` into `BuilderState`; `OutputState::new(…)` gains the id. `Service::stop` (365): before joining the agents, clear the attached bit:

```rust
        let s = attach::slot(&self._cnc, self.service_id);
        let (_, _, inc) = uc2_log::cnc::unpack_service_status(s.status.load_acquire());
        s.status.store_release(uc2_log::cnc::pack_service_status(self.service_id, false, inc));
```

`crash()` leaves it set (a crash is indistinguishable from a kill; the heartbeat ages instead — spec §8).

Verify no page-1 service write survives: `grep -n "cnc.service()\|service_heartbeat_ns\|service_snapshot_pos" uc2_service/src/` must return **zero** hits.

- [ ] **Step 5: Run**

```bash
cargo test -p uc2_node --lib services
cargo test -p uc2_node --test services
cargo test -p uc2_service
cargo test -p uc2_node --test query_barrier --test smoke --test learner --test purge_safety
```

Expected: PASS. `purge_safety`/`learner` are `none_for_tests` nodes: the node does not touch page 1, so their direct pokes at `service_snapshot_pos` still drive the purge floor.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(cnc): service progress on page-2 slots; node publishes min aggregates to page 1; reads certify on the slot"
```

---

### Task 6: Attach refusals, the per-id lock, per-id snapshot store, example flags — two FSMs apply one log

**Files:**
- Modify `uc2_service/Cargo.toml` (add `fs2 = { workspace = true }` under `[dependencies]`)
- Modify `uc2_service/src/config.rs` (`ServiceError` variants)
- Modify `uc2_service/src/attach.rs` (steps 1–3), `uc2_service/src/lib.rs` (`Service._lock`, `SnapshotStore::open` call at 238)
- Modify `uc2_service/src/snapshots.rs` (`open`, its tests at 168–253), `uc2_service/tests/snapshot_build.rs:84`
- Modify `examples/counter/src/bin/counter-service.rs`, `examples/uc2-crashtest/src/bin/uc2-crashtest-service.rs`
- Modify `uc2_node/tests/services.rs`

**Interfaces:**
- `ServiceError::ServiceNotDeclared { id: u8, declared: u64 }` (Display: `service id {id} is not declared on this node (declared set 0b{declared:b}); fix [services] ids or --service-id`), `ServiceError::AlreadyAttached { id: u8 }` (Display: `another process already holds service id {id} on this instance dir (service.{id}.lock)`).
- `SnapshotStore::open(instance_dir: &Path, service_id: u8) -> io::Result<SnapshotStore>` → `snapshots/<id>/`.
- `counter-service --service-id <u8>` (default 0); `uc2-crashtest-service --service-id <u8>` (default 0).

- [ ] **Step 1: Write the failing tests**

`uc2_node/tests/services.rs`:

```rust
#[test]
fn an_undeclared_id_is_refused_by_name_and_a_second_attach_on_the_same_id_is_refused() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(2), CountSm::default())
        .start()
        .err()
        .expect("id 2 is not declared");
    assert!(matches!(err, uc2_service::ServiceError::ServiceNotDeclared { id: 2, declared: 0b11 }), "{err:?}");
    assert!(err.to_string().contains("service id 2 is not declared"), "{err}");

    let svc1 = start_service(dir.path(), 1);
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(1), CountSm::default())
        .start()
        .err()
        .expect("id 1 is held");
    assert!(matches!(err, uc2_service::ServiceError::AlreadyAttached { id: 1 }), "{err:?}");
    svc1.stop();
    // The lock is released with the process's handle: a re-attach succeeds.
    let svc1b = start_service(dir.path(), 1);
    assert_eq!(svc1b.epoch(), 2);
    svc1b.stop();
    node.stop();
}

#[test]
fn two_fsms_apply_the_same_log_and_fsm_zero_answers_the_client() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = start_service(dir.path(), 1);
    let client = Client::connect(dir.path(), APP).unwrap();
    let mut last = 0;
    for _ in 0..100 {
        last = client.submit(&Cmd::Add(1)).unwrap();
    }
    assert_eq!(last, 100, "FSM 0's answers reach the client in order");
    let cnc = open_cnc(dir.path());
    wait_until("FSM 1 caught up", || {
        cnc.service_slot(1).applied.load_acquire() == cnc.service_slot(0).applied.load_acquire()
    });
    assert_eq!(cnc.service().service_applied.load_acquire(), cnc.service_slot(0).applied.load_acquire());
    assert_eq!(svc0.query(()), 100);
    assert_eq!(svc1.query(()), 100, "same log, same deterministic SM ⇒ same state");
    assert!(dir.path().join("snapshots").join("1").is_dir());
    client.shutdown();
    svc0.stop();
    svc1.stop();
    node.stop();
}
```

`uc2_service/src/snapshots.rs` `open_creates_the_directory_and_is_idempotent`: assert `dir.path().join("snapshots").join("3").is_dir()` after `SnapshotStore::open(dir.path(), 3)`.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_node --test services an_undeclared` — fails: the attach succeeds (no check yet). `cargo test -p uc2_service --lib snapshots` — compile error (arity).

- [ ] **Step 3: Implement**

`config.rs` — add to `ServiceError`:

```rust
    /// M14a: `service_id` is not in the node's declared set (cnc 4032).
    #[error(
        "service id {id} is not declared on this node (declared set 0b{declared:b}); \
         fix [services] ids on the node or --service-id on the service"
    )]
    ServiceNotDeclared { id: u8, declared: u64 },
    /// M14a: another live process holds `service.<id>.lock`.
    #[error("another process already holds service id {id} on this instance dir (service.{id}.lock)")]
    AlreadyAttached { id: u8 },
```

`attach.rs`, after step 1 (`CncPage::open_file`), before opening the log buffer:

```rust
    // 1b. M14a: the declared-set gate. `0` on the page is a harness node
    // (`ServicesConfig::none_for_tests`), which rings FSM 0 only.
    let declared = match cnc.services_declared() {
        0 => 1,
        d => d,
    };
    if declared & (1u64 << cfg.service_id) == 0 || cfg.service_id as usize >= CNC_MAX_SERVICES {
        return Err(ServiceError::ServiceNotDeclared { id: cfg.service_id, declared });
    }
    // 1c. M14a: one process per id. Exclusive flock, held for the service's
    // life (the OS releases it on any exit), mirroring the node's
    // `instance.lock`.
    let lock_path = dir.join(format!("service.{}.lock", cfg.service_id));
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    fs2::FileExt::try_lock_exclusive(&lock)
        .map_err(|_| ServiceError::AlreadyAttached { id: cfg.service_id })?;
```

`Attached` and `Service` gain `_lock: std::fs::File` (kept for the lifetime; dropped last). `snapshots.rs`:

```rust
    /// Open (creating if absent) `snapshots/<service_id>/` under `instance_dir`.
    pub fn open(instance_dir: &Path, service_id: u8) -> io::Result<SnapshotStore> {
        let dir = instance_dir.join(DIR_NAME).join(service_id.to_string());
        std::fs::create_dir_all(&dir)?;
        Ok(SnapshotStore { dir })
    }
```

Fix the unit-test call sites (168, 185, 208, 217, 235, 249, 253 — pass `0`), `lib.rs:238` (`SnapshotStore::open(&cfg.instance_dir, cfg.service_id)?`), `tests/snapshot_build.rs:84` (`open(dir.path(), 0)`).

`counter-service.rs` `Args`: `/// Which declared FSM slot this process is (see [services] ids). #[arg(long, default_value_t = 0)] service_id: u8,` and `ServiceConfig::new(...).service_id(args.service_id)`; print `service {} attached at {}`. Same in `uc2-crashtest-service.rs`.

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_service
cargo test -p uc2_node --test services
cargo test -p uc2-crashtest --test smoke
cargo test -p counter
```

Expected: PASS (`examples/counter/tests/{lifecycle,quickstart_local}.rs` attach as id 0 by default).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(service): service_id attach — declared-set refusal, service.<id>.lock, snapshots/<id>/; --service-id on the example bins"
```

---

### Task 7: The lag barrier — a target cap on `next_batch`

Deviation 1 in full. `uc2_service/src/lag.rs` holds the pure plan; `apply_cycle` computes it at the top of every `loop` iteration (N acquire loads — `floor` may advance mid-cycle). Lockstep applies **one** frame per `next_batch` when `cursor == floor`; bounded caps the target at `floor + fsm_lag`. Journal replay (`replay_into`) is untouched — a replaying FSM is by definition the one holding the floor down. The heartbeat store at the end of the cycle runs on a waiting cycle too, so a waiting FSM is never mistaken for a dead one.

**Files:**
- Create `uc2_service/src/lag.rs`; `mod lag;` in `lib.rs`
- Modify `uc2_service/src/attach.rs` (read `fsm_lag_bytes` + declared set at attach), `apply.rs` (`ApplyState.{lag_mode, declared, lag_waiting}`; the loop at 296–372)
- Modify `uc2_node/tests/services.rs`

**Interfaces:**
```rust
// uc2_service::lag (crate-private)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LagMode { Off, Lockstep, Bounded(u64) }   // Off ⇔ declared set empty on the page (harness node)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plan { Wait, Apply { target: u64, one_frame: bool } }
pub(crate) fn plan(mode: LagMode, floor: u64, cursor: u64, commit: u64, durable: u64) -> Plan;
pub(crate) fn mode_from_page(declared: u64, fsm_lag_bytes: u64) -> LagMode;
pub(crate) fn floor(cnc: &CncPage, declared: u64) -> u64;   // min slot.applied over declared bits
```

- [ ] **Step 1: Write the failing tests**

`uc2_service/src/lag.rs` (tests only, first):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: u64 = 10_000; // commit == durable == 10_000 unless stated

    #[test]
    fn off_is_todays_behaviour() {
        assert_eq!(plan(LagMode::Off, 0, 1_000, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
        assert_eq!(plan(LagMode::Off, 0, HEAD, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
    }

    #[test]
    fn head_is_min_commit_durable_in_every_mode() {
        assert_eq!(plan(LagMode::Off, 0, 0, 5_000, 7_000), Plan::Apply { target: 5_000, one_frame: false });
        assert_eq!(plan(LagMode::Bounded(1 << 20), 0, 0, 7_000, 5_000), Plan::Apply { target: 5_000, one_frame: false });
    }

    #[test]
    fn bounded_caps_the_target_at_floor_plus_lag() {
        // I am ahead of the floor by 1000 with a 4096 bound: 3096 more bytes may apply.
        assert_eq!(plan(LagMode::Bounded(4096), 2_000, 3_000, HEAD, HEAD), Plan::Apply { target: 6_096, one_frame: false });
        // Cap above head: head wins.
        assert_eq!(plan(LagMode::Bounded(1 << 20), 2_000, 3_000, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
        // Exactly at the bound: nothing can fit → wait.
        assert_eq!(plan(LagMode::Bounded(4096), 2_000, 6_096, HEAD, HEAD), Plan::Wait);
        // I AM the floor: the bound is measured from me.
        assert_eq!(plan(LagMode::Bounded(4096), 3_000, 3_000, HEAD, HEAD), Plan::Apply { target: 7_096, one_frame: false });
        // Caught up with head: never Wait, always the plain CaughtUp path.
        assert_eq!(plan(LagMode::Bounded(4096), 2_000, HEAD, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
    }

    #[test]
    fn lockstep_applies_one_frame_only_at_the_floor() {
        assert_eq!(plan(LagMode::Lockstep, 3_000, 3_000, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: true });
        assert_eq!(plan(LagMode::Lockstep, 3_000, 3_128, HEAD, HEAD), Plan::Wait);
        assert_eq!(plan(LagMode::Lockstep, 3_000, HEAD, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
    }

    #[test]
    fn mode_from_page_table() {
        assert_eq!(mode_from_page(0, 0), LagMode::Off);
        assert_eq!(mode_from_page(0, 4096), LagMode::Off);
        assert_eq!(mode_from_page(0b1, 0), LagMode::Lockstep);
        assert_eq!(mode_from_page(0b11, 65_536), LagMode::Bounded(65_536));
    }

    #[test]
    fn floor_is_the_min_over_declared_slots() {
        let page = uc2_log::cnc::CncPage::heap(&uc2_log::cnc::CncMeta {
            node_id: 1, instance_id: 7, app_id: "t".into(), buffer_bytes: 1 << 20, max_payload: 256,
        });
        page.service_slot(0).applied.store_release(900);
        page.service_slot(1).applied.store_release(300);
        page.service_slot(2).applied.store_release(100); // undeclared
        assert_eq!(floor(&page, 0b11), 300);
        assert_eq!(floor(&page, 0b1), 900);
    }
}
```

`uc2_node/tests/services.rs` — a slow SM and the two bound tests:

```rust
/// FSM 1's stand-in: 1 ms per apply, so FSM 0 would run ~1000 frames ahead
/// per second without the barrier.
#[derive(Default)]
pub struct SlowCountSm(CountSm);
impl StateMachine for SlowCountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        std::thread::sleep(Duration::from_millis(1));
        self.0.apply(position, cmd)
    }
    fn query(&self, q: ()) -> u64 { self.0.query(q) }
    fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
}

/// Drive `n` submits through the pipelined client while a sampler thread
/// records the largest `applied_0 - applied_1` it sees (applied_0 read FIRST,
/// so a racing sample can only under-read the gap). Returns `(max_gap, total)`.
fn drive_and_sample_gap(dir: &Path, n: u64) -> (u64, u64) {
    use uc2_client::{PipelinedClient, PipelinedConfig};
    let cnc = open_cnc(dir);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let cnc = std::sync::Arc::clone(&cnc);
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut max_gap = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let a0 = cnc.service_slot(0).applied.load_acquire();
                let a1 = cnc.service_slot(1).applied.load_acquire();
                max_gap = max_gap.max(a0.saturating_sub(a1));
                std::thread::sleep(Duration::from_micros(200));
            }
            max_gap
        })
    };
    // A long deadline: under lockstep every ticket waits behind the slow FSM.
    let client = PipelinedClient::connect(
        dir,
        APP,
        PipelinedConfig { request_timeout: Duration::from_secs(30), ..PipelinedConfig::default() },
    )
    .unwrap();
    let mut tickets = Vec::with_capacity(n as usize);
    for _ in 0..n {
        tickets.push(client.submit::<Cmd, u64>(&Cmd::Add(1)).unwrap());
    }
    let mut total = 0;
    for t in tickets {
        total = t.wait().unwrap();
    }
    wait_until("FSM 1 caught up", || {
        cnc.service_slot(1).applied.load_acquire() == cnc.service_slot(0).applied.load_acquire()
    });
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    client.shutdown();
    (sampler.join().unwrap(), total)
}

#[test]
fn bounded_lag_holds_between_a_fast_and_a_slow_fsm() {
    let _g = serialize();
    let dir = tempdir();
    const BOUND: u64 = 64 << 10;
    let node = Node::start(config(dir.path(), ids(&[0, 1], Some(FsmLag::Bounded(BOUND))))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(1), SlowCountSm::default())
        .start()
        .unwrap();
    // 3000 frames × 128 B = 384 KiB of log — six times the bound.
    let (max_gap, total) = drive_and_sample_gap(dir.path(), 3000);
    assert_eq!(total, 3000);
    assert!(max_gap <= BOUND, "applied_0 - applied_1 reached {max_gap} > bound {BOUND}");
    assert!(max_gap > BOUND / 2, "vacuity: the fast FSM never approached the bound (max gap {max_gap})");
    let cnc = open_cnc(dir.path());
    assert!(cnc.service_slot(0).lag_waits.load_acquire() > 0, "FSM 0 must have waited at least once");
    assert_eq!(cnc.service_slot(1).lag_waits.load_acquire(), 0, "the slow FSM never waits");
    svc0.stop();
    svc1.stop();
    node.stop();
}

#[test]
fn lockstep_holds_the_fsms_within_one_frame() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], Some(FsmLag::Lockstep)))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(1), SlowCountSm::default())
        .start()
        .unwrap();
    let (max_gap, total) = drive_and_sample_gap(dir.path(), 500);
    assert_eq!(total, 500);
    // One frame: header 32 + payload (≤ max_payload 256), 32-byte aligned.
    let one_frame = uc_protocol::v2::frame::align_frame_len(32 + 256) as u64;
    assert!(max_gap <= one_frame, "lockstep gap {max_gap} > one frame {one_frame}");
    assert!(max_gap > 0, "vacuity: no gap ever observed");
    svc0.stop();
    svc1.stop();
    node.stop();
}
```

- [ ] **Step 2: Run to verify they fail**

`cargo test -p uc2_service --lib lag` — compile error. `cargo test -p uc2_node --test services bounded_lag` — fails on `max_gap <= BOUND` (no barrier: FSM 0 runs the whole 384 KiB ahead).

- [ ] **Step 3: Implement**

`uc2_service/src/lag.rs`:

```rust
//! M14a: the FSM lag barrier as a TARGET CAP (spec §4.2, plan deviation 1).
//! `LogFollower::next_batch(target)` yields only frames whose END is
//! `<= target`, so "frame [p, p+len) may apply iff p + len - floor <= lag"
//! is exactly `target = min(head, floor + lag)`, and lockstep ("floor >= p")
//! is "apply one frame, only while cursor == floor". A capped batch simply
//! reads as `CaughtUp`; the agent's idle strategy is the wait.

use uc2_log::cnc::CncPage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LagMode {
    /// The page declares no FSMs (a harness node): today's behaviour.
    Off,
    Lockstep,
    Bounded(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plan {
    /// No frame may apply this cycle; count a wait episode and idle.
    Wait,
    /// Call `next_batch(target)`; if `one_frame`, stop after the first frame.
    Apply { target: u64, one_frame: bool },
}

pub(crate) fn mode_from_page(declared: u64, fsm_lag_bytes: u64) -> LagMode {
    match (declared, fsm_lag_bytes) {
        (0, _) => LagMode::Off,
        (_, 0) => LagMode::Lockstep,
        (_, b) => LagMode::Bounded(b),
    }
}

/// `min(slot.applied)` over the declared bits — N acquire loads.
pub(crate) fn floor(cnc: &CncPage, declared: u64) -> u64 {
    let mut f = u64::MAX;
    for id in 0..uc_protocol::v2::cnc::CNC_MAX_SERVICES {
        if declared & (1 << id) != 0 {
            f = f.min(cnc.service_slot(id).applied.load_acquire());
        }
    }
    f
}

pub(crate) fn plan(mode: LagMode, floor: u64, cursor: u64, commit: u64, durable: u64) -> Plan {
    let head = commit.min(durable);
    if cursor >= head {
        // Nothing new anyway — never report a wait for the log's own idleness.
        return Plan::Apply { target: head, one_frame: false };
    }
    match mode {
        LagMode::Off => Plan::Apply { target: head, one_frame: false },
        LagMode::Bounded(lag) => {
            let cap = floor.saturating_add(lag);
            if cap <= cursor {
                Plan::Wait
            } else {
                Plan::Apply { target: head.min(cap), one_frame: false }
            }
        }
        LagMode::Lockstep => {
            if cursor > floor {
                Plan::Wait
            } else {
                Plan::Apply { target: head, one_frame: true }
            }
        }
    }
}
```

`attach.rs`: after the declared-set gate, `let lag_mode = lag::mode_from_page(cnc.services_declared(), cnc.fsm_lag_bytes());` and the effective `declared` (already computed) go into `ApplyState { lag_mode, declared, lag_waiting: false, .. }`.

`apply.rs`, replace `let target = c.commit.load_acquire().min(durable);` and the loop head with:

```rust
    let commit = c.commit.load_acquire();
    let mut progressed = false;
    loop {
        // M14a: the lag barrier — re-planned every iteration so a floor that
        // moved mid-cycle is honoured (`floor` only increases; a stale sample
        // is conservative).
        let floor = crate::lag::floor(&st.cnc, st.declared);
        let (target, one_frame) =
            match crate::lag::plan(st.lag_mode, floor, st.follower.cursor, commit, durable) {
                crate::lag::Plan::Wait => {
                    if !st.lag_waiting {
                        st.lag_waiting = true;
                        slot(&st.cnc, st.service_id).lag_waits.fetch_add(1);
                    }
                    break;
                }
                crate::lag::Plan::Apply { target, one_frame } => (target, one_frame),
            };
        st.lag_waiting = false;
        let is_leader = st.cnc.status().flags.load_acquire() & NODE_FLAG_LEADER != 0;
        let cursor_before = st.follower.cursor;
        let overrun = match st.follower.next_batch(target) {
```

and inside the `Batch::Frames(frames)` arm restructure the per-frame body so `one_frame` stops after the first yielded frame:

```rust
                for (pos, hdr, payload) in frames {
                    if hdr.frame_type == FRAME_TYPE_MESSAGE && Some(pos) > sm.last_applied() {
                        st.resp_buf.clear();
                        sm.apply(pos, payload, &mut st.resp_buf);
                        if is_leader {
                            st.egress.publish(hdr.session_id, hdr.correlation_id, pos, &st.resp_buf);
                        }
                    }
                    if one_frame {
                        break; // lockstep: exactly one frame past the floor
                    }
                }
```

(The `#[cfg(feature = "apply-profile")]` counters stay where they are inside the `if`.) Everything after the loop — the heartbeat store at the old line 404, `drain_queries`, `maybe_build_snapshot` — is unchanged, so a waiting FSM keeps heart-beating and keeps answering queries.

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_service
cargo test -p uc2_node --test services
cargo test -p uc2_node --test lin_v2 smoke_3node_write_then_read
```

Expected: PASS. Note the `apply.rs` egress-layout test (`uc2_service/tests/apply.rs:182`) still passes — the barrier does not touch the record format.

- [ ] **Step 5: Verify the test discriminates**

Temporarily change `plan`'s `LagMode::Bounded` arm to `Plan::Apply { target: head, one_frame: false }` and run `cargo test -p uc2_node --test services bounded_lag` — expected: FAIL on `max_gap <= BOUND`. Revert. Record the observed `max_gap` in the commit message.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(service): FSM lag barrier as a next_batch target cap — bounded + lockstep, lag_waits (unbarriered gap measured <N> B vs bound 65536)"
```

---

### Task 8: The admission door's FSM term and the quorum-gated report ceiling (Q)

Both are one extra `min` on values the node already computes. The door reuses `admission_open` unchanged — `append − min_applied ≤ fsm_lag_eff` has the same shape as `append − commit ≤ admission_bytes` — so its signature and its existing unit test stay. Q clamps what `publish_validated_frontier` stores; the receiver (`uc2_net/src/receiver.rs:1730-1772`) reads the same two atomics and is untouched. `ElectionSm::term_at(pos)` returns the term of the byte **below** `pos` (`uc2_consensus/src/election.rs:983-1000`) — `validated_term()` is literally `term_at(validated_up_to)` — so `term_at(ceiling)` is the spec's "attest the byte below the report".

**Files:**
- Modify `uc2_node/src/services.rs` (`fsm_lag_eff`, `report_ceiling`)
- Modify `uc2_node/src/node.rs` (`Node.fsm_door: Option<u64>`, `Consensus.fsm_lag_eff: u64`; `Node::submit` at 1453–1467; `drain_ingress_ring` at 3214–3221; `publish_validated_frontier` at 4736–4746)
- Modify `uc2_node/tests/services.rs`

**Interfaces:**
```rust
// uc2_node::services
pub fn fsm_lag_eff(services: &ServicesConfig, buffer_bytes: u64, max_payload: usize) -> Option<u64>;
//   None ⇔ nothing declared (no FSM term); Lockstep ⇒ align_frame_len(HEADER_LEN + max_payload); Bounded(b) ⇒ b
pub fn report_ceiling(validated_up_to: u64, min_applied: u64, fsm_lag_eff: Option<u64>) -> u64;
```

- [ ] **Step 1: Write the failing unit tests** (`services.rs`)

```rust
    #[test]
    fn fsm_lag_eff_table() {
        let b = 4u64 << 20;
        assert_eq!(fsm_lag_eff(&ServicesConfig::none_for_tests(), b, 256), None);
        assert_eq!(fsm_lag_eff(&ServicesConfig::default(), b, 256), Some(1 << 20));
        assert_eq!(fsm_lag_eff(&ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded(4096))).unwrap(), b, 256), Some(4096));
        // Lockstep: one max-size frame — header 32 + 256 payload, 32-aligned = 288.
        assert_eq!(fsm_lag_eff(&ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap(), b, 256), Some(288));
        assert_eq!(fsm_lag_eff(&ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap(), b, 1), Some(64));
    }

    #[test]
    fn report_ceiling_never_exceeds_validated_and_is_inert_without_fsms() {
        assert_eq!(report_ceiling(10_000, 2_000, Some(4_096)), 6_096);
        assert_eq!(report_ceiling(10_000, 9_000, Some(4_096)), 10_000);
        assert_eq!(report_ceiling(10_000, 0, Some(4_096)), 4_096, "absent FSMs cap the report at the bound");
        assert_eq!(report_ceiling(10_000, u64::MAX, Some(4_096)), 10_000, "saturating add");
        assert_eq!(report_ceiling(10_000, 0, None), 10_000);
    }
```

And the two integration tests in `uc2_node/tests/services.rs`:

```rust
#[test]
fn the_leader_door_closes_at_the_bound_while_a_declared_fsm_is_absent() {
    use uc2_client::{ClientError, PipelinedClient, PipelinedConfig};
    let _g = serialize();
    let dir = tempdir();
    const BOUND: u64 = 64 << 10;
    let node = Node::start(config(dir.path(), ids(&[0, 1], Some(FsmLag::Bounded(BOUND))))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let client = PipelinedClient::connect(
        dir.path(),
        APP,
        PipelinedConfig { request_timeout: Duration::from_millis(500), ..PipelinedConfig::default() },
    )
    .unwrap();
    // Fire until the door refuses. `submit` retries backpressure for its
    // 1 s grace then fails as BackpressureFull; a timeout is the same story
    // seen from a ticket that got in just before the door shut.
    let mut refused = false;
    let mut tickets = Vec::new();
    for _ in 0..4000 {
        match client.submit::<Cmd, u64>(&Cmd::Add(1)) {
            Ok(t) => tickets.push(t),
            Err(ClientError::BackpressureFull) => {
                refused = true;
                break;
            }
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
    assert!(refused, "4000 × 128 B = 512 KiB must not all get through a 64 KiB door");
    let cnc = open_cnc(dir.path());
    let one_frame = uc_protocol::v2::frame::align_frame_len(32 + 256) as u64;
    let append = cnc.counters().append.load_acquire();
    assert!(append <= BOUND + one_frame, "append {append} ran past the door ({BOUND} + one frame)");
    let _ = tickets; // drop: whatever timed out, timed out
    // Attaching the missing FSM re-opens the door: both catch up, writes flow.
    let svc1 = start_service(dir.path(), 1);
    wait_until("door reopens", || {
        client.submit::<Cmd, u64>(&Cmd::Add(1)).and_then(|t| t.wait()).is_ok()
    });
    wait_until("both past the bound", || cnc.service_slot(1).applied.load_acquire() > BOUND);
    client.shutdown();
    svc0.stop();
    svc1.stop();
    node.stop();
}

/// Three in-process nodes. The LEADER declares nothing (door inert), the two
/// FOLLOWERS declare {0,1} with a 64 KiB bound and have no FSM attached, so
/// their reports are capped at 64 KiB and commit stalls there although the
/// leader appends freely. Attaching both FSMs on both followers releases it.
#[test]
fn q_a_follower_quorum_with_absent_fsms_stalls_commit_at_the_bound() {
    let _g = serialize();
    let root = tempdir();
    const BOUND: u64 = 64 << 10;
    let socks: Vec<std::net::UdpSocket> =
        (0..3).map(|_| std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).collect();
    let members: Vec<(uc2_consensus::election::NodeId, std::net::SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as u32, s.local_addr().unwrap())).collect();
    fn node_cfg(dir: &Path, i: usize, members: &[(u32, std::net::SocketAddr)], services: ServicesConfig) -> NodeConfig {
        let mut cfg = config(dir, services);
        cfg.id = i as u32;
        cfg.bind = members[i].1;
        cfg.members = members.to_vec();
        cfg.seed = 1 + i as u64;
        cfg
    }
    // `Node::crash(self)` consumes, so the slots are `Option`s.
    let mut nodes: Vec<Option<Node>> = Vec::new();
    let mut dirs = Vec::new();
    for (i, sock) in socks.into_iter().enumerate() {
        let dir = root.path().join(format!("n{i}"));
        let cfg = node_cfg(&dir, i, &members, ServicesConfig::none_for_tests());
        nodes.push(Some(Node::start_with_socket(cfg, sock).unwrap()));
        dirs.push(dir);
    }
    let serving = |n: &Option<Node>| n.as_ref().is_some_and(|n| n.is_leader() && n.can_serve());
    let mut leader = 0;
    wait_until("single serving leader", || {
        let ls: Vec<usize> = (0..3).filter(|&i| serving(&nodes[i])).collect();
        if ls.len() == 1 { leader = ls[0]; }
        ls.len() == 1
    });
    // Restart the two followers with the declared set (config is per node;
    // crash-then-rebind exactly as lincheck_v2::kill_and_restart_leader).
    for i in (0..3).filter(|&i| i != leader) {
        nodes[i].take().unwrap().crash();
        let sock = std::net::UdpSocket::bind(members[i].1).unwrap();
        let cfg = node_cfg(&dirs[i], i, &members, ids(&[0, 1], Some(FsmLag::Bounded(BOUND))));
        nodes[i] = Some(Node::start_with_socket(cfg, sock).unwrap());
    }
    wait_until("leader serving again", || serving(&nodes[leader]));
    let leader_node = nodes[leader].as_ref().unwrap();
    // 2000 × 64 B payloads ≈ 200 KiB of frames through the leader's own door
    // (256 KiB admission window, no FSM term).
    let payload = vec![0x42u8; 64];
    let mut sent = 0;
    while sent < 2000 {
        match leader_node.submit(payload.clone()) {
            Ok(()) => sent += 1,
            Err(_) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    std::thread::sleep(Duration::from_secs(2));
    let one_frame = uc_protocol::v2::frame::align_frame_len(32 + 256) as u64;
    let c = leader_node.counters();
    let (append, commit) = (c.append.load_acquire(), c.commit.load_acquire());
    assert!(append > 2 * BOUND, "vacuity: the leader appended only {append}");
    assert!(commit <= BOUND + one_frame, "commit {commit} ran past the followers' capped reports ({BOUND})");
    let lcnc = open_cnc(&dirs[leader]);
    for i in 0..8 {
        let s = lcnc.peer_slot(i);
        if s.id_and_role.load_acquire() == 0 { continue; }
        let rd = s.reported_durable.load_acquire();
        assert!(rd <= BOUND + one_frame, "peer slot {i} reported {rd} > cap");
    }
    // Release: attach both FSMs on both followers; each applies to commit,
    // min_applied rises, the ceiling rises, commit follows — to the end.
    let mut services = Vec::new();
    for i in (0..3).filter(|&i| i != leader) {
        services.push(start_service(&dirs[i], 0));
        services.push(start_service(&dirs[i], 1));
    }
    wait_until("commit reaches append", || {
        let c = leader_node.counters();
        c.commit.load_acquire() == c.append.load_acquire()
    });
    for s in services { s.stop(); }
    for n in nodes.into_iter().flatten() { n.stop(); }
}
```

The leader keeps `none_for_tests` for the whole test; only the two followers are restarted with the declared set (`Node::crash(self)` + rebind is the `lincheck_v2::kill_and_restart_leader` recipe, mod.rs:630-672).

- [ ] **Step 2: Run to verify they fail**

`cargo test -p uc2_node --lib services` — compile error. `cargo test -p uc2_node --test services the_leader_door` — fails: `refused` is false (all 4000 get through). `q_a_follower_quorum` — fails on `commit <= BOUND + one_frame`.

- [ ] **Step 3: Implement**

`services.rs`:

```rust
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};

/// The door/ceiling term (spec §5.2): the byte bound, or one max-size frame
/// under lockstep ("at most one frame past the FSMs"). `None` ⇔ nothing
/// declared ⇔ no FSM term at all.
pub fn fsm_lag_eff(services: &ServicesConfig, buffer_bytes: u64, max_payload: usize) -> Option<u64> {
    if services.declared() == 0 {
        return None;
    }
    Some(match services.resolve_lag(buffer_bytes) {
        FsmLag::Lockstep => align_frame_len(HEADER_LEN + max_payload) as u64,
        FsmLag::Bounded(b) => b,
    })
}

/// Q (spec §5.3): what this node attests toward the leader's commit ranking
/// — never more than it has validated, never more than `fsm_lag` past its
/// slowest FSM. Reporting less than you hold is always safe in Raft.
pub fn report_ceiling(validated_up_to: u64, min_applied: u64, fsm_lag_eff: Option<u64>) -> u64 {
    match fsm_lag_eff {
        None => validated_up_to,
        Some(lag) => validated_up_to.min(min_applied.saturating_add(lag)),
    }
}
```

`node.rs`: `Consensus.fsm_lag_eff: Option<u64>` and `Node.fsm_door: Option<u64>`, both from `crate::services::fsm_lag_eff(&cfg.services, cfg.buffer_bytes as u64, cfg.max_payload)` at construction.

`drain_ingress_ring` (3214–3221):

```rust
            if serving {
                let append = self.cnc.counters().append.load_acquire();
                let commit = self.cnc.counters().commit.load_acquire();
                if !admission_open(append, commit, self.admission_bytes) {
                    break; // door closed; leave the rest in the ring this cycle
                }
                // M14a (spec §5.2): the FSM term — the slowest declared FSM
                // may not fall more than fsm_lag behind the append head.
                // Same client-visible outcome as the window term: the record
                // stays in the ring and the client's try_write sees Full.
                if let Some(lag) = self.fsm_lag_eff
                    && !admission_open(append, self.min_applied, lag)
                {
                    break;
                }
            }
```

`Node::submit` (1453–1467), after the existing door check:

```rust
        if let Some(lag) = self.fsm_door {
            let min_applied = self.cnc.service().service_applied.load_acquire();
            if !admission_open(append, min_applied, lag) {
                return Err(SubmitError::Full);
            }
        }
```

`publish_validated_frontier`:

```rust
    fn publish_validated_frontier(&self) {
        self.reports_unattested.store(self.sm.reports_unattested(), Ordering::Relaxed);
        // M14a (Q, spec §5.3): the report ceiling — validated, and no more
        // than fsm_lag past the slowest FSM. `term_at(ceiling)` is the term
        // of the byte BELOW the ceiling (the same attestation as before:
        // `validated_term()` is `term_at(validated_up_to)`).
        let ceiling = crate::services::report_ceiling(
            self.sm.validated_up_to(),
            self.min_applied,
            self.fsm_lag_eff,
        );
        // Term first, then position (unchanged): a torn read fails the
        // leader's attestation check, the safe direction.
        self.validated_term.store(self.sm.term_at(ceiling), Ordering::Release);
        self.validated_frontier.store(ceiling, Ordering::Release);
    }
```

Check `term_at` is `pub` on `ElectionSm` (it is used by `validated_term` inside `uc2_consensus`; the report above quotes it as `pub fn term_at`). No `uc2_consensus` change.

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_node --lib services
cargo test -p uc2_node --test services
cargo test -p uc2_node --test lin_v2 --test lin_partition_v2 --test failover --test learner --test reconfig
cargo test -p uc2_node
```

Expected: PASS. The last line is the whole `uc2_node` suite — the `none_for_tests` sweep from Task 3 is what keeps the node-only suites green here; if any test in that list stalls on liveness, it is a node-only test that was missed in Task 3's list and gets `none_for_tests()`.

- [ ] **Step 5: Verify discrimination of the Q test**

Temporarily make `report_ceiling` return `validated_up_to` unconditionally; run `cargo test -p uc2_node --test services q_a_follower` — expected FAIL on `commit <= BOUND + one_frame`. Revert.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(node): FSM term on the admission door + quorum-gated report ceiling (Q): append/report ≤ min_applied + fsm_lag"
```

---

### Task 9: M11 backup / verify / restore learn `snapshots/<id>/` (spec §7.5)

Written **test-first against the flat copier**, as spec §13's mitigation prescribes: after Task 6 the service publishes into `snapshots/<id>/`, `copy_dir_sorted` (`backup.rs:~345`) is non-recursive and `scan_snapshots` (`backup.rs:645-667`) skips directories, so a backup of a purged two-FSM node silently contains **no snapshots** and `backup_instance`'s own `verify_artifact` returns `Hole`. The test fails red on exactly that line before the fix.

`BackupReport` keeps `Copy` by carrying `newest_snapshots: [Option<u64>; CNC_MAX_SERVICES]` (per id present on disk; backup is offline and config-blind). Coverage is per id; `MANIFEST` is `uc2-backup-v2` with one `newest_snapshot.<id>=` line per id.

**Files:**
- Modify `uc2_node/src/backup.rs` (`BackupReport` 109–133, `BackupError::Hole` 150–159, `MANIFEST_FORMAT` 102, `backup_instance` 373–397, `verify_artifact` 472–509, `restore_artifact` 570–602, `scan_snapshots` 645–667, `write_manifest`/`check_manifest` 669–788, unit tests `manifest_roundtrips` ~802–840)
- Modify `uc2ctl/src/main.rs:561-573`
- Modify `uc2_node/tests/backup.rs` (the `Hole` pattern match at 399; two new tests)
- Modify `docs/how-to/back-up-a-cluster.md` (§ "What gets copied", § "The `MANIFEST` file")

**Interfaces:**
```rust
pub struct BackupReport {
    pub journal_first_base: u64,
    pub journal_last_pos: u64,
    /// Per service id present in `snapshots/<id>/`: the newest complete artifact. `None` = no directory or empty.
    pub newest_snapshots: [Option<u64>; CNC_MAX_SERVICES],
    pub snapshot_floor: u64,
    pub healed_torn_tail: bool,
    pub files: usize,
}
impl BackupReport { pub fn newest_snapshot(&self) -> Option<u64> }  // min over ids present — the cluster-wide coverage point
BackupError::Hole { service: u8, first_base: u64, newest_snapshot: Option<u64> }
```

- [ ] **Step 1: Write the failing tests** (`uc2_node/tests/backup.rs`)

```rust
use uc_lincheck::register::{Cmd as RegCmd, RegisterSm};
use uc2_node::ServicesConfig;
use uc2_service::SnapshotPolicy;

/// Two snapshotting FSMs (`RegisterSm`, ids 0 and 1) on a purging node.
/// Returns once the journal has purged at least one segment.
fn two_fsm_purged_node(dir: &Path, app: &str) -> (Node, uc2_service::Service<RegisterSm>, uc2_service::Service<RegisterSm>, u64) {
    let mut cfg = config(dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 });
    cfg.services = ServicesConfig::from_ids(&[0, 1], None).unwrap();
    let node = Node::start(cfg).expect("node");
    wait_until("serving", || node.can_serve());
    let svc = |id: u8| {
        ServiceBuilder::new(
            ServiceConfig::new(dir, app).service_id(id).snapshot_policy(SnapshotPolicy { interval_bytes: 32 * 1024 }),
            RegisterSm::default(),
        )
        .start_with_snapshots()
        .expect("snapshot service")
    };
    let (s0, s1) = (svc(0), svc(1));
    let client = Client::connect(dir, app).expect("client");
    let mut v = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    while node.archive_first_base() == 0 {
        assert!(Instant::now() < deadline, "no purge after 60 s");
        v += 1;
        let _: uc_lincheck::register::CmdResp = client.submit(&RegCmd::Write(v)).expect("write");
    }
    client.shutdown();
    (node, s0, s1, v)
}

#[test]
fn restore_roundtrip_with_two_fsms_keeps_both_snapshot_trees() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "restore2";
    let (node, s0, s1, last) = two_fsm_purged_node(&dir, app);
    for id in ["0", "1"] {
        assert!(std::fs::read_dir(dir.join("snapshots").join(id)).unwrap().next().is_some(), "snapshots/{id} has an artifact");
    }
    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
    s0.stop();
    s1.stop();

    let artifact = root.path().join("restore2-artifact");
    let report = backup_instance(&dir, &artifact).expect("backup_instance — RED before Task 9: the flat copier drops snapshots/<id>/ and verify reports a Hole");
    assert!(report.journal_first_base > 0, "vacuity: the journal must have been purged");
    assert!(report.newest_snapshots[0].is_some() && report.newest_snapshots[1].is_some(), "{report:?}");
    assert!(report.newest_snapshots[2..].iter().all(Option::is_none));
    for id in ["0", "1"] {
        assert!(std::fs::read_dir(artifact.join("snapshots").join(id)).unwrap().next().is_some(), "artifact snapshots/{id}");
    }
    let manifest = std::fs::read_to_string(artifact.join("MANIFEST")).unwrap();
    assert!(manifest.contains("format=uc2-backup-v2\n"), "{manifest}");
    assert!(manifest.contains("newest_snapshot.0="), "{manifest}");
    assert!(manifest.contains("newest_snapshot.7=none\n"), "{manifest}");

    let fresh = root.path().join("n0-restored");
    restore_artifact(&artifact, &fresh).expect("restore");
    let mut cfg = config(&fresh, app, PurgePolicy::Disabled);
    cfg.services = ServicesConfig::from_ids(&[0, 1], None).unwrap();
    let rnode = Node::start(cfg).expect("restored node");
    wait_until("restored serving", || rnode.can_serve());
    let rs0 = ServiceBuilder::new(ServiceConfig::new(&fresh, app).service_id(0), RegisterSm::default())
        .start_with_snapshots().expect("restored svc 0");
    let rs1 = ServiceBuilder::new(ServiceConfig::new(&fresh, app).service_id(1), RegisterSm::default())
        .start_with_snapshots().expect("restored svc 1");
    let client = Client::connect(&fresh, app).expect("client");
    let got: Option<u64> = client.query_linearizable(&()).expect("read");
    assert_eq!(got, Some(last), "FSM 0 rebuilt from its own snapshot + tail");
    wait_until("FSM 1 rebuilt", || rs1.query(()) == Some(last));
    client.shutdown();
    rnode.stop();
    rs0.stop();
    rs1.stop();
}

#[test]
fn verify_reports_a_hole_for_the_id_whose_snapshot_is_missing() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "hole2";
    let (node, s0, s1, _) = two_fsm_purged_node(&dir, app);
    node.stop();
    s0.stop();
    s1.stop();
    let artifact = root.path().join("hole2-artifact");
    let report = backup_instance(&dir, &artifact).expect("backup");
    // Delete FSM 1's snapshots from the artifact: FSM 1 can no longer be rebuilt.
    for e in std::fs::read_dir(artifact.join("snapshots").join("1")).unwrap() {
        std::fs::remove_file(e.unwrap().path()).unwrap();
    }
    match verify_artifact(&artifact) {
        Err(BackupError::Hole { service: 1, first_base, newest_snapshot: None }) => {
            assert_eq!(first_base, report.journal_first_base);
        }
        other => panic!("expected Hole{{service: 1}}, got {other:?}"),
    }
    // A MANIFEST that still claims FSM 1's snapshot is a mismatch too — but
    // the Hole is reported first (coverage before cross-check, as today).
}
```

Update the existing match at `tests/backup.rs:399` to `Err(BackupError::Hole { first_base, newest_snapshot, .. })`.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_node --test backup restore_roundtrip_with_two_fsms` — expected: compile error on `newest_snapshots` (field missing). Temporarily comment the three `newest_snapshots` assertions and re-run: expected FAIL at `backup_instance(...).expect("… RED before Task 9 …")` with `Hole`. Restore the assertions. This is the "fails red on the flat copier" evidence — record the panic line in the commit message.

- [ ] **Step 3: Implement**

`backup.rs`:

```rust
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

const MANIFEST_FORMAT: &str = "uc2-backup-v2";

/// Per-id snapshot subdirectories present on disk, ascending. Offline and
/// config-blind: whatever `snapshots/<id>/` directories exist are the set.
fn snapshot_ids_present(root: &Path) -> io::Result<Vec<u8>> {
    let dir = snapshots_dir(root);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(id) = entry.file_name().to_str().and_then(|n| n.parse::<u8>().ok())
            && (id as usize) < CNC_MAX_SERVICES
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// `snapshots/<id>/` for every id present, journal-then-state-then-
/// snapshots ordering preserved by the caller; the per-directory filter is
/// the same `snap-<pos>.ultsnap` rule as before.
fn copy_snapshot_tree(src_root: &Path, dst_root: &Path) -> Result<(), BackupError> {
    for id in snapshot_ids_present(src_root)? {
        copy_dir_sorted(
            &snapshots_dir(src_root).join(id.to_string()),
            &snapshots_dir(dst_root).join(id.to_string()),
            |n| parse_snap_pos(n).is_some(),
        )?;
    }
    Ok(())
}

/// Per id present: the newest complete artifact; plus the total file count.
fn scan_snapshot_tree(root: &Path) -> io::Result<([Option<u64>; CNC_MAX_SERVICES], usize)> {
    let mut newest = [None; CNC_MAX_SERVICES];
    let mut count = 0;
    for id in snapshot_ids_present(root)? {
        let (n, c) = scan_snapshots(&snapshots_dir(root).join(id.to_string()))?;
        newest[id as usize] = n;
        count += c;
    }
    Ok((newest, count))
}
```

`scan_snapshots` stays as the per-directory scanner. In `backup_instance` and `restore_artifact`, replace the `src_snapshots` block with `copy_snapshot_tree(instance_dir, out)?;` / `copy_snapshot_tree(artifact, instance_dir)?;`. In `verify_artifact`:

```rust
    // 3. Snapshots: per id present, the newest complete `snap-<pos>.ultsnap`.
    let (newest_snapshots, snapshot_files) = scan_snapshot_tree(artifact)?;

    // 4. Coverage invariant, PER ID (M14a): every FSM whose directory exists
    // must be rebuildable from its own newest snapshot + the journal tail. A
    // purged journal with no snapshot directory at all is FSM 0's hole (the
    // one id every node declares).
    if journal_first_base > 0 {
        let ids: Vec<u8> = snapshot_ids_present(artifact)?;
        if ids.is_empty() {
            return Err(BackupError::Hole { service: 0, first_base: journal_first_base, newest_snapshot: None });
        }
        for id in ids {
            let n = newest_snapshots[id as usize];
            if n.is_none_or(|pos| pos < journal_first_base) {
                return Err(BackupError::Hole { service: id, first_base: journal_first_base, newest_snapshot: n });
            }
        }
    }
```

`BackupReport`: replace `newest_snapshot` with `newest_snapshots: [Option<u64>; CNC_MAX_SERVICES]` and add:

```rust
impl BackupReport {
    /// The cluster-wide coverage point: the LOWEST newest-snapshot over the
    /// ids present (a restore is only as fresh as its slowest FSM).
    pub fn newest_snapshot(&self) -> Option<u64> {
        self.newest_snapshots.iter().flatten().copied().min()
    }
}
```

`BackupError::Hole` gains `service: u8` (message: `hole: service {service}: journal first_base={first_base} is not covered by any retained snapshot (newest_snapshot={newest_snapshot:?})`). `write_manifest` writes `newest_snapshot.<id>=<pos|none>` for **every** id 0..8 (deterministic, easy to parse), and `check_manifest` compares each; an artifact whose `format=` is `uc2-backup-v1` is refused by the existing unknown-format branch (there are no v1 artifacts anywhere — no deployments). Update `manifest_roundtrips` and its siblings (`newest_snapshots: [None; 8]` with `[0] = Some(200)`).

`uc2ctl/src/main.rs::print_backup_report`: replace the `newest_snapshot=` line with one `newest_snapshot.<id>=<pos|none>` line per id whose value is `Some`, followed by `newest_snapshot=<min|none>` (the coverage point).

`docs/how-to/back-up-a-cluster.md` "What gets copied": the `snapshots/` bullet becomes "`snapshots/<id>/` for every FSM id present — one directory per declared service since M14 — filtered to complete `snap-<pos>.ultsnap` files"; "The `MANIFEST` file": the format line is `uc2-backup-v2` and the per-id `newest_snapshot.<id>` lines are listed; add the sentence "`verify` checks coverage **per FSM**: a missing or stale snapshot for any one id is a `hole: service <id>` refusal, because that FSM alone could not be rebuilt."

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_node --lib backup
cargo test -p uc2_node --test backup
cargo test -p uc2ctl
cargo test -p uc2-crashtest --features survival-tests   # the M11 survival capstone still round-trips
```

Expected: PASS, all 19 `tests/backup.rs` tests (17 + 2).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(backup): per-FSM snapshots/<id>/ in backup/verify/restore, per-id coverage holes, MANIFEST v2 (red-first: flat copier reported Hole at first_base=<N>)"
```

---

### Task 10: Documentation — the page, the reservation, the flag day, the operator rules

No code. `RELEASES.md` / `docs/releases.md` are M14d (the release), not here.

**Files:**
- Modify `docs/reference/cnc-page.md`
- Modify `docs/reference/instance-directory.md:84,100`
- Modify `docs/reference/wire-protocol.md:9-22`
- Modify `docs/how-to/upgrade-a-cluster.md` (new section after "Ring format change in 2.7.0", line 189)
- Modify `docs/how-to/run-a-cluster.md` ("Write one config file per host", line 94; "Supervise the processes", line 224)
- Modify `docs/ops/uc2-runbook.md` (instance-dir layout list)
- Modify `docs/VERIFICATION.md` (§7 target table row for `uc_protocol_cnc`; §11)

- [ ] **Step 1: `cnc-page.md`**

Line 2–3: "`cnc2.dat` is a fixed-layout **8 KiB** control page (two 4 KiB pages since cnc 3.0 / M14) in every instance directory." Version row: `CNC_V2_VERSION` = `(3 << 24) \| (0 << 16)`. "Page length is 4096 bytes." → "Page length is 8192 bytes: page 1 (`0..4096`) is the M1–M13 layout byte-for-byte; page 2 (`4096..8192`) is the service-slot band."

Counters table: change the `512 service_applied`, `640 output_completed`, `960 service_heartbeat_ns`, `1152 service_snapshot_pos` rows' Writer column to "node (consensus agent) — `min` over the declared FSMs' slots since 3.0"; `576 service_epoch` → "retired at 0 since 3.0 (per-FSM epoch lives in the slot)"; the `3976` row's note loses "so the last line (4032) stays free"; add rows:

```
| 4032 | `services_declared` | node, once at boot (bit *i* ⇔ id *i* declared) |
| 4040 | `fsm_lag_bytes` | node, once at boot (`0` ⇔ lockstep) — shares 4032's line |
```

New section after "Peer slots", same shape:

```markdown
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
| 0 | `status` — `service_id` (bits 0..8) \| attached (bit 8) \| incarnation (bits 32..64) | service, at attach / clean detach |
| 64 | `applied` | service apply agent |
| 128 | `epoch` | service, `fetch_add` at attach |
| 192 | `output_completed` | service output agent |
| 256 | `snapshot_pos` | service builder agent |
| 320 | `heartbeat_ns` | service apply agent |
| 384 | `lag_waits` | service apply agent (one per wait episode at the lag barrier) |
| 448 | reserved (zero) | — |

A slot whose `status` reads `0` has never been attached this page generation. The node re-creates the page at every boot, so incarnation and epoch restart at 0 with the node.
```

- [ ] **Step 2: `instance-directory.md`**

Row 84: "`buffer_bytes` + 14 MiB of rings + **5 MiB × (N − 1)** for N declared FSMs (`svc_query.<id>.ring` 1 MiB + `egress_service.<id>.broadcast` 4 MiB each) + 4 KiB for the second cnc page — ~78 MiB at the defaults with one FSM, ~113 MiB with eight; reserved at startup". Add the per-id files to the layout list: `svc_query.<id>.ring`, `egress_service.<id>.broadcast`, `service.<id>.lock` (exclusive flock, one process per id), `snapshots/<id>/`. Note that the singular `svc_query.ring` / `egress_service.broadcast` names are gone and any leftover is unlinked at boot.

- [ ] **Step 3: `wire-protocol.md`**

Line 13: the `version::CURRENT` row says `0.4.0` — it has been `0.5.0` since the content-attested reports; fix it to `0.5.0` (this plan does not bump it). Add a row `cnc page version | 3.0 (M14: 8 KiB page)` and a sentence: "cnc 3.0 changed the same-host shmem layout only; the UDP datagram format is unchanged at 0.5.0 — the two version lines are independent (M14c bumps the wire to 0.6.0 for `SNAP_BEGIN`)."

- [ ] **Step 4: `upgrade-a-cluster.md`** — new section, same shape as the 2.7.0 one:

```markdown
## Control-page change in 2.8.0: restart a host's processes together

M14 grows `cnc2.dat` from 4 KiB to 8 KiB and bumps its version to 3.0. Every
same-host party — the node, each service, every client, `uc2ctl`, the gateway
— refuses a page whose major version differs, by name (`VersionMismatch`),
so a 2.7 service cannot attach to a 2.8 node or vice-versa. This is a
**same-host** flag day, not a cluster-wide one: the node↔node wire stays 0.5.0
in 2.8.0, so hosts can be upgraded one at a time. On each host:

1. stop the clients, the gateway, then the services, then the node;
2. swap binaries;
3. start the node (it re-creates the page and unlinks the old singular ring
   names), then the services with their `--service-id`, then the clients.

The instance directory's journal, state and snapshots are reused as-is. If
`[services]` is absent, the node declares FSM 0 only and behaves exactly as
before, except that a service must now attach as id 0 (the default).
```

- [ ] **Step 5: `run-a-cluster.md`**

Under "Write one config file per host": a `[services]` paragraph — the set is static, must be identical on every node, and **an id may only be added while the journal is intact from position 0** (purge disabled or never fired): a new FSM rebuilds from genesis and no sibling's snapshot can stand in for it; with a purged prefix the new id fails its attach with `SnapshotRequired` and, being declared, holds admission closed until the set is put back (spec §8). Under "Supervise the processes": one service process **per declared id**, each with `--service-id`, all supervised; a declared id with no process attached closes the leader's admission door once the log is `fsm_lag` ahead of it.

- [ ] **Step 6: `uc2-runbook.md`, `VERIFICATION.md`**

Runbook: the instance-dir layout list gains the per-id files (same wording as Step 2). `VERIFICATION.md` §7 table: the `uc_protocol_cnc` row's seam text mentions "8 KiB page, page-2 slot band and the 4032 pair since M14a"; §11 ("What is not verified"): add "M14a's lag barrier and Q are unit-tested and integration-tested on one node and a 3-node in-process cluster; the sim scenario (report never exceeds validated, commit stalls iff a quorum is capped) is M14b."

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "docs(m14a): cnc 3.0 page layout, per-FSM instance-dir files + reservation formula, same-host flag day, declared-set-before-purge rule"
```

---

### Task 11: The local proof stack (smoke, not a gate)

Runs things and records what it saw. Rate numbers from this box are smoke (`docs/notes/dev-box-not-a-bench.md`); nothing here adjudicates a bar.

- [ ] **Step 1: A private target dir, the workspace suite**

```bash
export CARGO_TARGET_DIR=/home/claude/.cache/cargo-target-m14a
cd /home/claude/ultima/ultima_cluster/.claude/worktrees/uc2-multi-service
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tail -30
```

Expected: clippy clean; every test binary reports `0 failed`. Paste the tail into the commit body of Step 5.

- [ ] **Step 2: The capstones that exercise attach/apply under faults**

```bash
cargo test -p uc2_node --test lin_v2 2>&1 | tail -12
cargo test -p uc2_node --test lin_partition_v2 2>&1 | tail -12
cargo test -p uc2-crashtest --features hard-crash-tests 2>&1 | tail -12
```

Expected: `Linearizable` on every capstone (they run one FSM per node — the default `{0}` — and are unchanged in intent).

- [ ] **Step 3: The fuzz targets this plan touched**

```bash
scripts/fuzz_smoke.sh 60 --min-runs 10000 uc_protocol_cnc uc2_node_toml
```

Expected: both PASS with ≥ 10 000 runs; the `[services]` table parses under the existing `uc2_node_toml` target.

- [ ] **Step 4: The M5 smoke with two FSMs** — `m5_gate all` starts one service per node; a quick two-FSM variant is the `services.rs` `two_fsms_apply…` test at scale. Run:

```bash
UC2_M5_MAX_SECS=6 cargo run -p uc2_node --release --example m5_gate -- all --secs 6 --root /home/claude/m14a-smoke
```

Expected: `RESULT: PASS` (single FSM, default config) — the regression smoke that the page-2 move and the barrier's per-iteration `floor` loads did not visibly change the local number. Record the responses/s line **as smoke** in the commit body; do not compare it against any bar.

- [ ] **Step 5: Commit the evidence**

```bash
git add -A   # nothing but the plan checkboxes should change
git commit --allow-empty -m "test(m14a): local proof stack — workspace suite, lin capstones, hard-crash, fuzz smoke, m5 smoke (numbers are smoke, not a gate)"
```

---

## Self-review

Performed against the spec sections this plan claims (§3, §4.1–4.2, §4.4, §5.1–5.3, §5.5, §7.1–7.2, §7.4–7.5, and the relevant §8/§12 rows).

**Spec coverage**

| Spec clause | Where |
|---|---|
| §3.1 page 2 `ServiceSlot[8]`, stride 512, per-line writer, constants pinned in both crates | Task 1 (constants + `offsets_do_not_overlap`), Task 2 (`ServiceSlot` + `offset_of!` pins + `cnc_offsets_match_protocol_constants`) |
| §3.2 page-1 fields become node-written mins; `service_epoch` retired; store-on-change | Task 5 (`service_mins`, `publish_service_mins`) |
| §3.2 4032/4040 pair, one writer, plain `AtomicU64`s; page 1 full | Task 1 + Task 2 + Task 4 (node stores them at boot) |
| §3.3 `[services] ids / fsm_lag`, refusals: empty, duplicate, ≥ 8, unparsable, `>= buffer/2`; `deny_unknown_fields` | Task 3 (+ deviation 6: id 0 required) |
| §3.4 cnc 2.0 → 3.0 flag day; wire unchanged | Task 1; Task 10 (upgrade doc) |
| §4.1 attach: version/app/instance checks, `ServiceNotDeclared`, `service.<id>.lock`/`AlreadyAttached`, per-id rings, slot writes (applied → status → epoch), detach clears `attached` | Task 6 (gate + lock + rings), Task 5 (slot discipline, detach) |
| §4.2 lag barrier: lockstep / bounded, heartbeat during wait, `lag_waits`, not during replay, invariant | Task 7 (deviation 1) |
| §4.4 per-service state: `snapshots/<id>/`, slot fields | Task 6 (store), Task 5 (slot) — **except** `output_progress.<id>.state`: deviation 2 |
| §5.1 aggregates over declared ids; absent id holds the min | Task 5 (+ unit test asserting the 0 drag) |
| §5.2 door: second predicate, same sites, lockstep = one frame; same client outcome (`Backpressure`), no gateway change | Task 8 (deviation 4 for `max_claim`); the plan-check item "which outcome does a closed door produce today" is answered: the record stays in the ring, the client sees `RingError::Full` → `SubmitError::Backpressure` (`node.rs:3219`, `engine.rs:416`), and the FSM term produces the identical outcome |
| §5.3 Q: `ceiling = min(validated, min_applied + lag)`, term at the byte below, term-then-position order, receiver untouched; plan-check "no leader-side low-report-means-snapshot logic" | Task 8; the check item is answered by the node recon: the only snapshot-session trigger is the follower's below-floor NAK (`sender.rs:875`), the leader never initiates on a report |
| §5.5 per-id rings + dirs, legacy names not created, reservation formula | Task 4 (code), Task 10 (docs) |
| §7.1 purge floor = `min(snapshot_pos)`; never-snapshotted id ⇒ floor 0 | Task 5 (the min), `maybe_persist_snapshot_floor` unchanged |
| §7.2 per-FSM `output_completed`, aggregate min | Task 5 (deviation 2 for the state file) |
| §7.4 replay unchanged per FSM | Task 7 (barrier lives outside `replay_into`) |
| §7.5 backup/verify/restore per id, `Hole { service }`, MANIFEST per id, test red-first | Task 9 |
| §8 rows: crash/absent FSM back-pressure; undeclared refused; two processes same id refused; `fsm_lag` too large refused; declared set grew after purge | Task 8 tests (absent FSM closes the door; capped reports stall commit), Task 6 tests, Task 4 test, Task 10 (`run-a-cluster.md` rule) |
| §12 unit: page-2 offsets both crates; barrier table; door with FSM term; Q pair; config refusals | Tasks 1, 2, 7, 8, 3 respectively |
| §12 fuzz: `uc_protocol_cnc` covers the 8 KiB page; `uc2_node_toml` covers `[services]` | Task 1 (corpus regen), Task 11 |

Not covered here, by design (named in the header): §5.4/§6 (M14b), §7.3 (M14c, deviation 3), §9, the §12 capstones/sim/elle/fleet gate, the release writeup.

**Placeholder scan**: grepped the plan for `TBD`, `TODO`, `similar to`, `add error handling`, `fill in`. None. Two values are deliberately left to the run: `<N>` in the Task 7 and Task 9 commit messages (the measured unbarriered gap and the red-first `first_base`) — those *are* the evidence being recorded.

**Type consistency**: `ServicesConfig` is `Copy` (Task 3) so `Consensus.services` and `Node` can both hold it by value; `fsm_lag_eff` is `Option<u64>` in `services.rs`, on `Consensus.fsm_lag_eff` and `Node.fsm_door`, and `report_ceiling` takes `Option<u64>`; `min_applied: u64` with `u64::MAX` as the "no FSMs" value matches `report_ceiling`'s `saturating_add`. `service_id` is `u8` everywhere (`ServiceConfig`, `ServiceError`, `PendingRead`, `forward_svc_query`, `InstanceDir::*_for`, `SnapshotStore::open`, `--service-id`); slot indexing casts to `usize` at the `service_slot(i)` boundary only. `pack_service_status(u8, bool, u32) -> u64` / `unpack_service_status(u64) -> (u8, bool, u32)` match on both sides of the Task 5 tests. `BackupReport.newest_snapshots: [Option<u64>; CNC_MAX_SERVICES]` keeps `Copy` — `print_backup_report` and every test that passes a report by value are unaffected. `plan()`'s `Plan::Apply { target, one_frame }` is destructured with exactly those names in `apply_cycle`.

**Two facts worth re-checking during execution:**
1. `FrameIter` advances the follower's cursor **as it yields** (`uc2_log/src/reader.rs:100-125`). Lockstep's `break` after the first yielded frame relies on the cursor then sitting at the *next* frame's start — it does, because the advance happens before the yield returns. If that ever changes, lockstep silently becomes "one batch", not "one frame".
2. `publish_service_mins` must stay the **first** statement of `do_work` after the halt check. Moving it below step 0 makes `refresh_durable` publish a report ceiling from the previous cycle's `min_applied` — still safe (stale is conservative) but one cycle later than the spec's steady state.
