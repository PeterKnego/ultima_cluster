# UC2 Pipelined Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the m5_gate client pump into `uc2_client` as a public two-layer API — a bytes-level completion `Engine` (SendHalf/PollHalf) plus a `Ticket`-based `PipelinedClient` — with the existing blocking `Client` becoming a shim, per spec `docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`.

**Architecture:** Engine (B) is a passive, waitless, pure-sync correlation core: a generation-tagged slot table claimed by cloneable `SendHalf`s and drained by a single `PollHalf::poll(cb)` duty cycle. The Ticket layer (A) owns the one driver thread, resolving completions into per-ticket park/`Waker` cells. `Client` keeps its exact public API as a shim over A; `matcher.rs` is deleted; m5_gate's client role is rewritten on the public engine (acceptance = smoke parity now, fleet gate later, user-approved).

**Tech Stack:** Rust, std only. Existing deps: `uc_protocol` (rings, ipc consts), `uc2_log` (CncPage, AgentRunner precedent), `serde`, `bincode`, `thiserror`. NO new dependencies (no tokio, not even dev — async is tested with a hand-rolled `block_on`).

## Global Constraints

- Branch: all work on `uc2/pipelined-client` off current `main`.
- **No new crate dependencies** in `uc2_client` (its small dep set is an advertised property; spec §2).
- **No wire/protocol change**: same ring files, msg types, `header_extra` codec (`(client_id: u32, local_seq: u32)` LE), cnc layout. A new client talks to an old node.
- **`Client`'s public API and observable behavior are pinned** by the four existing test files (`roundtrip.rs`, `synthetic.rs`, `timeout_and_restart.rs`, `torn_header.rs`) — they must pass UNCHANGED (they are the compat oracle).
- `cargo clippy --workspace --all-targets -- -D warnings` must stay clean after every task.
- Test scratch dirs: `tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR"))` in new integration tests — NEVER bare `/tmp` (RAM tmpfs, no swap; standing CLAUDE.md rule). Tiny `NamedTempFile`s in unit tests are fine (existing pattern).
- 64-bit only assumption (`user_data` carries a pointer in layer A): add `#[cfg(not(target_pointer_width = "64"))] compile_error!` in `pipelined.rs`.
- The ported wait code is attributed to `ultima_rings` (`~/ultima/ultima_rings/src/wait.rs`) in a header comment, findings cited, per spec §5.
- `docs/superpowers/` artifacts are never deleted (CLAUDE.md).
- SPDX header (`// SPDX-License-Identifier: Apache-2.0` + copyright) on every new file, matching existing files.

## Spec deviations pre-agreed (record, don't re-litigate)

1. `SubmitError` gains `InstanceRestart { attached, current }` and `Outcome::InstanceRestart` carries `{ attached, current }` — the caller needs the ids and the spec enum sketch omitted them.
2. `EngineConfig`/`PipelinedConfig` gain `serving_gate: bool` (default `true`). The `Client` shim sets `false`: today's `Client` submits regardless of `CAN_SERVE` and learns NOT_LEADER from the wire — pinned by `synthetic.rs`/`timeout_and_restart.rs`.
3. `EngineConfig` gains `max_payload: Option<usize>` (default `None`): the node's `NodeConfig.max_payload` is NOT discoverable over IPC, so the fail-loud door check is config'd to match the deployment; `None` still maps the ring's own `TooLarge`.
4. `EngineConfig` gains `#[doc(hidden)] start_seq: u64` (default 0) — the wrap test needs to start the internal sequence near `u32::MAX`.
5. Module layout: the slot table lives in its own `slots.rs` (pure logic, separately unit-tested), not inside `engine.rs`.

## File Structure

```
uc2_client/src/
  lib.rs         # modify: module decls + public exports
  wait.rs        # NEW (Task 1): WaitStrategy + Idle ladder (ported)
  slots.rs       # NEW (Task 2): SlotTable — correlation core, no I/O
  engine.rs      # NEW (Tasks 3-4): EngineConfig/SubmitError/Outcome/Completion/
                 #   EngineStats, Engine::attach, SendHalf, PollHalf
  ticket.rs      # NEW (Task 5): TicketCore + Ticket<R> (wait / Future)
  pipelined.rs   # NEW (Task 6): PipelinedClient + driver thread
  client.rs      # REWRITE (Task 7): compat shim over PipelinedClient
  matcher.rs     # DELETE (Task 7)
  error.rs       # modify (Task 7): + PayloadTooLarge variant, doc notes
uc2_client/tests/
  engine_synthetic.rs  # NEW (Tasks 3-4): engine vs hand-rolled instance dirs
  pipelined.rs         # NEW (Task 6): real node+service round trips
  (roundtrip.rs / synthetic.rs / timeout_and_restart.rs / torn_header.rs UNCHANGED)
uc2_node/examples/m5_gate.rs  # REWRITE client role on public Engine (Task 8)
```

---

### Task 1: `wait.rs` — ported `WaitStrategy` + `Idle` ladder

**Files:**
- Create: `uc2_client/src/wait.rs`
- Modify: `uc2_client/src/lib.rs` (add `mod wait; pub use wait::WaitStrategy;`)

**Interfaces:**
- Consumes: nothing (std only).
- Produces: `pub enum WaitStrategy { BusySpin, BackoffYield, Backoff, Park }` (`Debug, Clone, Copy, PartialEq, Eq`); `pub(crate) struct Idle` with `pub(crate) fn for_strategy(WaitStrategy) -> Idle` and `pub(crate) fn idle(&mut self)`. Task 6's driver consumes both.

- [ ] **Step 1: Create the branch**

```bash
cd /home/claude/ultima/ultima_cluster && git checkout -b uc2/pipelined-client
```

- [ ] **Step 2: Write `wait.rs` (port, then adapt doc comments)**

Port from `/home/claude/ultima/ultima_rings/src/wait.rs` (read it first — it is ~277 lines including tests). Keep: the four-variant enum, `SPINS = 10`, `YIELDS = 20`, `PARK_MIN = 64µs`, `PARK_MAX = 1ms`, the `Idle` struct and its three tests verbatim (they are deterministic/wall-clock-bounded; keep the `#[cfg_attr(miri, ignore = ...)]` attribute as-is — harmless here). Drop: the `crate::notify` reference in the doc header (uc2_client's Park parks on the ring futex or a ticket condvar instead — say so). Add this attribution header under the SPDX lines:

```rust
//! Wait strategies, PORTED from `ultima_rings` (`ultima_rings/src/wait.rs`)
//! with attribution rather than taken as a dependency — `uc2_client`'s small
//! dep set is an advertised property (spec 2026-08-13, §5).
//!
//! The measured findings that picked these defaults (all from ultima_rings'
//! bench docs, 2026-08-12 topology sweep):
//! - `BusySpin` collapses once threads outnumber schedulable CPUs, and an RPC
//!   gateway machine is oversubscribed by construction.
//! - On a BUSY machine `Park` is the FASTEST strategy (5-24x), not the
//!   slowest, while keeping 70-95% of external throughput.
//! - `thread::park_timeout` cannot deliver sub-~60µs sleeps: `PARK_MIN` is
//!   64µs so the ladder's documented doubling is real, not fiction.
//!
//! In this crate: the engine (`engine.rs`) is WAITLESS by design — these
//! strategies belong to the layer that owns a thread (`pipelined.rs`'s
//! driver) and to `Ticket::wait` (always park/unpark; a REST worker spinning
//! through a ~1ms consensus round trip is exactly the oversubscription
//! failure the tables document).
```

`WaitStrategy` must be `pub`; `Idle`, `SPINS`, `YIELDS` stay `pub(crate)`.

- [ ] **Step 3: Wire into `lib.rs`**

```rust
mod wait;
pub use wait::WaitStrategy;
```

- [ ] **Step 4: Run the ported tests**

Run: `cargo test -p uc2_client wait`
Expected: PASS (3 tests: ladder ordering, PARK_MIN floor, yield-never-parks; plus the wall-clock one).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/wait.rs uc2_client/src/lib.rs
git commit -m "feat(uc2_client): port WaitStrategy + Idle ladder from ultima_rings (attributed, no dep)"
```

---

### Task 2: `slots.rs` — the correlation slot table

**Files:**
- Create: `uc2_client/src/slots.rs`
- Modify: `uc2_client/src/lib.rs` (add `mod slots;`)

**Interfaces:**
- Consumes: nothing (std atomics only).
- Produces (all `pub(crate)`, consumed by Tasks 3-4):

```rust
pub(crate) struct SlotTable { /* ... */ }
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ReqKind { Submit = 0, Query = 1 }
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ClaimError { WindowFull, SlotBusy }
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Resolve {
    Won { user_data: u64 },
    KindMismatch,
    Miss,
}
impl SlotTable {
    pub(crate) fn new(max_inflight: u32, start_seq: u64) -> SlotTable;
    pub(crate) fn claim(&self, user_data: u64, kind: ReqKind, deadline_ns: u64)
        -> Result<u64 /* seq */, ClaimError>;
    pub(crate) fn release(&self, seq: u64);          // un-claim after a failed ring write
    pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>) -> Resolve;
    pub(crate) fn sweep(&self, now_ns: u64, cb: impl FnMut(u64 /* user_data */));
    pub(crate) fn drain_abort(&self, cb: impl FnMut(u64 /* user_data */));
    pub(crate) fn inflight(&self) -> u64;
}
```

**The invariants this file owns** (put this in the module doc — it is the heart of the engine's "exactly one completion" contract):

1. A slot's `owner` word is `0` = FREE, `u64::MAX` = RESERVED (mid-claim, metadata not yet valid — resolve/sweep must skip), else `seq + 1` (the FULL u64 sequence: the generation tag; the wire only carries `seq as u32`).
2. Claim is three-phase: CAS `FREE -> RESERVED` (so a failed claim never stomps a live occupant's metadata), write metadata (`user_data`, `deadline_ns`, `kind`), publish `owner = seq + 1` with `Release`.
3. Exactly-once resolution: whoever CASes `owner: seq+1 -> FREE` (AcqRel) owns the completion — resolve, sweep, and drain_abort all race through that single CAS, so a request completes exactly once.
4. Wrap safety: `resolve(wire_seq)` recomputes `idx = wire_seq as usize & mask` (valid because `mask < 2^32` so `seq & mask == (seq as u32) & mask`), then checks `(stored_seq as u32) == wire_seq`. A stale collision would need the same slot AND the same low 32 bits — a 2^32 outstanding gap, impossible under a bounded window.
5. A `SlotBusy` claim burns its seq (gaps in the wire sequence are harmless — correlation is by value, not continuity) and surfaces as backpressure; it means an old in-flight (a full table-length of seqs ago) still holds the slot, which the deadline sweep will clear.

- [ ] **Step 1: Write the failing tests (bottom of `slots.rs`)**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_resolve_roundtrip_returns_user_data_and_decrements_inflight() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(0xAB, ReqKind::Submit, u64::MAX).unwrap();
        assert_eq!(t.inflight(), 1);
        match t.resolve(seq as u32, Some(ReqKind::Submit)) {
            Resolve::Won { user_data } => assert_eq!(user_data, 0xAB),
            other => panic!("expected Won, got {other:?}"),
        }
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn second_resolve_is_a_miss_exactly_once() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(1, ReqKind::Submit, u64::MAX).unwrap();
        assert!(matches!(t.resolve(seq as u32, None), Resolve::Won { .. }));
        assert_eq!(t.resolve(seq as u32, None), Resolve::Miss, "duplicate must not double-complete");
    }

    #[test]
    fn kind_mismatch_leaves_the_slot_for_the_real_answer() {
        // T14 semantics moved down from matcher.rs: a query-flagged delivery
        // must not satisfy a Submit claim; the slot survives for the real one.
        let t = SlotTable::new(8, 0);
        let seq = t.claim(2, ReqKind::Submit, u64::MAX).unwrap();
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Query)), Resolve::KindMismatch);
        assert_eq!(t.inflight(), 1, "slot must survive a kind mismatch");
        assert!(matches!(t.resolve(seq as u32, Some(ReqKind::Submit)), Resolve::Won { .. }));
    }

    #[test]
    fn window_full_refuses_and_releases_cleanly() {
        let t = SlotTable::new(2, 0);
        let a = t.claim(1, ReqKind::Submit, u64::MAX).unwrap();
        let _b = t.claim(2, ReqKind::Submit, u64::MAX).unwrap();
        assert_eq!(t.claim(3, ReqKind::Submit, u64::MAX), Err(ClaimError::WindowFull));
        t.release(a); // failed ring write path
        assert_eq!(t.inflight(), 1);
        t.claim(4, ReqKind::Submit, u64::MAX).expect("window reopened by release");
    }

    #[test]
    fn stuck_slot_a_table_length_later_is_slot_busy_not_corruption() {
        let t = SlotTable::new(4, 0); // slot count = next_pow2(4)*2 = 8
        let stuck = t.claim(7, ReqKind::Submit, u64::MAX).unwrap();
        // Force the sequence to wrap the table back onto `stuck`'s slot.
        t.set_next_seq_for_tests(stuck + t.slot_count() as u64);
        assert_eq!(t.claim(8, ReqKind::Submit, u64::MAX), Err(ClaimError::SlotBusy));
        // The stuck occupant is untouched and still resolvable.
        assert!(matches!(t.resolve(stuck as u32, None), Resolve::Won { user_data: 7 }));
    }

    #[test]
    fn sweep_expires_only_past_deadline() {
        let t = SlotTable::new(8, 0);
        let _early = t.claim(1, ReqKind::Submit, 100).unwrap();
        let late = t.claim(2, ReqKind::Submit, 10_000).unwrap();
        let mut expired = Vec::new();
        t.sweep(5_000, |ud| expired.push(ud));
        assert_eq!(expired, vec![1]);
        assert_eq!(t.inflight(), 1);
        assert!(matches!(t.resolve(late as u32, None), Resolve::Won { user_data: 2 }));
    }

    #[test]
    fn drain_abort_hands_back_every_live_user_data() {
        let t = SlotTable::new(8, 0);
        t.claim(10, ReqKind::Submit, u64::MAX).unwrap();
        t.claim(11, ReqKind::Query, u64::MAX).unwrap();
        let mut got = Vec::new();
        t.drain_abort(|ud| got.push(ud));
        got.sort_unstable();
        assert_eq!(got, vec![10, 11]);
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn wire_seq_wraps_u32_without_confusion() {
        // Start near the u32 boundary; run claims/resolves ACROSS the wrap.
        let t = SlotTable::new(8, u32::MAX as u64 - 4);
        for i in 0..16u64 {
            let seq = t.claim(i, ReqKind::Submit, u64::MAX).unwrap();
            match t.resolve(seq as u32, Some(ReqKind::Submit)) {
                Resolve::Won { user_data } => assert_eq!(user_data, i),
                other => panic!("wrap iteration {i}: {other:?}"),
            }
        }
        assert_eq!(t.inflight(), 0);
    }
}
```

(`set_next_seq_for_tests` and `slot_count` are `#[cfg(test)] pub(crate)` helpers.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc2_client slots`
Expected: FAIL to compile ("cannot find `SlotTable`").

- [ ] **Step 3: Implement `SlotTable`**

```rust
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

const FREE: u64 = 0;
const RESERVED: u64 = u64::MAX;

struct Slot {
    owner: AtomicU64,       // FREE / RESERVED / seq+1
    user_data: AtomicU64,
    deadline_ns: AtomicU64, // nanos since the engine's t0
    kind: AtomicU8,         // ReqKind as u8
}

pub(crate) struct SlotTable {
    slots: Box<[Slot]>,
    mask: usize,
    next_seq: AtomicU64,
    inflight: AtomicU64,
    max_inflight: u64,
}

impl SlotTable {
    pub(crate) fn new(max_inflight: u32, start_seq: u64) -> SlotTable {
        assert!(max_inflight >= 1);
        // 2x headroom over the window halves the odds a fresh seq lands on a
        // stuck (deadline-pending) occupant's slot; 64 floor keeps tiny
        // windows from degenerate tables.
        let n = (max_inflight.next_power_of_two() as usize * 2).max(64);
        let slots = (0..n)
            .map(|_| Slot {
                owner: AtomicU64::new(FREE),
                user_data: AtomicU64::new(0),
                deadline_ns: AtomicU64::new(0),
                kind: AtomicU8::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SlotTable {
            slots,
            mask: n - 1,
            next_seq: AtomicU64::new(start_seq),
            inflight: AtomicU64::new(0),
            max_inflight: max_inflight as u64,
        }
    }

    pub(crate) fn claim(
        &self,
        user_data: u64,
        kind: ReqKind,
        deadline_ns: u64,
    ) -> Result<u64, ClaimError> {
        if self.inflight.fetch_add(1, Ordering::AcqRel) >= self.max_inflight {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return Err(ClaimError::WindowFull);
        }
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let slot = &self.slots[(seq as usize) & self.mask];
        // Phase 1: reserve. A failed reserve must NOT touch the occupant's
        // metadata — that is why metadata writes come after this CAS.
        if slot
            .owner
            .compare_exchange(FREE, RESERVED, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return Err(ClaimError::SlotBusy); // seq burned; gaps are harmless
        }
        // Phase 2: metadata, invisible to readers while RESERVED.
        slot.user_data.store(user_data, Ordering::Relaxed);
        slot.deadline_ns.store(deadline_ns, Ordering::Relaxed);
        slot.kind.store(kind as u8, Ordering::Relaxed);
        // Phase 3: publish.
        slot.owner.store(seq + 1, Ordering::Release);
        Ok(seq)
    }

    pub(crate) fn release(&self, seq: u64) {
        let slot = &self.slots[(seq as usize) & self.mask];
        // Only the claiming thread calls release, and only before the request
        // was ever visible on the wire — the slot is necessarily ours.
        slot.owner.store(FREE, Ordering::Release);
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>) -> Resolve {
        let slot = &self.slots[(wire_seq as usize) & self.mask];
        let owner = slot.owner.load(Ordering::Acquire);
        if owner == FREE || owner == RESERVED {
            return Resolve::Miss;
        }
        let seq = owner - 1;
        if seq as u32 != wire_seq {
            return Resolve::Miss; // stale generation
        }
        if let Some(expect) = expect_kind {
            if slot.kind.load(Ordering::Relaxed) != expect as u8 {
                return Resolve::KindMismatch; // leave the slot for the real answer
            }
        }
        let user_data = slot.user_data.load(Ordering::Relaxed);
        if slot
            .owner
            .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Resolve::Miss; // lost the race to sweep/another delivery
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        Resolve::Won { user_data }
    }

    pub(crate) fn sweep(&self, now_ns: u64, mut cb: impl FnMut(u64)) {
        for slot in self.slots.iter() {
            let owner = slot.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            if slot.deadline_ns.load(Ordering::Relaxed) > now_ns {
                continue;
            }
            let user_data = slot.user_data.load(Ordering::Relaxed);
            if slot
                .owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
            }
        }
    }

    pub(crate) fn drain_abort(&self, mut cb: impl FnMut(u64)) {
        for slot in self.slots.iter() {
            let owner = slot.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            let user_data = slot.user_data.load(Ordering::Relaxed);
            if slot
                .owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
            }
        }
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_next_seq_for_tests(&self, v: u64) {
        self.next_seq.store(v, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }
}
```

(Plus the `ReqKind`/`ClaimError`/`Resolve` definitions from the Interfaces block, and `mod slots;` in `lib.rs`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc2_client slots`
Expected: PASS (8 tests).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/slots.rs uc2_client/src/lib.rs
git commit -m "feat(uc2_client): generation-tagged correlation slot table (exactly-once, wrap-safe)"
```

---

### Task 3: `engine.rs` — types, attach, SendHalf

**Files:**
- Create: `uc2_client/src/engine.rs`
- Modify: `uc2_client/src/lib.rs` (add `mod engine;` + exports)
- Test: `uc2_client/tests/engine_synthetic.rs` (new)

**Interfaces:**
- Consumes: `SlotTable`/`ReqKind`/`ClaimError`/`Resolve` (Task 2, exact signatures above); `CncPage::open_file`, `cnc.status().next_client_id` / `.flags.load_acquire()`, `cnc.meta().instance_id`, `cnc.try_instance_id()` (uc2_log); `MpscRing::open(..)?.into_split()`, `MpscProducer::try_write`, `BroadcastRing::open(..)?.subscribe()` (uc_protocol); ipc consts `MSG_V2_SUBMIT/MSG_V2_QUERY/FLAG_V2_LINEARIZABLE/extra_client`; `NODE_FLAG_CAN_SERVE` (uc_protocol::v2::cnc); `ClientError` (attach errors reuse it).
- Produces (public API, consumed by Tasks 4/6/8):

```rust
pub struct EngineConfig {
    pub max_inflight: u32,            // default 4096
    pub request_timeout: Duration,    // default 10s
    pub max_payload: Option<usize>,   // default None (ring TooLarge only)
    pub serving_gate: bool,           // default true
    #[doc(hidden)]
    pub start_seq: u64,               // default 0; test hook (wrap)
}
impl Default for EngineConfig { /* values above */ }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency { Linearizable, Snapshot }

#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("backpressure: inflight window or ingress ring full")]
    Backpressure,
    #[error("node is not a serving leader (CAN_SERVE clear)")]
    NotServing,
    #[error("payload too large: {len} > {max}")]
    PayloadTooLarge { len: usize, max: usize },
    #[error("node instance restarted: attached {attached:#x}, now {current:#x}")]
    InstanceRestart { attached: u128, current: u128 },
    #[error("ring error: {0}")]
    Ring(uc_protocol::ring::RingError),
}

pub struct Engine;   // namespace for attach()
pub struct SendHalf { /* Clone + Send, !Sync (MpscProducer's Cell) */ }
pub struct PollHalf { /* Send, single owner; Task 4 fills methods */ }

impl Engine {
    pub fn attach(instance_dir: &Path, app_id: &str, cfg: EngineConfig)
        -> Result<(SendHalf, PollHalf), ClientError>;
}
impl SendHalf {
    pub fn try_submit(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError>;
    pub fn try_query(&self, user_data: u64, query_bytes: &[u8], c: Consistency)
        -> Result<(), SubmitError>;
    pub fn client_id(&self) -> u32;
    pub fn instance_id(&self) -> u128;
    pub fn leader_hint(&self) -> Option<u32>;   // cnc status, u64::MAX -> None
    pub fn can_serve(&self) -> bool;
    pub fn stats(&self) -> EngineStats;          // snapshot (Task 4 fills counters)
    pub fn inflight(&self) -> u64;
}
```

The shared core (private): `struct Shared { cnc: Arc<CncPage>, client_id: u32, instance_id: u128, table: SlotTable, stats: StatCells, dead: AtomicBool, restart: Mutex<Option<(u128, u128)>>, t0: Instant, timeout_ns: u64, max_payload: Option<usize>, serving_gate: bool }` in an `Arc`, plus per-half ring handles. Well-known file names move here as `pub(crate) const CNC_FILE / INGRESS_RING / QUERY_RING / EGRESS_SERVICE / EGRESS_NODE` (Task 7's shim re-imports them; delete the duplicates in `client.rs` then).

`EngineStats` (public, `#[derive(Debug, Default, Clone, Copy)]`): `accepted, responses, duplicates, kind_mismatch, overwritten, corrupt, not_leader, retry, timed_out, restarts: u64`. Internal `StatCells` = same fields as `AtomicU64`s with a `snapshot()` method.

- [ ] **Step 1: Write the failing tests (`tests/engine_synthetic.rs`)**

Copy `make_instance`/`meta` helpers from `uc2_client/tests/synthetic.rs` (hand-rolled instance dirs: cnc + 4 ring files; no real node). Then:

```rust
use std::time::Duration;
use uc2_client::{Consistency, Engine, EngineConfig, SubmitError};

fn cfg() -> EngineConfig {
    EngineConfig { serving_gate: false, ..EngineConfig::default() }
}

#[test]
fn attach_allocates_distinct_client_ids() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-attach", 1 << 20, 1 << 20);
    let (a, _pa) = Engine::attach(dir.path(), "eng-attach", cfg()).unwrap();
    let (b, _pb) = Engine::attach(dir.path(), "eng-attach", cfg()).unwrap();
    assert_ne!(a.client_id(), b.client_id());
}

#[test]
fn serving_gate_refuses_when_can_serve_is_clear() {
    // Synthetic cnc pages have flags == 0 (no node ever sets CAN_SERVE).
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-gate", 1 << 20, 1 << 20);
    let (gated, _p) = Engine::attach(
        dir.path(), "eng-gate",
        EngineConfig { serving_gate: true, ..EngineConfig::default() },
    ).unwrap();
    assert!(matches!(gated.try_submit(1, b"x"), Err(SubmitError::NotServing)));

    let (open, _p) = Engine::attach(dir.path(), "eng-gate", cfg()).unwrap();
    open.try_submit(1, b"x").expect("gate off: accepted");
}

#[test]
fn window_full_is_backpressure_and_failed_ring_write_releases_the_slot() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    // Tiny ingress ring (64 B) that nothing drains; generous window.
    make_instance_caps(dir.path(), "eng-bp", 64, 1 << 20);
    let (s, _p) = Engine::attach(dir.path(), "eng-bp", cfg()).unwrap();
    // Fill the ring; every failed write must RELEASE its slot (inflight
    // returns to the pre-call value), so the window never leaks.
    let mut accepted = 0u64;
    loop {
        match s.try_submit(accepted, &[0u8; 8]) {
            Ok(()) => accepted += 1,
            Err(SubmitError::Backpressure) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert_eq!(s.inflight(), accepted, "ring-full rejections must not consume window");

    // Window-full backpressure, distinct path: window 2, roomy ring.
    let dir2 = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir2.path(), "eng-win", 1 << 20, 1 << 20);
    let (s2, _p2) = Engine::attach(
        dir2.path(), "eng-win",
        EngineConfig { max_inflight: 2, serving_gate: false, ..EngineConfig::default() },
    ).unwrap();
    s2.try_submit(1, b"a").unwrap();
    s2.try_submit(2, b"b").unwrap();
    assert!(matches!(s2.try_submit(3, b"c"), Err(SubmitError::Backpressure)));
}

#[test]
fn payload_too_large_fails_loud_at_the_door() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-big", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(
        dir.path(), "eng-big",
        EngineConfig { max_payload: Some(16), serving_gate: false, ..EngineConfig::default() },
    ).unwrap();
    match s.try_submit(1, &[0u8; 17]) {
        Err(SubmitError::PayloadTooLarge { len: 17, max: 16 }) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(s.inflight(), 0, "refused submit must not hold a slot");
}
```

(`make_instance_caps` = `make_instance` with explicit ingress/egress capacities, same as `synthetic.rs`'s version.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc2_client --test engine_synthetic`
Expected: FAIL to compile ("cannot find `Engine`").

- [ ] **Step 3: Implement `engine.rs` (attach + SendHalf + types)**

`attach` (mirror `Client::connect`'s order — subscribe BEFORE returning, so nothing published after attach is missable):

```rust
impl Engine {
    pub fn attach(
        instance_dir: &Path,
        app_id: &str,
        cfg: EngineConfig,
    ) -> Result<(SendHalf, PollHalf), ClientError> {
        let cnc = CncPage::open_file(&instance_dir.join(CNC_FILE), app_id)?;
        let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
        let instance_id = cnc.meta().instance_id;
        let (ingress, _ic) = MpscRing::open(&instance_dir.join(INGRESS_RING))?.into_split();
        let (query, _qc) = MpscRing::open(&instance_dir.join(QUERY_RING))?.into_split();
        let egress_service = BroadcastRing::open(&instance_dir.join(EGRESS_SERVICE))?.subscribe();
        let egress_node = BroadcastRing::open(&instance_dir.join(EGRESS_NODE))?.subscribe();
        let shared = Arc::new(Shared {
            cnc,
            client_id,
            instance_id,
            table: SlotTable::new(cfg.max_inflight, cfg.start_seq),
            stats: StatCells::default(),
            dead: AtomicBool::new(false),
            restart: Mutex::new(None),
            t0: Instant::now(),
            timeout_ns: cfg.request_timeout.as_nanos() as u64,
            max_payload: cfg.max_payload,
            serving_gate: cfg.serving_gate,
        });
        Ok((
            SendHalf { shared: Arc::clone(&shared), ingress, query },
            PollHalf { shared, egress_service, egress_node, buf: Vec::new(), cycle: 0 },
        ))
    }
}
```

Send path (one private fn, two public wrappers):

```rust
impl SendHalf {
    fn send(
        &self,
        ring: &MpscProducer,
        msg_type: u16,
        flags: u16,
        kind: ReqKind,
        user_data: u64,
        bytes: &[u8],
    ) -> Result<(), SubmitError> {
        let s = &self.shared;
        if s.dead.load(Ordering::Acquire) {
            let (attached, current) = s.restart.lock().unwrap().unwrap_or((s.instance_id, 0));
            return Err(SubmitError::InstanceRestart { attached, current });
        }
        if s.serving_gate
            && s.cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0
        {
            return Err(SubmitError::NotServing);
        }
        if let Some(max) = s.max_payload {
            if bytes.len() > max {
                return Err(SubmitError::PayloadTooLarge { len: bytes.len(), max });
            }
        }
        let deadline_ns = s.t0.elapsed().as_nanos() as u64 + s.timeout_ns;
        let seq = s
            .table
            .claim(user_data, kind, deadline_ns)
            .map_err(|_| SubmitError::Backpressure)?; // WindowFull and SlotBusy alike
        match ring.try_write(msg_type, flags, extra_client(s.client_id, seq as u32), bytes) {
            Ok(()) => {
                s.stats.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                s.table.release(seq);
                match e {
                    RingError::Full => Err(SubmitError::Backpressure),
                    RingError::TooLarge { len, max } => {
                        Err(SubmitError::PayloadTooLarge { len, max })
                    }
                    other => Err(SubmitError::Ring(other)),
                }
            }
        }
    }

    pub fn try_submit(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        self.send(&self.ingress, MSG_V2_SUBMIT, 0, ReqKind::Submit, user_data, cmd_bytes)
    }

    pub fn try_query(
        &self,
        user_data: u64,
        query_bytes: &[u8],
        c: Consistency,
    ) -> Result<(), SubmitError> {
        let flags = match c {
            Consistency::Linearizable => FLAG_V2_LINEARIZABLE,
            Consistency::Snapshot => 0,
        };
        self.send(&self.query, MSG_V2_QUERY, flags, ReqKind::Query, user_data, query_bytes)
    }
}

impl Clone for SendHalf {
    fn clone(&self) -> Self {
        SendHalf {
            shared: Arc::clone(&self.shared),
            ingress: self.ingress.clone(), // per-clone producer cache (MpscProducer contract)
            query: self.query.clone(),
        }
    }
}
```

`PollHalf` gets only its struct definition + a `poll` stub returning 0 in this task (Task 4 fills it); mark `#[allow(dead_code)]` on stub fields if clippy complains, removed in Task 4. Module doc for `engine.rs`: state the central contract verbatim from the spec — "every accepted try_submit/try_query produces exactly one completion for its user_data, in bounded time" — and the byte contract paragraph (engine is format-free; against today's uc2_service the payload must be `bincode(Command)`; cite spec §4).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc2_client --test engine_synthetic`
Expected: PASS (4 tests).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/engine.rs uc2_client/src/lib.rs uc2_client/tests/engine_synthetic.rs
git commit -m "feat(uc2_client): Engine attach + SendHalf (serving gate, window, fail-loud payload bound)"
```

---

### Task 4: `engine.rs` — PollHalf: poll, sweep, restart, drain_abort, stats

**Files:**
- Modify: `uc2_client/src/engine.rs`
- Test: `uc2_client/tests/engine_synthetic.rs` (extend)

**Interfaces:**
- Consumes: Task 2/3 items; `BroadcastConsumer::{try_read, wait_handle}`, `RingWaitHandle` (uc_protocol); ipc consts `MSG_V2_RESPONSE/MSG_V2_NOT_LEADER/MSG_V2_RETRY/FLAG_V2_IS_QUERY/client_from_extra`.
- Produces (consumed by Tasks 6/8):

```rust
pub struct Completion<'a> {
    pub user_data: u64,
    pub position: Option<u64>,   // Some for Response (stripped prefix)
    pub outcome: Outcome<'a>,
}
#[derive(Debug)]
pub enum Outcome<'a> {
    Response(&'a [u8]),                       // borrow of the engine read buffer
    NotLeader { hint: Option<u32> },
    Retry,
    TimedOut,
    InstanceRestart { attached: u128, current: u128 },
}
impl PollHalf {
    pub fn poll(&mut self, cb: impl FnMut(Completion<'_>)) -> usize; // completions emitted
    pub fn drain_abort(&mut self, cb: impl FnMut(u64 /* user_data */));
    pub fn wait_handle(&self) -> uc_protocol::ring::RingWaitHandle;  // service broadcast
    pub fn stats(&self) -> EngineStats;
}
```

**Behavior to implement** (each line traces to spec §4/§6):
- Per `poll` call: drain up to 128 records from EACH broadcast (service first), bounded work per call.
- Records for other client_ids: skip. `MSG_V2_RESPONSE`: payload `< 8` bytes → `stats.corrupt += 1`, skip WITHOUT resolving (the deadline backstops it). Kind from `FLAG_V2_IS_QUERY`; `table.resolve(wire_seq, Some(kind))`: `Won` → emit `Response(&buf[8..])` with `position = Some(u64 LE prefix)`, `stats.responses += 1`; `KindMismatch` → `stats.kind_mismatch += 1` (T14: slot survives); `Miss` → `stats.duplicates += 1`.
- `MSG_V2_NOT_LEADER`: hint = `u64 LE` payload, malformed → `[0xff;8]` fallback → `None` (copy the exact defensive decode from old `matcher.rs:185-191`); kind-agnostic resolve (`expect_kind: None`); `Won` → emit, `stats.not_leader += 1`; `Miss` → silent (stale, no side effect to guard).
- `MSG_V2_RETRY`: kind-agnostic; `Won` → emit `Retry`, `stats.retry += 1`.
- `Err(RingError::Overwritten)` from `try_read`: `stats.overwritten += 1`, CONTINUE — do NOT fail in-flight (spec §4 item 6: the engine cannot know which responses were in the lost window; the deadline is the honest backstop). Other `Err`: `stats.corrupt += 1`, continue.
- Maintenance, amortized: `self.cycle += 1`; when `cycle % 64 == 0` → (a) restart check: `cnc.try_instance_id()` returning `None` (torn header mid-recreate — M5 final-review semantics) or a different id → record `(attached, current)` in `shared.restart` (torn → `current = 0`), `dead.store(true)`, `stats.restarts += 1`, `table.drain_abort` emitting `InstanceRestart { attached, current }` for every in-flight; (b) deadline sweep: `table.sweep(t0.elapsed() as nanos, ..)` emitting `TimedOut`, `stats.timed_out += n`.
- After `dead` is set, `poll` still drains rings (records may be stale) but resolves nothing new (table is empty) — no special casing needed.

- [ ] **Step 1: Write the failing tests (extend `engine_synthetic.rs`)**

```rust
use uc_protocol::ring::BroadcastRing;
use uc_protocol::v2::ipc::{
    FLAG_V2_IS_QUERY, MSG_V2_NOT_LEADER, MSG_V2_RESPONSE, MSG_V2_RETRY, extra_client,
};

/// Collect completions into owned tuples (payload copied out of the borrow).
fn drain(poll: &mut uc2_client::PollHalf) -> Vec<(u64, Option<u64>, String)> {
    let mut out = Vec::new();
    poll.poll(|c| {
        let tag = match &c.outcome {
            uc2_client::Outcome::Response(b) => format!("resp:{}", b.len()),
            uc2_client::Outcome::NotLeader { hint } => format!("notleader:{hint:?}"),
            uc2_client::Outcome::Retry => "retry".into(),
            uc2_client::Outcome::TimedOut => "timeout".into(),
            uc2_client::Outcome::InstanceRestart { .. } => "restart".into(),
        };
        out.push((c.user_data, c.position, tag));
    });
    out
}

/// Egress producer for injecting answers into a synthetic dir.
fn egress(dir: &std::path::Path) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join("egress_service.broadcast")).unwrap().producer()
}

#[test]
fn response_resolves_with_position_and_payload_and_duplicate_is_counted() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-resp", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-resp", cfg()).unwrap();
    s.try_submit(0xCAFE, b"cmd").unwrap();

    let mut payload = 4096u64.to_le_bytes().to_vec();
    payload.extend_from_slice(b"answer");
    // wire_seq 0: first request of a fresh engine (start_seq 0).
    let mut prod = egress(dir.path());
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &payload).unwrap();
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &payload).unwrap(); // dup

    let got = drain(&mut p);
    assert_eq!(got, vec![(0xCAFE, Some(4096), "resp:6".to_string())]);
    assert_eq!(s.stats().duplicates, 1, "second delivery counted, not re-emitted");
    assert_eq!(s.inflight(), 0);
}

#[test]
fn kind_mismatched_response_is_dropped_counted_and_the_real_answer_still_lands() {
    // T14 moved from matcher.rs: query-flagged delivery vs a Submit slot.
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-t14", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-t14", cfg()).unwrap();
    s.try_submit(7, b"cmd").unwrap();

    let mut wrong = 0u64.to_le_bytes().to_vec();
    wrong.extend_from_slice(b"x");
    let mut prod = egress(dir.path());
    prod.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, extra_client(s.client_id(), 0), &wrong).unwrap();
    assert!(drain(&mut p).is_empty(), "kind mismatch must not complete");
    assert_eq!(s.stats().kind_mismatch, 1);
    assert_eq!(s.inflight(), 1, "slot survives for the real answer");

    let mut right = 9u64.to_le_bytes().to_vec();
    right.extend_from_slice(b"ok");
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &right).unwrap();
    assert_eq!(drain(&mut p), vec![(7, Some(9), "resp:2".to_string())]);
}

#[test]
fn not_leader_and_retry_resolve_kind_agnostic_with_hint_decode() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-nl", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-nl", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::Linearizable).unwrap(); // wire_seq 0
    s.try_submit(2, b"c").unwrap();                            // wire_seq 1

    let mut prod = egress(dir.path());
    prod.write(MSG_V2_NOT_LEADER, 0, extra_client(s.client_id(), 0), &2u64.to_le_bytes()).unwrap();
    prod.write(MSG_V2_RETRY, 0, extra_client(s.client_id(), 1), &[]).unwrap();

    let got = drain(&mut p);
    assert_eq!(got, vec![
        (1, None, "notleader:Some(2)".to_string()),
        (2, None, "retry".to_string()),
    ]);
}

#[test]
fn deadline_sweep_times_out_unanswered_requests() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-to", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(
        dir.path(), "eng-to",
        EngineConfig {
            request_timeout: Duration::from_millis(50),
            serving_gate: false,
            ..EngineConfig::default()
        },
    ).unwrap();
    s.try_submit(42, b"never answered").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    // Maintenance is amortized every 64 poll cycles — loop until it fires.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = drain(&mut p);
        if got == vec![(42, None, "timeout".to_string())] { break; }
        assert!(got.is_empty(), "unexpected completions: {got:?}");
        assert!(std::time::Instant::now() < deadline, "sweep never fired");
    }
    assert_eq!(s.stats().timed_out, 1);
    assert_eq!(s.inflight(), 0, "nothing accepted may leak");
}

#[test]
fn instance_restart_fails_all_inflight_and_poisons_the_send_half() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-rs", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-rs", cfg()).unwrap();
    let attached = s.instance_id();
    s.try_submit(1, b"a").unwrap();
    s.try_submit(2, b"b").unwrap();

    // Recreate the cnc in place with a fresh instance_id (Node::start's boot
    // behavior; same file/inode, mmap observes the new bytes).
    CncPage::create_file(
        &dir.path().join("cnc2.dat"),
        &meta_with_instance("eng-rs", 0xDEAD_BEEF),
    ).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    while got.len() < 2 {
        got.extend(drain(&mut p));
        assert!(std::time::Instant::now() < deadline, "restart sweep never fired");
    }
    got.sort();
    assert_eq!(got[0], (1, None, "restart".to_string()));
    assert_eq!(got[1], (2, None, "restart".to_string()));

    match s.try_submit(3, b"c") {
        Err(SubmitError::InstanceRestart { attached: a, current }) => {
            assert_eq!(a, attached);
            assert_eq!(current, 0xDEAD_BEEF);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn broadcast_overwrite_is_a_stat_and_the_deadline_backstops_hung_requests() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    // Tiny egress broadcast: easy to lap.
    make_instance_caps(dir.path(), "eng-ow", 1 << 20, 256);
    let (s, mut p) = Engine::attach(
        dir.path(), "eng-ow",
        EngineConfig {
            request_timeout: Duration::from_millis(100),
            serving_gate: false,
            ..EngineConfig::default()
        },
    ).unwrap();
    s.try_submit(5, b"lost").unwrap();

    // Lap the consumer with junk addressed to nobody.
    let mut prod = egress(dir.path());
    for _ in 0..64 {
        prod.write(MSG_V2_RESPONSE, 0, extra_client(u32::MAX, 0), &[0u8; 32]).unwrap();
    }
    let _ = drain(&mut p); // absorbs the Overwritten signal
    assert!(s.stats().overwritten >= 1, "overwrite must be counted");

    // The affected request must NOT be eagerly failed — it resolves via the
    // deadline (spec §4 item 6).
    std::thread::sleep(Duration::from_millis(150));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = drain(&mut p);
        if got == vec![(5, None, "timeout".to_string())] { break; }
        assert!(got.is_empty());
        assert!(std::time::Instant::now() < deadline);
    }
}

#[test]
fn wire_seq_wrap_roundtrips_through_a_real_ring() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-wrap", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(
        dir.path(), "eng-wrap",
        EngineConfig { start_seq: u32::MAX as u64 - 4, serving_gate: false, ..EngineConfig::default() },
    ).unwrap();
    let mut prod = egress(dir.path());
    for i in 0..16u64 {
        let wire = (u32::MAX as u64 - 4 + i) as u32; // == seq as u32, across the wrap
        s.try_submit(i, b"w").unwrap();
        let mut payload = i.to_le_bytes().to_vec();
        payload.extend_from_slice(b"z");
        prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), wire), &payload).unwrap();
        assert_eq!(drain(&mut p), vec![(i, Some(i), "resp:1".to_string())], "iteration {i}");
    }
}
```

(`meta_with_instance` = the `meta` helper with an explicit `instance_id` parameter — same shape as `timeout_and_restart.rs`'s.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc2_client --test engine_synthetic`
Expected: new tests FAIL (poll stub returns 0 → drains empty / compile errors for `Outcome`).

- [ ] **Step 3: Implement PollHalf**

Structure (the record handler is one private method so the two rings share it):

```rust
impl PollHalf {
    pub fn poll(&mut self, mut cb: impl FnMut(Completion<'_>)) -> usize {
        self.cycle += 1;
        let mut emitted = 0usize;
        if self.cycle % 64 == 0 {
            emitted += self.maintenance(&mut cb);
        }
        for which in [RingSel::Service, RingSel::Node] {
            for _ in 0..128 {
                // (split borrows: take the ring out of self or use a helper fn
                // taking (&mut BroadcastConsumer, &Shared, &mut Vec<u8>, ...))
                match ring.try_read(&mut self.buf) {
                    Ok(Some(rec)) => emitted += handle_record(&self.shared, &rec, &self.buf, &mut cb),
                    Ok(None) => break,
                    Err(RingError::Overwritten) => {
                        self.shared.stats.overwritten.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.shared.stats.corrupt.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        emitted
    }
}
```

`handle_record` implements the per-msg_type behavior spelled out in this task's header. `maintenance` runs the restart check then the sweep, emitting through `cb` and returning the count. NOTE on the restart check: read `try_instance_id()` ONCE; `None` → `current = 0` (torn header, M5 final-review semantics — cite `client.rs`'s old comment, then delete it in Task 7).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc2_client --test engine_synthetic`
Expected: PASS (11 tests total).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/engine.rs uc2_client/tests/engine_synthetic.rs
git commit -m "feat(uc2_client): PollHalf — completion drain, deadline sweep, restart detection, stats"
```

---

### Task 5: `ticket.rs` — `Ticket<R>`: blocking handle + Future

**Files:**
- Create: `uc2_client/src/ticket.rs`
- Modify: `uc2_client/src/lib.rs` (add `mod ticket; pub use ticket::Ticket;`)

**Interfaces:**
- Consumes: `ClientError` only.
- Produces (Task 6 consumes `TicketCore` internals; users consume `Ticket`):

```rust
pub(crate) struct TicketCore { /* Mutex<State> + Condvar */ }
impl TicketCore {
    pub(crate) fn new() -> TicketCore;
    /// First resolution wins; later calls are ignored (belt — the engine
    /// already guarantees exactly-once).
    pub(crate) fn resolve(&self, r: Result<(u64, Vec<u8>), ClientError>);
}
pub struct Ticket<R> { /* Arc<TicketCore> + PhantomData<fn() -> R> */ }
impl<R: serde::de::DeserializeOwned> Ticket<R> {
    pub fn wait(self) -> Result<R, ClientError>;
    pub fn wait_timeout(self, d: Duration) -> Result<R, ClientError>; // Err(Timeout(d)) on elapse
}
impl<R: DeserializeOwned> Future for Ticket<R> { type Output = Result<R, ClientError>; }
pub(crate) fn ticket_pair<R>() -> (Ticket<R>, Arc<TicketCore>);
```

State machine: `State { done: Option<Result<(u64, Vec<u8>), ClientError>>, waker: Option<Waker> }`. `resolve`: lock → if `done.is_some()` return → set `done` → take waker → unlock → `notify_all` → `waker.wake()`. `wait`: condvar loop on `done`. Future `poll`: if `done` take → `Ready(decode)`; else store `cx.waker().clone()`, `Pending`. Decode = `bincode::serde::decode_from_slice::<R>(&bytes, standard())` mapped to `ClientError::Decode` — the position is discarded at this layer (engine users get it; A is the convenience tier, YAGNI). Polling an already-consumed future panics with `"Ticket polled after completion"` (document it).

- [ ] **Step 1: Write the failing tests (bottom of `ticket.rs`)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// Hand-rolled block_on: thread-parker waker, no runtime dep (spec §9).
    fn block_on<F: std::future::Future>(mut fut: F) -> F::Output {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn raw(thread: std::thread::Thread) -> RawWaker {
            fn clone(p: *const ()) -> RawWaker {
                raw(unsafe { (*(p as *const std::thread::Thread)).clone() })
            }
            fn wake(p: *const ()) {
                unsafe { Box::from_raw(p as *mut std::thread::Thread) }.unpark();
            }
            fn wake_by_ref(p: *const ()) {
                unsafe { &*(p as *const std::thread::Thread) }.unpark();
            }
            fn drop_fn(p: *const ()) {
                drop(unsafe { Box::from_raw(p as *mut std::thread::Thread) });
            }
            static VT: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_fn);
            RawWaker::new(Box::into_raw(Box::new(thread)) as *const (), &VT)
        }
        let waker = unsafe { Waker::from_raw(raw(std::thread::current())) };
        let mut cx = Context::from_waker(&waker);
        let mut fut = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => std::thread::park(),
            }
        }
    }

    fn resolved_bytes(v: u64) -> Result<(u64, Vec<u8>), crate::ClientError> {
        Ok((7, bincode::serde::encode_to_vec(v, bincode::config::standard()).unwrap()))
    }

    #[test]
    fn resolve_then_wait_decodes() {
        let (t, core) = ticket_pair::<u64>();
        core.resolve(resolved_bytes(42));
        assert_eq!(t.wait().unwrap(), 42);
    }

    #[test]
    fn wait_blocks_until_a_late_resolve() {
        let (t, core) = ticket_pair::<u64>();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            core.resolve(resolved_bytes(9));
        });
        assert_eq!(t.wait().unwrap(), 9);
        h.join().unwrap();
    }

    #[test]
    fn wait_timeout_elapses_to_timeout_error() {
        let (t, _core) = ticket_pair::<u64>();
        let err = t.wait_timeout(Duration::from_millis(30)).unwrap_err();
        assert!(matches!(err, crate::ClientError::Timeout(d) if d == Duration::from_millis(30)));
    }

    #[test]
    fn future_resolves_under_hand_rolled_block_on() {
        let (t, core) = ticket_pair::<u64>();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            core.resolve(resolved_bytes(11));
        });
        assert_eq!(block_on(t).unwrap(), 11);
        h.join().unwrap();
    }

    #[test]
    fn second_resolve_is_ignored() {
        let (t, core) = ticket_pair::<u64>();
        core.resolve(resolved_bytes(1));
        core.resolve(Err(crate::ClientError::ShutDown)); // must not clobber
        assert_eq!(t.wait().unwrap(), 1);
    }

    #[test]
    fn error_resolution_surfaces_the_error() {
        let (t, core) = ticket_pair::<u64>();
        core.resolve(Err(crate::ClientError::Retry));
        assert!(matches!(t.wait(), Err(crate::ClientError::Retry)));
    }

    #[test]
    fn dropping_the_ticket_then_resolving_is_harmless() {
        let (t, core) = ticket_pair::<u64>();
        drop(t);
        core.resolve(resolved_bytes(5)); // orphan: no panic, no leak beyond core's Arc
        assert_eq!(Arc::strong_count(&core), 1, "ticket side released its ref");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc2_client ticket`
Expected: FAIL to compile ("cannot find `ticket_pair`").

- [ ] **Step 3: Implement per the Interfaces block**

Note on `ClientError::Timeout` comparison: the enum derives no `PartialEq` — the tests use `matches!` with a guard, which works as written.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc2_client ticket`
Expected: PASS (7 tests).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/ticket.rs uc2_client/src/lib.rs
git commit -m "feat(uc2_client): Ticket<R> — one slot-backed cell serving wait() and .await"
```

---

### Task 6: `pipelined.rs` — `PipelinedClient` + driver thread

**Files:**
- Create: `uc2_client/src/pipelined.rs`
- Modify: `uc2_client/src/lib.rs` (add `mod pipelined; pub use pipelined::{PipelinedClient, PipelinedConfig};`)
- Test: `uc2_client/tests/pipelined.rs` (new)

**Interfaces:**
- Consumes: `Engine::attach`, `SendHalf` (try_submit/try_query/leader_hint/stats/client_id/instance_id), `PollHalf` (poll/drain_abort/wait_handle), `SubmitError`, `Completion`, `Outcome`, `Consistency` (Tasks 3-4); `Ticket`, `TicketCore`, `ticket_pair` (Task 5); `WaitStrategy`, `Idle` (Task 1); `ClientError`.
- Produces (Task 7's shim consumes all of it):

```rust
pub struct PipelinedConfig {
    pub driver_wait: WaitStrategy,     // default Park
    pub max_inflight: u32,             // default 4096
    pub request_timeout: Duration,     // default 10s
    pub serving_gate: bool,            // default true
}
impl Default for PipelinedConfig { /* values above */ }

pub struct PipelinedClient { /* Send + Sync; share as Arc */ }
impl PipelinedClient {
    pub fn connect(instance_dir: &Path, app_id: &str, cfg: PipelinedConfig)
        -> Result<PipelinedClient, ClientError>;
    pub fn submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C)
        -> Result<Ticket<R>, ClientError>;            // blocks ≤1s grace on backpressure
    pub fn try_submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C)
        -> Result<Ticket<R>, ClientError>;            // fail-fast
    pub fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(&self, q: &Q)
        -> Result<Ticket<QR>, ClientError>;
    pub fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(&self, q: &Q)
        -> Result<Ticket<QR>, ClientError>;
    pub fn client_id(&self) -> u32;
    pub fn instance_id(&self) -> u128;
    pub fn leader_hint(&self) -> Option<u32>;
    pub fn stats(&self) -> EngineStats;
    pub fn shutdown(self);   // also runs on Drop
}
```

**Key mechanics** (pin these in code comments; they carry the design):

1. `user_data` = `Arc::into_raw(Arc<TicketCore>) as u64` (64-bit target asserted by `compile_error!`). The engine's exactly-one-completion contract makes this leak-free: every accepted request's raw Arc is reclaimed by exactly one `Arc::from_raw` in the driver's completion callback (or the shutdown drain). A submit that the ENGINE REFUSES reclaims immediately on the error path.
2. One driver thread, hand-spawned (NOT `AgentRunner`: its contract forbids blocking in the duty cycle, and the Park strategy parks up to 1ms). Loop: `poll(resolve_cb)`; on `stop` flag → `poll_half.drain_abort(|ud| resolve ShutDown + reclaim)` INSIDE the thread before exiting (the PollHalf lives and dies on the driver thread — no handoff). `shutdown`/`Drop`: `stop.store(true)`, `wait_handle.wake()` (interrupt a park), join.
3. Driver idle, per `driver_wait`: `BusySpin` → `spin_loop()`; `BackoffYield`/`Backoff` → `Idle::for_strategy(..).idle()`, reset on progress; `Park` → the broadcast futex protocol (reference: `uc_protocol/src/ring/broadcast.rs`'s `wake_all_unblocks_two_consumers` test): `let seq = wh.current_seq(); wh.arm(); if poll(..) == 0 && !stop { wh.park(seq, Duration::from_millis(1)); } wh.disarm();` — TIMED park (1ms cap): the handle watches only the service broadcast; the 1ms rung bounds pickup latency for the rare egress_node records (spec §5).
4. `SendHalf` is `!Sync` → `PipelinedClient` holds `Mutex<SendHalf>`; the critical section is claim+try_write (~100ns). Max-perf callers use the Engine directly with per-thread `SendHalf` clones (say so in the rustdoc).
5. Outcome→ticket mapping (driver): `Response` → `Ok((position, bytes.to_vec()))` (the ONE copy, spec §5); `NotLeader{hint}` → `Err(NotLeader{hint})`; `Retry` → `Err(Retry)`; `TimedOut` → `Err(Timeout(request_timeout))`; `InstanceRestart{attached,current}` → `Err(InstanceRestart{attached,current})`.
6. `submit` grace loop (parity with old `Client`: `BACKPRESSURE_GRACE = 1s`, retry sleep 100µs): `Backpressure` → sleep+retry; `NotServing` → sleep 1ms + retry (elections are 150-300ms); grace expiry → `Backpressure ⇒ ClientError::BackpressureFull`, `NotServing ⇒ ClientError::NotLeader{hint: self.leader_hint()}`; `PayloadTooLarge{len,max}` → `ClientError::PayloadTooLarge{len,max}` (new variant, Task 7 adds it — in THIS task map to `ClientError::Decode(format!(...))` temporarily? NO: add the variant here, `error.rs` is modified in this task instead), `InstanceRestart` → `ClientError::InstanceRestart`, `Ring(e)` → `ClientError::Ring(e)`. Every error path reclaims the leaked Arc.

(Adjust: `error.rs` gains `#[error("payload too large: {len} > {max}")] PayloadTooLarge { len: usize, max: usize }` in THIS task, since `pipelined.rs` needs it.)

- [ ] **Step 1: Write the failing tests (`tests/pipelined.rs`)**

Reuse `roundtrip.rs`'s harness verbatim: the `Cmd`/`CountSm` state machine, `node_config`, `wait_until` (copy them in — integration tests don't share modules). Node+service boot per test, `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`.

```rust
fn connect(dir: &std::path::Path) -> uc2_client::PipelinedClient {
    uc2_client::PipelinedClient::connect(
        dir, "pipe-test", uc2_client::PipelinedConfig::default(),
    ).unwrap()
}

#[test]
fn pipelined_submits_all_resolve_and_totals_are_a_permutation_free_prefix() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"), CountSm::default(),
    ).start().unwrap();

    let client = connect(dir.path());
    // WINDOW of outstanding tickets — the whole point of the layer.
    let tickets: Vec<uc2_client::Ticket<u64>> =
        (0..100).map(|_| client.submit(&Cmd::Add(1)).unwrap()).collect();
    let mut totals: Vec<u64> = tickets.into_iter().map(|t| t.wait().unwrap()).collect();
    // A single client's submits are applied in submission order (one MPSC
    // producer, FIFO ring, in-order apply): totals must be exactly 1..=100.
    totals.sort_unstable();
    assert_eq!(totals, (1..=100).collect::<Vec<u64>>());
}

#[test]
fn async_await_resolves_against_a_real_cluster() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"), CountSm::default(),
    ).start().unwrap();

    let client = connect(dir.path());
    let got: u64 = block_on(client.submit::<_, u64>(&Cmd::Add(7)).unwrap()).unwrap();
    assert_eq!(got, 7);
}

#[test]
fn queries_ride_the_same_engine() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"), CountSm::default(),
    ).start().unwrap();

    let client = connect(dir.path());
    client.submit::<_, u64>(&Cmd::Add(3)).unwrap().wait().unwrap();
    let snap: u64 = client.query_snapshot(&()).unwrap().wait().unwrap();
    assert_eq!(snap, 3);
    let lin: u64 = client.query_linearizable(&()).unwrap().wait().unwrap();
    assert_eq!(lin, 3);
}

#[test]
fn dropping_a_ticket_orphans_cleanly_and_later_traffic_is_unaffected() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"), CountSm::default(),
    ).start().unwrap();

    let client = connect(dir.path());
    drop(client.submit::<_, u64>(&Cmd::Add(1)).unwrap()); // abandon interest
    // The orphan's response is discarded by the driver; nothing wedges:
    let got: u64 = client.submit::<_, u64>(&Cmd::Add(1)).unwrap().wait().unwrap();
    assert_eq!(got, 2);
}

#[test]
fn shutdown_fails_inflight_tickets_with_shutdown() {
    // Synthetic dead dir (no node answers), serving gate off so the submit
    // is accepted and then never resolved — until shutdown drains it.
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "pipe-shut", 1 << 20, 1 << 20); // synthetic.rs helper, copied in
    let client = uc2_client::PipelinedClient::connect(
        dir.path(), "pipe-shut",
        uc2_client::PipelinedConfig { serving_gate: false, ..Default::default() },
    ).unwrap();
    let t = client.submit::<_, u64>(&1u8).unwrap();
    client.shutdown();
    assert!(matches!(t.wait(), Err(uc2_client::ClientError::ShutDown)));
}

#[test]
fn every_wait_strategy_round_trips() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"), CountSm::default(),
    ).start().unwrap();

    for ws in [
        uc2_client::WaitStrategy::BusySpin,
        uc2_client::WaitStrategy::BackoffYield,
        uc2_client::WaitStrategy::Backoff,
        uc2_client::WaitStrategy::Park,
    ] {
        let client = uc2_client::PipelinedClient::connect(
            dir.path(), "pipe-test",
            uc2_client::PipelinedConfig { driver_wait: ws, ..Default::default() },
        ).unwrap();
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap().wait().unwrap();
        client.shutdown();
    }
}
```

(Include the same hand-rolled `block_on` as Task 5's — copy it; integration tests can't reach `#[cfg(test)]` items.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc2_client --test pipelined`
Expected: FAIL to compile ("cannot find `PipelinedClient`").

- [ ] **Step 3: Implement `pipelined.rs`**

Driver skeleton (the load-bearing part; everything else is the Interfaces block):

```rust
fn spawn_driver(
    mut poll: PollHalf,
    stop: Arc<AtomicBool>,
    ws: WaitStrategy,
    request_timeout: Duration,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name("uc2-pipelined-driver".into()).spawn(move || {
        let wh = poll.wait_handle();
        let mut resolve = |c: Completion<'_>| {
            // SAFETY: user_data is the raw Arc<TicketCore> leaked by submit;
            // the engine emits exactly one completion per accepted request,
            // so this is the one matching from_raw.
            let core = unsafe { Arc::from_raw(c.user_data as *const TicketCore) };
            core.resolve(match c.outcome {
                Outcome::Response(bytes) => Ok((c.position.unwrap_or(0), bytes.to_vec())),
                Outcome::NotLeader { hint } => Err(ClientError::NotLeader { hint }),
                Outcome::Retry => Err(ClientError::Retry),
                Outcome::TimedOut => Err(ClientError::Timeout(request_timeout)),
                Outcome::InstanceRestart { attached, current } => {
                    Err(ClientError::InstanceRestart { attached, current })
                }
            });
        };
        let mut idle = Idle::for_strategy(ws);
        while !stop.load(Ordering::Relaxed) {
            let n = poll.poll(&mut resolve);
            if n > 0 {
                idle = Idle::for_strategy(ws); // progress resets the ladder
                continue;
            }
            match ws {
                WaitStrategy::BusySpin => std::hint::spin_loop(),
                WaitStrategy::BackoffYield | WaitStrategy::Backoff => idle.idle(),
                WaitStrategy::Park => {
                    let seq = wh.current_seq();
                    wh.arm();
                    if poll.poll(&mut resolve) == 0 && !stop.load(Ordering::Relaxed) {
                        wh.park(seq, Duration::from_millis(1));
                    }
                    wh.disarm();
                }
            }
        }
        // Shutdown drain ON this thread — the PollHalf never crosses threads.
        poll.drain_abort(|ud| {
            let core = unsafe { Arc::from_raw(ud as *const TicketCore) };
            core.resolve(Err(ClientError::ShutDown));
        });
    })
}
```

`submit`/`try_submit`/`query_*` per the Interfaces block and Key mechanics #1/#6. `shutdown(self)` = `Drop`-shared inner fn: `stop.store(true); wake.wake(); join()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc2_client --test pipelined`
Expected: PASS (6 tests). These boot real nodes — allow ~a minute.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/pipelined.rs uc2_client/src/error.rs uc2_client/src/lib.rs uc2_client/tests/pipelined.rs
git commit -m "feat(uc2_client): PipelinedClient — driver thread, wait strategies, sync+async tickets"
```

---

### Task 7: `client.rs` becomes a shim; `matcher.rs` deleted; existing tests are the oracle

**Files:**
- Modify: `uc2_client/src/client.rs` (rewrite as shim), `uc2_client/src/error.rs` (doc notes), `uc2_client/src/lib.rs` (exports)
- Delete: `uc2_client/src/matcher.rs`
- Test: existing `uc2_client/tests/{roundtrip,synthetic,timeout_and_restart,torn_header}.rs` — UNCHANGED

**Interfaces:**
- Consumes: `PipelinedClient`/`PipelinedConfig` (Task 6, exact signatures above).
- Produces: `Client` with the EXACT current public surface: `connect(&Path, &str) -> Result<Client, ClientError>`, `client_id() -> u32`, `instance_id() -> u128`, `kind_mismatch_drops() -> u64`, `leader_hint() -> Option<u32>`, `submit<C,R>(&self, &C) -> Result<R, ClientError>`, `query_snapshot<Q,QR>`, `query_linearizable<Q,QR>`, `shutdown(self)`.

**Behavior pins (why each config value):**
- `serving_gate: false` — today's `Client` writes regardless of `CAN_SERVE` and learns `NOT_LEADER` from the wire; `synthetic.rs` and `timeout_and_restart.rs` pin this (their instance dirs never set the flag).
- `request_timeout` from `UC2_CLIENT_TIMEOUT_MS` (default 10s), read at `connect` — `timeout_and_restart.rs` pins `Timeout(Duration::from_millis(200))` equality.
- `driver_wait: Park`, `max_inflight: 1024` (any value ≥ the old unbounded behavior's practical use; blocking callers rarely exceed thread count).
- `kind_mismatch_drops()` → `self.inner.stats().kind_mismatch`.
- `ClientError::ResponseOverwritten` becomes UNREACHABLE from `Client` (engine semantics: overwrite → stat + deadline backstop). Keep the variant (external matchers in `lincheck_v2/mod.rs`, `hard_crash.rs`, m6/m7 gates treat it identically to `Timeout` — verified 2026-08-15, classification-neutral) with a doc note: "as of the pipelined-client rework, the engine counts overwrites and lets the deadline backstop; this variant is retained for API compatibility and external matchers."

- [ ] **Step 1: Rewrite `client.rs`**

```rust
pub struct Client {
    inner: PipelinedClient,
}

impl Client {
    pub fn connect(instance_dir: &Path, app_id: &str) -> Result<Client, ClientError> {
        let request_timeout = std::env::var("UC2_CLIENT_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(10));
        let inner = PipelinedClient::connect(
            instance_dir,
            app_id,
            PipelinedConfig {
                driver_wait: WaitStrategy::Park,
                max_inflight: 1024,
                request_timeout,
                serving_gate: false, // pinned: pre-rework Client submits regardless
            },
        )?;
        Ok(Client { inner })
    }

    pub fn submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<R, ClientError> {
        self.inner.submit(cmd)?.wait()
    }
    // query_snapshot / query_linearizable: same one-liner shape.
    // client_id / instance_id / leader_hint / shutdown: direct delegation.
    pub fn kind_mismatch_drops(&self) -> u64 {
        self.inner.stats().kind_mismatch
    }
}
```

Keep the module-level rustdoc (updated: matcher description replaced by "a shim over `PipelinedClient` — one code path"). Move nothing else: the well-known file-name consts now live in `engine.rs` (Task 3); delete `client.rs`'s duplicates.

- [ ] **Step 2: Delete `matcher.rs`, update `lib.rs`**

```bash
git rm uc2_client/src/matcher.rs
```

`lib.rs` final export surface:

```rust
mod client; mod engine; mod error; mod pipelined; mod slots; mod ticket; mod wait;
pub use client::Client;
pub use engine::{Completion, Consistency, Engine, EngineConfig, EngineStats, Outcome, PollHalf, SendHalf, SubmitError};
pub use error::ClientError;
pub use pipelined::{PipelinedClient, PipelinedConfig};
pub use ticket::Ticket;
pub use wait::WaitStrategy;
```

Matcher's unit-test coverage already moved down: T14 kind-check → Task 2 (`kind_mismatch_leaves_the_slot...`) + Task 4 (`kind_mismatched_response_is_dropped...`); routing/skip/hint/overwrite → Task 4. The two `decode_response` byte-level tests (undersized payload, bincode failure) reappear as `Ticket` decode tests — add them to `ticket.rs` now:

```rust
#[test]
fn decode_failure_surfaces_as_decode_error() {
    let (t, core) = ticket_pair::<String>();
    core.resolve(Ok((0, vec![0xFF]))); // truncated bincode varint
    assert!(matches!(t.wait(), Err(crate::ClientError::Decode(_))));
}
```

(The "undersized payload" case is now structurally impossible — the ENGINE strips and validates the 8-byte prefix before the ticket ever sees bytes; note that in the test module doc.)

- [ ] **Step 3: Run the compat oracle — all four existing test files, UNCHANGED**

Run: `cargo test -p uc2_client`
Expected: PASS — especially `roundtrip.rs` (real cluster, ids, monotone totals), `synthetic.rs` (BackpressureFull after ≥900ms grace; injected RETRY), `timeout_and_restart.rs` (`Timeout(200ms)` exactly, then `InstanceRestart{attached, current}` with both ids), `torn_header.rs` (`current: 0` on a torn/zeroed header). If any fails, the SHIM (or engine) is wrong — do not edit the test.

- [ ] **Step 4: Run the wider blast radius**

Run: `cargo test -p uc2_service && cargo test -p uc2_node --test query_barrier`
Expected: PASS (both use `Client` against real clusters).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A uc2_client
git commit -m "refactor(uc2_client): Client is a shim over PipelinedClient; matcher.rs deleted — one code path"
```

---

### Task 8: m5_gate client role on the public Engine + full verification

**Files:**
- Modify: `uc2_node/examples/m5_gate.rs` (client role only: `run_client_measurement`, its consts and `MatcherCtx`; node/service/all roles and the PASS bars untouched)
- NOT touched: `uc2_node/examples/read_profile.rs` (historical harness — its recorded numbers' provenance depends on its code staying as-run; leave the raw pump there)

**Interfaces:**
- Consumes: `Engine`, `EngineConfig`, `SendHalf`, `PollHalf`, `Completion`, `Outcome`, `SubmitError` (public API only — that is the point).

**What changes:** the hand-rolled `owner` slot array, duplicate CAS, serving-gate pause, and NOT_LEADER/RETRY bookkeeping all DELETE — the engine owns them. What stays local: `send_ns` timestamp array (indexed by `user_data` = local send counter & mask), the hdrhistogram, the drain-grace, the PASS computation, and the stats printout (now sourced from `EngineStats` + outcome counts).

- [ ] **Step 1: Record the pre-change smoke baseline**

```bash
cargo run -p uc2_node --release --example m5_gate -- all --secs 2 2>&1 | tee /home/claude/.claude/jobs/eae46461/tmp/m5-smoke-before.txt
```

Note `responses/s` and `p50`. (Sandbox smoke, noisy core-starved box — this is a parity sanity check, NOT the gate.)

- [ ] **Step 2: Rewrite `run_client_measurement` on the engine**

Shape (sender thread + poll thread, mirroring the old two-thread split):

```rust
fn run_client_measurement(
    instance_dir: &Path,
    app_id: &str,
    secs: u64,
    payload_len: usize,
    inflight_cap: u64,
) -> ClientStats {
    // Attach through the PUBLIC engine — the measured path IS the shipped path.
    let (send, mut poll) = Engine::attach(
        instance_dir,
        app_id,
        EngineConfig {
            max_inflight: inflight_cap as u32,
            request_timeout: Duration::from_secs(30), // never the limiter in a healthy run
            max_payload: Some(NODE_MAX_PAYLOAD),
            serving_gate: true, // the engine now owns the redirect-flood defense
            ..EngineConfig::default()
        },
    )
    .unwrap_or_else(|e| panic!("engine attach {instance_dir:?}: {e}"));

    // wait for CAN_SERVE exactly as before (await_serving unchanged)…
    // cmd_bytes: encode ONCE, reuse (unchanged); the engine's max_payload
    // door replaces the old manual assert.

    let send_ns: Arc<Box<[AtomicU64]>> = /* SLOTS as before, indexed by user_data & SLOT_MASK */;
    let resolved = Arc::new(AtomicU64::new(0));
    // + responses / not_leader / retried / last_response_ns / hist as before

    let matcher = thread::Builder::new().name("m5-gate-poll".into()).spawn({
        let stop = Arc::clone(&stop);
        move || {
            while !stop.load(Ordering::Relaxed) {
                let n = poll.poll(|c| {
                    match c.outcome {
                        Outcome::Response(_) => {
                            let idx = (c.user_data as usize) & SLOT_MASK;
                            let now = t0.elapsed().as_nanos() as u64;
                            let lat = now
                                .saturating_sub(send_ns[idx].load(Ordering::Acquire))
                                .min(HIST_MAX_NS);
                            let _ = hist.lock().unwrap().record(lat);
                            responses.fetch_add(1, Ordering::Relaxed);
                            last_response_ns.fetch_max(now, Ordering::Relaxed);
                        }
                        Outcome::NotLeader { .. } => { not_leader.fetch_add(1, Ordering::Relaxed); }
                        Outcome::Retry => { retried.fetch_add(1, Ordering::Relaxed); }
                        Outcome::TimedOut | Outcome::InstanceRestart { .. } => {}
                    }
                    resolved.fetch_add(1, Ordering::Relaxed);
                });
                if n == 0 { std::hint::spin_loop(); } // dedicated-core caller: BusySpin is legitimate here
            }
        }
    }).expect("spawn poll thread");

    // Sender loop: user_data = send index; stamp send_ns BEFORE try_submit.
    let mut sent_idx: u64 = 0;
    while Instant::now() < deadline {
        let idx = (sent_idx as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        match send.try_submit(sent_idx, &cmd_bytes) {
            Ok(()) => { sent_idx += 1; }
            Err(SubmitError::Backpressure) => thread::yield_now(), // window OR ring full — engine's door
            Err(SubmitError::NotServing) => thread::sleep(Duration::from_millis(1)), // old serving-gate pause
            Err(e) => panic!("try_submit: {e}"),
        }
    }
    // drain-grace + ClientStats exactly as before; duplicates/overwritten now
    // read from send.stats() (duplicates, overwritten fields).
}
```

Delete: `MatcherCtx`, `poll_egress`, the `owner` array, the raw ring-open code and file-name consts of the client role (the node/service roles keep what they use). `ClientStats` fields keep their names — `duplicates: send.stats().duplicates`, `overwritten: send.stats().overwritten`, `inflight_at_end: send.inflight()`.

- [ ] **Step 3: Compile + smoke after**

```bash
cargo clippy -p uc2_node --all-targets --release -- -D warnings
cargo run -p uc2_node --release --example m5_gate -- all --secs 2 2>&1 | tee /home/claude/.claude/jobs/eae46461/tmp/m5-smoke-after.txt
```

Expected: completes, `in-flight at end == 0`, responses/s within the same order as the before-file (this box is noisy; a real regression shows as a collapse, not a few percent). If it collapses, suspect: poll budget too small for the response rate (raise the 128/ring budget), or the maintenance sweep running too often.

- [ ] **Step 4: Full workspace verification**

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p uc2_node --test lin_v2   # the linearizability capstone drives Client under failover
```

Expected: all PASS. (`lin_v2` is minutes-long; it is the real proof the shim preserved semantics under fire. Known-flaky setup waits are documented in memory — a setup-timeout failure reruns once before investigating.)

- [ ] **Step 5: Docs touch + final commit**

- `uc2_client/src/lib.rs` module doc: rewrite to describe the three tiers (Engine / PipelinedClient / Client shim) + the byte contract pointer to spec §4.
- `docs/QUICKSTART.md`: where it introduces the client, add one paragraph + example fragment for `PipelinedClient` (submit → ticket → wait/await), and a one-line pointer to the engine for max-throughput gateways.

```bash
git add uc2_node/examples/m5_gate.rs uc2_client/src/lib.rs docs/QUICKSTART.md
git commit -m "feat(uc2_client): m5_gate client role runs on the public Engine — measured path == shipped path"
```

- [ ] **Step 6: Buffer note for the fleet gate**

The M5 PASS bar re-run on real hardware (≥400k resp/s @ p50 ≤1ms, 3×c6id) is a SEPARATE, USER-APPROVED step (standing project rule — fleets cost money). Do not launch it from this plan; report the branch as ready for it.

---

## Self-review (run after writing, fixed inline)

- Spec coverage: §3 layering → Tasks 3-6; §4 engine API + hidden inventory items 1-7 → Tasks 2-4 (attach discipline T3, exactly-once T2, wrap T2/T4, flow control T3, serving gate T3, slot liveness T4, payload bound T3); §5 ticket layer + wait placement → Tasks 1, 5, 6; §6 failure table → Tasks 4/6/7 tests; §7 compat → Task 7 (oracle files unchanged); §8 layout → File Structure (slots.rs split recorded as deviation 5); §9 tests → Tasks 2-7, acceptance → Task 8; §10 deferrals untouched.
- Placeholders: none — every step carries code or an exact command.
- Type consistency: `Resolve::Won{user_data}`, `SubmitError` variants, `Completion{user_data, position, outcome}`, `ticket_pair`, `PipelinedConfig` fields checked against every consuming task.
