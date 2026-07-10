# UC v2 M3 — Static-Leader Commit Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The static-leader commit pipeline (spec §6 steady state): followers report durable positions, the leader ranks the quorum and advances a commit counter, gossips it back — **the project's go/no-go gate: ≥400k committed/s, p50 ≤1ms, fsync on, 3 nodes (spec §9 M3 row)** — before elections or SDK exist.

**Architecture:** Control rides the existing M2 socket as two new header-only datagram kinds (AppendPosition, CommitPosition). Commit ranking is a pure, allocation-free `CommitTracker` in a new `uc2_consensus` crate (the M4 election SM grows around it); the leader's sender agent drives it each duty cycle (single writer of the leader's `commit` counter), the follower's receiver stores gossiped commit (single writer of the follower's). Admission control becomes a position window against `commit` (spec §7), closing M2's fleet-pacing wedge. Plus the M2 final review's hardening wave.

**Tech Stack:** Rust 2024, existing `uc2_net`/`uc2_log`/`uc_protocol::v2` (M2, all committed and green), new dep-free `uc2_consensus` crate.

## Global Constraints

Every task's requirements implicitly include all of these (M1/M2 plan constraints remain in force for any file they cover):

- **Commit means quorum-fsync'd** (spec §6): commit = the majority-th highest of {leader's own durable} ∪ {reported follower durables}, **bounded by the leader's own durable**, **monotonic**. Quorum = `cluster_size/2 + 1`. NO PHANTOM COMMITS: reports from unknown sources or stale terms never influence commit; a lost quorum stalls commit cleanly (it does NOT advance on the leader's own durable alone in a 3-node cluster).
- **Commit counter is single-writer**: on the leader, written only by the sender-agent thread (which runs the tracker); on a follower, written only by the receiver-agent thread (gossip, monotonic max). `LogCounters` stays `#[repr(C)]`; new counters append LAST. `prime()` does NOT set `commit` (a restart re-derives it from quorum reports / gossip; persisting commit is revisited in M4/M5 — priming it to `durable` would overstate what the quorum holds = a phantom commit).
- **Control stays single-digit kHz regardless of message rate** (spec §6): AppendPosition is sent on follower durable-ADVANCE (block/fsync granularity ≈ kHz) plus a 100 ms floor; CommitPosition is gossiped on commit-ADVANCE (same granularity, since commit only moves when a report or own durable moves) plus the same floor. Never per-message.
- **AppendPosition (kind 5) and CommitPosition (kind 6) are HEADER-ONLY datagrams**: the existing 16-byte header's `position` field carries the durable/commit position; `leadership_term_id` carries the (static, M3) term. No bodies — the NakBody/StatusBody DRY refactor stays deferred until a body-carrying control frame appears.
- **Static leadership**: one fixed term for the cluster's lifetime; ALL inbound datagrams (data AND control, both roles) with a mismatched term are dropped and counted. Elections are M4; nothing in M3 may make term mutable.
- **Admission control is a position window vs commit** (spec §7): the load driver stalls appends when `append − commit > admission budget`. This is the production-shaped pacing (leader-local counters only — works cross-host) and supersedes M2's pace-vs-follower-durable harness hack and its documented fleet wedge.
- **Frame/datagram layouts are frozen** (M1/M2): kinds 5–8 were reserved in `uc_protocol::v2::datagram`; M3 promotes 5 and 6 with pinned tests. Kinds 7–8 (RequestVote/Vote) stay reserved for M4.
- **The TSO adjudication (carried from M1, resolved here as documentation):** the seqlock overwrite-race in `read_frame_validated`/`read_run_validated` (plain copy, acquire fence, re-check) is sound on TSO (x86) and formally racy under the C++ model — like every mmap-ring design including Aeron (M1 review record). A faithful loom model of it would *correctly fail* under loom's full C++ semantics, so the M2-final-review carry "loom model for read_run_validated" is delivered as a pinned doc note in `loom_frame.rs`, NOT as a test.
- `Durability::Consistent` everywhere; test journals 4 MiB segments (`test_cfg`); test data small (quota'd tmpfs tempdirs). Gate/example runs put journal dirs on ext4 under `/home/claude`, NEVER `/tmp`; `UC2_M3_MAX_BYTES` caps bounded runs. Drain-inclusive gate accounting (ONE wall clock around load + drain — the M1 accounting lesson).
- SPDX headers (`Apache-2.0`, `Copyright 2026 Peter Knego`) on new files. `cargo clippy --workspace -- -D warnings` after every task; additionally `cargo clippy -p uc2_net --all-targets -- -D warnings` must be clean from Task 1 on. `cargo fmt --check` fails workspace-wide pre-existing — do NOT reformat out-of-scope files.
- Implementers stage ONLY their own task's files (never `git add -A`).

**Non-goals (M3):** elections/votes/term changes (M4 — kinds 7–8 stay reserved); replay sessions (M4 — the `overruns` seam stands; a live follower >1 ring behind remains NAK-unrecoverable, documented M2 envelope); apply path/SDK/cnc mmap (M5 — counters stay heap-side); persisting commit (M4/M5); learner/membership anything (M6).

---

### Task 1: M2 hardening wave + minors sweep

Everything the M2 final review triaged carry-to-M3, in one mechanical task: corrupt-header arithmetic guards, wire-position alignment, `NakConfig` invariant, sender NAK-queue cap, two doc fixes, the `--all-targets` clippy tidy, and the TSO/loom adjudication note.

**Files:**
- Modify: `uc2_net/src/receiver.rs` (guards in `on_datagram`; let-chain tidy in its test module)
- Modify: `uc2_net/src/rebuild.rs` (`NakTimer::new` assert)
- Modify: `uc2_net/src/sender.rs` (NAK queue cap + `naks_dropped` stat)
- Modify: `uc2_log/src/buffer.rs` (RunRead doc wording)
- Modify: `uc2_log/src/writer.rs` (FRAME_ALIGNMENT constant in the debug_assert)
- Modify: `uc2_log/tests/loom_frame.rs` (header doc: the TSO adjudication)

**Interfaces:**
- Consumes: everything as committed at M2 merge (aa53d27).
- Produces: `SenderStats.naks_dropped: AtomicU64` (used by nothing yet — observability); no other API changes. Later tasks rely on these files being otherwise byte-stable.

- [ ] **Step 1: Write the failing tests (receiver guards)**

Append to the `tests` module in `uc2_net/src/receiver.rs` (the module already has `FakeLeader`, `follower`, `frame_runs`, `drive_until` helpers — reuse them):

```rust
    #[test]
    fn misaligned_wire_position_is_malformed() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64]], 4096);
        // legit frame bytes, but position not on a 32-byte frame boundary
        leader.send(to, DGRAM_KIND_DATA, 16, TERM, &runs[0].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_malformed.load(std::sync::atomic::Ordering::Relaxed) < 1 {
            assert!(Instant::now() < deadline, "misaligned datagram never observed");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), 0, "misaligned position advanced the log");
    }

    #[test]
    fn position_overflow_is_malformed_not_accepted() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64]], 4096);
        // u64 wrap: position + advance overflows; the wrapped sum must not
        // sneak past the overrun gate (accept-rule arithmetic escape)
        let pos = u64::MAX - 63; // 32-aligned (u64::MAX - 63 = ...FFC0), advance 96 wraps
        assert_eq!(pos % 32, 0);
        leader.send(to, DGRAM_KIND_DATA, pos, TERM, &runs[0].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_malformed.load(std::sync::atomic::Ordering::Relaxed) < 1 {
            assert!(Instant::now() < deadline, "overflowing datagram never observed");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), 0);
    }
```

- [ ] **Step 2: Run — expect failure**

Run: `cargo test -p uc2_net misaligned_wire position_overflow`
Expected: FAIL — the misaligned test trips `write_run`'s `debug_assert_eq!(position % 32, 0)` (a panic, not a drop) in debug builds; the overflow test wraps `h.position + advance` and the tiny sum passes the guard, so `dropped_malformed` never reaches 1 (deadline panic).

- [ ] **Step 3: Implement the receiver guards**

In `uc2_net/src/receiver.rs`, `on_datagram`'s `DGRAM_KIND_DATA` arm, immediately after the `h.position < contiguous` dup check and before `walk_advance`:

```rust
                // Corrupt-header hardening (M2 final review): the wire has no
                // CRC, so a flipped position bit must fail closed. Misaligned
                // positions would corrupt reader framing; a position whose
                // sum with `advance` wraps u64 would sneak past the overrun
                // gate below as a tiny wrapped value.
                if h.position % frame::FRAME_ALIGNMENT as u64 != 0 {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                }
```

and replace the overrun-guard block's arithmetic (keep its existing comment):

```rust
                let durable = self.buffer.counters().durable.load_acquire();
                let Some(end) = h.position.checked_add(advance) else {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                };
                if end > durable + self.buffer.capacity() {
                    self.stats.dropped_overrun.fetch_add(1, Relaxed);
                    return;
                }
```

(`durable + capacity` cannot overflow in practice: capacity ≤ 2³¹ and durable is real local progress, not attacker-controlled.) Note `FRAME_ALIGNMENT` needs adding to the existing `uc_protocol::v2::frame` import list.

- [ ] **Step 4: NakConfig invariant + failing test**

In `uc2_net/src/rebuild.rs`, add to the tests module:

```rust
    #[test]
    #[should_panic(expected = "delay_min_ns must be <= delay_max_ns")]
    fn nak_config_min_above_max_is_rejected() {
        let cfg = NakConfig { delay_min_ns: 2, delay_max_ns: 1, backoff_ns: 5 };
        let _ = NakTimer::new(cfg, 1);
    }
```

Run `cargo test -p uc2_net nak_config_min` — FAIL (no panic; the underflow only bites later in `delay()`). Then implement in `NakTimer::new`:

```rust
    pub fn new(cfg: NakConfig, seed: u64) -> Self {
        assert!(
            cfg.delay_min_ns <= cfg.delay_max_ns,
            "delay_min_ns must be <= delay_max_ns"
        );
        Self { cfg, rng: XorShift64::new(seed), armed: None }
    }
```

- [ ] **Step 5: Sender NAK-queue cap + failing test**

Append to `uc2_net/src/sender.rs` tests:

```rust
    #[test]
    fn nak_queue_is_capped_dropping_oldest() {
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(4096);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
        );
        // flood 1100 NAKs in one control drain; the queue must trim to the cap
        for i in 0..1100u64 {
            tx.send(CtrlMsg::Nak { from: f1.addr(), position: i * 96, length: 96 }).unwrap();
        }
        s.do_work(); // drains all 1100, serves 1
        assert_eq!(
            s.stats().naks_dropped.load(std::sync::atomic::Ordering::Relaxed),
            1100 - NAK_QUEUE_MAX as u64,
            "overflow beyond the cap must be counted as dropped"
        );
    }
```

Run `cargo test -p uc2_net nak_queue_is_capped` — FAIL (`NAK_QUEUE_MAX`/`naks_dropped` undefined). Implement in `sender.rs`:

```rust
/// Bound on queued NAK requests (M2 final review: a flooding/hostile
/// follower must not grow the deque unboundedly). Oldest entries drop first —
/// a re-NAK after backoff re-requests anything still missing, so dropping is
/// always recoverable. 1024 entries ≈ 24 KB; the worst storm observed in the
/// M2 gate was ~10k NAKs over a whole run.
const NAK_QUEUE_MAX: usize = 1024;
```

add `pub naks_dropped: AtomicU64,` to `SenderStats`, and replace the NAK push in `do_work`:

```rust
                CtrlMsg::Nak { from, position, length } => {
                    if self.naks.len() >= NAK_QUEUE_MAX {
                        self.naks.pop_front();
                        self.stats.naks_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    self.naks.push_back((from, position, length))
                }
```

- [ ] **Step 6: Doc fixes + clippy tidy + TSO note (no behavior)**

1. `uc2_log/src/buffer.rs` — `RunRead.advance` doc: replace "(> `bytes` iff the run ends in a padding frame, which is copied header-only)" with "(≥ `bytes`; strictly greater when the run ends in a padding frame whose span exceeds its 32-byte header — a 32-byte padding span gives advance == bytes)".
2. `uc2_log/src/writer.rs` — replace `debug_assert_eq!(position % 32, 0, "runs start at frame boundaries");` with `debug_assert_eq!(position % uc_protocol::v2::frame::FRAME_ALIGNMENT as u64, 0, "runs start at frame boundaries");`.
3. `uc2_net/src/receiver.rs` test module — fix the four `clippy::collapsible_if` lints flagged under `--all-targets` (collapse to let-chains, matching `rebuild.rs`'s existing pattern). Verify with `cargo clippy -p uc2_net --all-targets -- -D warnings` (must be exit 0; do NOT chase pre-existing `--all-targets` lints in OTHER crates, e.g. ultima_journal's test-code `identity_op`s).
4. `uc2_log/tests/loom_frame.rs` — append to the header doc comment (after the "Run:" line):

```rust
//!
//! DELIBERATELY NOT MODELED — the seqlock overwrite-race: `read_frame_validated`
//! and `read_run_validated` copy with plain loads, then acquire-fence and
//! re-check the `append` margin. That discipline is sound on TSO (x86: stores
//! become visible in program order, so a stale post-check bound implies
//! unoverwritten bytes) but is formally racy under the C++ memory model —
//! like every mmap-ring design including Aeron's (M1 review adjudication,
//! 2026-07-09). A faithful loom model of it CORRECTLY FAILS under loom's full
//! C++ semantics (later plain stores may be observed before an earlier
//! release-store of `append`), so do not add one and conclude the code is
//! broken: the two visibility pairs above are the loom-modelable properties.
```

- [ ] **Step 7: Run everything**

Run: `cargo test -p uc2_net && cargo test -p uc2_log && cargo clippy --workspace -- -D warnings && cargo clippy -p uc2_net --all-targets -- -D warnings && RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release`
Expected: all green (uc2_net lib 32 = 28 + 4 new; replication 5; loom 2).

- [ ] **Step 8: Commit**

```bash
git add uc2_net/src/receiver.rs uc2_net/src/rebuild.rs uc2_net/src/sender.rs uc2_log/src/buffer.rs uc2_log/src/writer.rs uc2_log/tests/loom_frame.rs
git commit -m "fix(uc2_net): M2 final-review hardening — corrupt-header guards, NakConfig assert, NAK-queue cap (review carry)"
```

---

### Task 2: `uc_protocol::v2` — AppendPosition + CommitPosition kinds (header-only)

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (promote two reserved kind codes; doc semantics; extend the pinned tests)

**Interfaces:**
- Consumes: the frozen 16-byte datagram header (M2).
- Produces (used by Tasks 5–8): `DGRAM_KIND_APPEND_POSITION: u8 = 5`, `DGRAM_KIND_COMMIT_POSITION: u8 = 6` — both HEADER-ONLY: `position` = the reported durable position (kind 5, follower→leader) / the gossiped commit position (kind 6, leader→followers); `leadership_term_id` as always. Kinds 7–8 stay reserved (M4: REQUEST_VOTE, VOTE).

- [ ] **Step 1: Write the failing test**

Extend `kind_codes_are_stable` in `uc_protocol/src/v2/datagram.rs`:

```rust
    #[test]
    fn kind_codes_are_stable() {
        assert_eq!(DGRAM_KIND_DATA, 1);
        assert_eq!(DGRAM_KIND_HEARTBEAT, 2);
        assert_eq!(DGRAM_KIND_NAK, 3);
        assert_eq!(DGRAM_KIND_STATUS, 4);
        assert_eq!(DGRAM_KIND_APPEND_POSITION, 5);
        assert_eq!(DGRAM_KIND_COMMIT_POSITION, 6);
    }
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_protocol kind_codes`
Expected: FAIL — constants not defined.

- [ ] **Step 3: Implement**

Replace the reserved-kinds comment in `datagram.rs` with:

```rust
/// Header-only (spec §6): `position` = the sender's DURABLE position.
/// Follower → leader, on durable advance (block/fsync granularity) plus a
/// 100 ms floor. Feeds the leader's quorum commit ranking.
pub const DGRAM_KIND_APPEND_POSITION: u8 = 5;
/// Header-only (spec §6): `position` = the cluster COMMIT position (quorum-
/// fsync'd). Leader → followers, on commit advance plus the same floor.
pub const DGRAM_KIND_COMMIT_POSITION: u8 = 6;
// 7..=8 reserved: REQUEST_VOTE, VOTE (M4).
```

- [ ] **Step 4: Run**

Run: `cargo test -p uc_protocol && cargo clippy -p uc_protocol -- -D warnings`
Expected: PASS (76 tests, the extended one included). Confirm the module stays core-only (no `use std`).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/datagram.rs
git commit -m "feat(uc_protocol): v2 AppendPosition/CommitPosition header-only control kinds"
```

---

### Task 3: `uc2_log` — the `commit` counter

**Files:**
- Modify: `uc2_log/src/counters.rs`

**Interfaces:**
- Consumes: M2's `LogCounters { append, durable, sent }`.
- Produces (used by Tasks 5–8): `LogCounters.commit: PaddedAtomicU64` — the quorum-fsync'd commit position (spec §6: "the cnc commit counter — that IS the apply notification", consumed by M5's apply agent). Written only by the sender-agent thread (leader) / receiver-agent thread (follower, gossip). **`prime()` does NOT set it** — see the doc contract below.

- [ ] **Step 1: Write the failing test**

Update the existing `counters_start_at_zero_and_prime` test in `uc2_log/src/counters.rs` (it currently asserts `append`/`durable`/`sent`):

```rust
    #[test]
    fn counters_start_at_zero_and_prime() {
        let c = LogCounters::new();
        assert_eq!(c.append.load_acquire(), 0);
        assert_eq!(c.durable.load_acquire(), 0);
        assert_eq!(c.sent.load_acquire(), 0);
        assert_eq!(c.commit.load_acquire(), 0);
        c.prime(4096);
        assert_eq!(c.append.load_acquire(), 4096);
        assert_eq!(c.durable.load_acquire(), 4096);
        assert_eq!(c.sent.load_acquire(), 4096);
        // commit is NOT primed: locally-durable bytes are not necessarily
        // quorum-durable — priming commit would be a phantom commit. It is
        // re-derived from quorum reports (leader) or gossip (follower).
        assert_eq!(c.commit.load_acquire(), 0);
    }
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc2_log counters_start`
Expected: FAIL — no field `commit`.

- [ ] **Step 3: Implement**

In `uc2_log/src/counters.rs`, extend the struct (append the field LAST — the `#[repr(C)]` layout becomes the mmap'd cnc page in M5) and its doc:

```rust
/// The M1+M2+M3 counter set. append: written only by the appender (leader) /
/// receiver (follower), after the frame commit word (so any position below
/// `append` is a committed frame). durable: written only by the archive,
/// after write+fdatasync of the block. sent: written only by the sender
/// agent, after the datagram send (leader only; follower leaves it 0).
/// commit: the cluster's quorum-fsync'd position (spec §6) — written only by
/// the sender-agent thread on the leader (quorum ranking) and only by the
/// receiver-agent thread on a follower (CommitPosition gossip, monotonic).
/// NOT primed on restart: locally-durable bytes are not necessarily
/// quorum-durable, so priming commit would manufacture a phantom commit; it
/// is re-derived live. (Commit persistence is revisited in M4/M5.)
#[repr(C)]
pub struct LogCounters {
    pub append: PaddedAtomicU64,
    pub durable: PaddedAtomicU64,
    pub sent: PaddedAtomicU64,
    pub commit: PaddedAtomicU64,
}
```

Update `new()` to initialize it. `prime()` is UNCHANGED (does not touch `commit`) — extend its doc's last line with: "`commit` is deliberately not primed (see the struct doc)."

- [ ] **Step 4: Run**

Run: `cargo test -p uc2_log && cargo clippy --workspace -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc2_log/src/counters.rs
git commit -m "feat(uc2_log): commit counter — quorum-fsync'd position, deliberately not primed"
```

---

### Task 4: `uc2_consensus` crate — `CommitTracker`

The seed of the pure-sync consensus SM (spec §3.2): quorum commit ranking, no I/O, no clock, no allocation on the hot path. M4's election SM grows around it.

**Files:**
- Create: `uc2_consensus/Cargo.toml`
- Create: `uc2_consensus/src/lib.rs`
- Create: `uc2_consensus/src/commit.rs`
- Modify: `Cargo.toml` (workspace `members`: add `"uc2_consensus"` after `"uc2_net"`)

**Interfaces:**
- Consumes: nothing (dep-free).
- Produces (used by Task 5):
  - `CommitTracker::new(n_followers: usize, cluster_size: usize) -> Self` — asserts `cluster_size > n_followers` (the leader is a member too) AND `n_followers + 1 >= cluster_size/2 + 1` (enough followers to ever reach quorum). `n_followers` may be LESS than `cluster_size − 1`: a member with no follower slot is treated as a permanently-zero report (conservative — it can never help commit), which is exactly what the sender's harness tests exercise (1 tracked follower in a 3-cluster).
  - `on_durable(&mut self, follower_idx: usize, durable: u64)` — record a follower's reported durable; **monotonic per follower** (a stale/reordered UDP report never regresses).
  - `advance(&mut self, own_durable: u64) -> Option<u64>` — commit = the quorum-th highest of `{own_durable} ∪ reported`, **bounded by `own_durable`**, **monotonic**; returns `Some(new_commit)` iff it advanced. Allocation-free (reusable scratch).
  - `commit(&self) -> u64`.

- [ ] **Step 1: Create the crate**

`uc2_consensus/Cargo.toml`:

```toml
[package]
name = "uc2_consensus"
description = "UC v2 pure-sync consensus state machine: quorum commit ranking (spec 2026-07-09 §6, M3); elections/truncation land in M4"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true

[dependencies]
```

`uc2_consensus/src/lib.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 consensus state machine (M3: commit ranking only).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §6.
//! Pure and sync by construction: no I/O, no threads, no clock, no
//! allocation on the hot path — the agent driving it does all I/O. The M4
//! election SM (votes, terms, truncation) grows in this crate around this
//! module, gated by the deterministic simulation (uc2_sim).

pub mod commit;
```

Add `"uc2_consensus"` to the workspace `members` array in the root `Cargo.toml` (after `"uc2_net"`).

- [ ] **Step 2: Write the failing tests**

Tests at the bottom of `uc2_consensus/src/commit.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_node_commit_is_second_highest_bounded_by_own() {
        let mut t = CommitTracker::new(2, 3);
        assert_eq!(t.commit(), 0);
        // no reports yet: {own=1000, 0, 0} -> 2nd highest = 0
        assert_eq!(t.advance(1000), None);
        assert_eq!(t.commit(), 0);
        // one follower at 400: {1000, 400, 0} -> 2nd = 400
        t.on_durable(0, 400);
        assert_eq!(t.advance(1000), Some(400));
        // second follower at 700: {1000, 400, 700} -> 2nd = 700
        t.on_durable(1, 700);
        assert_eq!(t.advance(1000), Some(700));
        // followers ahead of the leader's own durable: bounded by own.
        // (Possible in practice: the leader's archive can lag its own sends.)
        t.on_durable(0, 5000);
        t.on_durable(1, 5000);
        assert_eq!(t.advance(1000), Some(1000));
        assert_eq!(t.advance(1000), None); // no re-advance without movement
        // own durable catches up -> commit follows
        assert_eq!(t.advance(4000), Some(4000));
    }

    #[test]
    fn reports_are_monotonic_per_follower_and_commit_never_regresses() {
        let mut t = CommitTracker::new(2, 3);
        t.on_durable(0, 800);
        t.on_durable(1, 900);
        assert_eq!(t.advance(1000), Some(900));
        // a stale, UDP-reordered report must not regress anything
        t.on_durable(1, 100);
        assert_eq!(t.advance(1000), None);
        assert_eq!(t.commit(), 900);
    }

    #[test]
    fn five_node_commit_is_third_highest() {
        let mut t = CommitTracker::new(4, 5);
        // {own=100, 90, 80, 70, 0} -> quorum 3 -> 3rd highest = 80
        t.on_durable(0, 90);
        t.on_durable(1, 80);
        t.on_durable(2, 70);
        assert_eq!(t.advance(100), Some(80));
    }

    #[test]
    fn quorum_loss_never_commits_on_own_durable_alone() {
        // 3 nodes, both followers silent forever: {own, 0, 0} -> 2nd = 0.
        // The no-phantom-commits property under quorum loss.
        let mut t = CommitTracker::new(2, 3);
        assert_eq!(t.advance(u64::MAX), None);
        assert_eq!(t.commit(), 0);
    }

    #[test]
    fn untracked_member_counts_as_permanent_zero() {
        // 3-node cluster, only 1 tracked follower (the sender-test shape):
        // quorum 2 over {own, f1, missing=0} -> commit = min(own, f1)
        let mut t = CommitTracker::new(1, 3);
        t.on_durable(0, 700);
        assert_eq!(t.advance(1000), Some(700));
        t.on_durable(0, 2000);
        assert_eq!(t.advance(1000), Some(1000)); // still bounded by own
    }

    #[test]
    #[should_panic(expected = "cluster_size")]
    fn leader_must_be_a_member() {
        let _ = CommitTracker::new(3, 3);
    }

    #[test]
    #[should_panic(expected = "quorum")]
    fn too_few_tracked_followers_is_rejected() {
        let _ = CommitTracker::new(1, 5); // quorum 3 > 2 tracked members
    }
}
```

- [ ] **Step 3: Run — expect compile failure**

Run: `cargo test -p uc2_consensus`
Expected: FAIL — module/type not defined.

- [ ] **Step 4: Implement**

`uc2_consensus/src/commit.rs` above the tests:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quorum commit ranking (spec §6): commit = the quorum-th highest of
//! {leader's own durable} ∪ {reported follower durables}, bounded by the
//! leader's own durable, monotonic. "Commit means quorum-fsync'd."
//!
//! Bounded-by-own is not redundant with the rank: followers can legitimately
//! out-fsync the leader (their archives run independently), making the
//! quorum-th highest exceed what the leader itself holds durably — and the
//! leader must never declare committed what it could not itself serve.
//!
//! A member without a tracked follower slot counts as a permanently-zero
//! report: conservative by construction — an untracked member can never help
//! reach quorum, only a real report can.
//!
//! Pure and allocation-free after construction: the agent (uc2_net's sender
//! duty cycle in M3) feeds reports in and stores the result out.

pub struct CommitTracker {
    /// Latest reported durable per follower index; monotonic per slot
    /// (stale UDP-reordered reports never regress).
    reported: Vec<u64>,
    /// Reusable ranking scratch: {own} ∪ reported.
    scratch: Vec<u64>,
    quorum: usize,
    commit: u64,
}

impl CommitTracker {
    pub fn new(n_followers: usize, cluster_size: usize) -> Self {
        // The leader is a member, so cluster_size must exceed the follower
        // count; and the rank below indexes scratch[quorum-1], so there must
        // be enough tracked members to ever reach quorum. n_followers MAY be
        // smaller than cluster_size - 1: an untracked member is a
        // permanently-zero report — conservative, it can never help commit.
        assert!(
            cluster_size > n_followers,
            "cluster_size must exceed n_followers (the leader is a member)"
        );
        assert!(
            n_followers + 1 >= cluster_size / 2 + 1,
            "not enough tracked followers to ever reach quorum"
        );
        Self {
            reported: vec![0; n_followers],
            scratch: Vec::with_capacity(cluster_size),
            quorum: cluster_size / 2 + 1,
            commit: 0,
        }
    }

    /// Record a follower's reported durable position (AppendPosition).
    pub fn on_durable(&mut self, follower_idx: usize, durable: u64) {
        let r = &mut self.reported[follower_idx];
        *r = (*r).max(durable);
    }

    #[inline]
    pub fn commit(&self) -> u64 {
        self.commit
    }

    /// Rank the quorum. Returns `Some(new_commit)` iff commit advanced.
    pub fn advance(&mut self, own_durable: u64) -> Option<u64> {
        self.scratch.clear();
        self.scratch.push(own_durable);
        self.scratch.extend_from_slice(&self.reported);
        self.scratch.sort_unstable_by(|a, b| b.cmp(a));
        let ranked = self.scratch[self.quorum - 1].min(own_durable);
        if ranked > self.commit {
            self.commit = ranked;
            Some(ranked)
        } else {
            None
        }
    }
}
```

- [ ] **Step 5: Run**

Run: `cargo test -p uc2_consensus && cargo clippy --workspace -- -D warnings`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock uc2_consensus/Cargo.toml uc2_consensus/src/lib.rs uc2_consensus/src/commit.rs
git commit -m "feat(uc2_consensus): crate seed — pure quorum CommitTracker (rank, bound-by-own, monotonic)"
```

---

### Task 5: leader side — AppendPosition demux, commit advance, CommitPosition gossip

**Files:**
- Modify: `uc2_net/src/sender.rs` (CtrlMsg variant; CommitTracker wiring; gossip; stats)
- Modify: `uc2_net/src/receiver.rs` (LeaderReceiver: term check + kind-5 demux; signature change)
- Modify: `uc2_net/Cargo.toml` (dep: `uc2_consensus = { path = "../uc2_consensus" }`)
- Modify: `uc2_net/tests/replication.rs` (LeaderReceiver::new call site — one line)
- Modify: `uc2_net/examples/m2_gate.rs` (LeaderReceiver::new call site — one line)

**Interfaces:**
- Consumes: `uc2_consensus::commit::CommitTracker` (Task 4), `DGRAM_KIND_APPEND_POSITION`/`DGRAM_KIND_COMMIT_POSITION` (Task 2), `LogCounters.commit` (Task 3).
- Produces (used by Tasks 6–8):
  - `CtrlMsg::AppendPos { from: SocketAddr, durable: u64 }` (new variant).
  - `LeaderReceiver::new(sock: UdpSocket, to_sender: mpsc::SyncSender<CtrlMsg>, term_id: u32) -> io::Result<Self>` (**signature change**: term added; all inbound control with a mismatched term is dropped and counted in the new `pub dropped_stale_term: u64`).
  - Sender behavior: each duty cycle, after the control drain, ranks the tracker against its own durable; on advance stores `counters().commit` (Release) and sends a header-only CommitPosition to every follower; the heartbeat block additionally re-gossips the current commit on the same interval (the 100 ms floor). New stats: `SenderStats.commit_gossips: AtomicU64`.
  - Unknown-source AppendPos (an address not in the configured follower set) is ignored by the sender (no tracker slot).

- [ ] **Step 1: Write the failing tests**

Append to `uc2_net/src/sender.rs` tests (the `Fake` helper and `sender_to` already exist; `sender_to` disables heartbeats):

```rust
    fn ctrl_ap(from: SocketAddr, durable: u64) -> CtrlMsg {
        CtrlMsg::AppendPos { from, durable }
    }

    #[test]
    fn commit_advances_on_quorum_reports_and_gossips() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b);
        // leader's own durable: 10 frames' worth
        b.counters().durable.store_release(960);
        // no reports -> {960, 0, 0} -> 2nd highest = 0 -> no commit
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 0);
        // one follower reports 480 -> {960, 480, 0} -> commit 480 + gossip
        tx.send(ctrl_ap(f1.addr(), 480)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 480);
        for f in [&f1, &f2] {
            let (h, body) = f.recv().expect("commit gossip");
            assert_eq!(h.kind, DGRAM_KIND_COMMIT_POSITION);
            assert_eq!(h.position, 480);
            assert_eq!(h.leadership_term_id, 9);
            assert!(body.is_empty(), "CommitPosition is header-only");
        }
        // second follower overtakes: {960, 480, 700} -> commit 700
        tx.send(ctrl_ap(f2.addr(), 700)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 700);
        // bounded by own durable: reports at 5000 -> commit = 960
        tx.send(ctrl_ap(f1.addr(), 5000)).unwrap();
        tx.send(ctrl_ap(f2.addr(), 5000)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 960);
        assert!(s.stats().commit_gossips.load(std::sync::atomic::Ordering::Relaxed) >= 3);
    }

    #[test]
    fn unknown_source_report_is_ignored() {
        let b = buffer();
        let f1 = Fake::new();
        let ghost = Fake::new(); // not in the follower set
        let (mut s, tx) = sender_to(&[&f1], &b);
        b.counters().durable.store_release(960);
        tx.send(ctrl_ap(ghost.addr(), 960)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 0, "unknown source advanced commit");
    }

    #[test]
    fn heartbeat_block_regossips_commit_on_the_floor() {
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = 1; // fire every cycle
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
        );
        b.counters().durable.store_release(960);
        tx.send(ctrl_ap(f1.addr(), 480)).unwrap();
        s.do_work(); // advances commit to 480, gossips + heartbeats
        // drain until we have seen BOTH a heartbeat and >= 2 CommitPosition
        // datagrams (the on-advance gossip plus the floor re-gossip)
        let mut commits = 0;
        let mut heartbeats = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while commits < 2 || heartbeats < 1 {
            assert!(Instant::now() < deadline, "floor re-gossip never arrived");
            s.do_work();
            while let Some((h, _)) = f1.recv() {
                match h.kind {
                    DGRAM_KIND_COMMIT_POSITION => {
                        assert_eq!(h.position, 480);
                        commits += 1;
                    }
                    DGRAM_KIND_HEARTBEAT => heartbeats += 1,
                    _ => {}
                }
                if commits >= 2 && heartbeats >= 1 {
                    break;
                }
            }
        }
    }
```

And append to `uc2_net/src/receiver.rs` tests (LeaderReceiver section — note the constructor gains `TERM`):

```rust
    #[test]
    fn leader_receiver_demuxes_append_position_and_drops_stale_term() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let mut lr = LeaderReceiver::new(sock, tx, TERM).unwrap();
        let mut f = FakeLeader::new(); // reuse as a fake follower endpoint
        f.send(addr, DGRAM_KIND_APPEND_POSITION, 4096, TERM, &[]);
        f.send(addr, DGRAM_KIND_APPEND_POSITION, 9999, TERM - 1, &[]); // stale term
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = None;
        while got.is_none() {
            assert!(Instant::now() < deadline);
            lr.do_work();
            if let Ok(m) = rx.try_recv() {
                got = Some(m);
            }
        }
        assert!(matches!(got, Some(CtrlMsg::AppendPos { durable: 4096, .. })));
        // the stale one must be counted dropped, never demuxed
        let deadline = Instant::now() + Duration::from_secs(5);
        while lr.dropped_stale_term < 1 {
            assert!(Instant::now() < deadline, "stale-term control never observed");
            lr.do_work();
        }
        assert!(rx.try_recv().is_err(), "stale-term control reached the sender");
    }
```

- [ ] **Step 2: Run — expect compile failures**

Run: `cargo test -p uc2_net commit_advances unknown_source heartbeat_block leader_receiver_demuxes_append`
Expected: FAIL — variant/constant/field not defined; `LeaderReceiver::new` arity.

- [ ] **Step 3: Implement the sender side**

In `uc2_net/Cargo.toml` add `uc2_consensus = { path = "../uc2_consensus" }` to `[dependencies]`.

In `uc2_net/src/sender.rs`:

1. Extend the imports: add `DGRAM_KIND_COMMIT_POSITION` to the `uc_protocol::v2::datagram` list; add `use std::collections::HashMap;` and `use uc2_consensus::commit::CommitTracker;`.
2. Extend `CtrlMsg`:

```rust
#[derive(Debug, Clone, Copy)]
pub enum CtrlMsg {
    Nak { from: SocketAddr, position: u64, length: u32 },
    Status { from: SocketAddr, contiguous: u64, window: u32 },
    /// A follower's AppendPosition report (spec §6): its durable position.
    AppendPos { from: SocketAddr, durable: u64 },
}
```

3. Add to `SenderStats`: `pub commit_gossips: AtomicU64,`.
4. Add fields to `Sender`:

```rust
    /// Quorum commit ranking (spec §6) — this thread is the single writer of
    /// the leader's commit counter.
    tracker: CommitTracker,
    follower_idx: HashMap<SocketAddr, usize>,
```

initialized in `new()` (before the `Sender { .. }` literal):

```rust
        let tracker = CommitTracker::new(followers.len(), cluster_size);
        let follower_idx: HashMap<SocketAddr, usize> =
            followers.iter().enumerate().map(|(i, a)| (*a, i)).collect();
```

5. In `do_work`'s control drain, add the arm (unknown sources have no tracker slot — ignored by design):

```rust
                CtrlMsg::AppendPos { from, durable } => {
                    if let Some(&i) = self.follower_idx.get(&from) {
                        self.tracker.on_durable(i, durable);
                    }
                }
```

6. After the control drain (before the NAK service), rank and gossip:

```rust
        // Commit ranking (spec §6): once per duty cycle, quorum-th highest of
        // {own durable} ∪ reports, bounded by own durable, monotonic. Advances
        // at block/fsync granularity (reports and own durable both move per
        // archive block), so the on-advance gossip stays ~kHz — never
        // per-message.
        let own_durable = self.buffer.counters().durable.load_acquire();
        if let Some(c) = self.tracker.advance(own_durable) {
            self.buffer.counters().commit.store_release(c);
            self.gossip_commit(c);
            did = true;
        }
```

7. In the heartbeat block, after the heartbeat fan-out and before the `stats.heartbeats` increment, add the floor re-gossip:

```rust
            // CommitPosition floor (spec §6: same 100 ms floor as heartbeats)
            self.gossip_commit(self.tracker.commit());
```

8. Add the helper next to `fan_out`:

```rust
    /// Header-only CommitPosition to every follower.
    fn gossip_commit(&mut self, commit: u64) {
        self.assemble(commit, DGRAM_KIND_COMMIT_POSITION, 0);
        for &to in &self.followers {
            let _ = self.sock.send_to(&self.scratch, to);
        }
        self.stats.commit_gossips.fetch_add(1, Ordering::Relaxed);
    }
```

- [ ] **Step 4: Implement the leader-receiver side**

In `uc2_net/src/receiver.rs`, change `LeaderReceiver`:

```rust
/// The leader-side inbound demux: NAK/status/AppendPosition → the sender's
/// channel. All control is term-checked (static term in M3; stale terms are
/// dropped and counted). Vote kinds (7-8) arrive in M4 with their own route.
pub struct LeaderReceiver {
    sock: UdpSocket,
    to_sender: mpsc::SyncSender<CtrlMsg>,
    term_id: u32,
    recv_buf: Vec<u8>,
    pub dropped_full: u64,
    pub dropped_stale_term: u64,
}

impl LeaderReceiver {
    pub fn new(
        sock: UdpSocket,
        to_sender: mpsc::SyncSender<CtrlMsg>,
        term_id: u32,
    ) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        Ok(Self {
            sock,
            to_sender,
            term_id,
            recv_buf: vec![0u8; 2048],
            dropped_full: 0,
            dropped_stale_term: 0,
        })
    }
```

and in `do_work`, after `read_datagram_header` and before the kind match:

```rust
            if h.leadership_term_id != self.term_id {
                self.dropped_stale_term += 1;
                continue;
            }
```

and extend the kind match with the header-only demux (before the `_ => None` arm):

```rust
                DGRAM_KIND_APPEND_POSITION => {
                    Some(CtrlMsg::AppendPos { from, durable: h.position })
                }
```

(add `DGRAM_KIND_APPEND_POSITION` to the datagram import list).

- [ ] **Step 5: Ripple the signature change**

Exactly two call sites outside this file:
- `uc2_net/tests/replication.rs` (`spawn_leader`): `LeaderReceiver::new(recv, tx, TERM).unwrap()`
- `uc2_net/examples/m2_gate.rs` (`leader_node`): `LeaderReceiver::new(recv, tx, TERM).unwrap()`

Plus the pre-existing `leader_receiver_demuxes_control_to_sender_channel` test in receiver.rs itself.

- [ ] **Step 6: Run everything**

Run: `cargo test -p uc2_net && cargo test -p uc2_net --test replication && cargo clippy --workspace -- -D warnings && cargo clippy -p uc2_net --all-targets -- -D warnings && cargo build -p uc2_net --release --example m2_gate`
Expected: all green (lib 36 = 32 + 4 new; replication 5 unchanged).

- [ ] **Step 7: Commit**

```bash
git add uc2_net/Cargo.toml Cargo.lock uc2_net/src/sender.rs uc2_net/src/receiver.rs uc2_net/tests/replication.rs uc2_net/examples/m2_gate.rs
git commit -m "feat(uc2_net): leader commit pipeline — AppendPosition demux, quorum ranking, CommitPosition gossip"
```

---

### Task 6: follower side — AppendPosition emission + CommitPosition handling

**Files:**
- Modify: `uc2_net/src/receiver.rs` (`FollowerConfig`, `FollowerReceiver`, `FollowerStats`)

**Interfaces:**
- Consumes: Task 2 kinds, Task 3 `commit` counter.
- Produces (used by Tasks 7–8):
  - `FollowerConfig.append_pos_floor_ns: u64` (default `100_000_000`; `FollowerConfig::new` sets it).
  - Follower behavior: `upkeep()` sends a header-only AppendPosition to the leader whenever its durable counter has ADVANCED past the last reported value, or on the floor. `on_datagram` handles `DGRAM_KIND_COMMIT_POSITION`: stores the follower's `commit` counter as a monotonic max (this thread is its single writer). Stale terms are already dropped by the existing common term check.
  - New stats: `FollowerStats.append_positions_sent: AtomicU64`, `FollowerStats.commits_received: AtomicU64`.

- [ ] **Step 1: Write the failing tests**

Append to `uc2_net/src/receiver.rs` tests:

```rust
    #[test]
    fn durable_advance_emits_append_position() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        // simulate the archive: durable advances by one block
        b.counters().durable.store_release(960);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no AppendPosition");
            r.do_work();
            if let Some((h, body)) = leader.recv() {
                if h.kind == DGRAM_KIND_APPEND_POSITION {
                    assert_eq!(h.position, 960);
                    assert_eq!(h.leadership_term_id, TERM);
                    assert!(body.is_empty(), "AppendPosition is header-only");
                    break;
                }
            }
        }
        assert!(r.stats().append_positions_sent.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        // no further advance -> no immediate re-send (floor is u64::MAX-ish in
        // the `follower` helper via status_floor_ns; append_pos floor is set
        // long in Step 3's helper tweak)
        for _ in 0..50 {
            r.do_work();
        }
        let sent = r.stats().append_positions_sent.load(std::sync::atomic::Ordering::Relaxed);
        b.counters().durable.store_release(1920); // next block
        let deadline = Instant::now() + Duration::from_secs(5);
        while r.stats().append_positions_sent.load(std::sync::atomic::Ordering::Relaxed) == sent {
            assert!(Instant::now() < deadline, "advance did not re-report");
            r.do_work();
        }
    }

    #[test]
    fn commit_position_gossip_is_stored_monotonically() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 4096, TERM, &[]);
        let deadline = Instant::now() + Duration::from_secs(5);
        while b.counters().commit.load_acquire() < 4096 {
            assert!(Instant::now() < deadline, "commit gossip never landed");
            r.do_work();
        }
        // stale/reordered gossip must not regress; stale TERM must be dropped
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 1024, TERM, &[]);
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 9999, TERM - 1, &[]);
        let st = r.stats();
        let before_stale = st.dropped_stale_term.load(std::sync::atomic::Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_stale_term.load(std::sync::atomic::Ordering::Relaxed) == before_stale {
            assert!(Instant::now() < deadline, "stale-term gossip never observed");
            r.do_work();
        }
        assert_eq!(b.counters().commit.load_acquire(), 4096);
        assert!(st.commits_received.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    }
```

- [ ] **Step 2: Run — expect compile failures**

Run: `cargo test -p uc2_net durable_advance_emits commit_position_gossip`
Expected: FAIL — stats fields not defined.

- [ ] **Step 3: Implement**

In `uc2_net/src/receiver.rs`:

1. `FollowerConfig`: add `pub append_pos_floor_ns: u64,` and set `append_pos_floor_ns: 100_000_000,` in `new()`. In the test helper `follower()`, add `cfg.append_pos_floor_ns = u64::MAX;` next to the existing `status_floor_ns` line (tests assert advance-driven emission, not the floor).
2. `FollowerStats`: add `pub append_positions_sent: AtomicU64,` and `pub commits_received: AtomicU64,`.
3. `FollowerReceiver` fields: add

```rust
    /// Durable value last reported via AppendPosition.
    ap_reported: u64,
    last_ap_ns: u64,
    /// Highest commit gossip accepted (shadow of the counter — this thread is
    /// the counter's single writer, so a plain field avoids the re-load).
    commit_seen: u64,
```

initialized in `new()` as `ap_reported: start, last_ap_ns: 0, commit_seen: 0,`.

4. `on_datagram`: add the arm before the catch-all:

```rust
            DGRAM_KIND_COMMIT_POSITION => {
                // Monotonic max: UDP-reordered gossip never regresses. The
                // stored value is the CLUSTER commit; M5's apply agent clamps
                // to min(commit, local contiguous durable) at consumption
                // (spec §6) — the counter itself stays raw.
                self.stats.commits_received.fetch_add(1, Relaxed);
                if h.position > self.commit_seen {
                    self.commit_seen = h.position;
                    self.buffer.counters().commit.store_release(self.commit_seen);
                }
            }
```

(add `DGRAM_KIND_COMMIT_POSITION` and `DGRAM_KIND_APPEND_POSITION` to the datagram imports).

5. `upkeep()`: hoist the durable load to the top (the status block already loads it — load ONCE at the top and reuse in both blocks), then add before the status block:

```rust
        // AppendPosition (spec §6): report our durable on advance (block/
        // fsync granularity, ~kHz) or on the floor. Feeds the leader's
        // quorum commit ranking.
        let durable = self.buffer.counters().durable.load_acquire();
        if durable > self.ap_reported || now - self.last_ap_ns >= self.cfg.append_pos_floor_ns {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: durable,
                    leadership_term_id: self.cfg.term_id,
                    kind: DGRAM_KIND_APPEND_POSITION,
                    flags: 0,
                },
            );
            let _ = self.sock.send_to(&d, self.cfg.leader);
            self.ap_reported = durable;
            self.last_ap_ns = now;
            self.stats.append_positions_sent.fetch_add(1, Relaxed);
            did = true;
        }
```

and change the status block to reuse this `durable` local instead of re-loading.

- [ ] **Step 4: Run everything**

Run: `cargo test -p uc2_net && cargo test -p uc2_net --test replication && cargo clippy --workspace -- -D warnings && cargo clippy -p uc2_net --all-targets -- -D warnings`
Expected: all green (lib 38 = 36 + 2 new; replication 5 unchanged — the new AppendPosition traffic is inert there: M2's `spawn_leader` senders now rank/gossip too, which is harmless).

- [ ] **Step 5: Commit**

```bash
git add uc2_net/src/receiver.rs
git commit -m "feat(uc2_net): follower commit pipeline — AppendPosition emission + CommitPosition gossip intake"
```

---

### Task 7: the 3-node commit harness — quorum semantics end to end

**Files:**
- Create: `uc2_net/tests/common/mod.rs` (harness helpers extracted verbatim from replication.rs)
- Modify: `uc2_net/tests/replication.rs` (use the common module; five tests unchanged)
- Create: `uc2_net/tests/commit.rs`

**Interfaces:**
- Consumes: everything above.
- Produces: the L2 proof of spec §6 steady state: commit reaches quorum durable and followers learn it; a minority failure doesn't stall it; quorum loss stalls it CLEANLY (no phantom commits); forged/stale reports can't move it.

- [ ] **Step 1: Extract the common harness (mechanical, no behavior change)**

Create `uc2_net/tests/common/mod.rs` and MOVE these items verbatim from `uc2_net/tests/replication.rs`, making each `pub`: `TERM`, `CAP`, `MAX_PAYLOAD`, `test_cfg`, `buffer`, `Node` (+ its `stop`), `spawn_archive`, `Follower`, `spawn_follower`, `Leader`, `spawn_leader`, `load`, `await_pos`, `replayed`, `converge_and_compare`, plus the `use` lines they need. Top of the file:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared 3-node harness for uc2_net integration tests. Each test binary
//! compiles this module separately, so items unused by one binary are
//! expected — hence the file-level allow.
#![allow(dead_code)]
```

In `replication.rs`: add `mod common;` + `use common::*;` and delete the moved items; the five tests stay byte-identical. Run `cargo test -p uc2_net --test replication` — 5/5 green before proceeding.

- [ ] **Step 2: Write the commit tests**

Full file `uc2_net/tests/commit.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! End-to-end quorum commit semantics (spec §6) over real loopback UDP:
//! commit = quorum-fsync'd, bounded by the leader's durable, monotonic;
//! minority failure tolerated; quorum loss stalls cleanly (no phantom
//! commits); forged/stale reports are inert. Same eventual-with-deadline
//! discipline as replication.rs.

mod common;

use std::net::UdpSocket;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use uc2_net::fault::{FaultConfig, FaultSocket};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DatagramHeader, write_datagram_header,
};

#[test]
fn commit_reaches_end_and_followers_learn_it() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let clean = FaultConfig::default();
    let f1 = spawn_follower("cm-f1", leader_addr, clean);
    let f2 = spawn_follower("cm-f2", leader_addr, clean);
    let (b1, b2) = (Arc::clone(&f1.node.buffer), Arc::clone(&f2.node.buffer));
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], clean);
    let end = load(&leader.node.buffer, &[&b1, &b2], 5_000);
    // the leader's commit reaches the full stream (quorum-fsync'd)...
    await_pos(&leader.node.buffer.counters().commit, end, "leader commit");
    // ...and never exceeds its own durable (spot check at the converged point)
    assert!(
        leader.node.buffer.counters().commit.load_acquire()
            <= leader.node.buffer.counters().durable.load_acquire()
    );
    // ...and the followers learn it via gossip
    await_pos(&b1.counters().commit, end, "f1 commit");
    await_pos(&b2.counters().commit, end, "f2 commit");
    converge_and_compare(leader, vec![f1, f2], end);
}

#[test]
fn minority_failure_does_not_stall_commit() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("cm2-f1", leader_addr, FaultConfig::default());
    let b1 = Arc::clone(&f1.node.buffer);
    // follower B: bound socket, no agents — silent minority
    let dead = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader =
        spawn_leader(raw, vec![f1.addr, dead.local_addr().unwrap()], FaultConfig::default());
    let end = load(&leader.node.buffer, &[&b1], 5_000);
    // quorum = leader + f1: commit must reach end despite the dead follower
    await_pos(&leader.node.buffer.counters().commit, end, "leader commit (minority down)");
    await_pos(&b1.counters().commit, end, "f1 commit (minority down)");
    converge_and_compare(leader, vec![f1], end);
    drop(dead);
}

#[test]
fn quorum_loss_stalls_commit_cleanly() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let _ = leader_addr;
    // BOTH followers silent: the leader can fsync locally but must never
    // commit on its own durable alone — no phantom commits under quorum loss.
    let dead1 = FaultSocket::bind("127.0.0.1:0").unwrap();
    let dead2 = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader = spawn_leader(
        raw,
        vec![dead1.local_addr().unwrap(), dead2.local_addr().unwrap()],
        FaultConfig::default(),
    );
    // small load, unpaced (500 x 96 B = 48 KB < the dead initial window and
    // far below CAP, so the appender/sender never block)
    let end = load(&leader.node.buffer, &[], 500);
    await_pos(&leader.node.buffer.counters().durable, end, "leader durable (quorum lost)");
    // generous settle: commit must STAY at zero
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        leader.node.buffer.counters().commit.load_acquire(),
        0,
        "phantom commit under quorum loss"
    );
    let ldir = leader.node.stop();
    let _ = ldir;
    drop((dead1, dead2));
}

#[test]
fn forged_and_stale_reports_cannot_move_commit() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("cm4-f1", leader_addr, FaultConfig::default());
    let b1 = Arc::clone(&f1.node.buffer);
    let dead = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader =
        spawn_leader(raw, vec![f1.addr, dead.local_addr().unwrap()], FaultConfig::default());
    let end = load(&leader.node.buffer, &[&b1], 1_000);
    await_pos(&leader.node.buffer.counters().commit, end, "leader commit");

    // ghost reports far beyond the stream: (a) correct term from an UNKNOWN
    // address -> ignored (no tracker slot); (b) stale term from anywhere ->
    // dropped at the demux. Commit must not move past `end` (which equals
    // quorum durable here) in either case.
    let mut ghost = FaultSocket::bind("127.0.0.1:0").unwrap();
    for term in [TERM, TERM - 1] {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: end + (1 << 30),
                leadership_term_id: term,
                kind: DGRAM_KIND_APPEND_POSITION,
                flags: 0,
            },
        );
        ghost.send_to(&d, leader_addr).unwrap();
    }
    std::thread::sleep(Duration::from_millis(300));
    let commit = leader.node.buffer.counters().commit.load_acquire();
    assert_eq!(commit, end, "forged/stale report moved commit ({commit} != {end})");
    converge_and_compare(leader, vec![f1], end);
    drop(dead);
}
```

Note the harness gives ghost reports their strongest shot: `end + (1 << 30)` exceeds everything durable anywhere, so ANY leakage into the tracker would advance commit (bounded by leader durable = end + padding… bounded at `min(rank, own_durable)`; own durable > end is impossible here since the stream ends at `end`). The assertion `commit == end` is exact.

- [ ] **Step 3: Run**

Run: `cargo test -p uc2_net --test commit && cargo test -p uc2_net --test replication && cargo test -p uc2_net`
Expected: commit 4/4, replication 5/5, lib 38 — all green. Run the two integration binaries twice each (timing-sensitive; deadlines make regressions red, not hung).

- [ ] **Step 4: Run workspace gates**

Run: `cargo clippy --workspace -- -D warnings && cargo clippy -p uc2_net --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add uc2_net/tests/common/mod.rs uc2_net/tests/replication.rs uc2_net/tests/commit.rs
git commit -m "test(uc2_net): 3-node quorum commit harness — advance/minority/quorum-loss/forged-report"
```

---

### Task 8: `m3_gate` — the go/no-go measurement + benchmark doc

**Files:**
- Create: `uc2_net/examples/m3_gate.rs`
- Create: `docs/benchmarks/uc2-m3-gate-2026-07-10.md` (written from the runs' output)

**Interfaces:**
- Consumes: everything above.
- Produces: the M3 gate measurement (spec §9: **≥400k committed/s, p50 ≤1 ms, fsync on, 3 nodes — the go/no-go**) and the admission-control-vs-commit pacing that closes M2's T10a.

- [ ] **Step 1: Write the example**

`uc2_net/examples/m3_gate.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M3 gate: static-leader commit pipeline (spec §9 GO/NO-GO: >= 400k
//! committed/s, p50 <= 1 ms, fsync on, 3 nodes).
//!
//! Local (single host, loopback, all three nodes in-process):
//!   cargo run -p uc2_net --release --example m3_gate -- local <journal_root> \
//!       [secs=10] [payload=64] [admission_mib=4] [buffer_mib=256]
//!
//! Fleet (one process per host; start followers first):
//!   m3_gate follower <bind_addr> <journal_dir> <leader_addr> [buffer_mib]
//!   m3_gate leader <bind_addr> <journal_dir> <f1_addr> <f2_addr> \
//!       [secs=10] [payload=64] [admission_mib=4] [buffer_mib=256]
//!
//! Journal dirs MUST be on a real filesystem (dev sandbox: /home/claude/...,
//! NEVER /tmp — RAM-backed tmpfs). UC2_M3_MAX_BYTES caps the appended stream.
//!
//! MEASUREMENT: committed/s = messages / ONE wall clock around load + drain
//! (drain = leader commit reaches the stream end — every counted message is
//! quorum-fsync'd; the M1 accounting lesson). Commit latency is sampled every
//! SAMPLE_EVERY appends: (position, Instant) pairs resolved when the commit
//! counter passes them; p50/p99/max over the samples. ADMISSION CONTROL is a
//! position window vs commit (spec §7): append stalls when
//! append - commit > admission budget — leader-local counters only, so it
//! works identically cross-host (this closes M2's fleet sent-pacing wedge).

use std::collections::VecDeque;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::counters::LogCounters;
use uc2_log::region::Region;
use uc2_net::fault::FaultSocket;
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, FollowerStats, LeaderReceiver};
use uc2_net::sender::{Sender, SenderConfig, SenderStats};

const TERM: u32 = 1;
const MAX_PAYLOAD: usize = 1024;
const SAMPLE_EVERY: u64 = 1024;

fn buffer(mib: usize) -> Arc<LogBuffer> {
    let counters = Arc::new(LogCounters::new());
    Arc::new(LogBuffer::new(Region::heap_zeroed(mib << 20), counters, MAX_PAYLOAD))
}

fn archive_agent(name: &str, b: &Arc<LogBuffer>, dir: &str) -> AgentRunner {
    std::fs::create_dir_all(dir).unwrap();
    let mut archive = Archive::open(ArchiveConfig::new(dir)).unwrap();
    let b = Arc::clone(b);
    AgentRunner::spawn(name, IdleStrategy::BusySpin, move || {
        archive.do_work(&b).expect("archive fail-stop")
    })
    .unwrap()
}

fn follower_node(
    name: &str,
    sock: FaultSocket,
    leader: SocketAddr,
    journal_dir: &str,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<FollowerStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let cfg = FollowerConfig::new(TERM, leader);
    let mut rx = FollowerReceiver::new(Arc::clone(&b), sock, cfg);
    let stats = rx.stats();
    let rxa =
        AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::BusySpin, move || rx.do_work())
            .unwrap();
    let ara = archive_agent(&format!("{name}-ar"), &b, journal_dir);
    (b, stats, vec![rxa, ara])
}

fn leader_node(
    raw: UdpSocket,
    followers: Vec<SocketAddr>,
    journal_dir: &str,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<SenderStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let recv = raw.try_clone().unwrap();
    let send = FaultSocket::from_socket(raw).unwrap();
    let (tx, rx) = mpsc::sync_channel(4096);
    let mut sender = Sender::new(Arc::clone(&b), send, followers, 3, rx, SenderConfig::new(TERM));
    let stats = sender.stats();
    let txa = AgentRunner::spawn("leader-tx", IdleStrategy::BusySpin, move || sender.do_work())
        .unwrap();
    let mut lr = LeaderReceiver::new(recv, tx, TERM).unwrap();
    let lra =
        AgentRunner::spawn("leader-ctrl", IdleStrategy::BusySpin, move || lr.do_work()).unwrap();
    let ara = archive_agent("leader-ar", &b, journal_dir);
    (b, stats, vec![txa, lra, ara])
}

fn max_bytes_cap() -> u64 {
    std::env::var("UC2_M3_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(u64::MAX)
}

struct LoadResult {
    end: u64,
    msgs: u64,
    latencies_ns: Vec<u64>,
}

/// Append until `secs` elapse (on the shared clock) or the byte cap, pacing
/// by the ADMISSION WINDOW: append - commit <= budget (spec §7). Samples
/// commit latency every SAMPLE_EVERY appends.
fn drive_load(lb: &Arc<LogBuffer>, secs: u64, payload: usize, budget: u64, clock: Instant) -> LoadResult {
    let body = vec![0u8; payload];
    let mut a = Appender::new(Arc::clone(lb), TERM);
    let max_bytes = max_bytes_cap();
    let mut msgs = 0u64;
    let mut pending: VecDeque<(u64, Instant)> = VecDeque::new();
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(1 << 20);
    let mut drain = |pending: &mut VecDeque<(u64, Instant)>, lat: &mut Vec<u64>, commit: u64| {
        while pending.front().is_some_and(|&(p, _)| p <= commit) {
            let (_, t) = pending.pop_front().unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
    };
    while clock.elapsed().as_secs() < secs && a.position() < max_bytes {
        match a.append(1, msgs, &body) {
            Ok(_) => {
                msgs += 1;
                if msgs % SAMPLE_EVERY == 0 {
                    pending.push_back((a.position(), Instant::now()));
                }
            }
            Err(AppendError::WouldOverrun) => std::thread::yield_now(),
            Err(e) => panic!("{e}"),
        }
        // admission window vs commit (leader-local; works cross-host)
        loop {
            let commit = lb.counters().commit.load_acquire();
            drain(&mut pending, &mut latencies_ns, commit);
            if a.position() - commit <= budget {
                break;
            }
            std::thread::yield_now();
        }
    }
    let end = a.position();
    // commit drain: every appended byte quorum-fsync'd before the clock stops
    let t = Instant::now();
    loop {
        let commit = lb.counters().commit.load_acquire();
        drain(&mut pending, &mut latencies_ns, commit);
        if commit >= end {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(300), "commit drain stuck at {commit} < {end}");
        std::thread::yield_now();
    }
    LoadResult { end, msgs, latencies_ns }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("local") => local(&args[1..]),
        Some("leader") => leader_role(&args[1..]),
        Some("follower") => follower_role(&args[1..]),
        _ => {
            eprintln!("usage: m3_gate local|leader|follower ... (see file header)");
            std::process::exit(2);
        }
    }
}

fn local(args: &[String]) {
    let root = args.first().expect("usage: m3_gate local <journal_root> ...").clone();
    let secs: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(64);
    let admission_mib: u64 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(4);
    let buffer_mib: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(256);

    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let f2s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let (a1, a2) = (f1s.local_addr().unwrap(), f2s.local_addr().unwrap());
    let (f1b, _f1st, f1a) =
        follower_node("f1", f1s, leader_addr, &format!("{root}/f1"), buffer_mib);
    let (f2b, _f2st, f2a) =
        follower_node("f2", f2s, leader_addr, &format!("{root}/f2"), buffer_mib);
    let (lb, lst, la) = leader_node(raw, vec![a1, a2], &format!("{root}/leader"), buffer_mib);

    println!("== uc2 M3 gate (local loopback) ==");
    println!(
        "payload {payload} B, admission {admission_mib} MiB, buffers {buffer_mib} MiB x3, {secs} s"
    );

    let (p, pl) = (Arc::clone(&lb), Arc::clone(&f1b));
    let progress_start = Instant::now();
    let printer = AgentRunner::spawn("printer", IdleStrategy::Sleep(Duration::from_secs(1)), {
        let mut last = (0u64, 0u64);
        move || {
            let now = (p.counters().commit.load_acquire(), pl.counters().durable.load_acquire());
            println!(
                "t={:>3}s  commit +{:>6.1} MB/s  f1 durable +{:>6.1} MB/s  inflight {:>9} B",
                progress_start.elapsed().as_secs(),
                (now.0 - last.0) as f64 / 1e6,
                (now.1 - last.1) as f64 / 1e6,
                p.counters().append.load_acquire() - now.0,
            );
            last = now;
            false
        }
    })
    .unwrap();

    let clock = Instant::now();
    let mut res = drive_load(&lb, secs, payload, admission_mib << 20, clock);
    let full = clock.elapsed().as_secs_f64();
    printer.stop();

    res.latencies_ns.sort_unstable();
    let (p50, p99, pmax) = (
        percentile(&res.latencies_ns, 0.50),
        percentile(&res.latencies_ns, 0.99),
        res.latencies_ns.last().copied().unwrap_or(0),
    );
    let committed_per_s = res.msgs as f64 / full;
    use Ordering::Relaxed as R;
    println!("== uc2 M3 gate ==");
    println!(
        "stream               {} B ({} msgs) committed in {full:.2} s (drain-inclusive)",
        res.end, res.msgs
    );
    println!("committed/s          {committed_per_s:>9.0}");
    println!(
        "commit latency       p50 {:.3} ms  p99 {:.3} ms  max {:.3} ms  ({} samples)",
        p50 as f64 / 1e6,
        p99 as f64 / 1e6,
        pmax as f64 / 1e6,
        res.latencies_ns.len()
    );
    println!(
        "sender               dgrams {}  commit_gossips {}  flow_stalls {}  overruns {}",
        lst.datagrams.load(R),
        lst.commit_gossips.load(R),
        lst.flow_stalls.load(R),
        lst.overruns.load(R),
    );
    let pass = committed_per_s >= 400_000.0 && p50 as f64 / 1e6 <= 1.0 && lst.overruns.load(R) == 0;
    println!(
        "GATE (>=400k committed/s, p50 <= 1 ms, fsync on): {}",
        if pass { "PASS" } else { "FAIL" }
    );
    for a in f1a.into_iter().chain(f2a).chain(la) {
        a.stop();
    }
    let _ = (f1b, f2b);
    if !pass {
        std::process::exit(1);
    }
}

/// Fleet follower: runs until killed, printing durable/commit progress.
fn follower_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let leader: SocketAddr = args.get(2).expect("leader addr").parse().unwrap();
    let buffer_mib: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(256);
    let sock = FaultSocket::bind(bind.as_str()).unwrap();
    let (b, st, _agents) = follower_node("follower", sock, leader, journal, buffer_mib);
    let mut last = 0u64;
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let c = b.counters();
        let d = c.durable.load_acquire();
        println!(
            "durable {:>7.1} MB/s (at {d})  commit {}  ap_sent {}  naks {}",
            (d - last) as f64 / 1e6,
            c.commit.load_acquire(),
            st.append_positions_sent.load(Ordering::Relaxed),
            st.naks_sent.load(Ordering::Relaxed),
        );
        last = d;
    }
}

/// Fleet leader: identical measurement to local mode — commit and the
/// latency samples are leader-local, so the cross-host numbers are the real
/// gate numbers (unlike M2, nothing here needs remote counters).
fn leader_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let f1: SocketAddr = args.get(2).expect("f1 addr").parse().unwrap();
    let f2: SocketAddr = args.get(3).expect("f2 addr").parse().unwrap();
    let secs: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(64);
    let admission_mib: u64 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(4);
    let buffer_mib: usize = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(256);
    let raw = UdpSocket::bind(bind.as_str()).unwrap();
    let (lb, lst, agents) = leader_node(raw, vec![f1, f2], journal, buffer_mib);
    let clock = Instant::now();
    let mut res = drive_load(&lb, secs, payload, admission_mib << 20, clock);
    let full = clock.elapsed().as_secs_f64();
    res.latencies_ns.sort_unstable();
    let (p50, p99) =
        (percentile(&res.latencies_ns, 0.50), percentile(&res.latencies_ns, 0.99));
    let committed_per_s = res.msgs as f64 / full;
    use Ordering::Relaxed as R;
    println!(
        "leader: {} msgs committed in {full:.2} s = {committed_per_s:.0}/s; p50 {:.3} ms p99 {:.3} ms; gossips {} overruns {}",
        res.msgs,
        p50 as f64 / 1e6,
        p99 as f64 / 1e6,
        lst.commit_gossips.load(R),
        lst.overruns.load(R),
    );
    println!(
        "GATE (>=400k committed/s, p50 <= 1 ms): {}",
        if committed_per_s >= 400_000.0 && p50 as f64 / 1e6 <= 1.0 { "PASS" } else { "FAIL" }
    );
    for a in agents {
        a.stop();
    }
}
```

- [ ] **Step 2: Build + local run**

Run:
```bash
cargo build -p uc2_net --release --example m3_gate
df -h /home/claude   # before
UC2_M3_MAX_BYTES=2000000000 cargo run -p uc2_net --release --example m3_gate -- \
    local /home/claude/uc2-m3-gate 10 64 4 256
```
Expected: per-second progress, final report with committed/s + p50/p99. On this 4-core sandbox a FAIL on either bar is the likely honest outcome (M2 precedent: 8 hot threads on 4 cores; and local ext4 fsync is ~ms-scale, which alone can sink p50) — report the numbers exactly as printed; do NOT tune the verdict. The correctness signals that MUST hold: `overruns 0`, commit drain completes, commit ≤ durable throughout.

- [ ] **Step 3: Clean up run artifacts**

```bash
rm -rf /home/claude/uc2-m3-gate
```
Verify `df -h /home/claude` is back to baseline. NEVER leave gate journals behind.

- [ ] **Step 4: Write the benchmark doc**

`docs/benchmarks/uc2-m3-gate-2026-07-10.md`, mirroring the M2 doc's structure (`docs/benchmarks/uc2-m2-gate-2026-07-10.md`):
- Date 2026-07-10. **Prominent banner:** single-host loopback smoke on a 4-vCPU sandbox — NOT the official gate; **the official M3 gate is THE PROJECT'S GO/NO-GO** (spec §9: ≥400k committed/s, p50 ≤1 ms, fsync on, 3×c6id over a real LAN) and runs on the fleet, appended here. If M3 misses badly there, the spec's instruction is stop and re-diagnose before M4–M6.
- What the gate measures (the full commit round trip: append → UDP fan-out → follower rebuild → follower fsync → AppendPosition → quorum rank → commit counter), host table, exact command, verbatim output, interpretation separating correctness (overruns 0, clean commit drain, commit ≤ durable, no phantom commits — cross-reference the harness tests) from the throughput/latency numbers and their sandbox caveats (core starvation per the M2 doc's analysis; ~ms ext4 fsync directly floors p50 locally — c6id NVMe fsync is tens of µs).
- Methodology: drain-inclusive single clock; sampled latency (every 1024th append, position-resolved against the commit counter); admission window vs commit (spec §7) — note this closes the M2 fleet-pacing wedge and cite the M2 doc's residual-risk paragraph as now-resolved.
- "Fleet (3×c6id) result — not yet run" placeholder ending with: the go/no-go decision happens THERE.

- [ ] **Step 5: Full workspace gates**

Run:
```bash
cargo test -p uc2_net && cargo test -p uc2_net --test replication && cargo test -p uc2_net --test commit
cargo test -p uc2_log && cargo test -p uc_protocol && cargo test -p uc2_consensus
cargo clippy --workspace -- -D warnings && cargo clippy -p uc2_net --all-targets -- -D warnings
cargo build -p uc2_net --release --example m3_gate
```
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add uc2_net/examples/m3_gate.rs docs/benchmarks/uc2-m3-gate-2026-07-10.md
git commit -m "feat(uc2_net): m3_gate commit-pipeline example + sandbox loopback smoke run"
```

---

## Self-review notes (already applied)

1. **Spec §6 steady-state coverage:** AppendPosition on durable advance + floor (T6), quorum rank bounded-by-own + monotonic once per duty cycle (T4/T5), cnc commit counter as the notification (T3/T5), CommitPosition gossip on advance + floor (T5), followers store gossip monotonically with the apply-time clamp explicitly deferred to M5 (T6 doc), control ≈ kHz by construction (block-granularity triggers — stated in Global Constraints and the T5 comment). Elections/votes/reconfiguration: correctly absent (M4/M6); linearizable reads: M5/M6. Failure-mode theorems at M3 scope: minority tolerated, quorum loss stalls cleanly, no phantom commits — all pinned in T7.
2. **The M2 final review's carry-list is fully dispatched:** hardening wave (T1), loom carry resolved-as-documentation with the TSO adjudication (T1 — a faithful model would false-fail; recorded in Global Constraints so the M3 final review sees it was resolved, not dropped), admission-vs-commit closes T10a (T8), control-body DRY stays moot (kinds 5/6 are header-only — noted in Global Constraints), `FollowerConfig.term_id` mutability deferred to M4 (Non-goals).
3. **Type consistency:** `CtrlMsg::AppendPos { from, durable }` produced in T5's demux and consumed in T5's sender arm; `CommitTracker::{new, on_durable, advance, commit}` signatures identical in T4 and T5; `LeaderReceiver::new(sock, to_sender, term_id)` with exactly three external call sites named in T5; counter name `commit` everywhere; kind constants `DGRAM_KIND_APPEND_POSITION`/`DGRAM_KIND_COMMIT_POSITION` consistent across T2/T5/T6/T7/T8.
4. **Single-writer audit for the new counter:** leader `commit` written only in the sender's `do_work` (tracker advance); follower `commit` written only in `on_datagram` (receiver thread). The m3_gate printer and load driver only read. No third writer anywhere in the plan.
5. **Harness honesty:** the forged-report test would catch BOTH leakage paths (unknown-source and stale-term) because the ghost position exceeds every real durable; the quorum-loss test keeps its load strictly under the dead followers' initial flow window so nothing blocks; the T7 extraction step gates on replication.rs's 5/5 before commit.rs exists.
6. **Gate example honesty:** verdict computed from the printed numbers, FAIL exits 1 (M2 precedent — fleet roles carry the real go/no-go; the leader role IS the real measurement cross-host since commit and latency are leader-local, an improvement over m2_gate's console-read fleet verdict). The doc must present a sandbox FAIL plainly if that is the outcome.
