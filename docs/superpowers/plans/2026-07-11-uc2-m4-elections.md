# UC v2 M4 — Elections, Term Map, Truncation + Deterministic Simulation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raft's safety core over positions (spec §6): leader elections with persisted votes, the fsync'd term map (RecordingLog analog), log truncation for reconciliation, replay sessions off the M2 Overrun seam, and the deterministic simulation (`uc_sim`) that gates all of it — gate: sim invariants green across seeded fuzz + scripted nasties, and sub-second failover on the 3-node harness (spec §9 M4 row).

**Architecture:** The election SM lives entirely in `uc_consensus` — pure, sync, message-in/action-out, time injected; the agent (in a new minimal `uc_node` crate) performs all I/O including StableValue persistence with a persist-BEFORE-answer contract. Commit ranking MOVES from the sender thread into the consensus agent (the spec §3.1 shape), making the consensus thread the commit counter's single writer in both roles forever — this dissolves the M3 "commit-writer handoff" carry. `uc_sim` exists and is green BEFORE the first networked election (spec §8 L1).

**Tech Stack:** Rust 2024; existing `uc_consensus`/`uc_net`/`uc_log`/`uc_protocol` (M3, committed); `uc_journal::{StableValue, Journal::truncate_after}` (verified present: `truncate_after(&self, keep_seq) -> Notifier` synchronous with pending-fence; `StableValue<T: Serialize + DeserializeOwned + Clone + Send + Sync>` with `open/load/store→Notifier/clear`); serde (workspace, for the persisted records only — the SM stays dep-free).

## Global Constraints

Every task's requirements implicitly include all of these (M1–M3 plan constraints remain in force for the files they cover):

- **Election safety invariants (spec §8 L1) are the acceptance bar, checked by `uc_sim` after EVERY step:** ≤1 leader per term; term-map prefix consistency across nodes; commit monotonicity (within a run); committed-never-truncated; leader completeness (every committed `(term, position)` range is in every later leader's history).
- **Vote rule (spec §6, verbatim):** grant `RequestVote(new_term_id, last_leadership_term_id, last_durable_position)` iff the term is new, no conflicting vote this term, and the candidate's `(last_term, durable_pos)` ≥ ours lexicographically. **Only durable positions count** — a crash discards the non-durable tail anyway. **The vote is persisted to StableValue BEFORE it is answered** — in the SM this is encoded as a single `GrantVote` action whose contract (documented on the action) is persist-then-send; the agent must `Notifier::wait()` the StableValue store before the datagram goes out.
- **New leader protocol (spec §6):** `term_id += 1`; `base_position` = own **durable** (bytes beyond durable are discarded when opening the term — `append` collapses to `durable`); term map appended + persisted BEFORE anything else in the new term; a **NewTerm no-op frame** is appended immediately and the leader **does not serve until it commits** (Raft §5.4.2 leader completeness). The SM exposes `can_serve()` for the gate.
- **Reconciliation (spec §6):** the leader ships its term map (suffix, capped — see wire task); a diverged follower truncates to the end of the last common `(term, base)` prefix — only ever uncommitted bytes, by vote/commit safety — then catches up via NAK/replay.
- **Truncation (spec §4):** `truncate_after` the last fully-valid journal block, re-append the surviving partial block prefix, reset the buffer counters to the truncation point, update the term map. Rare-path; correctness gated by the sim AND by the harness observing it after a partition heal. Truncation position is always frame-aligned (32) and never below the first archived block.
- **The commit counter's single writer is the consensus-agent thread, both roles.** M3's sender-thread ranking is REMOVED in this plan (Task 7); the receiver's follower-side CommitPosition store also moves to the consensus agent. Grep-provable: exactly one `commit.store_release` site after Task 8.
- **Commit is NOT persisted** (decision, resolving the M3 carry): Raft-standard — commit re-derives after restart from quorum reports/gossip; persisting it buys nothing for safety and risks staleness lies. The within-run-monotonic / cross-run-re-derived contract is documented on the counter (already, M3) and in the M5 sketch.
- **Terms become mutable:** `current_term` is an `Arc<AtomicU32>` written ONLY by the consensus agent (on transitions), read by data-path agents for stamping and checking. Data path accepts ONLY the exact current term; datagrams racing ahead of a local term transition are dropped and recovered via NAK (documented, bounded by election settle time). The M3 implausibility guard becomes term-scoped exactly this way: within the current term, `> own append ⇒ corrupt` still holds for the leader.
- **Buffer prefill on restart is explicitly CUT from M4** (spec §10 sizes it M4/M6 — we take M6): after restart or truncation, counters prime to the recovery point and positions below it read as `Overrun` → journal replay (the M2 Task-1 guard makes this safe). Documented, not silent.
- **`uc_sim` is deterministic**: virtual time, seeded xorshift faults (delay/drop/dup/reorder), injected crashes/restarts (persisted state survives: vote, term map, journal-durable; volatile resets: append collapses to durable, commit to 0). No `Instant`, no threads, no I/O anywhere in `uc_sim` or `uc_consensus`.
- Wire layouts in `uc_protocol::v2` stay core-only; kinds 7/8 promoted (RequestVote/Vote), kind 9 added (TermMap); `FRAME_TYPE_NEW_TERM = 3` added to the frame layer. All pinned with literal-byte tests (the M2 Task-2 standard).
- `Durability::Consistent`; 4 MiB test segments; test data small; gate runs journal on ext4 `/home/claude`, never `/tmp`; deadline-bounded waits everywhere; SPDX headers; `cargo clippy --workspace -- -D warnings` AND `cargo clippy -p uc_net -p uc_consensus -p uc_sim -p uc_node --all-targets -- -D warnings` clean after every task. Toolchain gotcha: clippy denies `manual_is_multiple_of` and `int_plus_one` — rewrite equivalently and report the deviation.
- Implementers stage ONLY their own task's files.

**Non-goals (M4):** membership changes/learners (M6 — static voting set); snapshots/purge (M6); the SDK/apply path and cnc mmap (M5 — `uc_node` here is the minimal agent-composition seed only: no discovery dir, no instance.lock, no client attach); leader leases / linearizable reads (M5/v2.x); wire auth (v2.0 posture); full WGL lincheck (M5 — it needs apply; M4's partition/failover assertions are commit-safety-level).

---

### Task 1: wire layer — vote frames, TermMap frame, NewTerm frame type

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (kinds 7/8/9 + three bodies)
- Modify: `uc_protocol/src/v2/frame.rs` (`FRAME_TYPE_NEW_TERM`)

**Interfaces:**
- Consumes: the frozen 16-byte datagram header; the frozen 32-byte frame header.
- Produces (used by Tasks 3–9):
  - `DGRAM_KIND_REQUEST_VOTE: u8 = 7` — body `RequestVoteBody { new_term: u32, last_term: u32, last_durable: u64 }` = 16 B (spec §6's exact triple; the header's `leadership_term_id` carries `new_term` too — readers use the body as authoritative, the header field lets generic term filters work).
  - `DGRAM_KIND_VOTE: u8 = 8` — body `VoteBody { term: u32, granted: bool }` encoded `{ term u32 @0, granted u8 @4, reserved 11 B zero }` = 16 B.
  - `DGRAM_KIND_TERM_MAP: u8 = 9` — body: `count u32 @0, reserved u32 @4`, then `count` entries of `TermMapEntryWire { term u32, reserved u32, base u64 }` = 16 B each. `MAX_TERM_MAP_WIRE_ENTRIES = 64` (64×16+8 = 1032 B ≤ the 1392 B MTU budget). The wire carries the **suffix** (most recent 64 terms); a follower whose common prefix is older than the shipped suffix cannot reconcile incrementally and falls back to full replay from 0 (safe, documented on the constant).
  - `FRAME_TYPE_NEW_TERM: u8 = 3` in `frame.rs` — a zero-payload no-op message frame appended by a new leader; archive/replay/walk treat it as a normal (non-padding) frame; M5's apply skips non-MESSAGE types.
  - Write/read fns for all three bodies, mirroring the NakBody/StatusBody pattern. (The DRY adjudication, again explicit: the five bodies now have three distinct shapes — u64+u32, u32+u32+u64, and variable-length — a shared helper still buys nothing; write them plainly.)

- [ ] **Step 1: Write the failing tests**

Append to the `tests` module in `uc_protocol/src/v2/datagram.rs`:

```rust
    #[test]
    fn vote_bodies_roundtrip_and_pin_layout() {
        let rv = RequestVoteBody { new_term: 7, last_term: 6, last_durable: 0x0000_0001_0000_0040 };
        let mut buf = [0u8; REQUEST_VOTE_BODY_LEN];
        write_request_vote_body(&mut buf, &rv);
        assert_eq!(read_request_vote_body(&buf), rv);
        // literal LE pin: new_term 7, last_term 6, last_durable 2^32+64
        assert_eq!(buf, [7, 0, 0, 0, 6, 0, 0, 0, 0x40, 0, 0, 0, 1, 0, 0, 0]);

        let v = VoteBody { term: 7, granted: true };
        let mut buf = [0u8; VOTE_BODY_LEN];
        write_vote_body(&mut buf, &v);
        assert_eq!(read_vote_body(&buf), v);
        assert_eq!(buf, [7, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let v = VoteBody { term: 7, granted: false };
        write_vote_body(&mut buf, &v);
        assert_eq!(buf[4], 0);
    }

    #[test]
    fn term_map_body_roundtrips_and_pins_layout() {
        let entries =
            [TermMapEntryWire { term: 1, base: 0 }, TermMapEntryWire { term: 3, base: 4096 }];
        let mut buf = [0u8; TERM_MAP_HEADER_LEN + 2 * TERM_MAP_ENTRY_LEN];
        let n = write_term_map_body(&mut buf, &entries);
        assert_eq!(n, 8 + 32);
        let mut out = [TermMapEntryWire { term: 0, base: 0 }; MAX_TERM_MAP_WIRE_ENTRIES];
        let m = read_term_map_body(&buf[..n], &mut out).expect("well-formed");
        assert_eq!(&out[..m], &entries);
        // literal pin: count 2, reserved 0, entry0 {1, rsvd, base 0}, entry1 {3, rsvd, base 4096}
        assert_eq!(
            &buf[..n],
            &[
                2, 0, 0, 0, 0, 0, 0, 0, // count + reserved
                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // term 1, base 0
                3, 0, 0, 0, 0, 0, 0, 0, 0, 0x10, 0, 0, 0, 0, 0, 0, // term 3, base 4096
            ][..]
        );
        // malformed: truncated entry -> None
        assert!(read_term_map_body(&buf[..n - 1], &mut out).is_none());
        // malformed: count beyond the cap -> None
        let mut big = [0u8; TERM_MAP_HEADER_LEN];
        big[0..4].copy_from_slice(&(MAX_TERM_MAP_WIRE_ENTRIES as u32 + 1).to_le_bytes());
        assert!(read_term_map_body(&big, &mut out).is_none());
    }

    // extend kind_codes_are_stable with:
    //   assert_eq!(DGRAM_KIND_REQUEST_VOTE, 7);
    //   assert_eq!(DGRAM_KIND_VOTE, 8);
    //   assert_eq!(DGRAM_KIND_TERM_MAP, 9);
```

And in `uc_protocol/src/v2/frame.rs`, extend the tests:

```rust
    #[test]
    fn frame_type_codes_are_stable() {
        assert_eq!(FRAME_TYPE_MESSAGE, 1);
        assert_eq!(FRAME_TYPE_PADDING, 2);
        assert_eq!(FRAME_TYPE_NEW_TERM, 3);
    }
```

- [ ] **Step 2: Run — expect compile failures**

Run: `cargo test -p uc_protocol vote_bodies term_map_body frame_type_codes`
Expected: FAIL — names not defined.

- [ ] **Step 3: Implement**

In `frame.rs`, after `FRAME_TYPE_PADDING`:

```rust
/// New-term no-op (spec §6, Raft §5.4.2): a zero-payload frame the new
/// leader appends immediately on opening a term and must see COMMIT before
/// serving. Replicated/archived/replayed like any message frame; the apply
/// layer (M5) skips every non-MESSAGE type.
pub const FRAME_TYPE_NEW_TERM: u8 = 3;
```

In `datagram.rs`, replace the `7..=8 reserved` comment and add the bodies:

```rust
/// Body = `RequestVoteBody` (spec §6): candidate solicits a vote for
/// `new_term` carrying its log position credentials. The header's
/// `leadership_term_id` also carries `new_term` (body is authoritative).
pub const DGRAM_KIND_REQUEST_VOTE: u8 = 7;
/// Body = `VoteBody`: the response. Granted votes are PERSISTED by the
/// granter before this datagram is sent (spec §6).
pub const DGRAM_KIND_VOTE: u8 = 8;
/// Body = term-map suffix (count + entries): the leader's term history for
/// follower reconciliation (spec §6). Ships at most
/// `MAX_TERM_MAP_WIRE_ENTRIES` most-recent entries; a follower whose common
/// prefix is older than the suffix falls back to full replay from 0.
pub const DGRAM_KIND_TERM_MAP: u8 = 9;

pub const REQUEST_VOTE_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVoteBody {
    pub new_term: u32,
    pub last_term: u32,
    pub last_durable: u64,
}

pub fn write_request_vote_body(buf: &mut [u8], b: &RequestVoteBody) {
    buf[0..4].copy_from_slice(&b.new_term.to_le_bytes());
    buf[4..8].copy_from_slice(&b.last_term.to_le_bytes());
    buf[8..16].copy_from_slice(&b.last_durable.to_le_bytes());
}

pub fn read_request_vote_body(buf: &[u8]) -> RequestVoteBody {
    RequestVoteBody {
        new_term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        last_term: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        last_durable: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
    }
}

pub const VOTE_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteBody {
    pub term: u32,
    pub granted: bool,
}

pub fn write_vote_body(buf: &mut [u8], b: &VoteBody) {
    buf[0..4].copy_from_slice(&b.term.to_le_bytes());
    buf[4] = b.granted as u8;
    buf[5..16].fill(0);
}

pub fn read_vote_body(buf: &[u8]) -> VoteBody {
    VoteBody {
        term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        granted: buf[4] != 0,
    }
}

pub const TERM_MAP_HEADER_LEN: usize = 8;
pub const TERM_MAP_ENTRY_LEN: usize = 16;
/// 64 × 16 + 8 = 1032 B — fits the 1392 B MTU body budget with room.
pub const MAX_TERM_MAP_WIRE_ENTRIES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermMapEntryWire {
    pub term: u32,
    pub base: u64,
}

/// Writes header + entries; returns bytes written. `entries.len()` must be
/// ≤ `MAX_TERM_MAP_WIRE_ENTRIES` (caller ships a suffix).
pub fn write_term_map_body(buf: &mut [u8], entries: &[TermMapEntryWire]) -> usize {
    debug_assert!(entries.len() <= MAX_TERM_MAP_WIRE_ENTRIES);
    buf[0..4].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    buf[4..8].copy_from_slice(&0u32.to_le_bytes());
    let mut o = TERM_MAP_HEADER_LEN;
    for e in entries {
        buf[o..o + 4].copy_from_slice(&e.term.to_le_bytes());
        buf[o + 4..o + 8].copy_from_slice(&0u32.to_le_bytes());
        buf[o + 8..o + 16].copy_from_slice(&e.base.to_le_bytes());
        o += TERM_MAP_ENTRY_LEN;
    }
    o
}

/// Returns the entry count read into `out`, or None if malformed (short
/// buffer, count over the cap, or trailing garbage length).
pub fn read_term_map_body(buf: &[u8], out: &mut [TermMapEntryWire]) -> Option<usize> {
    if buf.len() < TERM_MAP_HEADER_LEN {
        return None;
    }
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if count > MAX_TERM_MAP_WIRE_ENTRIES || count > out.len() {
        return None;
    }
    if buf.len() != TERM_MAP_HEADER_LEN + count * TERM_MAP_ENTRY_LEN {
        return None;
    }
    let mut o = TERM_MAP_HEADER_LEN;
    for slot in out.iter_mut().take(count) {
        *slot = TermMapEntryWire {
            term: u32::from_le_bytes(buf[o..o + 4].try_into().unwrap()),
            base: u64::from_le_bytes(buf[o + 8..o + 16].try_into().unwrap()),
        };
        o += TERM_MAP_ENTRY_LEN;
    }
    Some(count)
}
```

Extend `kind_codes_are_stable` with the three new asserts.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_protocol && cargo clippy -p uc_protocol -- -D warnings`
Expected: PASS; module stays core-only (no `use std`).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/datagram.rs uc_protocol/src/v2/frame.rs
git commit -m "feat(uc_protocol): v2 RequestVote/Vote/TermMap frames + NewTerm frame type"
```

---

### Task 2: `uc_log` — persisted node state (vote + term map) and `Archive::truncate_to`

**Files:**
- Create: `uc_log/src/state.rs`
- Modify: `uc_log/src/lib.rs` (add `pub mod state;`)
- Modify: `uc_log/Cargo.toml` (add `serde = { workspace = true, features = ["derive"] }` — check the workspace dep name/features in the root Cargo.toml and mirror how uc_journal declares it)
- Modify: `uc_log/src/archive.rs` (`truncate_to`)

**Interfaces:**
- Consumes: `uc_journal::{StableValue, StableValueConfig, Journal::truncate_after, Notifier}`.
- Produces (used by Tasks 3–9):
  - `state::VoteRecord { pub term: u32, pub voted_for: u32 }` (serde derive; `voted_for` = NodeId raw).
  - `state::TermMapEntry { pub term: u32, pub base: u64 }` and `state::TermMap = Vec<TermMapEntry>` (type alias).
  - `state::NodeState` — `open(dir: &Path) -> Result<Self, StableValueError>` (creates/opens `vote.state` + `term_map.state` in `dir`), `vote(&self) -> Option<VoteRecord>` (cached), `store_vote(&self, v: VoteRecord) -> Result<(), StableValueError>` (**store + Notifier::wait — durable on return**; this is what the persist-before-answer contract leans on), `term_map(&self) -> TermMap` (cached, empty if never stored), `store_term_map(&self, m: &TermMap) -> Result<(), StableValueError>` (durable on return).
  - `Archive::truncate_to(&mut self, pos: u64) -> Result<(), ArchiveError>` — truncates the journal so the archived stream ends exactly at `pos`: drops whole blocks with `base ≥ pos` via `truncate_after`, re-appends the partial prefix of the block containing `pos` (read block, `truncate_after(seq-1)`, `append(seq, base, &bytes[..pos-base])`, wait), updates `durable_pos`/`next_block_seq`. Errors: `pos` below the first archived base (`PositionPurged`), `pos` beyond the durable frontier (invalid — truncation never extends), `pos` not frame-aligned (debug_assert; positions come from term-map bases and block walks). `pos == durable frontier` is a no-op. The CALLER (consensus agent, Task 8) resets the buffer counters afterward (`prime(pos)`) — documented on the method.

- [ ] **Step 1: Write the failing tests (state)**

`uc_log/src/state.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_and_term_map_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = NodeState::open(dir.path()).unwrap();
            assert_eq!(s.vote(), None);
            assert!(s.term_map().is_empty());
            s.store_vote(VoteRecord { term: 3, voted_for: 2 }).unwrap();
            s.store_term_map(&vec![
                TermMapEntry { term: 1, base: 0 },
                TermMapEntry { term: 3, base: 4096 },
            ])
            .unwrap();
        }
        // "restart": reopen from the same dir
        let s = NodeState::open(dir.path()).unwrap();
        assert_eq!(s.vote(), Some(VoteRecord { term: 3, voted_for: 2 }));
        assert_eq!(
            s.term_map(),
            vec![TermMapEntry { term: 1, base: 0 }, TermMapEntry { term: 3, base: 4096 }]
        );
    }

    #[test]
    fn store_vote_overwrites_previous_term() {
        let dir = tempfile::tempdir().unwrap();
        let s = NodeState::open(dir.path()).unwrap();
        s.store_vote(VoteRecord { term: 1, voted_for: 0 }).unwrap();
        s.store_vote(VoteRecord { term: 2, voted_for: 1 }).unwrap();
        assert_eq!(s.vote(), Some(VoteRecord { term: 2, voted_for: 1 }));
    }
}
```

- [ ] **Step 2: Implement state.rs**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Persisted per-node consensus state (spec §6): the vote record and the
//! term map (the RecordingLog analog), each a rotating two-slot
//! `StableValue`. Both stores are DURABLE ON RETURN (`Notifier::wait`) —
//! the vote's persist-before-answer contract and the term map's
//! open-term-before-serving contract both depend on that, so this module
//! never exposes a fire-and-forget store.

use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use uc_journal::{StableValue, StableValueConfig, StableValueError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRecord {
    pub term: u32,
    /// NodeId of the candidate voted for in `term`.
    pub voted_for: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TermMapEntry {
    pub term: u32,
    /// Absolute stream position where this leadership term begins.
    pub base: u64,
}

pub type TermMap = Vec<TermMapEntry>;

pub struct NodeState {
    vote: StableValue<VoteRecord>,
    term_map: StableValue<TermMap>,
    /// Cached copies (StableValue::load re-reads the file; consensus reads
    /// these on every SM recovery/step and must not do I/O).
    cache: Mutex<(Option<VoteRecord>, TermMap)>,
}

impl NodeState {
    pub fn open(dir: &Path) -> Result<Self, StableValueError> {
        let vote = StableValue::open(StableValueConfig::new(dir.join("vote.state")))?;
        let term_map = StableValue::open(StableValueConfig::new(dir.join("term_map.state")))?;
        let v = vote.load()?;
        let m = term_map.load()?.unwrap_or_default();
        Ok(Self { vote, term_map, cache: Mutex::new((v, m)) })
    }

    pub fn vote(&self) -> Option<VoteRecord> {
        self.cache.lock().unwrap().0
    }

    pub fn term_map(&self) -> TermMap {
        self.cache.lock().unwrap().1.clone()
    }

    /// Durable on return — the caller may answer the vote request after this.
    pub fn store_vote(&self, v: VoteRecord) -> Result<(), StableValueError> {
        self.vote.store(&v)?.wait().map_err(StableValueError::from)?;
        self.cache.lock().unwrap().0 = Some(v);
        Ok(())
    }

    /// Durable on return — the new term exists before the leader acts in it.
    pub fn store_term_map(&self, m: &TermMap) -> Result<(), StableValueError> {
        self.term_map.store(m)?.wait().map_err(StableValueError::from)?;
        self.cache.lock().unwrap().1 = m.clone();
        Ok(())
    }
}
```

NOTE for the implementer: check what error type `Notifier::wait()` returns and how it converts into `StableValueError` — if there is no `From` impl, wrap it in the closest existing `StableValueError` variant (read the enum in `uc_journal/src/error.rs`) and report what you chose. The durable-on-return property is the requirement; the exact error plumbing is yours.

- [ ] **Step 3: Write the failing tests (truncate_to)**

Append to `uc_log/src/archive.rs` tests (the `setup`/`test_cfg` helpers exist):

```rust
    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn truncate_to_drops_tail_and_reappends_partial_block() {
        let (b, _c, dir) = setup(1 << 16);
        // small blocks so the stream spans several: 2 frames per block
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..8 {
            a.append(1, i, &[i as u8; 64]).unwrap(); // 8 x 96 B = 768
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 768); // 4 blocks of 192
        // truncate mid-block-2: keep [0, 480) = blocks 0,1 whole + 96 of block 2
        arch.truncate_to(480).unwrap();
        assert_eq!(arch.recovered_position(), 480);
        // replay sees exactly frames 0..4 (positions 0..480), nothing beyond
        let mut r = arch.replay_from(0).unwrap();
        for i in 0..5u64 {
            let f = r.next().unwrap().expect("frame");
            assert_eq!(f.header.correlation_id, i);
        }
        assert!(r.next().unwrap().is_none());
        // the archive keeps working after truncation: append + record resumes
        let (b2, c2, _) = setup(1 << 16);
        c2.prime(480);
        let mut a2 = Appender::new(Arc::clone(&b2), 2);
        assert_eq!(a2.append(1, 100, &[9u8; 64]).unwrap(), 480);
        assert!(arch.do_work(&b2).unwrap());
        assert_eq!(arch.recovered_position(), 576);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_to_block_boundary_and_noop_and_errors() {
        let (b, _c, dir) = setup(1 << 16);
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 384);
        // exact block boundary: drop block 1 whole, no re-append needed
        arch.truncate_to(192).unwrap();
        assert_eq!(arch.recovered_position(), 192);
        // no-op at the frontier
        arch.truncate_to(192).unwrap();
        assert_eq!(arch.recovered_position(), 192);
        // beyond the frontier: error (truncation never extends)
        assert!(arch.truncate_to(500).is_err());
        // survives reopen: recovery sees the truncated frontier
        drop(arch);
        let arch = Archive::open(ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) })
            .unwrap();
        assert_eq!(arch.recovered_position(), 192);
    }
```

- [ ] **Step 4: Implement `truncate_to`**

In `uc_log/src/archive.rs`, a new method on `Archive` (below `do_work`) plus one error variant:

```rust
    /// Truncate the archived stream to end exactly at `pos` (spec §4,
    /// election reconciliation): drop whole blocks at/above `pos`, re-append
    /// the partial prefix of the block containing it. `pos` must be a frame
    /// boundary within (first archived base ..= durable frontier]. The
    /// CALLER resets the buffer counters afterward (`counters.prime(pos)`)
    /// and re-derives everything volatile — this method touches only the
    /// journal and the archive's own cursors.
    pub fn truncate_to(&mut self, pos: u64) -> Result<(), ArchiveError> {
        debug_assert_eq!(pos % 32, 0, "truncation positions are frame boundaries");
        if pos == self.durable_pos {
            return Ok(());
        }
        if pos > self.durable_pos {
            return Err(ArchiveError::PositionPurged { pos, first_base: self.durable_pos });
        }
        let (Some(first), Some(last)) = (self.journal.first_seq(), self.journal.last_seq())
        else {
            return Err(ArchiveError::PositionPurged { pos, first_base: 0 });
        };
        let (first_base, _) = self.journal.read(first)?.expect("first block readable");
        if pos < first_base {
            return Err(ArchiveError::PositionPurged { pos, first_base });
        }
        // binary search: greatest block with base <= pos (replay_from's shape)
        let (mut lo, mut hi) = (first, last);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            let (meta, _) = self.journal.read(mid)?.expect("block readable");
            if meta <= pos { lo = mid } else { hi = mid - 1 }
        }
        let (base, bytes) = self.journal.read(lo)?.expect("block readable");
        if base == pos {
            // pos is exactly this block's start: keep everything before it
            debug_assert!(lo > 0, "pos == first_base was handled above");
            self.journal.truncate_after(lo - 1)?.wait()?;
            self.next_block_seq = lo;
        } else {
            // partial block: keep [base, pos) — truncate it away, re-append
            // the prefix at the same seq
            let keep = (pos - base) as usize;
            debug_assert!(keep < bytes.len());
            if lo == 0 {
                // truncate_after keeps >= one block; drop-and-reappend via
                // the truncation fence at seq 0 is expressed as keep_seq = 0
                // AFTER rewriting: read uc_journal's truncate_after
                // contract — it keeps seq 0. So: truncate to keep block 0,
                // then we cannot shrink it in place; instead truncate to
                // keep nothing before it is impossible — handle by
                // re-appending at seq 0 after truncate_after(0) has removed
                // blocks 1.. and then using a fresh append at seq 1? NO —
                // see the implementer note below; the correct sequence is
                // uniform for lo >= 1 and lo == 0 differs only in whether a
                // truncate_after(lo - 1) call exists.
                self.journal.truncate_after(0)?.wait()?;
            } else {
                self.journal.truncate_after(lo - 1)?.wait()?;
            }
            // after truncate_after(lo-1) the journal's last seq is lo-1 (or,
            // for lo == 0, truncate_after(0) leaves block 0 — which we are
            // about to replace and CANNOT, so lo == 0 partial truncation
            // requires journal support for full clear. See implementer note.
            self.journal.append(lo, base, &bytes[..keep])?.wait()?;
            self.next_block_seq = lo + 1;
        }
        self.durable_pos = pos;
        Ok(())
    }
```

**IMPLEMENTER NOTE — resolve before writing code (the plan flags this honestly rather than guessing):** the `lo == 0` partial-block branch above is written defensively because `truncate_after(keep_seq)` cannot express "remove every block". Read `uc_journal/src/journal/mod.rs::truncate_after` and determine the real contract: (a) if `truncate_after(0)` keeps seq 0 and a subsequent `append(0, ...)` at an existing seq is rejected (NonMonotonicSeq), the partial-`lo==0` case needs `truncate_after` semantics that support re-appending at `keep_seq`… check whether appending at `keep_seq + 1`-style is the only legal continuation. If partial truncation of block 0 is genuinely inexpressible, make `truncate_to` return a clear error for that case and document it: in practice it is unreachable in M4 — reconciliation truncation points are term-map bases or beyond, and term 1's base is position 0 = block 0's start, which takes the `base == pos` (whole-block) path or is a no-op; a PARTIAL cut inside block 0 requires a divergence within the very first block of the whole cluster history while a competing committed prefix exists, which the sim can be used to confirm never happens. Whatever you determine, encode it as a test (either the working path or the documented error) and state the resolution in your report. Add the error variant if needed:

```rust
    #[error("cannot truncate to {pos}: partial cut inside the first archived block")]
    UnsupportedTruncation { pos: u64 },
```

- [ ] **Step 5: Run everything**

Run: `cargo test -p uc_log && cargo clippy --workspace -- -D warnings`
Expected: all green (2 state + 2 truncate tests new).

- [ ] **Step 6: Commit**

```bash
git add uc_log/src/state.rs uc_log/src/lib.rs uc_log/Cargo.toml Cargo.lock uc_log/src/archive.rs
git commit -m "feat(uc_log): persisted vote/term-map NodeState + Archive::truncate_to"
```

---

### Task 3: `uc_consensus` — the election SM core (roles, votes, timeouts)

The heart of M4. Pure, sync, deterministic: events in, actions out, time injected. The SM never does I/O — the persist-before-answer contract is encoded in action semantics the agent must honor.

**Files:**
- Create: `uc_consensus/src/election.rs`
- Modify: `uc_consensus/src/lib.rs` (add `pub mod election;`)
- Modify: `uc_consensus/src/commit.rs` (add `CommitTracker::reset_reports(&mut self)` — clears per-follower reports on term transitions; commit itself stays monotonic)

**Interfaces:**
- Consumes: `crate::commit::CommitTracker`.
- Produces (used by Tasks 5, 8, 9):

```rust
pub type NodeId = u32;

pub struct ElectionConfig {
    pub id: NodeId,
    /// Static voting membership, self included. Position in this Vec is the
    /// follower index used by CommitTracker when leader.
    pub members: Vec<NodeId>,
    pub election_timeout_min_ns: u64, // default 150_000_000
    pub election_timeout_max_ns: u64, // default 300_000_000
    pub seed: u64,
}

pub enum Event {
    /// Time advanced; the ONLY driver of timeouts.
    Tick { now_ns: u64 },
    /// Local durable advanced (from the archive, via the agent).
    DurableAdvanced { durable: u64 },
    /// Any datagram from the current leader observed (data, heartbeat,
    /// commit gossip) — leadership liveness (spec §6).
    LeaderSeen { term: u32 },
    /// AppendPosition report (leader role input to commit ranking).
    Report { from: NodeId, term: u32, durable: u64 },
    /// CommitPosition gossip (follower role input).
    CommitGossip { term: u32, commit: u64 },
    RequestVote { from: NodeId, new_term: u32, last_term: u32, last_durable: u64 },
    Vote { from: NodeId, term: u32, granted: bool },
    /// The NewTerm frame this node appended (leader) reached position P.
    NewTermAppended { position: u64 },
}

pub enum Action {
    /// PERSIST the vote record durably, THEN send the (granted) vote.
    /// The agent MUST NOT send before the persist completes.
    PersistAndSendVote { to: NodeId, vote: VoteOut },
    /// Send a rejection (no persistence needed — nothing was promised).
    SendVoteRejection { to: NodeId, term: u32 },
    /// Broadcast RequestVote{new_term, last_term, last_durable} to peers.
    StartElection { new_term: u32, last_term: u32, last_durable: u64 },
    /// Open a term as leader: the agent must, IN ORDER: (1) append the new
    /// TermMapEntry{term, base} + persist the term map durably; (2) collapse
    /// volatile append to `base` (durable) — discarding the unreplicated
    /// tail; (3) append the NewTerm no-op frame and feed NewTermAppended
    /// back; (4) switch data-plane roles to leader.
    BecomeLeader { term: u32, base: u64 },
    /// Step down / adopt: switch data-plane roles to follower of `term`.
    BecomeFollower { term: u32, leader: Option<NodeId> },
    /// Store the commit counter (the agent owns the store; single writer).
    AdvanceCommit { commit: u64 },
    /// Gossip CommitPosition{commit} to followers (leader only).
    GossipCommit { commit: u64 },
}

pub struct VoteOut { pub term: u32, pub voted_for: NodeId, pub granted_to: NodeId }

pub enum Role { Follower, Candidate, Leader }

pub struct ElectionSm { /* private */ }

impl ElectionSm {
    /// `recovered_vote`/`recovered_term_map` come from NodeState;
    /// `durable` from archive recovery. current_term starts at
    /// max(vote.term, last term-map term).
    pub fn new(
        cfg: ElectionConfig,
        recovered_vote: Option<(u32, NodeId)>,
        recovered_term_map: &[(u32, u64)],
        durable: u64,
        now_ns: u64,
    ) -> Self;
    pub fn step(&mut self, ev: Event, out: &mut Vec<Action>);
    pub fn role(&self) -> Role;
    pub fn current_term(&self) -> u32;
    /// Leader-only: true once the NewTerm frame has committed (Raft §5.4.2).
    pub fn can_serve(&self) -> bool;
    /// The term map including any entry opened this run.
    pub fn term_map(&self) -> &[(u32, u64)];
}
```

Semantics (the tests below pin each):
- **Follower**: on `Tick`, if `now - last_leader_activity > timeout` (randomized per arming from `[min,max)`, xorshift seeded) → become Candidate: `term+1`, vote for self (a `PersistAndSendVote` to SELF is silly — self-vote is persisted via the SAME action addressed to self; the agent recognizes `to == cfg.id` and skips the network send), then `StartElection`.
- **Vote rule**: grant iff `new_term > current_term` OR (`new_term == current_term` AND not yet voted this term AND same candidate re-request → re-grant idempotently); AND `(last_term, last_durable) ≥ (our last term-map term, our durable)` lexicographically. Granting a vote for a higher term also adopts that term (BecomeFollower with `leader: None`). Rejections carry our current term.
- **Candidate**: counts grants for its term (self included); majority (`members.len()/2 + 1`) → `BecomeLeader{term, base: durable}`. A `RequestVote`/any event with a HIGHER term → adopt, step down. Timeout while candidate → new election (term+1 again).
- **Leader**: `Report` events feed the embedded CommitTracker (reports keyed by member index; `reset_reports` on term open); once per `Tick` ranks against own durable — BUT while `can_serve()` is false, commit advancing past the NewTerm position flips `can_serve` true. Emits `AdvanceCommit`/`GossipCommit` on advance. `LeaderSeen`/any event with a higher term → `BecomeFollower`.
- **Stale everything dropped**: events carrying `term < current_term` are ignored (except `RequestVote`, which gets a rejection carrying our term so the stale candidate learns).
- Follower commit intake: `CommitGossip{term == current}` → `AdvanceCommit{max(seen)}` (monotonic within run — the SM tracks `commit_seen`).

- [ ] **Step 1: Write the failing tests**

Tests at the bottom of `uc_consensus/src/election.rs` (a driver helper keeps them readable):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(id: NodeId) -> ElectionConfig {
        ElectionConfig {
            id,
            members: vec![0, 1, 2],
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
            seed: 42 + id as u64,
        }
    }

    fn sm(id: NodeId) -> ElectionSm {
        ElectionSm::new(cfg(id), None, &[], 0, 0)
    }

    fn step(sm: &mut ElectionSm, ev: Event) -> Vec<Action> {
        let mut out = Vec::new();
        sm.step(ev, &mut out);
        out
    }

    #[test]
    fn timeout_starts_election_and_majority_wins() {
        let mut s = sm(0);
        assert!(matches!(s.role(), Role::Follower));
        // no leader activity: tick past the max timeout
        let acts = step(&mut s, Event::Tick { now_ns: 301 });
        assert!(matches!(s.role(), Role::Candidate));
        assert_eq!(s.current_term(), 1);
        // self-vote persisted + broadcast
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::PersistAndSendVote { to, vote } if *to == 0 && vote.voted_for == 0 && vote.term == 1
        )));
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::StartElection { new_term: 1, last_term: 0, last_durable: 0 }
        )));
        // one grant (self) + one from node 1 = majority of 3
        let acts = step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeLeader { term: 1, base: 0 })));
        assert!(matches!(s.role(), Role::Leader));
        assert!(!s.can_serve(), "must not serve before NewTerm commits");
    }

    #[test]
    fn vote_rule_lexicographic_on_durable_credentials() {
        // our node has durable 1000 in term 2
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 500)], 1000, 0);
        // candidate behind on durable: reject
        let acts = step(
            &mut s,
            Event::RequestVote { from: 2, new_term: 3, last_term: 2, last_durable: 900 },
        );
        assert!(acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 2, .. })));
        assert!(!acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { .. })));
        // candidate with a newer last_term but lower durable: lexicographic -> grant
        let acts = step(
            &mut s,
            Event::RequestVote { from: 0, new_term: 4, last_term: 3, last_durable: 100 },
        );
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::PersistAndSendVote { to: 0, vote } if vote.term == 4 && vote.voted_for == 0
        )));
        assert_eq!(s.current_term(), 4);
    }

    #[test]
    fn one_vote_per_term_and_idempotent_regrant() {
        let mut s = sm(1);
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { to: 0, .. })));
        // different candidate, same term: reject (no double vote)
        let acts =
            step(&mut s, Event::RequestVote { from: 2, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 2, .. })));
        // same candidate re-requests (lost datagram): idempotent re-grant
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { to: 0, .. })));
    }

    #[test]
    fn recovered_vote_is_honored_across_restart() {
        // restarted node had voted for 2 in term 5
        let mut s = ElectionSm::new(cfg(1), Some((5, 2)), &[], 0, 0);
        assert_eq!(s.current_term(), 5);
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 5, last_term: 0, last_durable: 0 });
        assert!(
            acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 0, .. })),
            "must not double-vote in a term after restart"
        );
    }

    #[test]
    fn leader_gates_serving_on_new_term_commit_and_ranks_reports() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        // agent appended the NewTerm frame at [0, 32)
        step(&mut s, Event::NewTermAppended { position: 32 });
        // own durable covers it; follower 1 reports durable 32
        step(&mut s, Event::DurableAdvanced { durable: 32 });
        let acts = step(&mut s, Event::Report { from: 1, term: 1, durable: 32 });
        let acts2 = step(&mut s, Event::Tick { now_ns: 310 });
        let advanced = acts
            .iter()
            .chain(acts2.iter())
            .any(|a| matches!(a, Action::AdvanceCommit { commit: 32 }));
        assert!(advanced, "quorum on the NewTerm frame must commit it");
        assert!(s.can_serve());
    }

    #[test]
    fn higher_term_deposes_leader_and_stale_events_ignored() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        // stale report: ignored, no panic, no action
        let acts = step(&mut s, Event::Report { from: 1, term: 0, durable: 999 });
        assert!(acts.is_empty());
        // a higher-term RequestVote deposes
        let acts =
            step(&mut s, Event::RequestVote { from: 2, new_term: 2, last_term: 1, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 2, .. })));
        assert!(matches!(s.role(), Role::Follower));
    }

    #[test]
    fn follower_commit_gossip_is_monotonic_and_term_checked() {
        let mut s = sm(1);
        // adopt term 1 via a grant
        step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        let acts = step(&mut s, Event::CommitGossip { term: 1, commit: 4096 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 4096 })));
        // stale-term and regressing gossip: no action
        assert!(step(&mut s, Event::CommitGossip { term: 0, commit: 9999 }).is_empty());
        assert!(step(&mut s, Event::CommitGossip { term: 1, commit: 1024 }).is_empty());
    }

    #[test]
    fn split_vote_retries_with_new_term_and_randomized_timeout() {
        let mut a = sm(0);
        step(&mut a, Event::Tick { now_ns: 301 });
        assert_eq!(a.current_term(), 1);
        // nobody answers; candidate times out again -> term 2
        step(&mut a, Event::Tick { now_ns: 1000 });
        assert!(a.current_term() >= 2);
        assert!(matches!(a.role(), Role::Candidate));
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_consensus election`
Expected: FAIL — module/types not defined.

- [ ] **Step 3: Implement the SM**

Write `uc_consensus/src/election.rs` implementing exactly the interface + semantics above. Structure guidance (the implementer owns the internals; these invariants are binding):

- State: `cfg`, `role`, `current_term`, `voted_for: Option<(u32 term, NodeId)>`, `term_map: Vec<(u32, u64)>`, `durable`, `commit_seen`, `last_leader_activity_ns`, `timeout_deadline_ns` (re-randomized on every arming via the crate-local xorshift — copy the 10-line XorShift64 from `uc_net/src/fault.rs` into this crate as a private helper; `uc_consensus` stays dep-free), `votes_received: Vec<NodeId>` (candidate), `tracker: CommitTracker` (leader), `new_term_pos: Option<u64>`, `serving: bool`.
- EVERY event first passes the term filter: `event_term > current_term` → adopt (update term, clear candidate state, `BecomeFollower`) then process; `event_term < current_term` → drop (RequestVote → rejection). `Tick`/`DurableAdvanced`/`NewTermAppended` carry no term.
- `reset_reports` on `BecomeLeader` (fresh tracker slots; commit stays monotonic across the transition — a new leader's commit starts at its recovered `commit_seen`, never regresses within the run).
- Leader ranking on `Tick` AND on `Report` (cheap); `AdvanceCommit` + `GossipCommit` only on advance. `can_serve` flips when `commit ≥ new_term_pos`.
- The action `Vec` is drained by the caller; `step` never allocates beyond pushing actions (Vec reuse is the caller's).
- Doc comment on the module: the I/O contract table (which action requires what agent behavior, verbatim from the Interfaces block).

Also add to `commit.rs`:

```rust
    /// Clear per-follower reports (term transition: stale-term reports must
    /// not certify bytes in the new term). Commit itself stays monotonic.
    pub fn reset_reports(&mut self) {
        for r in &mut self.reported {
            *r = 0;
        }
    }
```

with a test asserting reports clear but `commit()` is unchanged.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_consensus && cargo clippy --workspace -- -D warnings`
Expected: all green (8 election tests + 9 commit tests).

- [ ] **Step 5: Commit**

```bash
git add uc_consensus/src/election.rs uc_consensus/src/lib.rs uc_consensus/src/commit.rs
git commit -m "feat(uc_consensus): election SM — roles, persisted-vote rule, NewTerm serving gate"
```

---

### Task 4: `uc_consensus` — reconciliation (term-map compare → truncation)

**Files:**
- Create: `uc_consensus/src/reconcile.rs`
- Modify: `uc_consensus/src/election.rs` (wire the events/actions)
- Modify: `uc_consensus/src/lib.rs` (add `pub mod reconcile;`)

**Interfaces:**
- Consumes: Task 3's `ElectionSm`.
- Produces (used by Tasks 5, 8, 9):

```rust
// reconcile.rs — a PURE function, exhaustively testable:
pub enum Reconcile {
    /// Histories agree up to our durable — nothing to do.
    Clean,
    /// Truncate our log to `to` and replace our term map with `new_map`
    /// (the common prefix + adopted leader entries with base <= to).
    Truncate { to: u64, new_map: Vec<(u32, u64)> },
    /// No common entry (the leader's shipped suffix is beyond our history)
    /// — incremental reconciliation impossible; M6's snapshot install is
    /// the real answer. M4 surfaces it loudly (sim + harness prove it
    /// unreachable at <= MAX_TERM_MAP_WIRE_ENTRIES terms).
    NoCommonPrefix,
}

/// `own`/`leader` are (term, base) maps, ascending; `own_durable` bounds
/// what we actually hold. Rules:
/// - find the longest common prefix of entries;
/// - our bytes are valid up to min(own_durable,
///     own[k+1].base   if we have a divergent next entry,
///     leader[k+1].base if the leader opened a newer term below our durable
///                      — our bytes beyond it belong to a term the leader
///                      never had);
/// - if that bound < own_durable -> Truncate, else Clean;
/// - new_map additionally ADOPTS leader entries with base <= the bound
///   (terms that genuinely cover our surviving bytes).
pub fn reconcile(own: &[(u32, u64)], own_durable: u64, leader: &[(u32, u64)]) -> Reconcile;
```

- `ElectionSm` additions:
  - `Event::TermMapReceived { term: u32, entries: Vec<(u32, u64)> }` (follower; term-filtered like CommitGossip) → runs `reconcile(own_map, durable, &entries)`; on `Truncate` emits `Action::Truncate { to, new_map }` and enters a `truncating` latch (ignores data-plane events until the feedback); on `NoCommonPrefix` emits `Action::Fatal { reason: &'static str }` (the agent logs-and-panics; sim asserts it never fires).
  - `Event::Truncated { to: u64 }` (agent feedback after `Archive::truncate_to` + counter re-prime) → SM sets `durable = to`, adopts `new_map`, clears the latch.
  - `Action::ShipTermMap { entries: Vec<(u32, u64)> }` — emitted by the leader on `BecomeLeader` (right after the term-map append) and re-emitted on the same floor cadence as `GossipCommit` (the agent piggybacks it; entries = the last `MAX_TERM_MAP_WIRE_ENTRIES`).
  - Follower term-map ADOPTION without divergence: when `reconcile` returns `Clean` but the leader map has entries we lack with `base <= durable`, adopt them (keeps vote credentials honest as data streams in) — the SM emits `Action::PersistTermMap { new_map }` for the agent (durable store).

- [ ] **Step 1: Write the failing tests (pure function first)**

`uc_consensus/src/reconcile.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_histories_are_clean() {
        let m = [(1, 0), (3, 4096)];
        assert!(matches!(reconcile(&m, 8000, &m), Reconcile::Clean));
    }

    #[test]
    fn divergent_own_tail_truncates_at_own_divergent_base() {
        // common: (1,0). We opened term 2 at 4096 (we were a leader that
        // never won quorum); real history went term 3 at 4096.
        let own = [(1, 0), (2, 4096)];
        let leader = [(1, 0), (3, 4096)];
        match reconcile(&own, 6000, &leader) {
            Reconcile::Truncate { to, new_map } => {
                assert_eq!(to, 4096);
                assert_eq!(new_map, vec![(1, 0)]);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn within_term_overhang_truncates_at_leaders_next_base() {
        // We stayed in term 1 and wrote to 6000; the cluster moved to term 2
        // at 5000. Our [5000, 6000) bytes are term-1 bytes the quorum never
        // certified — truncate to 5000 and ADOPT the term-2 entry? No:
        // adoption only for entries with base <= the bound; (2,5000) has
        // base == bound, and we hold zero bytes of term 2 -> not adopted.
        let own = [(1, 0)];
        let leader = [(1, 0), (2, 5000)];
        match reconcile(&own, 6000, &leader) {
            Reconcile::Truncate { to, new_map } => {
                assert_eq!(to, 5000);
                assert_eq!(new_map, vec![(1, 0)]);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn behind_follower_is_clean_and_adopts_covering_entries() {
        // We hold 3000 bytes, all term 1; leader history has term 2 at 2000:
        // our bytes [2000, 3000) were streamed by the term-2 leader — the
        // entry covers our bytes, adopt it. Clean-with-adoption is returned
        // as Truncate { to: own_durable } by convention? NO — keep the
        // variants honest: Clean means no byte is invalid. Adoption without
        // truncation is signaled by new_map on a third variant? Simplest
        // correct shape: reconcile returns the BOUND and the map; the caller
        // compares bound with durable. Refactor the enum:
        //   pub struct Outcome { pub valid_up_to: u64, pub new_map: Vec<(u32,u64)> }
        //   pub enum Reconcile { Ok(Outcome), NoCommonPrefix }
        // Truncation needed iff valid_up_to < own_durable; map adoption
        // applies either way. THE IMPLEMENTER MUST USE THIS SHAPE (the enum
        // sketch above in Interfaces is superseded by this test's shape —
        // deliberate: the test is the contract).
        let own = [(1, 0)];
        let leader = [(1, 0), (2, 2000)];
        match reconcile(&own, 3000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 3000);
                assert_eq!(o.new_map, vec![(1, 0), (2, 2000)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        // and the divergence cases above return Ok with valid_up_to < durable
    }

    #[test]
    fn no_common_prefix_is_surfaced() {
        let own = [(1, 0)];
        let leader = [(40, 1 << 20), (41, 2 << 20)]; // suffix-capped map
        assert!(matches!(reconcile(&own, 5000, &leader), Reconcile::NoCommonPrefix));
    }

    #[test]
    fn empty_own_map_adopts_leader_prefix_below_durable() {
        // fresh follower, no history, no bytes
        match reconcile(&[], 0, &[(1, 0), (2, 5000)]) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 0);
                assert_eq!(o.new_map, vec![]); // no bytes -> nothing to adopt yet
            }
            other => panic!("{other:?}"),
        }
    }
}
```

**Contract note (deliberate, binding):** the fourth test supersedes the Interfaces sketch — implement `Reconcile::Ok(Outcome { valid_up_to, new_map }) | NoCommonPrefix`; the SM derives `Truncate` (valid_up_to < durable) vs `PersistTermMap`-only (map grew) vs nothing (identical) from the outcome. Adoption rule: leader entries with `base < valid_up_to` are adopted (an entry AT the bound covers zero of our bytes); entries we share are kept; a leader map whose FIRST entry's base > our first entry's base with no overlap → NoCommonPrefix. Special case: `own` empty ⇒ common prefix is trivially empty; valid_up_to = own_durable (0 for a fresh node); adopt leader entries with base < valid_up_to.

- [ ] **Step 2: SM wiring tests**

Append to `election.rs` tests:

```rust
    #[test]
    fn follower_truncates_on_divergent_term_map_and_resumes_after_feedback() {
        // node 1 was a failed leader: own map (1,0),(2,4096), durable 6000
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
        // adopt term 3 via a grant, then the term-3 leader ships its map
        step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 1, last_durable: 7000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] },
        );
        let trunc = acts.iter().find_map(|a| match a {
            Action::Truncate { to, new_map } => Some((*to, new_map.clone())),
            _ => None,
        });
        let (to, new_map) = trunc.expect("must truncate the divergent tail");
        assert_eq!(to, 4096);
        assert_eq!(new_map, vec![(1, 0)]);
        // while truncating: data-plane events latched (no commit advance)
        assert!(step(&mut s, Event::CommitGossip { term: 3, commit: 5000 }).is_empty());
        // agent feedback: truncation done
        step(&mut s, Event::Truncated { to: 4096 });
        assert_eq!(s.term_map(), &[(1, 0)]);
        // commit gossip clamps nothing here — it flows again (bounded by
        // durable at apply time, M5; the counter itself is raw)
        let acts = step(&mut s, Event::CommitGossip { term: 3, commit: 5000 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 5000 })));
    }

    #[test]
    fn leader_ships_term_map_on_open() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        let acts = step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeLeader { .. })));
        assert!(acts.iter().any(
            |a| matches!(a, Action::ShipTermMap { entries } if entries == &vec![(1, 0)])
        ));
    }
```

- [ ] **Step 3: Implement** (`reconcile.rs` pure fn + the SM wiring per the contracts above; `Action::{Truncate, ShipTermMap, PersistTermMap, Fatal}` and `Event::{TermMapReceived, Truncated}` added; the truncating latch drops data-plane events but still processes term/vote events — a higher term must always be adoptable).

- [ ] **Step 4: Run**

Run: `cargo test -p uc_consensus && cargo clippy --workspace -- -D warnings`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add uc_consensus/src/reconcile.rs uc_consensus/src/election.rs uc_consensus/src/lib.rs
git commit -m "feat(uc_consensus): reconciliation — term-map compare, truncation actions, adoption"
```

---

### Task 5: `uc_sim` — the deterministic simulation (spec §8 L1)

Exists and is green BEFORE any networked election. One thread, virtual time, seeded faults, injected crashes; invariants after every step.

**Files:**
- Create: `uc_sim/Cargo.toml` (dep: `uc_consensus = { path = "../uc_consensus" }` only)
- Create: `uc_sim/src/lib.rs`
- Create: `uc_sim/src/world.rs` (the simulator)
- Create: `uc_sim/src/invariants.rs`
- Create: `uc_sim/tests/scenarios.rs`
- Modify: root `Cargo.toml` (members += `"uc_sim"`)

**Interfaces:**
- Consumes: `ElectionSm`/`Event`/`Action`/`reconcile` (Tasks 3–4).
- Produces: `World::new(SimConfig) -> World`, `World::run(&mut self) -> Result<Stats, InvariantViolation>`, plus scripted-scenario hooks (`partition(a, b)`, `heal()`, `crash(node)`, `restart(node)`, `run_until(pred)`).

**The model (binding design):**
- A node's LOG CONTENT is fully described by `(term_map, append)` — within a term, bytes are identical cluster-wide because one leader wrote them. Divergence, truncation, and leader completeness are therefore all checkable on `(term_map, position)` pairs alone; no byte arrays.
- Node state: `sm: ElectionSm` + volatile `{append, durable, commit}` + persisted `{vote, term_map}` (plain fields — the sim IS the StableValue) + `up: bool`.
- Virtual time: `u64` ns; event queue `BinaryHeap<Reverse<(u64 time, u64 seq, SimEvent)>>` (`seq` breaks ties deterministically).
- `SimEvent`: `Deliver { to: usize, msg: Msg }`, `Tick { node: usize }`, `ArchiveStep { node: usize }`, `Crash { node: usize }`, `Restart { node: usize }`.
- `Msg` mirrors the wire: `Data { term, from_pos, to_pos }` (leader→follower; follower accepts iff `term == current && from_pos == append` — contiguity — else it is dropped and the leader retries from the follower's last-acked position on a later tick), `Ack { from, term, append }` (drives the leader's per-follower send cursor), `Report { from, term, durable }`, `CommitGossip`, `RequestVote`, `Vote`, `TermMap` — the last five translate 1:1 into SM events.
- Faults: per-message seeded xorshift decides drop / duplicate / delay (extra latency draw) / reorder (delay does it naturally). Partitions: a blocked-pairs set consulted at delivery.
- Leader behavior per Tick (agent logic, mirrored from Task 8's real agent): if `can_serve`, append one frame (96 B) — record nothing but the position; send `Data` to each follower from its cursor; run SM tick; translate actions into world effects (`BecomeLeader` → collapse append to durable, term-map append — all instantaneous+durable in the model; `Truncate` → set durable=append=to, feed `Truncated` back NEXT event (models the latch window); `AdvanceCommit` → set commit; vote persistence → set persisted vote BEFORE the Vote msg is enqueued — the model enforces the ordering contract structurally).
- `ArchiveStep`: `durable += min(random_step, append - durable)`; reschedule; a durable advance enqueues `Report` to the current leader (if known).
- `Crash`: `up = false`, volatile cleared; `Restart`: `ElectionSm::new(recovered vote/term_map, durable, now)`, `append = durable`, `commit = 0`.
- **Invariants (checked after EVERY event, spec §8 verbatim):**
  1. Election safety: ≤1 leader per term (a `leaders_by_term: BTreeMap<u32, usize>` — a second BecomeLeader in the same term = violation).
  2. Term-map prefix consistency **over committed positions**: for any two nodes, their maps restricted to entries with `base < global_max_commit` must be identical prefixes.
  3. Commit monotonicity per node per run (resets on restart are exempt and expected).
  4. Committed-never-truncated: every `Truncate { to }` must satisfy `to >= global_max_commit` at that moment.
  5. Leader completeness: at every `BecomeLeader { term, base }`, `base >= global_max_commit` AND the new leader's term map covers every committed position with the same entries every other node has for them.
- `global_max_commit` = max commit any node has ever set (commits are quorum-certified by construction of the SM; the sim tracks the max as ground truth).
- Fuzz: `#[test] fn fuzz_default()` runs seeds `0..50`, 20_000 steps each, moderate fault rates + crash/restart injection; feature `sim-heavy` bumps to seeds `0..1000` (CI knob). EVERY failure message includes the seed and step number (pinned-regression workflow: a failing seed becomes a named test).

- [ ] **Step 1: Scaffold + write the scripted-scenario tests first**

`uc_sim/tests/scenarios.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Scripted nasties (spec §8): each drives the world to a specific dangerous
//! configuration and asserts the invariants held + the expected outcome.

use uc_sim::world::{SimConfig, World};

fn base_cfg(seed: u64) -> SimConfig {
    SimConfig { n_nodes: 3, seed, max_steps: 30_000, ..SimConfig::default() }
}

#[test]
fn quiet_cluster_elects_exactly_one_leader_and_commits() {
    let mut w = World::new(SimConfig { drop_per_million: 0, ..base_cfg(1) });
    let stats = w.run().expect("invariants");
    assert_eq!(stats.leaders_elected, 1, "stable cluster must elect once");
    assert!(stats.max_commit > 0, "a serving leader must commit data");
}

#[test]
fn split_vote_converges() {
    // drop ALL vote traffic for the first virtual 500ms, then heal: forced
    // split votes, then convergence
    let mut w = World::new(base_cfg(7));
    w.drop_all_votes_until(500_000_000);
    let stats = w.run().expect("invariants");
    assert!(stats.max_commit > 0, "cluster must converge after split votes");
}

#[test]
fn minority_partition_cannot_commit_and_heals() {
    let mut w = World::new(SimConfig { drop_per_million: 0, ..base_cfg(3) });
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    let commit_before = w.max_commit();
    // partition the leader away from BOTH followers
    w.partition_node(leader);
    w.run_steps(5_000).expect("invariants");
    // the old leader alone must not have advanced commit
    assert_eq!(
        w.node_commit_high_water(leader),
        commit_before.max(w.node_commit_high_water(leader)),
        "stale leader must not certify new bytes; its commit is frozen"
    );
    assert!(w.max_commit_from(&w.majority_excluding(leader)) >= commit_before);
    // heal: the deposed leader truncates its uncommitted tail and rejoins
    w.heal();
    let stats = w.run().expect("invariants");
    assert!(stats.truncations >= 1, "the deposed leader's tail must truncate");
}

#[test]
fn crash_during_truncate_recovers() {
    let mut w = World::new(base_cfg(11));
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    w.partition_node(leader);
    w.run_steps(5_000).expect("invariants");
    w.heal();
    // crash the deposed node the moment its Truncate action fires
    w.crash_on_next_truncate();
    let stats = w.run().expect("invariants (crash mid-truncate)");
    assert!(stats.restarts >= 1);
}

#[test]
fn fuzz_default_seeds() {
    for seed in 0..50u64 {
        let mut w = World::new(SimConfig {
            n_nodes: 3,
            seed,
            max_steps: 20_000,
            drop_per_million: 20_000,
            dup_per_million: 5_000,
            crash_per_million: 500,
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed}: {v}");
        }
    }
}

#[cfg(feature = "sim-heavy")]
#[test]
fn fuzz_heavy_seeds() {
    for seed in 0..1000u64 {
        let mut w = World::new(SimConfig {
            n_nodes: if seed % 4 == 0 { 5 } else { 3 },
            seed,
            max_steps: 20_000,
            drop_per_million: 50_000,
            dup_per_million: 10_000,
            crash_per_million: 1_000,
            ..SimConfig::default()
        });
        if let Err(v) = w.run() {
            panic!("seed {seed}: {v}");
        }
    }
}
```

- [ ] **Step 2: Implement the world + invariants**

Implement `world.rs` + `invariants.rs` to the binding design above. Size guidance: the world is ~400 lines; keep `Msg` translation table-flat; the xorshift is the same 10-liner (private copy). `SimConfig::default()`: tick interval 10ms virtual, archive step 5ms, latency 1–5ms drawn, election timeouts 150–300ms — the SM's own defaults. The scenario hooks (`drop_all_votes_until`, `partition_node`, `heal`, `crash_on_next_truncate`, `run_until_leader`, `run_steps`, accessors) are part of `World`'s pub API; `InvariantViolation` is a `Display`able struct carrying (invariant name, step, seed, detail).

The `Stats` struct: `{ leaders_elected: u32, max_commit: u64, truncations: u32, restarts: u32, steps: u64 }`.

- [ ] **Step 3: Run**

Run: `cargo test -p uc_sim && cargo clippy --workspace -- -D warnings`
Expected: all 6 tests green in seconds (virtual time — if fuzz_default takes >60s something is accidentally quadratic; fix before committing). Then run the heavy tier once locally: `cargo test -p uc_sim --features sim-heavy --release` — must be green; report its wall time.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock uc_sim/
git commit -m "feat(uc_sim): deterministic election simulation — invariants, scripted nasties, seeded fuzz"
```

---

### Task 6: `uc_net` — replay sessions off the Overrun seam (the M2 envelope closes)

**Files:**
- Modify: `uc_log/src/archive.rs` (journal handle exposure + `find_block` extraction)
- Modify: `uc_net/src/sender.rs` (journal-replay fallback in `serve_nak`)
- Modify: `uc_net/tests/replication.rs` + `uc_net/tests/common/mod.rs` (the companion test)

**Interfaces:**
- Consumes: `Journal` (`Arc`-shared; verify `Journal: Send + Sync` with a compile-time assert — it is `&self` throughout with internal locking), `read_run_validated`'s wire discipline (padding header-only, run never crosses block boundary needs).
- Produces:
  - `Archive` internally holds `Arc<Journal>`; new `pub fn journal_arc(&self) -> Arc<Journal>` (the existing `journal()` borrow stays for replay/tests).
  - `pub(crate) fn find_block(journal: &Journal, pos: u64) -> Result<Option<(u64 /*seq*/, u64 /*base*/)>, ArchiveError>` — the binary search extracted from `replay_from` (which now calls it; behavior identical, replay tests must stay green unchanged).
  - `Sender::set_replay_source(&mut self, journal: Arc<Journal>)` — no constructor change (M2/M3 call sites untouched).
  - `serve_nak`'s `SliceRead::Overrun` arm: with a replay source set, serve the NAK from the JOURNAL — read the block containing `pos` (`find_block` + `read`), walk its frames from `pos`'s offset, emit MTU-budget DATA datagrams (identical wire format: position-stamped, padding header-only-and-last — the same chunking rule the buffer path produces), at most `REPLAY_DGRAMS_PER_NAK = 8` datagrams per served NAK (bounded duty cycle; the follower's NAK backoff re-requests the rest — that IS the pacing, spec §5 "bounded, separately paced"). `overruns` now increments ONLY when there is no replay source or the position is below the first archived block (purged — M6). New stat: `replay_datagrams: AtomicU64`.
  - A `pub(crate) fn chunk_frames(block: &[u8], base: u64, from: u64, budget: usize, mut emit: impl FnMut(u64, &[u8]))` helper in sender.rs — walks frames (header lengths), starts at `from` (a frame boundary at/after `base`), cuts at budget and AT padding (padding emitted header-only, ends its datagram) — unit-tested directly against `read_run_validated`'s output for the same stream (the two chunkers must produce interchangeable wire runs).

- [ ] **Step 1: Unit test the chunker + the fallback**

Append to `uc_net/src/sender.rs` tests:

```rust
    #[test]
    fn journal_replay_serves_deep_nak_with_identical_wire_format() {
        // leader with a TINY buffer (4096) laps it 3x while archiving; a NAK
        // for lap-0 positions must be served from the journal
        let counters = Arc::new(uc_log::counters::LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            uc_log::region::Region::heap_zeroed(4096),
            counters,
            256,
        ));
        let dir = tempfile::tempdir().unwrap();
        let cfg = uc_log::archive::ArchiveConfig {
            segment_size_bytes: 4 * 1024 * 1024,
            ..uc_log::archive::ArchiveConfig::new(dir.path())
        };
        let mut arch = uc_log::archive::Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        let mut n = 0u64;
        while a.position() < 3 * 4096 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(_) => n += 1,
                Err(uc_log::buffer::AppendError::WouldOverrun) => {
                    arch.do_work(&b).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
        }
        while arch.do_work(&b).unwrap() {}

        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(64);
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
        s.set_replay_source(arch.journal_arc());
        // NAK for position 0 (lapped long ago)
        tx.send(CtrlMsg::Nak { from: f1.addr(), position: 0, length: 4096 }).unwrap();
        s.do_work();
        // served from the journal: DATA datagrams, self-locating from 0,
        // frames byte-identical to the original appends
        let (h, body) = f1.recv().expect("replayed datagram");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 0);
        assert_eq!(read_header(&body).correlation_id, 0);
        assert_eq!(&body[HEADER_LEN..HEADER_LEN + 64], &[0u8; 64]);
        assert!(s.stats().replay_datagrams.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        assert_eq!(
            s.stats().overruns.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the seam is now served, not counted"
        );
    }
```

- [ ] **Step 2: The M2-envelope companion test** (extend `uc_net/tests/replication.rs`; `spawn_leader` in common/mod.rs gains the `set_replay_source` call — one line, using the archive it already constructs; NOTE: `spawn_archive` currently constructs the Archive inside the closure — restructure it minimally so the `Arc<Journal>` is extracted before the agent spawn):

```rust
#[test]
fn paused_follower_recovers_via_replay_sessions() {
    // THE M2-envelope test: a live follower falls >1 ring behind, which was
    // NAK-unrecoverable in M2/M3 (permanent wedge, overruns>0). With replay
    // sessions the sender serves deep NAKs from the journal. overruns>0 was
    // EXPECTED in M2's framing; with the seam SERVED the counter stays 0 and
    // replay_datagrams>0 is the proof the path ran.
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("rp-f1", leader_addr, FaultConfig::default());
    let b1 = Arc::clone(&f1.node.buffer);
    // follower 2 starts PAUSED: socket bound (no ICMP noise) but agents not
    // yet spawned — it will join >1 ring behind
    let f2_sock = FaultSocket::bind("127.0.0.1:0").unwrap();
    let f2_addr = f2_sock.local_addr().unwrap();
    let leader = spawn_leader(raw, vec![f1.addr, f2_addr], FaultConfig::default());
    let sstats = Arc::clone(&leader.stats);
    // stream ~3 rings (CAP = 1 MiB) paced by the live follower only
    let end = load(&leader.node.buffer, &[&b1], 32_768);
    assert!(end >= 3 * CAP);
    // now start follower 2 from zero: its first NAKs are >1 ring deep
    let f2 = spawn_follower_on("rp-f2", f2_sock, leader_addr, FaultConfig::default());
    let b2 = Arc::clone(&f2.node.buffer);
    await_pos(&b2.counters().append, end, "paused follower rebuilt");
    await_pos(&b2.counters().durable, end, "paused follower durable");
    assert!(
        sstats.replay_datagrams.load(Ordering::Relaxed) > 0,
        "recovery must have used the journal replay path"
    );
    converge_and_compare(leader, vec![f1, f2], end);
}
```

(`spawn_follower_on` = the existing `spawn_follower` body taking a pre-bound socket — extract it in common/mod.rs; `spawn_follower` becomes a two-line wrapper.)

- [ ] **Step 3: Implement** (archive handle + find_block extraction + chunker + the serve_nak fallback + the stat). Run `cargo test -p uc_log` FIRST after the archive refactor — the replay tests pin `find_block`'s extraction is behavior-identical.

- [ ] **Step 4: Run everything**

Run: `cargo test -p uc_log && cargo test -p uc_net && cargo test -p uc_net --test replication && cargo test -p uc_net --test commit && cargo clippy --workspace -- -D warnings && cargo clippy -p uc_net --all-targets -- -D warnings`
Expected: all green; the new integration test runs in seconds (replay of ~3 MiB at 8-datagrams-per-NAK with 2ms backoff ≈ ~40 NAK rounds — if it takes >60s, raise REPLAY_DGRAMS_PER_NAK's per-call bound or shrink the stream; deadline-bounded either way).

- [ ] **Step 5: Commit**

```bash
git add uc_log/src/archive.rs uc_net/src/sender.rs uc_net/tests/replication.rs uc_net/tests/common/mod.rs
git commit -m "feat(uc_net): replay sessions — deep NAKs served from the journal (M2 envelope closed)"
```

---

### Task 7: `uc_net` — mutable terms, consensus routing, partitionable faults

**Files:**
- Modify: `uc_net/src/sender.rs`, `uc_net/src/receiver.rs`, `uc_net/src/fault.rs`
- Modify: `uc_net/tests/common/mod.rs`, `uc_net/tests/replication.rs`, `uc_net/tests/commit.rs`, `uc_net/examples/m2_gate.rs`, `uc_net/examples/m3_gate.rs` (term-handle ripple)

**Interfaces:**
- Consumes: Task 1 wire frames.
- Produces (used by Tasks 8–9):
  - `pub type TermHandle = Arc<AtomicU32>;` (in `uc_net/src/lib.rs`). `SenderConfig.term_id: u32` → `Sender::new` takes an extra `term: TermHandle` parameter (config stays Copy; the handle is a constructor arg). `FollowerConfig` likewise: `term_id` field REMOVED, `FollowerReceiver::new` gains `term: TermHandle`. `LeaderReceiver::new`'s `term_id: u32` → `TermHandle`. The consensus agent (Task 8) is the only WRITER of the handle; data-path agents load Relaxed per datagram/duty-cycle. Ripple: every call site constructs `Arc::new(AtomicU32::new(TERM))` — mechanical, ~12 sites.
  - **Consensus routing** on `FollowerReceiver`: new optional channel `set_consensus_route(&mut self, tx: mpsc::SyncSender<NetEvent>)` where

```rust
pub enum NetEvent {
    Report { from: SocketAddr, term: u32, durable: u64 },
    CommitGossip { term: u32, commit: u64 },
    RequestVote { from: SocketAddr, body: RequestVoteBody },
    Vote { from: SocketAddr, body: VoteBody },
    TermMap { term: u32, entries: Vec<TermMapEntryWire> },
    /// Any current-term leader traffic seen (data/heartbeat) — liveness.
    LeaderActivity { term: u32 },
}
```

  With the route set: kinds 5–9 are forwarded RAW (with their term — the SM does term filtering and adoption; higher-term votes MUST reach it) and the follower-side COMMIT_POSITION counter store is DISABLED (the consensus agent owns the store — single-writer moves); kinds 1–2 additionally emit a (rate-limited: once per duty cycle) `LeaderActivity`. WITHOUT the route (legacy mode): M3 behavior byte-for-byte (term-filtered drop + local commit store) — the M3 tests keep passing unchanged.
  - Same on `LeaderReceiver`: `set_consensus_route(...)` forwards kinds 5–9 (raw) to consensus instead of AppendPos-to-sender; NAK/STATUS always to the sender channel (flow control is data-plane). Legacy mode unchanged.
  - `Sender`: the M3 commit-ranking block (tracker, gossip_commit, AppendPos arm) is REMOVED when constructed in node mode — concretely: `Sender::new` keeps the tracker for legacy callers, and a new `set_node_mode(&mut self)` disables ranking/gossip (the consensus agent does both; the sender still serves NAK/STATUS/stream). Deferred cleanup: M5 deletes the legacy path once the old gates retire — noted, not done now.
  - `FaultSocket`: `pub fn partition_handle(&self) -> PartitionHandle` where `PartitionHandle(Arc<RwLock<HashSet<SocketAddr>>>)` with `block(addr)/unblock(addr)/clear()`; `send_to` drops silently to blocked peers (checked before the fault rolls — a partition is not a random fault). Empty-set fast path: one `read()` on a never-contended lock (~20 ns, loopback-test-only concern; document).

- [ ] **Step 1: Tests** — (a) term handle: a sender stamping term 3 whose handle is bumped to 4 mid-test stamps 4 on the next datagram (assert via Fake recv); a follower whose handle moved drops old-term DATA (`dropped_stale_term`). (b) Routing: with a consensus route set, a RequestVote datagram (kind 7, HIGHER term) arrives at the route as `NetEvent::RequestVote` (not dropped); COMMIT_POSITION no longer stores the counter locally (assert counter still 0, event at the route). (c) Legacy mode: construct without routes — the existing M3 test suite IS the assertion (must stay green untouched except the constructor ripple). (d) Partition: two FaultSockets, block one direction, assert delivery stops; unblock, resumes. Write these as ~5 focused unit tests in the respective test modules — full code per the established Fake/FakeLeader patterns of those modules.

- [ ] **Step 2: Implement + ripple the ~12 call sites.** The M3 stale-term semantics in node mode move into the SM; the receiver's `dropped_stale_term` stat in node mode counts only DATA/HEARTBEAT drops.

- [ ] **Step 3: Run everything** (all uc_net suites + both example builds + both clippy gates). Expected: green with only constructor-line changes in the pre-existing tests.

- [ ] **Step 4: Commit**

```bash
git add uc_net/src/ uc_net/tests/ uc_net/examples/
git commit -m "feat(uc_net): mutable term handles, consensus routing, partitionable fault layer"
```

---

### Task 8: `uc_node` — minimal node composition + the consensus agent

The seed of spec §3.2's composition crate: agent wiring and role switching ONLY (no discovery dir, no instance.lock, no cnc, no client IPC — M5). The consensus agent is the single writer of the term handle AND the commit counter, executes every SM action honoring the persistence contracts, and owns the appender when leader.

**Files:**
- Create: `uc_node/Cargo.toml` (deps: uc_log, uc_net, uc_consensus, uc_protocol, uc_journal workspace)
- Create: `uc_node/src/lib.rs`
- Create: `uc_node/src/node.rs`
- Modify: root `Cargo.toml` (members += `"uc_node"`)
- Modify: `uc_net/src/sender.rs` (`set_role_flag(Arc<AtomicBool>)` — heartbeats + streaming gated on leader role; `set_node_mode()` from Task 7)
- Modify: `uc_net/src/receiver.rs` (`set_intake_pause(Arc<AtomicBool>)` — DATA dropped while paused, for the truncation window)

**Interfaces:**
- Consumes: everything above.
- Produces (used by Tasks 9–10):

```rust
pub struct NodeConfig {
    pub id: NodeId,
    /// Static membership: (id, addr) for every member INCLUDING self.
    pub members: Vec<(NodeId, SocketAddr)>,
    pub bind: SocketAddr,
    pub dirs: NodeDirs, // { journal: PathBuf, state: PathBuf }
    pub buffer_bytes: usize,          // power of two
    pub max_payload: usize,
    pub election_timeout_min_ns: u64, // 150ms default
    pub election_timeout_max_ns: u64, // 300ms default
    pub seed: u64,
    pub faults: FaultConfig,
}

pub struct Node { /* agents, handles */ }

impl Node {
    /// Recover (NodeState + Archive), prime counters, spawn the four agents
    /// (archive, consensus, sender, receiver). Every node boots a FOLLOWER;
    /// leadership only ever comes from an election.
    pub fn start(cfg: NodeConfig) -> io::Result<Node>;
    pub fn is_leader(&self) -> bool;          // role snapshot (AtomicU8)
    pub fn can_serve(&self) -> bool;          // leader + NewTerm committed
    pub fn current_term(&self) -> u32;
    pub fn counters(&self) -> &Arc<LogCounters>;
    /// Leader-only ingress (M5 replaces with the ring): enqueue a payload for
    /// the consensus agent to append. Errors if not serving.
    pub fn submit(&self, payload: Vec<u8>) -> Result<(), SubmitError>;
    /// Stats/handles the harness needs:
    pub fn partition_handles(&self) -> Vec<PartitionHandle>; // both sockets
    pub fn truncations(&self) -> u64;         // AtomicU64 stat
    /// Graceful stop (joins agents). Crash-stop for the harness:
    pub fn stop(self);
    pub fn crash(self); // stops agents WITHOUT any extra flushing
}
```

**Consensus-agent duty cycle (binding order):**
1. Drain the `NetEvent` channel → SM events (Report/CommitGossip/RequestVote/Vote/TermMap/LeaderActivity → `LeaderSeen`).
2. Poll the durable counter; on change feed `DurableAdvanced` (and, as follower, the receiver's AppendPosition upkeep already reports it on the wire — unchanged from M3).
3. Drain the ingress channel (leader && can_serve only): append each payload via the consensus-owned `Appender` (bounded per cycle, e.g. 256).
4. Feed `Tick { now_ns }` (real clock — the ONLY place real time enters).
5. Execute the action list IN ORDER; the contracts:
   - `PersistAndSendVote` → `state.store_vote(...)` (durable on return) THEN the Vote datagram (skip the send when `to == self.id` — self-vote).
   - `BecomeLeader { term, base }` → `state.store_term_map(map + (term, base))` durable → `term_handle.store(term)` → `counters.prime(base)` (collapses append/sent to durable; commit untouched) → construct the fresh `Appender` with `term` → append the `FRAME_TYPE_NEW_TERM` frame → feed `NewTermAppended { position }` → `is_leader.store(true)`, role = Leader.
   - `BecomeFollower { term, .. }` → `term_handle.store(term)`, `is_leader.store(false)`, drop the appender.
   - `Truncate { to, new_map }` → `intake_pause = true` → send `TruncateCmd { to, ack }` to the archive agent's command channel; the archive agent (its own duty cycle) runs `archive.truncate_to(to)`, `counters.prime(to)`, acks; consensus (a later cycle, on ack) → `state.store_term_map(new_map)` durable → `intake_pause = false` → feed `Truncated { to }`; `truncations += 1`.
   - `AdvanceCommit { commit }` → `counters.commit.store_release(commit)` — the ONLY commit store in the binary, both roles.
   - `GossipCommit`/`ShipTermMap`/`StartElection`/`SendVoteRejection` → datagrams via the consensus agent's own `FaultSocket` (a second clone of the node socket; its `PartitionHandle` is included in `partition_handles()`).
   - `PersistTermMap { new_map }` → durable store.
   - `Fatal { reason }` → `panic!` (fail-stop; the harness treats it as a red test).

**Wiring notes (binding):** one `UdpSocket` per node: receiver gets the raw recv clone; sender and consensus each wrap a clone in `FaultSocket` (same `FaultConfig`). Receiver: consensus route + sender route + term handle + intake-pause set. Sender: node mode + role flag + replay source (`archive.journal_arc()` — grab it before the archive moves into its agent). Archive agent: `do_work` + drain the command channel each cycle. Agent idles: `Yield` in tests/harness (`BusySpin` is m4_gate's knob).

- [ ] **Step 1: Sender/receiver gates first (small, test in uc_net)** — `set_role_flag`: a sender with the flag false streams nothing and sends no heartbeats even with appended data + elapsed interval (unit test); flag flipped true → streams. `set_intake_pause`: paused receiver drops DATA (stat `dropped_paused`), control still routes (unit test). Commit.

```bash
git add uc_net/src/sender.rs uc_net/src/receiver.rs
git commit -m "feat(uc_net): role-flag gating + intake pause for node composition"
```

- [ ] **Step 2: Write `uc_node` with a smoke test** — `uc_node/tests/smoke.rs`: start ONE node with `members = [(0, self)]` (cluster of 1: quorum 1 — CommitTracker::new(0, 1), needed=0 → commit = own durable): it must elect itself (timeout → candidate → instant majority-of-1), open the term, commit the NewTerm frame, `can_serve` within a deadline; `submit` then drives commit forward. This pins the whole action-execution loop without any networking peers.

```rust
#[test]
fn single_node_cluster_elects_itself_and_serves() {
    let dir = tempfile::tempdir().unwrap();
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    // bind first to learn the addr, then start
    let sock = std::net::UdpSocket::bind(bind).unwrap();
    let addr = sock.local_addr().unwrap();
    drop(sock); // Node::start rebinds; races are a harness non-issue locally
    let node = Node::start(NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        bind: addr,
        dirs: NodeDirs { journal: dir.path().join("j"), state: dir.path().join("s") },
        buffer_bytes: 1 << 20,
        max_payload: 256,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
    })
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "single node never elected itself");
        std::thread::yield_now();
    }
    assert!(node.is_leader());
    assert_eq!(node.current_term(), 1);
    for i in 0..100u64 {
        node.submit(vec![i as u8; 64]).unwrap();
    }
    let end = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let c = node.counters();
            let (a, k) = (c.append.load_acquire(), c.commit.load_acquire());
            if k == a && k > 32 {
                break k;
            }
            assert!(Instant::now() < deadline, "commit never caught append");
            std::thread::yield_now();
        }
    };
    assert!(end > 32); // NewTerm frame + data
    node.stop();
}
```

(NOTE the port-rebind pattern: fine for the smoke test; the 3-node harness binds all sockets first and hands them to `Node::start_with_socket` — provide that constructor variant, `start` wraps it.)

- [ ] **Step 3: Implement `node.rs`** to the binding design. Size guidance ~400 lines. The consensus agent is one `AgentRunner` closure owning: `ElectionSm`, `NodeState`, `Appender: Option<Appender>`, the two mpsc receivers (net events, ingress), the archive command sender + ack receiver, sockets, peer addr map (`NodeId → SocketAddr`), stats. Role snapshots for the API: `Arc<AtomicU8>` (role) + `Arc<AtomicBool>` (can_serve) + `Arc<AtomicU32>` (term handle, shared with data plane) — all written only by the consensus thread.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_node && cargo test -p uc_net && cargo clippy --workspace -- -D warnings && cargo clippy -p uc_node --all-targets -- -D warnings`
Expected: smoke green in <15 s; everything else untouched-green.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock uc_node/
git commit -m "feat(uc_node): minimal composition — consensus agent, role switching, action contracts"
```

---

### Task 9: the failover/partition harness — networked elections proven

**Files:**
- Create: `uc_node/tests/failover.rs`

**Interfaces:**
- Consumes: `Node` (Task 8), `PartitionHandle` (Task 7), journal replay comparison (reuse the pattern from uc_net's harness — a local `replayed(dir)` helper reading `Archive::open` + `replay_from(0)`; NewTerm frames are included in replay output and MUST match across nodes like any frame).
- Produces: the M4 networked gate evidence: single-leader boot, sub-second failover, minority-partition safety, heal-with-truncation, restart-rejoin.

- [ ] **Step 1: Write the harness + five tests** (full file; helpers: `spawn_cluster(n, dir) -> Vec<NodeHandleWrapper>` binding all sockets first, per-node tempdir subdirs, seeds derived from index; `await_single_leader(&nodes) -> usize` deadline-bounded asserting EXACTLY one `can_serve` leader and returning it; `submit_n(leader, n)`; `await_commit_all(&nodes, target)`; `replay_equal(dirs)` — replayed frame streams identical INCLUDING NewTerm frames):

```rust
#[test]
fn boot_elects_exactly_one_leader_and_commits() {
    // 3 nodes from nothing: one leader, term 1..=small, 1000 msgs commit on
    // ALL nodes (commit counters converge; journals replay-identical)
}

#[test]
fn leader_kill_fails_over_subsecond_without_losing_committed_data() {
    // boot, commit 1000, note commit position C and wall clock; crash() the
    // leader; await a NEW leader with can_serve among survivors; assert
    // elapsed < 1s; assert survivors' commit >= C (nothing lost); submit
    // 1000 more to the new leader; commit advances; survivors replay-equal.
    // Record the failover duration in the test output (println) — Task 10's
    // gate run cites it.
}

#[test]
fn minority_partitioned_leader_cannot_commit_phantom() {
    // boot, commit, then partition the leader from both followers (block on
    // every partition handle, both directions). Submit K msgs to the OLD
    // leader (it still believes; can_serve stays true until it sees the new
    // term). Assert over a settle window its commit does NOT advance past
    // the pre-partition point. Meanwhile the majority elects a new leader
    // and commits fresh data. No phantom: old leader's commit < new
    // cluster commit, and its extra appends are UNcommitted.
}

#[test]
fn heal_truncates_divergent_tail_and_reconverges() {
    // continue the partition scenario: heal all handles; the deposed leader
    // must adopt the higher term, receive the term map, TRUNCATE its
    // uncommitted tail (truncations() >= 1 on that node), catch up (replay
    // sessions may serve it), and end replay-identical with the others —
    // including the two NewTerm frames and none of its phantom appends.
}

#[test]
fn restarted_follower_recovers_state_and_rejoins() {
    // graceful stop() one follower; commit more traffic; restart it from the
    // SAME dirs (start_with_socket on a fresh bind — membership addr changes
    // are out of scope, reuse the port via the pre-bound-socket pattern);
    // it must rejoin as follower in the CURRENT term (vote/term-map state
    // recovered — assert current_term matches, no spurious election is
    // observable as no term bump), catch up, replay-equal.
}
```

Write each body fully in the implementation (the comments above are the specification of each; every wait deadline-bounded 30–60 s; journal data small — 1000 × 96 B per phase; election timeouts 150–300 ms as configured; the failover assert budget is <1 s MEASURED, spec §9).

- [ ] **Step 2: Run** — `cargo test -p uc_node --test failover` TWICE, plus once with `--test-threads=1` (the five tests each run a 3-node cluster: ~12 busy threads under default parallelism on 4 cores — if parallel runs flake on timing, keep tests parallel-safe but document that the harness is core-hungry; do NOT weaken deadlines below the 1 s failover budget).

- [ ] **Step 3: Full workspace gates** (every crate's tests + both clippy invocations + `cargo test -p uc_sim`).

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/failover.rs
git commit -m "test(uc_node): networked election harness — boot/failover/partition/heal/restart"
```

---

### Task 10: `m4_gate` — failover measurement + benchmark doc

**Files:**
- Create: `uc_node/examples/m4_gate.rs`
- Create: `docs/benchmarks/uc2-m4-gate-2026-07-11.md`

**Interfaces:**
- Consumes: `Node`; the M1–M3 gate-doc discipline (verbatim outputs, honest verdicts, fleet placeholder).
- Produces: the M4 gate record: sim fuzz summary + measured failover distribution.

- [ ] **Step 1: Write the example** — `m4_gate <journal_root> [iterations=10]`: boots a 3-node local cluster (journals under `<journal_root>/n{0,1,2}`, ext4 — `/home/claude`, never `/tmp`), then loops `iterations` times: await leader + serving → drive load for 2 s (submit batches, admission-paced vs commit at 1 MiB) → record commit position → `crash()` the leader → measure (a) time-to-new-`can_serve`, (b) time-to-first-commit-advance past the pre-kill position → assert nothing committed was lost (survivor commit ≥ recorded) → restart the killed node from its dirs (it rejoins) → next iteration. Prints per-iteration lines + a summary: failover p50/p90/max for both metrics, truncations observed, terms consumed. Verdict line: `GATE (sub-second failover, no committed loss): PASS/FAIL` (PASS = every iteration's time-to-serve < 1 s and zero loss). Full code in the plan-executor's hands — mirror m3_gate's structure (arg parsing, deadline asserts, cleanup note); ~250 lines.

- [ ] **Step 2: Run the gate** — `cargo run -p uc_node --release --example m4_gate -- /home/claude/uc2-m4-gate 10`; capture verbatim; `rm -rf /home/claude/uc2-m4-gate` after; df baseline check. Also run `cargo test -p uc_sim --features sim-heavy --release` and capture its summary + wall time. Loopback failover at 150–300 ms timeouts should land ~200–400 ms p50 — a PASS is plausible on this box (unlike M2/M3's throughput bars); report honestly either way. Correctness signals that must hold: zero committed loss across all iterations, exactly-one-leader throughout, truncations only on deposed leaders.

- [ ] **Step 3: Write the doc** — `docs/benchmarks/uc2-m4-gate-2026-07-11.md`: banner (loopback measurement; the spec's "sub-second LAN failover" gets its fleet confirmation on 3×c6id later — loopback is REPRESENTATIVE here because failover time is timeout-dominated, not wire-dominated — state this reasoning); what the gate measures (kill → re-election → NewTerm commit → serving); the sim evidence (default + heavy seed counts, wall time); verbatim outputs; the M4-carry items that remain open (buffer prefill cut to M6; NoCommonPrefix fallback = M6 snapshot install; commit-not-persisted contract for M5); fleet placeholder.

- [ ] **Step 4: Full gates + commit**

```bash
git add uc_node/examples/m4_gate.rs docs/benchmarks/uc2-m4-gate-2026-07-11.md
git commit -m "feat(uc_node): m4_gate failover measurement + sim evidence doc"
```

---

## Self-review notes (already applied)

1. **Spec §6 election coverage:** vote rule with persisted-before-answer (T2/T3 + structural enforcement in the sim), randomized timeouts (T3), liveness via LeaderSeen from any leader traffic (T3/T7), new-leader protocol — durable base, discard tail, term-map-first, NewTerm commit gate (T3/T8), reconciliation via shipped term-map suffix + truncate-to-common-prefix (T1/T4), truncation mechanics per §4 (T2), replay/NAK catch-up (T6). §8 L1 sim with the five named invariants + scripted nasties + seeded fuzz (T5). §9 M4 gate = sim green + sub-second failover measured (T9/T10).
2. **Every M3-carry dispatched:** term mutability (T7 TermHandle), commit-writer handoff DISSOLVED (consensus thread owns the counter both roles — T7/T8), replay sessions + the overruns-expected companion test — note the framing improved: the counter stays 0 because the seam is SERVED; `replay_datagrams > 0` is the proof (T6), CommitTracker→SM with stable NodeIds (T3; the addr→id map lives at the demux edge in T8), vote kinds 7–8 (T1/T7), implausibility-guard term-scoping (T7 note: within-term the M3 guard stands; cross-term reports reach the SM which term-filters), commit persistence DECIDED (not persisted; Global Constraints) with the M5 restart contract restated, NakBody/StatusBody DRY re-adjudicated (still moot — three body shapes now).
3. **Known honest gaps, stated not hidden:** `truncate_to`'s partial-block-0 case is flagged as an implementer-resolved contract (Task 2 note) — unreachable in M4 per the reconciliation analysis, must be encoded as either working code or a tested error; `NoCommonPrefix` → `Fatal` until M6's snapshot install; buffer prefill cut to M6; legacy (M3-mode) paths in sender/receiver retained for the old gates and deleted in M5.
4. **Type consistency pass:** `NodeId = u32` everywhere (`VoteRecord.voted_for: u32` matches); `TermMapEntry {term: u32, base: u64}` (state) vs `TermMapEntryWire` (wire) vs `(u32, u64)` tuples (SM) — three shapes by design (serde / core-only / dep-free), converted at the T8 edges; `Reconcile::Ok(Outcome)` shape is the Task-4 Step-1 CONTRACT (the Interfaces sketch is explicitly superseded by the test — called out inline); `Event`/`Action` names consistent across T3/T4/T5/T8.
5. **Sequencing honesty:** T5 (sim) lands BEFORE T8/T9 (networked elections) — the spec's ordering requirement is the task order. T6/T7 are independent of T3–T5 (could interleave) but the plan orders them after so the sim exists before any uc_net election plumbing is written.
6. **Placeholder scan:** Task 9's test bodies are specified by binding comments with a full-code requirement on the implementer, and Task 10's example is spec'd to m3_gate's established structure — these are deliberate implementer-latitude points at the integration edge (same posture as M2's harness task), each with explicit acceptance criteria; everything algorithmic (SM, reconcile, sim, chunker, truncate) has complete code or a complete contract-by-test in the plan.
