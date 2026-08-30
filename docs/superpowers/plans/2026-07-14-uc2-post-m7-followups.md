# UC v2 Post-M7 Follow-up Wave Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the entire M7 post-merge follow-up ledger: root-cause + harden the archive `Replay::next` panic, land five behavior changes (self-demote refusal, tombstone boot refusal, ConfigObserved belt, equal-version divergence check, admin-band audit), and clear the observability/test/doc minors.

**Architecture:** All changes are local hardening/refusals at already-identified sites — no protocol-shape changes (one additive cnc field + two new reason-code values ⇒ a single wire **minor** version bump, Task 9). The archive work adds structured error surfaces first (Tasks 1–2) so the investigation (Task 3) observes failures as diagnosable errors, not panics.

**Tech Stack:** Rust workspace (uc_protocol / uc_log / uc_consensus / uc_node / uc_sim / examples), spec `docs/superpowers/specs/2026-07-14-uc2-post-m7-followups-design.md`.

## Global Constraints

- Branch: `uc2/post-m7-followups` off `main` @ 2665baa. Ledger: `.superpowers/sdd/progress-followups.md` (append one entry per completed task).
- `cargo clippy --workspace --all-targets -- -D warnings` must stay clean after every task.
- `uc_consensus` stays pure-sync, no I/O (belts that log live in `uc_node`, never in `ElectionSm`).
- cnc page offsets pinned in BOTH `uc_protocol` and `uc_log` with offset-assertion tests; new fields go in the reserved band (next free line: 3712).
- Wire reason codes: **11 = malformed/unknown op**, **12 = self-demote refused** (pinned in the spec — do not renumber).
- One protocol version bump for the whole wave: `CURRENT` 0.2.0 → 0.3.0, in Task 9 only.
- Implementers stage ONLY their own files; never `git add -A`.
- Do not delete anything under `docs/superpowers/` (retained artifacts).
- Every behavior change lands with a discriminating test; where the M7 red/green pattern applies (a fix whose test must fail on revert), verify red before commit.

---

### Task 0: Branch + ledger

**Files:**
- Create: `.superpowers/sdd/progress-followups.md`

- [ ] **Step 1: Create the branch**

```bash
cd /home/claude/ultima/ultima_cluster
git checkout -b uc2/post-m7-followups
```

- [ ] **Step 2: Seed the ledger**

Write `.superpowers/sdd/progress-followups.md`:

```markdown
# Progress ledger: UC v2 post-M7 follow-up wave

Branch: uc2/post-m7-followups (base main @ 2665baa)
Plan: docs/superpowers/plans/2026-07-14-uc2-post-m7-followups.md
Spec: docs/superpowers/specs/2026-07-14-uc2-post-m7-followups-design.md
Reason codes pinned: 11=malformed op, 12=self-demote. cnc admission_bytes @ 3712. Version bump 0.2.0->0.3.0 in Task 9 only.

(tasks appended as completed)
```

- [ ] **Step 3: Commit**

```bash
git add .superpowers/sdd/progress-followups.md
git commit -m "chore(followups): open post-M7 follow-up wave (branch + ledger)"
```

---

### Task 1: `Replay::next` corrupt-block guard

**Files:**
- Modify: `uc_log/src/archive.rs:30-41` (ArchiveError), `uc_log/src/archive.rs:477-515` (`Replay::next`)
- Test: same file, `#[cfg(test)] mod tests` (existing module, tests near lines 753/904 show the journal/replay helpers in use)

**Interfaces:**
- Produces: `ArchiveError::CorruptBlock { seq: u64, base: u64, off: usize, claimed_len: u32, block_len: usize }` — Task 3's stress harness matches on it.

- [ ] **Step 1: Write the failing test**

In `archive.rs`'s test module, crib the file's existing test setup (the tests around line 753 construct an `Archive`/`Journal` in a tempdir and drive `replay_from`). The test appends a hand-corrupted block directly to the journal and asserts `next()` returns `Err(CorruptBlock)` instead of panicking. Frame bytes: build ONE valid frame with the same frame-encoding helper the existing tests use, then append a second "frame" whose length word claims more bytes than the block holds.

```rust
#[test]
fn replay_surfaces_corrupt_block_instead_of_panicking() {
    // setup: same tempdir + journal construction as the existing replay tests
    // block payload: [valid frame bytes][8-byte header stub claiming len = 4096]
    // (the stub is HEADER_LEN bytes of a header whose `length` field = 4096,
    //  frame_type = FRAME_TYPE_APP — use the same header-writing helper the
    //  appender/tests use so the field offsets are right)
    // journal.append(0, 0, &block).unwrap(); notifier.wait().unwrap();
    let mut replay = /* replay_from(0) as in the existing tests */;
    let first = replay.next().unwrap();                    // the valid frame
    assert!(first.is_some());
    let err = replay.next().unwrap_err();
    match err {
        ArchiveError::CorruptBlock { off, claimed_len, block_len, .. } => {
            assert_eq!(claimed_len, 4096);
            assert!(off + claimed_len as usize > block_len);
        }
        other => panic!("expected CorruptBlock, got {other:?}"),
    }
}
```

Also add a second case: a block whose tail has fewer than `HEADER_LEN` bytes left after the last valid frame (sub-header remainder) → `CorruptBlock` with `claimed_len: 0`.

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p uc_log replay_surfaces_corrupt_block -- --nocapture
```
Expected: FAIL — today the first case panics with an out-of-bounds slice (`index out of range`), which the test harness reports as test failure, not the expected `Err`.

- [ ] **Step 3: Implement**

Add the variant to `ArchiveError` (after `AdoptFloorConflict`):

```rust
/// Post-M7 follow-up: a recorded block whose frame headers are inconsistent
/// with the block length (sub-header claimed length, or a frame overrunning
/// the block). Archived blocks are journal-CRC-covered, so this is a
/// recorder-side bug or on-disk corruption — surfaced as a diagnosable
/// error (previously an unlabeled OOB slice panic in `Replay::next`).
#[error("corrupt archived block seq {seq} (base {base}): frame at off {off} claims len {claimed_len}, block len {block_len}")]
CorruptBlock { seq: u64, base: u64, off: usize, claimed_len: u32, block_len: usize },
```

In `Replay::next`, guard BOTH hazards before touching header/payload bytes. Replace lines 499-504 with:

```rust
            // Defense in depth (mirrors `observe_terms`'s walk guard): a
            // sub-header remainder or a frame overrunning the block would
            // previously OOB-panic at the header read / payload slice below.
            if self.block.len() - self.off < HEADER_LEN {
                return Err(ArchiveError::CorruptBlock {
                    seq: self.seq.saturating_sub(1),
                    base: self.block_base,
                    off: self.off,
                    claimed_len: 0,
                    block_len: self.block.len(),
                });
            }
            let hdr = frame::read_header(&self.block[self.off..]);
            let total = hdr.length as usize;
            let aligned = frame::align_frame_len(total);
            if total < HEADER_LEN || self.off + aligned > self.block.len() {
                return Err(ArchiveError::CorruptBlock {
                    seq: self.seq.saturating_sub(1),
                    base: self.block_base,
                    off: self.off,
                    claimed_len: hdr.length,
                    block_len: self.block.len(),
                });
            }
            let position = self.block_base + self.off as u64;
            let payload_range = self.off + HEADER_LEN..self.off + total;
            self.off += aligned;
```

(`self.seq` was already incremented at refill; `saturating_sub(1)` names the block actually being walked.)

- [ ] **Step 4: Run tests**

```bash
cargo test -p uc_log
```
Expected: new tests PASS, all existing uc_log tests PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_log/src/archive.rs
git commit -m "fix(uc_log): Replay::next surfaces CorruptBlock instead of OOB-panicking on a malformed block"
```

---

### Task 2: `recordable_slice` checked consistency

**Files:**
- Modify: `uc_log/src/buffer.rs:185-221` (`recordable_slice`), `uc_log/src/archive.rs:30-41` (new error variant), `uc_log/src/archive.rs:238-244` (`Archive::do_work` caller)
- Test: `uc_log/src/buffer.rs` test module

**Interfaces:**
- Produces: `recordable_slice(&self, from: u64, max_bytes: usize) -> Result<&[u8], RecordableCorrupt>`; `pub struct RecordableCorrupt { pub from: u64, pub append: u64, pub end: u64, pub claimed_len: u32 }` (buffer.rs); `ArchiveError::RecorderCorrupt(RecordableCorrupt)`.
- Consumes: nothing from Task 1 (independent), but commit after it to keep archive.rs churn ordered.

- [ ] **Step 1: Find all callers**

```bash
grep -rn "recordable_slice" --include=*.rs .
```
Expected: `buffer.rs` (definition), `archive.rs:239` (`do_work`), possibly buffer.rs unit tests. Every caller updates in Step 3.

- [ ] **Step 2: Write the failing test**

In buffer.rs's test module (crib the file's existing append helpers): append two frames, then poison the second frame's length word through the same `commit_word` atomic the appender uses (store a value larger than the remaining committed span), and assert `recordable_slice` returns `Err` rather than debug-panicking or walking garbage:

```rust
#[test]
fn recordable_slice_surfaces_torn_length_word() {
    // setup: buffer with two appended+committed frames as in existing tests
    // let second_frame_off = /* offset of frame 2's length word */;
    // buf.commit_word(second_frame_off).store(1 << 20, Ordering::Release); // absurd len
    let err = buf.recordable_slice(0, 1 << 20).unwrap_err();
    assert_eq!(err.claimed_len, 1 << 20);
    assert!(err.end > 0, "the first, intact frame was walked before the tear");
}
```

- [ ] **Step 3: Run to verify it fails**

```bash
cargo test -p uc_log recordable_slice_surfaces_torn -- --nocapture
```
Expected: FAIL — compile error (`recordable_slice` returns `&[u8]`, no `unwrap_err`). That is the red state for a signature-change task.

- [ ] **Step 4: Implement**

In buffer.rs, above `recordable_slice`:

```rust
/// Post-M7 follow-up: an impossible length word inside the committed region
/// `[from, append)` during `recordable_slice`'s frame walk. The committed
/// region is immutable until recorded, so this is a recorder-side invariant
/// break (torn write, mis-primed `from`, or memory-ordering bug) — the
/// archive must fail-stop loudly rather than record a malformed block
/// (previously a `debug_assert!`, i.e. silent garbage in release builds —
/// the leading upstream suspect for the once-seen `Replay::next` panic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordableCorrupt {
    pub from: u64,
    pub append: u64,
    pub end: u64,
    pub claimed_len: u32,
}
```

Change the signature and replace the `debug_assert!` at line 212:

```rust
    pub fn recordable_slice(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> Result<&[u8], RecordableCorrupt> {
        let append = self.cnc.counters().append.load_acquire();
        if append <= from {
            return Ok(&[]);
        }
        ...
            let aligned = align_frame_len(len as usize) as u64;
            if aligned == 0 || end + aligned > hard {
                return Err(RecordableCorrupt { from, append, end, claimed_len: len });
            }
        ...
        Ok(unsafe { std::slice::from_raw_parts(self.region.ptr_at(off), end as usize) })
    }
```

(Frames never span the wrap and padding fills exactly to it, so `end + aligned > hard` — whether `hard` is the wrap clamp or the append clamp — is genuinely impossible for intact committed frames; keep that reasoning in the comment.)

In archive.rs add the variant and update `do_work`:

```rust
#[error("recorder-side corrupt frame walk: {0:?}")]
RecorderCorrupt(crate::buffer::RecordableCorrupt),
```

```rust
        let slice = buffer
            .recordable_slice(self.durable_pos, self.cfg.max_block_bytes)
            .map_err(ArchiveError::RecorderCorrupt)?;
```

Update any test callers found in Step 1 (`.unwrap()` on the new `Result`).

- [ ] **Step 5: Run tests**

```bash
cargo test -p uc_log && cargo clippy -p uc_log --all-targets -- -D warnings
```
Expected: PASS, clippy clean.

- [ ] **Step 6: Commit**

```bash
git add uc_log/src/buffer.rs uc_log/src/archive.rs
git commit -m "fix(uc_log): recordable_slice promotes its torn-walk debug_assert to a checked RecorderCorrupt error"
```

---

### Task 3: Archive-panic investigation — stress harness + verdict

**Files:**
- Create: `uc_log/tests/archive_stress.rs`
- Modify (only if root cause confirmed): site named by the verdict
- Modify: `.superpowers/sdd/progress-followups.md` (verdict entry), `docs/benchmarks/uc2-m7-gate-2026-07-13.md` (known-issues update)

**Interfaces:**
- Consumes: `ArchiveError::CorruptBlock` (Task 1), `ArchiveError::RecorderCorrupt` (Task 2) — the repro signal is one of these errors surfacing.

This is an investigation task (systematic-debugging shape): the deliverable is a **verdict**, not necessarily a fix. Context: `archive.rs` Replay panic seen ONCE in the failover capstone (`lin_v2.rs` failover arm — elections + truncation churn, NO config frames). Hypotheses, in prior order:

1. **H1 — torn `recordable_slice` walk** (now surfaces as `RecorderCorrupt`): the one live-buffer reader without the seqlock post-copy re-check. Its safety argument holds ONLY if `[durable, append)` is truly immutable and `from` is truly a frame boundary.
2. **H2 — truncation seam**: `truncate_to` (archive) + election truncation reset `durable_pos`/counters; a window where `recordable_slice(from)` walks bytes whose buffer offsets hold PRE-truncation frames (stale lengths at re-used offsets) would record a malformed block. The capstone does exactly this churn.
3. **H3 — journal pending-tail read**: `Replay` reading a block appended-but-not-fsynced (`uc_journal` 1de711a append-readability contract) — verify `Journal::read` CRC-checks and what a torn tail returns.
4. **H4 — restart counter priming**: `LogCounters::prime` after crash-restart leaving `durable_pos` mid-frame relative to buffer content.

- [ ] **Step 1: Build the stress harness**

`uc_log/tests/archive_stress.rs`, three arms sharing one driver (small buffer 64 KiB so wrap is constant; frames 100-4000 B mixed):

```rust
//! Post-M7 archive stress: concurrent appender + archiver + replayer,
//! wrap-heavy, with truncation and reopen arms. Repro harness for the
//! once-seen Replay::next OOB panic (M7 ledger open ticket). A structured
//! CorruptBlock/RecorderCorrupt error IS the repro signal — fail the test
//! and print the full error + iteration seed.
//! Budget: UC2_ARCHIVE_STRESS_MS (default 2000 for CI; run 60000+ locally).
```

- Arm A `stress_append_archive_replay`: appender thread appends frames; archiver thread loops `do_work`; replayer thread loops `replay_from(random archived pos)` draining `next()` to the end. Any `Err` → panic with seed + error.
- Arm B `stress_with_truncation`: same + periodically `truncate_to(pos)` at a frame boundary below the frontier (mimic election reconciliation), then resume appending from there — mirror how `uc_node` drives it (crib the call pattern from `grep -n "truncate_to" uc_node/src/node.rs uc_log/src/archive.rs`).
- Arm C `stress_reopen`: periodically drop + reopen `Archive`/`Journal` mid-load (crash-restart shape, prime counters as node boot does).

- [ ] **Step 2: Run short (CI budget) and long (repro attempt)**

```bash
cargo test -p uc_log --test archive_stress --release
UC2_ARCHIVE_STRESS_MS=60000 cargo test -p uc_log --test archive_stress --release -- --nocapture
```
Run the 60 s form at least 5 times. Record every outcome.

- [ ] **Step 3: Code-audit the four hypotheses**

Regardless of repro: read each seam and write one paragraph per hypothesis (holds / broken / can't-tell + why) into the ledger entry. For H2 specifically: trace `truncate_to`'s handling of `durable_pos` vs the buffer's `append` counter reset and answer "can `recordable_slice(from)` ever walk offsets holding stale pre-truncation lengths?". For H3: read `uc_journal`'s `read()` CRC path and the pending-tail contract.

- [ ] **Step 4: Fix if confirmed**

If a mechanism is confirmed (by repro or by an airtight code argument): fix at the root site, add a deterministic red-verified regression test alongside the stress harness, and note the fix in the gate doc's known-issues section. If NOT reproduced and no hypothesis is confirmed: the Task 1+2 hardening stands as the mitigation; update the gate doc known-issues entry from "open" to "hardened: panics converted to structured fail-stop errors; not reproduced in N stress-hours; hypotheses audited" — an honest non-repro is an acceptable verdict.

- [ ] **Step 5: Run the capstone that saw the original panic**

```bash
cargo test -p uc_node --test lin_v2 --release
```
Expected: PASS (Linearizable).

- [ ] **Step 6: Commit**

```bash
git add uc_log/tests/archive_stress.rs .superpowers/sdd/progress-followups.md docs/benchmarks/uc2-m7-gate-2026-07-13.md
git commit -m "test(uc_log): archive stress harness (append/truncate/reopen arms) + Replay-panic verdict"
```
(plus the fix commit if Step 4 confirmed something — separate commit, own red-verified test)

---

### Task 4: Refuse `DemoteVoter{self}` + reason codes 11/12

**Files:**
- Modify: `uc_consensus/src/config.rs:47-59,163-176,224-241`, `uc_consensus/src/election.rs:821-839`, `uc_node/src/node.rs:2130-2163`, `uc_node/examples/uc2ctl.rs:135-153`
- Test: `uc_consensus/src/election.rs` test module, `uc_node/tests/reconfig.rs:1271` (`every_refusal_surfaces`)
- Docs: `docs/ops/uc2-runbook.md` §6 (self-demote refusal + recourse)

**Interfaces:**
- Produces: `ProposeError::SelfDemote` (wire reason 12); node-level `REASON_MALFORMED_OP: u32 = 11`.

- [ ] **Step 1: Write the failing SM test**

In election.rs's test module (crib an existing `propose_config` test for the leader-harness setup):

```rust
#[test]
fn self_demote_is_refused_other_demote_still_works() {
    // harness: 3-voter cluster, self = leader id 1, serving, nothing pending
    assert_eq!(
        sm.propose_config(ConfigOp::DemoteVoter { id: 1 }, SLACK),
        Err(ProposeError::SelfDemote)
    );
    assert!(sm.propose_config(ConfigOp::DemoteVoter { id: 2 }, SLACK).is_ok());
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p uc_consensus self_demote -- --nocapture
```
Expected: FAIL — `SelfDemote` variant does not exist (compile error).

- [ ] **Step 3: Implement the consensus side**

config.rs — add to `ProposeError` (after `NotCaughtUp`):

```rust
    SelfDemote,               // 12 (a leader may not demote itself — see propose_config)
```

Note in the enum's comment block: `// 11 is NOT a ProposeError: it is the node-level malformed/unknown-op reply (uc_node::REASON_MALFORMED_OP).`
Add `ProposeError::SelfDemote => 12,` to `reason_code`, and a row to `op_code_and_reason_code_match_the_wire_table`:

```rust
        assert_eq!(ClusterConfig::reason_code(&ProposeError::SelfDemote), 12);
```

election.rs `propose_config` — insert after the `ChangePending` check (line 830), before the promote catch-up check:

```rust
        if matches!(op, ConfigOp::DemoteVoter { id } if id == self.id) {
            // A leader demoting ITSELF would lead-as-learner forever:
            // `StepDownRemoved` covers only self-REMOVAL (rank_leader's
            // commit-crossing check), and nothing else ever demotes a
            // serving leader. Refuse; the operator recourse is
            // RemoveVoter{self} (the proven step-down path) plus a
            // fresh-id learner rejoin. `ClusterConfig::apply` stays
            // id-blind — this is the only validation site that knows self.
            return Err(ProposeError::SelfDemote);
        }
```

- [ ] **Step 4: Implement the node + CLI side**

node.rs — near `propose_and_append` (line 2147):

```rust
/// Wire reason for a malformed/unknown admin op field (NOT a `ProposeError`:
/// codes 1-10 and 12 are the SM's; 11 is the node's own defensive catch-all,
/// previously a deliberate reuse of 6/NotFound).
const REASON_MALFORMED_OP: u32 = 11;
```

Change line 2149 to `return (1, REASON_MALFORMED_OP, self.cnc.config_version());` and update the doc comment at 2136-2138 (drop the "6/NotFound's code reused" sentence, reference `REASON_MALFORMED_OP`).

uc2ctl.rs `reason_str` — add before the `_` arm:

```rust
        11 => "malformed/unknown op (node didn't recognize the request — CLI/node version mismatch?)",
        12 => "SelfDemote (a leader cannot demote itself; RemoveVoter it and rejoin a fresh id as learner)",
```

- [ ] **Step 5: Check for exhaustive `ProposeError` matches elsewhere**

```bash
grep -rn "ProposeError::" --include=*.rs uc_sim uc_node | grep -v "Err(ProposeError"
cargo build --workspace
```
Fix any non-wildcard match arms the compiler flags (world.rs:1746-1756 pattern-matches specific variants with passthrough — expected no change, verify).

- [ ] **Step 6: Extend the integration refusal matrix**

In `every_refusal_surfaces` (reconfig.rs, after the WrongRole block at line 1291 — cluster ids are the spawn indices; the AlreadyPresent row already uses id 0 as an existing voter):

```rust
    // ---- SelfDemote (12): demote the LEADER's own id ----
    let resp = admin_request(&leader_cnc, 3 /* DemoteVoter */, leader as u32, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 12, "SelfDemote expected, got {resp:?}");

    // ---- Malformed op (11): an op code the node doesn't know ----
    let resp = admin_request(&leader_cnc, 99, 5, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 11, "malformed-op reason expected, got {resp:?}");
```

- [ ] **Step 7: Run tests**

```bash
cargo test -p uc_consensus && cargo test -p uc_node --test reconfig every_refusal_surfaces
```
Expected: PASS.

- [ ] **Step 8: Runbook + commit**

Add to runbook §6 (live reconfiguration ops), after the demote recipe: "**Demoting the leader itself is refused (reason 12).** To turn a leader into a learner: `remove-voter` its id (self-removal is supported — the leader replicates its own removal, steps down when it commits), then `add-learner` a FRESH id on that host (tombstoned ids never rejoin)."

```bash
git add uc_consensus/src/config.rs uc_consensus/src/election.rs uc_node/src/node.rs uc_node/examples/uc2ctl.rs uc_node/tests/reconfig.rs docs/ops/uc2-runbook.md
git commit -m "feat(uc2): refuse DemoteVoter{self} (reason 12); dedicated malformed-op reason 11"
```

---

### Task 5: Refuse to start on recovered self-tombstone

**Files:**
- Modify: `uc_node/src/node.rs:423-425` (after `config`/`prev_config` are derived)
- Test: `uc_node/tests/reconfig.rs` (new integration test)
- Docs: `docs/ops/uc2-runbook.md` (decommission section)

**Interfaces:**
- Consumes: `Node::start(cfg) -> io::Result<Node>` (node.rs:303) — the error path this adds.

- [ ] **Step 1: Audit existing restart-of-removed-node coverage**

```bash
grep -n "restart\|Node::start\|spawn_node" uc_node/tests/reconfig.rs examples/uc_crashtest/src/*.rs | head -40
```
Identify any test that restarts a node whose id is tombstoned in its own recovered config (T9's `crash_mid_pending` restarts a node with a missed config; the T8 zombie test rebinds a port without restarting). Any such test's expectation flips to the new construction error — update it deliberately, noting it in the ledger.

- [ ] **Step 2: Write the failing integration test**

In reconfig.rs (crib `spawn_cluster` / removal / instance-dir helpers from the existing removal tests):

```rust
/// Post-M7 follow-up: a node restarted on an instance dir whose recovered
/// config tombstones its OWN id must refuse to start (previously: booted as
/// a permanently-idle zombie — the runtime HaltRemoved latch is version-
/// gated and never re-fires on boot).
#[test]
fn restart_of_removed_node_refuses_to_start() {
    let _g = serialize();
    // spawn 3 voters + add learner id 100; await settle
    // remove-learner 100 (tombstones it); await config settle on survivors
    // drop learner's Node handle (clean stop), keep its instance dir
    let err = /* Node::start(same NodeConfig for id 100, same instance_dir) */
        .expect_err("a tombstoned id must not boot");
    let msg = err.to_string();
    assert!(msg.contains("tombstoned"), "error must name the cause: {msg}");
    assert!(msg.contains("fresh id"), "error must name the recourse: {msg}");
}
```

- [ ] **Step 3: Run to verify it fails**

```bash
cargo test -p uc_node --test reconfig restart_of_removed_node -- --nocapture
```
Expected: FAIL — `Node::start` currently succeeds (zombie boots).

- [ ] **Step 4: Implement**

node.rs, immediately after line 424 (`let prev_config = ...`):

```rust
        // Post-M7 follow-up: a node whose OWN id is tombstoned in the
        // recovered config can never rejoin under this id (fresh-forever
        // ids) and would otherwise boot as a permanently-idle zombie — the
        // runtime HaltRemoved latch cannot re-fire (adoption is version-
        // gated; no higher-version ConfigObserved ever arrives for an
        // already-adopted removal). Fail loudly at construction: an
        // orchestrator sees a failed unit, not a healthy idle one. (The T8
        // truncation-revert edge — a durable-but-uncommitted self-removal
        // later truncated cluster-wide — previously recovered via restart;
        // its recourse is now wipe-and-rejoin, documented in the runbook.)
        if config.tombstones.contains(&cfg.id) {
            return Err(io::Error::other(format!(
                "node id {} is tombstoned in the recovered cluster config (v{}): \
                 this id was permanently removed and can never rejoin; \
                 decommission this instance dir, or wipe it and rejoin with a fresh id",
                cfg.id, config.version
            )));
        }
```

- [ ] **Step 5: Run tests**

```bash
cargo test -p uc_node --test reconfig
cargo test -p uc_crashtest --features hard-crash-tests
```
Expected: PASS (including any expectation updated in Step 1; the crashtest run guards the phantom-learner design against the new refusal).

- [ ] **Step 6: Runbook + commit**

Runbook decommission subsection: add "a removed node's binary refuses to restart on its old instance dir (`tombstoned in the recovered cluster config`) — this is the intended decommission backstop, not an error to work around. If a node was removed while its removal was still uncommitted and the cluster later truncated it (rare; requires losing the removal's quorum), the wrongly-halted node's recourse is the same wipe-and-rejoin."

```bash
git add uc_node/src/node.rs uc_node/tests/reconfig.rs docs/ops/uc2-runbook.md
git commit -m "feat(uc_node): refuse to start when the recovered config tombstones our own id (zombie boot closed)"
```

---

### Task 6: `ConfigObserved` position≤durable belt

**Files:**
- Modify: `uc_node/src/node.rs:1160-1165` (do_work step 1c drain)
- Test: node.rs's in-file test module — the harness at node.rs:3035/3610 already owns a `cfg_obs` sender (`_cfg_obs_tx`) and a driveable `Consensus`.

**Interfaces:**
- Consumes: `ElectionSm::config()` (election.rs:794), cnc `counters().durable`.

- [ ] **Step 1: Verify the ordering claim**

Read node.rs:685-700: the archive agent calls `archive.do_work(..)` (which `store_release`s the durable counter as its LAST step — archive.rs:263) and only THEN drains `take_config_observations` into `cfg_obs_tx`. Therefore at the consensus drain, `position <= durable` is an invariant for every legitimately-recorded CONFIG frame. Record the confirmation (or the correction, if the agent loop differs) in the ledger — the belt's soundness rests on it.

- [ ] **Step 2: Write the failing test**

Using the node.rs Consensus test harness (the `h.cons.feed(...)` tests near node.rs:3610 — the harness constructor at node.rs:3035 keeps the `cfg_obs` sender):

```rust
#[test]
fn implausible_config_observation_is_ignored() {
    // harness: durable counter at some small value D (e.g. 4096)
    // send via the harness's cfg_obs sender: (position = D + 1_000_000,
    //   payload = encode_config of a valid v1 config)
    // drive one do_work cycle
    assert_eq!(h.cons.sm.config().version, 0, "implausible obs must not adopt");
    // then send the SAME config at position <= D and drive a cycle:
    assert_eq!(h.cons.sm.config().version, 1, "plausible obs must still adopt");
}
```

- [ ] **Step 3: Run to verify it fails**

```bash
cargo test -p uc_node implausible_config_observation -- --nocapture
```
Expected: FAIL — today the implausible observation is adopted (first assert fires).

- [ ] **Step 4: Implement**

Replace the 1c drain body (node.rs:1160-1165):

```rust
        while let Ok((position, payload)) = self.cfg_obs_rx.try_recv() {
            let wire = decode_config(&payload)
                .unwrap_or_else(|| panic!("corrupt CONFIG frame at {position}"));
            // Belt (post-M7 follow-up): observations are drained AFTER the
            // archive agent's do_work returned, and do_work store_release's
            // durable as its LAST step — so a durably-recorded CONFIG
            // frame's end position can never exceed the durable counter
            // here. A violation is a mis-based observation (recorder bug):
            // adopting it would park config_position above durable, where
            // config_pending could never clear. Skip + log, don't adopt.
            let durable = self.cnc.counters().durable.load_acquire();
            if position > durable {
                eprintln!(
                    "node {}: ignoring implausible ConfigObserved at {position} (durable {durable})",
                    self.id
                );
                did = true;
                continue;
            }
            self.feed(Event::ConfigObserved { position, config: wire_to_cluster_config(&wire) });
            did = true;
        }
```

(Deliberately NOT a `debug_assert!` — the discriminating test must be able to drive the violating path in debug builds; the eprintln + skip IS the belt. This refines the spec's "debug-assert + release ignore-with-log" to "ignore-with-log in all builds" for testability.)

- [ ] **Step 5: Run tests + commit**

```bash
cargo test -p uc_node --lib && cargo test -p uc_node --test reconfig
```
Expected: PASS.

```bash
git add uc_node/src/node.rs
git commit -m "fix(uc_node): belt on follower ConfigObserved — skip observations above durable instead of adopting them"
```

---

### Task 7: Equal-version content-divergence check

**Files:**
- Modify: `uc_node/src/node.rs:1160-1180` (same 1c drain, after Task 6's shape)
- Test: node.rs test module (same harness as Task 6) + a pure-function unit test

**Interfaces:**
- Produces: `pub(crate) fn config_content_diverges(current: &ClusterConfig, incoming: &ClusterConfig) -> bool` (node.rs free function).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn config_content_divergence_detector() {
    let a = /* ClusterConfig v3, voters {1,2,3} */;
    let mut b = a.clone();
    assert!(!config_content_diverges(&a, &b));         // identical: benign
    b.voters.pop();
    assert!(config_content_diverges(&a, &b));           // same version, different content
    let mut c = a.clone();
    c.version += 1;
    assert!(!config_content_diverges(&a, &c));          // different version: not this check's job
}
```

Plus a harness test mirroring Task 6's: adopt v1, then send a same-version-different-content payload at a plausible position — assert the adopted config is unchanged (the version gate already drops it; this task is about the loud signal, which the next step wires).

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p uc_node config_content_divergence -- --nocapture
```
Expected: FAIL — function does not exist (compile error).

- [ ] **Step 3: Implement**

node.rs free function + call site in the 1c drain (between Task 6's belt and the `feed`):

```rust
/// Post-M7 follow-up: same version + different content = divergence (two
/// configs minted at one version — possible only via the wipe-fiat position
/// reset or a bug), never a benign re-observation. The version gate in
/// `Event::ConfigObserved` SILENTLY drops equal versions, so without this
/// check divergence is invisible.
pub(crate) fn config_content_diverges(current: &ClusterConfig, incoming: &ClusterConfig) -> bool {
    incoming.version == current.version && incoming != current
}
```

```rust
            let config = wire_to_cluster_config(&wire);
            if config_content_diverges(self.sm.config(), &config) {
                eprintln!(
                    "node {}: DIVERGENT config observed at {position}: version {} content differs from adopted",
                    self.id, config.version
                );
            }
            self.feed(Event::ConfigObserved { position, config });
```

- [ ] **Step 4: Run tests + commit**

```bash
cargo test -p uc_node --lib
git add uc_node/src/node.rs
git commit -m "feat(uc_node): surface equal-version config content divergence instead of silently ignoring it"
```

---

### Task 8: Admin-band single-writer audit

**Files:**
- Modify: `uc_log/src/cnc.rs:476,514` (contract comments), `uc_node/examples/uc2ctl.rs:155-169` (doc), `docs/ops/uc2-runbook.md` §6

No behavior change expected; the audit decides. Writers found up front: `write_admin_req` ← uc2ctl only (external process, line 169); `write_admin_resp` ← consensus agent only (node.rs:2211).

- [ ] **Step 1: Complete the audit**

```bash
grep -rn "write_admin_req\|write_admin_resp" --include=*.rs .
```
Confirm no writer beyond the two known ones (tests excepted). The REAL hazard is two CONCURRENT `uc2ctl` invocations: both read the slot's `seq`, both write `seq+1` — the seqlock protects reader-vs-writer, not writer-vs-writer, so fields can interleave. If the audit finds an in-tree second writer instead, implement the seqlock re-check option and stop here; otherwise document (Step 2).

- [ ] **Step 2: Document the contract at all three sites**

cnc.rs, on `write_admin_req`:

```rust
    /// CONTRACT (post-M7 audit): at most ONE admin client writes this band
    /// at a time. The seqlock (seq store_release'd last) protects the
    /// node's reader from torn fields, NOT two concurrent writers from
    /// interleaving — two uc2ctl processes racing this slot can compose a
    /// request neither sent (worst case: a refused/nonsense op, never data
    /// corruption — the node validates every field). Operators: one uc2ctl
    /// at a time per instance dir.
```

Mirror one-line versions on `write_admin_resp` (single writer = the consensus agent; enforced by the four-agent single-writer design) and in uc2ctl's `run_mutate` doc. Runbook §6: add the "one concurrent uc2ctl per instance dir" operational rule with the same worst-case note.

- [ ] **Step 3: Verify + commit**

```bash
cargo build --workspace
git add uc_log/src/cnc.rs uc_node/examples/uc2ctl.rs docs/ops/uc2-runbook.md
git commit -m "docs(uc2): admin-band single-writer contract pinned at both accessors + runbook (audit: no in-tree second writer)"
```

---

### Task 9: cnc publishes `admission_bytes` + version bump

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs:110-124,410-420` (new offset + asserts + offset test), `uc_protocol/src/version.rs:25`, `uc_log/src/cnc.rs` (accessors + tests, model on `config_version` at 422-435), `uc_node/src/node.rs:430` (boot write), `uc_node/examples/uc2ctl.rs:82-91,252-258`
- Test: offset-pin tests in BOTH uc_protocol and uc_log; a boot assertion in an existing reconfig.rs test

**Interfaces:**
- Produces: `CNC_OFF_ADMISSION_BYTES: usize = 3712` (uc_protocol); `CncPage::admission_bytes() -> u64` / `store_admission_bytes(u64)` (uc_log). Zero ⇒ written by a pre-0.3.0 node (reader falls back).

- [ ] **Step 1: Write the failing offset tests**

uc_protocol cnc.rs offset test (extend the existing `assert_eq!` block at 414-417): `assert_eq!(CNC_OFF_ADMISSION_BYTES, 3712);`. uc_log cnc.rs: a roundtrip + offset-pin test modeled on the existing config_version test:

```rust
#[test]
fn admission_bytes_roundtrip_and_offset_pin() {
    // page: same tempdir construction as the sibling tests
    assert_eq!(page.admission_bytes(), 0, "fresh page reads 0 (pre-0.3.0 sentinel)");
    page.store_admission_bytes(256 * 1024);
    assert_eq!(page.admission_bytes(), 256 * 1024);
    let raw = page.page();
    assert_eq!(
        u64::from_le_bytes(raw[3712..3720].try_into().unwrap()),
        256 * 1024,
        "offset pin: the value must live at 3712 exactly"
    );
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
cargo test -p uc_protocol && cargo test -p uc_log admission_bytes
```
Expected: FAIL — constant/accessors don't exist.

- [ ] **Step 3: Implement**

uc_protocol cnc.rs after `CNC_OFF_ADMIN_RESP` (line 122):

```rust
/// Post-M7 (0.3.0): the node's configured admission window
/// (`NodeConfig::admission_bytes`), published once at boot. 0 = written by a
/// pre-0.3.0 node (readers fall back to their own default). Next free
/// reserved-band offset after this line: 3776.
pub const CNC_OFF_ADMISSION_BYTES: usize = 3712;
const _: () = assert!(CNC_OFF_ADMISSION_BYTES + 64 <= CNC_PAGE_LEN);
```

uc_log cnc.rs — accessors verbatim on the `config_version` pattern (lines 422-435), named `admission_bytes` / `store_admission_bytes`, offset `CNC_OFF_ADMISSION_BYTES`, same SAFETY comments with `3712`.

node.rs line 430 area (with the other boot mirrors):

```rust
        cnc.store_admission_bytes(cfg.admission_bytes);
```

uc2ctl.rs — `StatusArgs.admission_bytes` becomes `Option<u64>`:

```rust
    /// Override for the staleness warning's admission window. Since 0.3.0
    /// the node publishes its configured value on the cnc page and this
    /// flag is only needed against pre-0.3.0 nodes (whose page reads 0 —
    /// then this default applies).
    #[arg(long)]
    admission_bytes: Option<u64>,
```

and in `run_status`, before the loop (replacing direct uses of `a.admission_bytes` at lines 253/258):

```rust
    let admission_bytes = a.admission_bytes.unwrap_or_else(|| {
        match cnc.admission_bytes() {
            0 => 256 * 1024, // pre-0.3.0 node: fall back to the old default
            v => v,
        }
    });
```

version.rs line 25: `pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 3, 0);` with a one-line comment: `// 0.3.0: post-M7 follow-ups — cnc admission_bytes @3712, admin reason codes 11/12 (additive).` Then check nothing pins 0.2.0:

```bash
grep -rn "0, 2, 0\|0\.2\.0" --include=*.rs uc_protocol uc_log uc_node uc_net | grep -v target
```

- [ ] **Step 4: Boot assertion in an existing test**

In reconfig.rs's cluster-spawn settle helper area, extend one existing status-path test (or `every_refusal_surfaces`'s setup) with: `assert_eq!(leader_cnc.admission_bytes(), 256 * 1024);` (the harness's configured window — read the harness's actual value first).

- [ ] **Step 5: Run tests + commit**

```bash
cargo test -p uc_protocol && cargo test -p uc_log && cargo test -p uc_node --test reconfig && cargo clippy --workspace --all-targets -- -D warnings
```
Expected: PASS.

```bash
git add uc_protocol/src/v2/cnc.rs uc_protocol/src/version.rs uc_log/src/cnc.rs uc_node/src/node.rs uc_node/examples/uc2ctl.rs uc_node/tests/reconfig.rs
git commit -m "feat(uc2): node publishes admission_bytes on cnc @3712; uc2ctl reads it; protocol 0.3.0"
```

---

### Task 10: Fiat install clears cnc `config_pending`

**Files:**
- Modify: `uc_node/src/node.rs:1444` (fiat adopt block in `maybe_adopt_incoming_snapshot`)
- Test: extend the fiat-routing test in `uc_node/tests/learner.rs` (the T9 `rebuild_net_for_config` peer-band test — locate: `grep -n "fiat\|adopt_snapshot" uc_node/tests/learner.rs`)

- [ ] **Step 1: Write the failing assertion**

In the existing learner fiat test, after the install completes, add:

```rust
    assert_eq!(
        joiner_cnc.config_pending(), 0,
        "a fiat install is never pending — the cnc mirror must read stable"
    );
```

To make it discriminating, the test must first put the mirror in the pending state (if the scenario doesn't naturally: `joiner_cnc.store_config_pending(true)` before triggering the install — a stale mirror from a pre-crash pending change is exactly the real-world shape).

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p uc_node --test learner -- --nocapture
```
Expected: FAIL on the new assertion (fiat path never touches the mirror today).

- [ ] **Step 3: Implement**

node.rs, in the fiat block after `self.cnc.store_config_version(cfg.version);` (line 1444):

```rust
                // Post-M7 follow-up: a fiat install has no in-flight change
                // by construction (cur == prev at the floor) — clear the
                // pending mirror too, or a stale pre-crash `true` sticks
                // until the NEXT live change commits.
                self.cnc.store_config_pending(false);
```

- [ ] **Step 4: Run tests + commit**

```bash
cargo test -p uc_node --test learner
git add uc_node/src/node.rs uc_node/tests/learner.rs
git commit -m "fix(uc_node): fiat snapshot install clears the cnc config_pending mirror"
```

---

### Task 11: Codec/cnc/comment minors — ledger (b), (c), (f)

**Files:**
- Modify: `uc_protocol/src/v2/config.rs` (test module), `uc_log/src/cnc.rs` (test module), `uc_consensus/src/election.rs` (`ElectionSm::new` — locate the tracker-sizing line: `grep -n "fn new" uc_consensus/src/election.rs`)

- [ ] **Step 1: (b) decode_config minimal-boundary test**

In uc_protocol config.rs tests:

```rust
    #[test]
    fn decode_config_minimal_boundary() {
        // Ledger minor (b): the exact-CONFIG_FIXED_LEN success case (an
        // empty config) was untested — only failures and populated configs.
        let empty = WireConfig {
            version: 1,
            prev_position: 0,
            voters: vec![],
            learners: vec![],
            tombstones: vec![],
        };
        let mut buf = Vec::new();
        encode_config(&empty, &mut buf);
        assert_eq!(buf.len(), CONFIG_FIXED_LEN);
        assert_eq!(decode_config(&buf), Some(empty));
        assert_eq!(decode_config(&buf[..CONFIG_FIXED_LEN - 1]), None);
    }
```

- [ ] **Step 2: (c) admin-band port raw-byte pin**

In uc_log cnc.rs tests (same page construction as `admin_req_roundtrip_and_seq_discipline`, line 829):

```rust
    #[test]
    fn admin_req_port_is_u32_wide_at_plus_28_raw_bytes() {
        // Ledger minor (c): the roundtrip test is width-blind — pin the
        // wire fact directly: port occupies the u32 at +28 (T1 review fix).
        page.write_admin_req(&AdminReq { seq: 1, nonce: 0, op: 1, id: 1, ip: 0, port: 0x4A9C });
        let raw = page.page();
        assert_eq!(
            &raw[CNC_OFF_ADMIN_REQ + 28..CNC_OFF_ADMIN_REQ + 32],
            &[0x9C, 0x4A, 0x00, 0x00],
            "port must be LE u32 at +28"
        );
    }
```

(If `page()` is private to the impl, the test module is in the same file — it already has access; mirror however the existing raw-read at read_admin_req:467 gets the bytes.)

- [ ] **Step 3: (f) ElectionSm::new sizing footgun comment**

At the tracker-construction line inside `ElectionSm::new`, add:

```rust
        // FOOTGUN (ledger minor f): this initial sizing is NOT can_vote-
        // aware — it is correct only because every adoption immediately
        // re-derives via rebuild_membership (which IS can_vote-aware, see
        // its sizing subtlety doc). If `new` ever adopts a non-genesis
        // config directly, size with rebuild_membership's rule instead.
```

- [ ] **Step 4: Run + commit**

```bash
cargo test -p uc_protocol && cargo test -p uc_log && cargo test -p uc_consensus
git add uc_protocol/src/v2/config.rs uc_log/src/cnc.rs uc_consensus/src/election.rs
git commit -m "test(uc2): ledger minors (b) decode boundary, (c) admin port raw-byte pin, (f) sizing footgun comment"
```

---

### Task 12: Sim minors — ledger (g) parked violation, (x) run_until timeout signal

**Files:**
- Modify: `uc_sim/src/world.rs:596-640` (`run`, `run_until_leader`, `run_until`, `run_steps`)
- Modify: `uc_sim/tests/scenarios.rs` (all `run_until` callers — 25 sites across the two files)

**Interfaces:**
- Produces: `run_until(&mut self, pred) -> Result<bool, InvariantViolation>` — `Ok(true)` iff the predicate held. `run`/`run_until_leader`/`run_steps` keep their signatures but gain the entry violation check.

- [ ] **Step 1: Write the failing tests**

In world.rs's test module (or scenarios.rs if world.rs has none — check `grep -n "mod tests" uc_sim/src/world.rs`):

```rust
#[test]
fn run_until_reports_timeout_distinctly() {
    // minimal 3-node world, tiny max_steps budget
    let held = w.run_until(|_| false).unwrap();
    assert!(!held, "an unsatisfiable predicate must report Ok(false), not silent success");
    let held = w.run_until(|_| true).unwrap();
    assert!(held);
}

#[test]
fn parked_violation_surfaces_without_a_step() {
    // world with w.pending_violation = Some(v) (drive it via the scripted
    // propose_config self-feed path that parks violations — see
    // world.rs:414 and the step_once take at :647 — or a test-only setter
    // if that path can't be reached cheaply; prefer the real path)
    assert!(w.run_until(|_| true).is_err(), "ledger (g): a parked violation must not be dropped when pred is already true");
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
cargo test -p uc_sim run_until_reports -- --nocapture
```
Expected: FAIL — compile error (`Ok(())` is not `bool`) for the first; the second would silently pass today (violation dropped).

- [ ] **Step 3: Implement**

```rust
    /// Step until `pred` holds (or the budget runs out). `Ok(true)` iff the
    /// predicate held; `Ok(false)` = budget/queue/cap exhausted first
    /// (ledger minor x: the old `Ok(())` let scenarios silently "pass"
    /// phases that had timed out).
    pub fn run_until(
        &mut self,
        mut pred: impl FnMut(&World) -> bool,
    ) -> Result<bool, InvariantViolation> {
        if let Some(v) = self.pending_violation.take() {
            return Err(v); // ledger minor g: don't drop a parked violation
        }
        while !pred(self) && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(pred(self))
    }
```

Add the identical 3-line `pending_violation` entry check to `run` (line 596), `run_until_leader` (609), and `run_steps` (632) — signatures unchanged.

- [ ] **Step 4: Update all callers**

```bash
grep -rn "run_until(" uc_sim | grep -v "fn run_until"
```
For each of the ~25 sites, choose deliberately: where the scenario REQUIRES convergence, `assert!(w.run_until(p)?, "<phase> timed out");` (or `.unwrap()` + assert in non-Result tests); where timeout is genuinely tolerated (e.g. bounded-wait probes followed by their own assertion), `let _ = w.run_until(p)?;` with a one-line comment saying why timeout is acceptable. Do NOT blanket-`let _` — the point of (x) is to force each site to state its intent.

- [ ] **Step 5: Run the full sim suite**

```bash
cargo test -p uc_sim
```
Expected: PASS. If a scenario NOW fails its new timeout assert, that is (x) doing its job — investigate whether the phase was silently timing out before (fix the scenario's budget or its predicate, and note it in the ledger).

- [ ] **Step 6: Commit**

```bash
git add uc_sim/src/world.rs uc_sim/tests/scenarios.rs
git commit -m "fix(uc_sim): run_until returns timeout signal; run helpers surface parked violations at entry"
```

---

### Task 13: Test minors — ledger (k), (q), (r)

**Files:**
- Modify: `uc_node/src/node.rs` test module (k — near the `recover_config_record` unit tests: `grep -n "recover_config_record" uc_node/src/node.rs`), `uc_node/tests/reconfig.rs` (q — the self-removal handoff test: `grep -n "monoton" uc_node/tests/reconfig.rs`), `uc_consensus/src/election.rs` test module (r)

- [ ] **Step 1: (k) T5-revert-then-rederive composition test**

The pair was only tested separately. In node.rs's `recover_config_record` unit tests (crib the existing preseed helpers there):

```rust
#[test]
fn boot_revert_then_journal_rederive_compose() {
    // Ledger minor (k): (1) persist a ConfigRecord whose position is AHEAD
    // of recovered durable (T5 crash-window shape) — boot must revert to
    // prev; (2) the journal ALSO retains a CONFIG frame BELOW durable that
    // is NEWER than prev — rederivation must then win over the reverted
    // prev. Assert the recovered record is the journal-derived config (its
    // version), not prev's, and its position is the frame-END position.
}
```

Fill in with the existing tests' journal/state builders; the discriminating assert is `recovered.config.version == journal_cfg_version && recovered.position == frame_end`, which fails if recovery stops at the revert (rederive skipped).

- [ ] **Step 2: (q) strengthen committed-monotonicity**

In the self-removal handoff test, locate the current weak assertion (commit non-regression across handoff). Strengthen: capture `old_commit` = the removed leader's last published commit BEFORE the handoff, drive post-handoff traffic, and assert the new leader's commit is `> old_commit` (strictly — proving the new leader actually commits NEW entries, not merely inherits the counter).

- [ ] **Step 3: (r) SM-level latch-vs-raw discrimination pin**

In election.rs tests (SM-level — the 40 s integration test is currently the only discriminator for the T9 tombstone-predicate fix):

```rust
#[test]
fn self_removed_latch_is_tombstone_based_not_absence_based() {
    // A node adopting a config that does NOT contain its id but does NOT
    // tombstone it (the not-yet-admitted replay shape) must NOT latch:
    // feed ConfigObserved{v1 excluding self, no tombstone} -> assert no
    // HaltRemoved action and a later ConfigObserved{v2 including self}
    // adopts normally. Then feed a config that TOMBSTONES self -> assert
    // the latch (HaltRemoved for a follower). A raw !contains() predicate
    // fails the first half; the tombstone predicate passes both.
}
```

(Crib the harness from the existing adopt_config tests; `self_removed` is observable via the emitted `HaltRemoved` action for a follower.)

- [ ] **Step 4: Run + commit**

```bash
cargo test -p uc_node --lib && cargo test -p uc_node --test reconfig && cargo test -p uc_consensus
git add uc_node/src/node.rs uc_node/tests/reconfig.rs uc_consensus/src/election.rs
git commit -m "test(uc2): ledger minors (k) revert+rederive composition, (q) strict commit monotonicity, (r) SM-level tombstone-latch pin"
```

---

### Task 14: Rename (y) + docs wave (releases.md, CLAUDE.md, runbook, crash-window note)

**Files:**
- Modify: `uc_node/tests/lincheck_v2/mod.rs` + `uc_node/tests/lin_v2.rs` (y), `CLAUDE.md`, `docs/ops/uc2-runbook.md`
- Create: `docs/releases.md`

- [ ] **Step 1: (y) rename `config_ops_committed`**

```bash
grep -rn "config_ops_committed" --include=*.rs .
```
Rename to `config_ops_accepted` everywhere it appears (field, uses, log strings) with a doc line: `// counts LOCAL leader accepts (status=0 replies), not durable commits — a late-crash accept may be reverted; the capstone's non-vacuity floor only needs "the arm exercised reconfig", so accepts are the right denominator.`

- [ ] **Step 2: CLAUDE.md durable-state list**

In the Storage primitives bullet: `vote, term map, snapshot floor, output progress` → `vote, term map, snapshot floor, output progress, cluster-config record (config.state)`.

- [ ] **Step 3: Runbook crash-window note (p)**

In §6 near the admin-op recipes: "**If the leader crashes between accepting an admin op and writing the reply**, `uc2ctl` times out with no response line — the op may still have landed. Recourse: `uc2ctl status` on the new leader; if `config version` advanced, the op committed (do NOT blind-retry an add/remove — the refusal matrix (AlreadyPresent/Tombstoned) makes a duplicate harmless but noisy)."

- [ ] **Step 4: docs/releases.md**

```markdown
# ultima_cluster releases

## v2.1.0 — 2026-07-14
M7 live single-server reconfiguration (promote/demote/add/remove under load,
no restarts, `uc2ctl` admin path, tombstone-based fresh-forever ids, leader
self-removal). 5-host fleet gate passed: worst transition dip 4.7% (<10%),
self-removal gap 3.22 s (<10 s), zero loss/divergence, snapshots+purge paired.
Wire protocol 0.2.0 (FRAME_TYPE_CONFIG=4, admin datagram kinds 16/17).

## v2.0.0 — known issues
- **MPSC ingress ring free-space underflow under producer contention**
  (clients→node ingress only): a stale `claim_pos` snapshot overtaken by the
  consumer could underflow the free-space computation — debug builds panic,
  release builds see spurious backpressure. **Not data corruption** (the CAS
  re-validates before any write). Fixed in v2.1.0 (8c1ae01, regression test
  98900fd). Remedy: upgrade to v2.1.0; no v2.0.1 is planned.
```

- [ ] **Step 5: Run + commit**

```bash
cargo test -p uc_node --test lin_v2 --release
git add uc_node/tests/lincheck_v2/mod.rs uc_node/tests/lin_v2.rs CLAUDE.md docs/ops/uc2-runbook.md docs/releases.md
git commit -m "docs(uc2): releases.md w/ v2.0.0 MPSC known-issue; config record in durable lists; crash-window ops note; rename config_ops_accepted (y)"
```

---

### Task 15: Full proof stack + wave close-out

**Files:**
- Modify: `.superpowers/sdd/progress-followups.md` (final entry), `.superpowers/sdd/progress.md` (mark follow-up list DISCHARGED with pointer)

- [ ] **Step 1: Full local proof stack**

```bash
cargo build --workspace
cargo test
cargo test -p uc_node --test lin_v2 --release
cargo test -p uc_node --test lin_partition_v2 --release
cargo test -p uc_crashtest --features hard-crash-tests
cargo test -p uc_service --features ultima_db
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p uc_node --release --example m7_gate -- all --secs 6
cargo run -p uc_node --release --example m6_gate -- all --secs 6 --cycles 3
```
Expected: ALL PASS / exit 0. Any failure: fix before proceeding (no gate skips).

- [ ] **Step 2: Ledger close-out**

Append the wave summary to `progress-followups.md` (per-task one-liners + the Task 3 verdict); in `progress.md`, annotate the POST-MERGE FOLLOW-UP LIST line: `(DISCHARGED by the post-M7 follow-up wave — see progress-followups.md; v2.0.x MPSC = release-note-only per docs/releases.md)`.

- [ ] **Step 3: Commit + hand off**

```bash
git add .superpowers/sdd/progress-followups.md .superpowers/sdd/progress.md
git commit -m "chore(followups): wave complete — full local proof stack green"
```
Then: final whole-branch review, then superpowers:finishing-a-development-branch (merge to main; push is a separate user-approved step).
