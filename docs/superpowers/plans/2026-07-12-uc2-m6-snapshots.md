# UC v2 M6 — snapshots, purge, learners, ops — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** service-built snapshots make journal purge safe and learner join possible; every "reader below the floor" path (follower NAK, service replay, fresh learner, no-common-prefix rejoin) resolves through one snapshot + tail-replay story — proven by purge-safety lincheck, a learner-join-under-load gate, and reconstruction-under-load tests.

**Architecture:** the service grows an optional `SnapshotStateMachine` capability (freeze → stream → install, positions in/out — the v1 u64-return rule kept) with the `ultima_db` `StoreStateMachine` adapter as its reference impl (`snapshot_stream` wire format end-to-end); a service-side builder thread writes position-tagged snapshot files into the instance dir and publishes the newest complete position on the cnc page; the node validates + durably persists that marker (StableValue, `output_progress` pattern) and — only when explicitly enabled — drives `Journal::purge_before` below it via the archive agent; the sender gains a bounded, strictly-lower-priority **snapshot session** (new datagram kinds 12–15) that ships the snapshot file to a peer whose NAK falls below the purge floor; a **learner** is a node that appears in the fan-out list and nowhere else (no vote, no quorum, no flow accounting, no read-probe quorum); `NoCommonPrefix` stops being fail-stop and becomes wipe-and-rejoin (safe by the leader-completeness argument, documented in-code). Purge ships **OFF by default** — every M6 bug class is "purged something someone still needed," and the gates are the mitigation.

**Tech Stack:** Rust workspace (edition 2024), `uc_protocol` v2 (no_std), `uc2_log`/`uc2_net`/`uc2_consensus`/`uc2_node`/`uc2_sim`/`uc2_service`/`uc2_client`, `ultima_journal` (in-tree: `purge_before`, `StableValue`, `TailReader`), sibling `ultima-db` (`persistence` feature: `snapshot_stream`/`install_snapshot_stream`), `uc-lincheck` (checker unchanged — again).

## Global Constraints

Copied from the spec (`docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` §4–§7, §9) and the M5 final review:

- **M6 gate (spec §9):** learner join under load (no quorum stall, bounded catch-up); purge safety (a purged reader recovers via snapshot + tail, lincheck stays green); reconstruction under load.
- **Purge is OFF by default.** `PurgePolicy::Disabled` unless explicitly configured. A deployment whose SM does not implement `SnapshotStateMachine` never registers a marker and never purges — journal grows; that is the documented price (v1 had the same contract).
- **A snapshot is only ever built at an applied position** (≤ min(commit, durable) at freeze time) — snapshot state is committed state by construction; snapshots from any incarnation/host remain valid forever.
- **Purge only below a durable snapshot marker** (StableValue-persisted, validated ≤ durable before persist), never into the covering journal block, and `Journal::purge_before` never drops the active segment (as-built guarantee, mod.rs:578).
- **`build_snapshot`/`install_snapshot` return `u64`** (the position represented / post-install) — the v1 rule that kills the decision-vs-call race.
- **Learners never affect safety quantities:** not in `ElectionSm.members`, not in `CommitTracker`, not in `FlowControl`'s quorum statistic, never sent a `READ_PROBE`, and their `READ_PROBE_ACK`s (impossible, but belt-and-suspenders) never count toward read quorum.
- **Snapshot-session traffic is strictly lower priority than live DATA and NAK-replay** — fixed per-duty-cycle budgets, never starving the live stream (spec §5 "separately paced").
- **Wipe-and-rejoin safety argument (must appear as a code comment where the wipe happens):** if any locally-durable byte were committed, quorum intersection + leader completeness put its term in the leader's map, so a common prefix would exist; `NoCommonPrefix` therefore proves nothing local is committed and discarding everything is safe.
- **Apply stays sync/deterministic/no-I/O**; `install_snapshot` runs on the apply thread inside the SM lock (it IS state mutation, positioned exactly like a batch of applies); `freeze` is O(1) (MVCC pin) and `stream_snapshot` runs off-thread with no SM lock.
- **Commit monotonic within-run only; apply target = min(commit, durable)** — unchanged M3/M4/M5 restart contract.
- **Every cnc counter has exactly one writer;** new slots land in the reserved 1152..4096 band with the writer named at the offset constant. `uc_protocol` stays no_std/core-only.
- **clippy `--workspace -- -D warnings` + `cargo clippy -p uc2_consensus -p uc2_sim -p uc2_node -p uc2_net -p uc2_log -p uc2_service -p uc2_client --all-targets -- -D warnings` stay clean.** Denied lints history: `manual_is_multiple_of`, `int_plus_one`, `collapsible_if` (let-chains).
- **Journals/instance dirs never on `/tmp` for load runs** (RAM tmpfs); unit tests with tiny buffers may use `tempfile::tempdir()`.
- **Implementers stage ONLY their own files** (never `git add -A`); branch `uc2/m6-snapshots`. `Cargo.lock` staged explicitly and named when touched.
- **`../ultima_db` is a SHARED checkout** (another session may have it open): Task 2's ultima_db change is a normal additive commit on its current branch — no tree-switching, no rebase, no branch deletion there, ever.
- **Honest gates:** binaries print the bar and `exit(1)` on FAIL; sandbox runs recorded as smoke, never as the gate; "Task 10 complete" ≠ "M6 gate passed" (fleet/e2e evidence per gate definition).

## File Structure (what this plan creates/modifies)

```
ultima_db (SIBLING REPO): src/snapshot_stream/install.rs   MOD  honor InstallOptions::commit_version
uc2_log/src/archive.rs               MOD  first_base tracking, purge_below(pos), gap-aware replay errors
ultima_journal/src/journal/tail_reader.rs MOD  scan_from(start_meta) segment skip + first_base()
uc2_service/src/traits.rs            MOD  SnapshotStateMachine trait + SnapshotError
uc2_service/src/snapshots.rs         NEW  snapshot dir layout, file naming, temp+rename, retention, discovery
uc2_service/src/builder_agent.rs     NEW  snapshot builder thread (policy, freeze/stream, cnc publish)
uc2_service/src/ultima_db.rs         NEW  StoreStateMachine v2 adapter (feature "ultima_db")
uc2_service/src/replay.rs            MOD  gap guard + snapshot-install reconstruction path
uc2_service/src/apply.rs             MOD  install-inside-apply-thread wiring
uc2_service/src/config.rs            MOD  SnapshotPolicy knobs, ServiceError::SnapshotRequired
uc_protocol/src/v2/cnc.rs            MOD  new slots in 1152.. band (snapshot marker, per-follower obs)
uc2_log/src/cnc.rs                   MOD  accessor structs for the new slots
uc2_log/src/state.rs                 MOD  NodeState gains snapshot marker StableValue
uc_protocol/src/v2/datagram.rs       MOD  kinds 12-15 SNAP_* + bodies + pins
uc2_net/src/sender.rs                MOD  snapshot session state + duty-cycle budget arbitration
uc2_net/src/receiver.rs              MOD  snapshot chunk intake + SNAP_NAK + completion handoff
uc2_net/src/flow.rs                  MOD  voting-only construction (explicit voter list + cluster_size)
uc2_consensus/src/election.rs        MOD  NoCommonPrefix → Action::WipeAndRejoin; learner-aware helpers
uc2_node/src/node.rs                 MOD  marker sampling/persist, purge driver, ArchiveCmd enum,
                                          learner config/wiring, snapshot-session orchestration,
                                          per-follower obs publication, wipe-and-rejoin exec
uc2_sim/src/world.rs                 MOD  purge + wipe-and-rejoin modeled (oracle unchanged)
uc2_sim/tests/scenarios.rs           MOD  no-common-prefix + purge pins
uc-lincheck/src/register.rs          MOD  RegisterSm implements SnapshotStateMachine (feature v2)
uc2_node/tests/purge_safety.rs       NEW  purge + reconstruction + lincheck-under-purge
uc2_node/tests/learner.rs            NEW  learner join/live/isolation scenarios
uc2_node/examples/m6_gate.rs         NEW  learner-join-under-load + purge-safety gate roles
docs/ops/uc2-runbook.md              NEW  ops runbook (promoted fleet gotchas)
docs/benchmarks/uc2-m6-gate-2026-07-XX.md NEW gate doc
```

Dependency order: Task 1 (floor primitives) → 2 (snapshot capability) → 3 (builder) → 4 (marker+purge driver) → 5 (service reconstruction) — the local story; 6 (wire session) → 7 (learner role) → 8 (join e2e + NoCommonPrefix) — the remote story (6 needs 3–4; 7 needs 6; 8 needs 7); 9 (ops/hardening, needs 4+7 for the slots' content); 10 (gates + docs, last). Tasks 1–2 are independent of each other.

## Decisions locked by this plan (resolving the sketch's open questions against M4/M5-as-built)

1. **Sender arbitration (sketch Q1):** fixed per-duty-cycle budgets in priority order — live fan-out first (existing, flow-limited), then NAK-replay (existing `REPLAY_DGRAMS_PER_NAK = 8`), then at most `SNAP_DGRAMS_PER_CYCLE = 4` snapshot chunks. Budgets are constants, not adaptive — the live stream can never be starved by construction, and a session under contention just takes longer (bounded-catch-up is the gate's job to measure, not a hard deadline).
2. **Purge coordination (sketch Q2): none needed in v2.0.** Any node may purge below its OWN durable snapshot marker (policy-gated, default off). No cluster-wide floor agreement: a peer needing purged bytes recovers via the snapshot session (remote) or the local snapshot file (same-host service) — the recovery path IS the coordination. `NoCommonPrefix` flips from `Action::Fatal` to wipe-and-rejoin and composes with the same machinery (a wiped node is an empty follower; if the leader purged, its first NAK below the floor triggers the session).
3. **Learner placement (sketch Q3):** learners live ONLY in the sender fan-out list (they receive DATA/heartbeats/gossip like any follower) and in `addr_to_id` for identification. They are NOT in `ElectionSm.members`, so every existing membership check (RequestVote drop, Vote count, Report ranking via `follower_slot`) excludes them for free. `FlowControl::new` changes to take the VOTING follower list + explicit `cluster_size` (as-built it derives `needed` from one list — the learner split makes the two roles explicit). Read probes are sent to voting peers only, and ack counting re-checks the acker against voting members.
4. **Epoch/seqlock final form (sketch Q4): unchanged from M5.** Attach-captured `my_epoch` + `expected_epoch` RETRY + instance_id fail-stop already close the TOCTOU inventory; snapshot install happens on the apply thread inside the SM lock, so no new window opens (a query cannot interleave with a half-installed SM).
5. **task14 reconcile-driver (sketch Q5): NOT ported.** The v2 service self-polls and self-recovers; the node never drives service reconstruction. The full v1 TOCTOU inventory reduces to the M5 mechanisms — re-derived, not assumed (the plan's reconstruction tests exercise install-under-churn to prove it).
6. **Buffer prefill (sketch item 4): REJECTED, with evidence.** M5's buffer-file reuse preserves ring bytes below durable across restarts, and journal NAK-replay (M4) serves anything older on demand; the M5 fleet run recorded zero NAKs at 1.6 M/s. Task 9 adds the doc note + a pin test (restarted node serves a below-boot-frontier NAK from the journal) and the decision is closed.
7. **ultima_db `commit_version` (found by the seam survey):** `InstallOptions::commit_version` exists but is ignored (install lands at `latest_version + 1`). Task 2 implements it in the sibling repo (small additive change + tests) so `install_snapshot` can land the store at exactly position S — the "snapshot version = position" lockstep requires it. Positions are sparse u64s; Task 2 verifies `begin_write(Some(v))` accepts gapped versions (it takes an arbitrary u64 today; the adapter's doc records the invariant v = frame position, strictly increasing).
8. **Snapshot artifact = a file, node-readable:** `instance_dir/snapshots/snap-<S>.ultsnap` (temp + rename, retention keep-2). The node ships that file over the wire for sessions; the same-host service installs from it directly. One artifact, both consumers.
9. **Wipe-and-rejoin owns NoCommonPrefix** (not a bespoke snapshot-install protocol at the consensus layer): the SM emits `Action::WipeAndRejoin`; the node truncates ALL local log state (archive `truncate_all` path + prime(0) + fresh term-map adoption via the normal reconcile that follows), then ordinary catch-up (NAK-replay or snapshot session) does the rest. Consensus stays small; the data plane already knows how to fill an empty follower.
10. **In-memory SMs and purge:** `RegisterSm` implements `SnapshotStateMachine` (trivially — it is a few words of state) so the L3 harness exercises the REAL purge/snapshot path; `CountSm`-style test SMs that don't implement it get `ServiceError::SnapshotRequired` fail-stop when a gap demands a snapshot, with a message naming the contract.

---

### Task 1: Floor primitives — archive purge, gap-aware replay, TailReader scan_from

Everything below-the-floor rests on these. Makes `PositionPurged` real (reachable + correctly bounded) and closes the M5-carry perf minor (replay re-reading the whole journal each pass).

**Files:**
- Modify: `uc2_log/src/archive.rs`
- Modify: `ultima_journal/src/journal/tail_reader.rs`
- Modify: `uc2_service/src/replay.rs` (use `scan_from`; the gap GUARD itself is Task 5)
- Tests: inline in both files

**Interfaces:**
- Consumes: `Journal::purge_before(seq)` (mod.rs:563 — drops whole non-active segments ≤ seq, never the active one, no intent file), `find_block(journal, pos) -> Option<(seq, base)>` (archive.rs:368), `Archive { journal, durable_pos, next_block_seq, .. }`.
- Produces:

```rust
// uc2_log/src/archive.rs
impl Archive {
    /// Lowest position still replayable from this archive: base of the first
    /// retained block; == durable_pos when empty. Task 4's purge driver and
    /// Task 5's gap guard both read this.
    pub fn first_base(&self) -> u64;
    /// Purge journal blocks strictly below the block COVERING `pos` (the
    /// covering block is retained — a replay at `pos` must succeed after).
    /// No-op if pos <= first_base or no block covers pos. Returns the new
    /// first_base. Never touches blocks >= pos; Journal::purge_before's
    /// active-segment guard is extra slack on top.
    pub fn purge_below(&mut self, pos: u64) -> Result<u64, ArchiveError>;
}
```

```rust
// ultima_journal/src/journal/tail_reader.rs
impl TailReader {
    /// Like scan(), but skips whole segment FILES whose records all carry
    /// meta+payload ends <= start_meta (checked via each segment's first
    /// record meta vs the NEXT segment's first record meta — O(#segments)
    /// probes, not O(bytes)). `visit` still receives every record of the
    /// first relevant segment onward; callers keep their own per-record
    /// skip. scan(v) == scan_from(0, v).
    pub fn scan_from(&self, start_meta: u64,
        visit: impl FnMut(u64, u64, &[u8]) -> bool) -> Result<(), JournalError>;
    /// meta of the first readable record (the archive's first block base),
    /// None if the journal is empty/fully purged. Concurrent-safe like scan.
    pub fn first_meta(&self) -> Result<Option<u64>, JournalError>;
}
```

- `replay_into` switches its full `scan` to `scan_from(start_pos, ..)` (behavior identical, cost proportional to the tail — closes the O(journal)-per-overrun M5 carry). The gap guard lands in Task 5; here just the plumbing.

- [ ] **Step 1: failing archive tests** (`uc2_log/src/archive.rs` tests — reuse the existing `archive_with_one_block`-style helpers, extend to multi-block):

```rust
#[test]
fn purge_below_keeps_covering_block_and_replay_at_pos_succeeds() {
    let (mut archive, _buffer, frames) = archive_with_blocks(4); // helper: 4 blocks, ~4 frames each, fsynced
    let cut = frames[9]; // a frame inside block 2
    let new_first = archive.purge_below(cut).unwrap();
    assert!(new_first <= cut, "covering block retained");
    assert_eq!(archive.first_base(), new_first);
    // replay at the cut still works…
    let mut r = archive.replay_from(cut).unwrap();
    assert!(r.next().unwrap().is_some());
    // …and replay BELOW the new floor is PositionPurged with correct bounds
    match archive.replay_from(new_first.saturating_sub(1)) {
        Err(ArchiveError::PositionPurged { pos, first_base }) => {
            assert_eq!(first_base, new_first);
            assert!(pos < first_base);
        }
        other => panic!("expected PositionPurged, got {other:?}"),
    }
}

#[test]
fn purge_below_is_noop_at_or_below_floor_and_survives_reopen() {
    let (mut archive, _b, frames) = archive_with_blocks(4);
    let cut = frames[9];
    let first = archive.purge_below(cut).unwrap();
    assert_eq!(archive.purge_below(first).unwrap(), first, "no-op at floor");
    let dir = archive_dir(&archive); // helper: cfg dir accessor
    drop(archive);
    let re = Archive::open(ArchiveConfig::new(&dir)).unwrap();
    assert_eq!(re.first_base(), first, "floor recovered from journal.first_seq");
    assert_eq!(re.recovered_position(), frames_end(&frames), "frontier untouched by purge");
}
```

(`Archive::open` must recover `first_base` from `journal.first_seq()`'s block meta — a purged-then-reopened archive keeps correct bounds.)
- [ ] **Step 2: run** `cargo test -p uc2_log purge_below` → FAIL (methods missing).
- [ ] **Step 3: implement** `first_base` (recovered at `open` from the first block's meta; maintained by `truncate_to`'s first-block arm and `purge_below`) and `purge_below` (`find_block(pos)` → covering seq → `journal.purge_before(seq.saturating_sub(1))` → recompute `first_base` from `journal.first_seq()`'s meta). Update `replay_from`/`truncate_to`'s `PositionPurged{first_base}` fields to use the tracked value (as-built they pass `durable_pos` in one arm — fix to the real floor).
- [ ] **Step 4: failing TailReader tests** (`tail_reader.rs` tests):

```rust
#[test]
fn scan_from_skips_leading_segments_but_yields_the_covering_one() {
    let dir = tempfile::tempdir().unwrap();
    // tiny segments so multiple files exist: ~8 records/segment
    let j = Journal::open(small_segment_config(dir.path())).unwrap();
    for s in 0..40 { j.append(s, s * 100, &[7u8; 64]).unwrap().wait().unwrap(); }
    let r = TailReader::open(dir.path()).unwrap();
    let mut first_seen = None;
    r.scan_from(2_500, |seq, meta, _| { first_seen.get_or_insert((seq, meta)); true }).unwrap();
    let (seq, meta) = first_seen.unwrap();
    assert!(meta <= 2_500, "covering record yielded, not skipped");
    assert!(seq >= 8, "at least one leading segment file was skipped entirely");
    assert_eq!(r.first_meta().unwrap(), Some(0));
}
```

- [ ] **Step 5: run** → FAIL; implement `scan_from` (probe each segment's first record meta; skip file i when file i+1's first meta ≤ start_meta) + `first_meta`. `scan` delegates to `scan_from(0, ..)`.
- [ ] **Step 6:** switch `uc2_service::replay::replay_into` to `scan_from(start_pos, ..)` — no behavior change; existing reconstruction tests stay green and are the regression net.
- [ ] **Step 7:** note-and-close (no code): the sketch's "M1-triaged `blocks_recorded` rename" is already as-built — the field is `next_block_seq`, `blocks_recorded()` is the public accessor; record the closure in the commit message, rename nothing.
- [ ] **Step 8: verify** — `cargo test -p ultima-journal -p uc2_log -p uc2_service`, both clippy gates.
- [ ] **Step 9: commit** — `git add uc2_log/src/archive.rs ultima_journal/src/journal/tail_reader.rs uc2_service/src/replay.rs && git commit -m "feat(uc2): archive purge_below + first_base tracking; TailReader scan_from — PositionPurged becomes real with correct bounds (M6 floor primitives)"`

### Task 2: SnapshotStateMachine capability + ultima_db StoreStateMachine adapter

The v1 snapshot contract, grown onto the v2 trait as an OPTIONAL capability trait (existing SMs untouched), plus the reference adapter and the sibling-repo `commit_version` fix it needs.

**Files:**
- Modify: `uc2_service/src/traits.rs`, `uc2_service/src/config.rs` (`SnapshotError` re-export shape), `uc2_service/Cargo.toml` (feature `ultima_db`, optional dep `ultima-db` workspace)
- Create: `uc2_service/src/ultima_db.rs`
- Modify (SIBLING REPO — shared checkout, additive commit only): `/home/claude/ultima/ultima_db/src/snapshot_stream/install.rs`
- Modify: `uc-lincheck/src/register.rs` (feature `v2`: RegisterSm implements the capability)
- Tests: inline + `uc2_service/tests/snapshot_roundtrip.rs`

**Interfaces:**
- Produces (`uc2_service::traits`):

```rust
/// Optional capability: SMs that can serialize/restore their full state.
/// Purge is gated on it — a deployment without it never purges (documented).
pub trait SnapshotStateMachine: StateMachine {
    type SnapshotHandle: Send + 'static;
    /// O(1) consistent pin of current state; returns (handle, position) where
    /// position == last_applied().unwrap_or(0) at pin time. Called on the
    /// APPLY thread (SM lock held) so the position cannot move underneath.
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError>;
    /// Stream the pinned state; runs OFF-thread, no SM lock (v1 rule).
    fn stream_snapshot(handle: Self::SnapshotHandle, dst: &mut dyn std::io::Write)
        -> Result<(), SnapshotError>;
    /// Replace state wholesale; returns the post-install position (== the S
    /// the artifact was tagged with). Runs on the apply thread, SM lock held.
    fn install_snapshot(&mut self, src: &mut dyn std::io::Read)
        -> Result<u64, SnapshotError>;
}
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io: {0}")] Io(#[from] std::io::Error),
    #[error("codec: {0}")] Codec(String),
}
```

- Produces (`uc2_service::ultima_db`, feature `ultima_db`): `StoreStateMachine<C, R, Q, QR>` mirroring v1's shape (uc_service/src/ultima_db/store_state_machine.rs — apply pins `store.begin_write(Some(position))`, `last_applied` = `latest_version()` with 0 ⇒ None, `freeze` = `(store.snapshot_stream(Some(v))?, v)`, `stream_snapshot` = `io::copy`, `install_snapshot` = `install_snapshot_stream(src, InstallOptions { commit_version: Some(S_from_stream_trailer…) })` — see the sibling change below) with `ApplyFn`/`QueryFn` boxed closures exactly as v1. Doc comment states the **position-as-version invariant**: versions are sparse strictly-increasing u64s (byte positions); Task 2 verifies `begin_write(Some(v))` accepts gaps (it does — add the pin test) and documents it in the adapter.
- Produces (sibling `ultima_db`): `InstallOptions::commit_version = Some(v)` honored — the installed snapshot lands at exactly version `v` instead of `latest_version + 1` (install.rs:64-70 currently ignores it). Additive change + unit tests in ultima_db's own suite; validate `v > latest_version` else `SnapshotStreamError::InvalidPayload`-class error. **Commit it in the sibling repo on its CURRENT branch (no tree-switching — shared checkout).**
- Produces (`uc-lincheck`, feature `v2`): `impl SnapshotStateMachine for RegisterSm` — `SnapshotHandle = Vec<u8>` (bincode of `(value, last_applied)`), freeze/stream/install trivial. This is what lets the L3 harness drive the REAL purge path.

- [ ] **Step 1: failing sibling test first** (`ultima_db/src/snapshot_stream/install.rs` tests): install with `commit_version: Some(4242)` over a store at version 7 → `latest_version() == 4242`; install with `Some(3)` over version 7 → error. Run `cargo test -p ultima-db snapshot_stream` in the sibling → FAIL → implement → PASS. Commit there: `feat(snapshot_stream): honor InstallOptions::commit_version (SMR position-as-version hook)`.
- [ ] **Step 2: failing adapter roundtrip** (`uc2_service/tests/snapshot_roundtrip.rs`):

```rust
#[test]
fn store_sm_freeze_stream_install_roundtrip_at_position() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = kv_store_sm(dir.path().join("a")); // helper: StoreStateMachine with a put/get ApplyFn
    // positions are sparse byte positions — apply at 96, 192, 4096
    for (pos, k, v) in [(96, "a", 1u64), (192, "b", 2), (4096, "c", 3)] {
        sm.apply(pos, Cmd::Put(k.into(), v));
    }
    assert_eq!(sm.last_applied(), Some(4096));
    let (handle, s) = sm.freeze().unwrap();
    assert_eq!(s, 4096);
    let mut buf = Vec::new();
    StoreStateMachine::stream_snapshot(handle, &mut buf).unwrap();
    let mut fresh = kv_store_sm(dir.path().join("b"));
    let installed = fresh.install_snapshot(&mut buf.as_slice()).unwrap();
    assert_eq!(installed, 4096);
    assert_eq!(fresh.last_applied(), Some(4096), "position-as-version lockstep");
    assert_eq!(fresh.query(Query::Get("c".into())), Some(3));
}
```

- [ ] **Step 3: run** → FAIL (trait/adapter missing); implement the trait, the adapter (feature-gated), and the RegisterSm impl + its own small roundtrip unit in uc-lincheck.
- [ ] **Step 4: sparse-version pin** — a `#[test]` in the adapter proving `begin_write(Some(v))` with gapped v works (apply at 96 then 4096, read back both keys at latest).
- [ ] **Step 5: verify** — `cargo test -p uc2_service -p uc-lincheck --features v2` (lincheck) + sibling `cargo test -p ultima-db`, both clippy gates (workspace + all-targets set).
- [ ] **Step 6: commit** (ultima_cluster side) — `git add uc2_service/src/traits.rs uc2_service/src/ultima_db.rs uc2_service/src/lib.rs uc2_service/src/config.rs uc2_service/Cargo.toml uc-lincheck/src/register.rs uc2_service/tests/snapshot_roundtrip.rs Cargo.lock && git commit -m "feat(uc2_service): SnapshotStateMachine capability + ultima_db StoreStateMachine adapter (position-as-version, v1 u64-return rule)"`

### Task 3: Service-side snapshot builder — files, policy, cnc publish

**Files:**
- Create: `uc2_service/src/snapshots.rs`, `uc2_service/src/builder_agent.rs`
- Modify: `uc2_service/src/lib.rs` (builder spawn for snapshot-capable SMs), `uc2_service/src/config.rs` (`SnapshotPolicy`), `uc2_service/src/apply.rs` (freeze hook on the apply thread)
- Modify: `uc_protocol/src/v2/cnc.rs` + `uc2_log/src/cnc.rs` (ONE new slot now; the rest of the 1152.. band is Task 9)
- Tests: `uc2_service/tests/snapshot_build.rs` + inline

**Interfaces:**
- cnc: `CNC_OFF_SERVICE_SNAPSHOT_POS: usize = 1152` — "writer: service snapshot builder thread; position S of the newest COMPLETE on-disk snapshot; 0 = none". New `#[repr(C)] SnapshotSlots { service_snapshot_pos: PaddedAtomicU64, node_snapshot_floor: PaddedAtomicU64 /* 1216, writer: consensus (Task 4 mirror) */ }`, `CncPage::snapshots() -> &SnapshotSlots`, offset-pin tests extended (the two-crates-never-drift discipline).
- `uc2_service::snapshots`:

```rust
pub struct SnapshotStore { dir: PathBuf } // instance_dir/snapshots
impl SnapshotStore {
    pub fn open(instance_dir: &Path) -> io::Result<SnapshotStore>; // mkdir -p
    pub fn path_for(&self, pos: u64) -> PathBuf;                   // snap-<pos>.ultsnap
    /// newest complete snapshot with position <= at (or any if at == u64::MAX)
    pub fn newest(&self, at_most: u64) -> io::Result<Option<(u64, PathBuf)>>;
    /// write via temp file + fsync + rename (atomic completeness), then
    /// retention: keep the newest 2, unlink older.
    pub fn publish(&self, pos: u64, write: impl FnOnce(&mut dyn Write) -> Result<(), SnapshotError>)
        -> Result<PathBuf, SnapshotError>;
}
```

- `SnapshotPolicy { interval_bytes: u64 /* default 64 MiB; 0 = never */ }` on `ServiceConfig` (builder-pattern setter, default never — purge-off-by-default starts here: no snapshots, no marker, no purge).
- Builder flow (split across the two threads to honor the lock rules): the APPLY thread, once per cycle, checks `service_applied - last_snapshot_pos >= interval_bytes` and — holding the SM lock it already holds — calls `freeze()` and hands `(handle, s)` over a 1-slot channel to the BUILDER thread; the builder streams to `SnapshotStore::publish(s, ..)` off-lock, then `cnc.snapshots().service_snapshot_pos.store_release(s)`. One in-flight build max (the slot); a failed build logs + drops (next interval retries). `Service::stop` joins the builder; `crash` doesn't.

- [ ] **Step 1: failing e2e build test** (`uc2_service/tests/snapshot_build.rs`, single node + RegisterSm-with-snapshots, the M5 test-harness helpers):

```rust
#[test]
fn builder_publishes_position_tagged_snapshot_and_cnc_marker() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path(), "snapb");
    wait_until(|| node.can_serve());
    let svc = ServiceBuilder::new(cfg_with_policy(dir.path(), "snapb", /*interval*/ 4 * 1024), RegisterSm::default())
        .start().unwrap();
    let client = Client::connect(dir.path(), "snapb").unwrap();
    for i in 0..400u64 { let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap(); } // >> 4 KiB of frames
    let cnc = open_cnc(dir.path(), "snapb");
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > 0);
    let s = cnc.snapshots().service_snapshot_pos.load_acquire();
    assert!(s <= cnc.service().service_applied.load_acquire(), "snapshot at an applied position");
    let store = SnapshotStore::open(dir.path()).unwrap();
    let (pos, path) = store.newest(u64::MAX).unwrap().expect("file exists");
    assert_eq!(pos, s);
    assert!(path.ends_with(format!("snap-{s}.ultsnap")));
    // a second interval produces a newer one and retention holds (<= 2 files)
    for i in 0..400u64 { let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap(); }
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > s);
    assert!(count_snapshots(dir.path()) <= 2);
    client.shutdown(); svc.stop(); node.stop();
}
```

- [ ] **Step 2: run** → FAIL; implement slots + SnapshotStore + builder wiring per Interfaces. cnc offset pin tests first (layout discipline), then behavior.
- [ ] **Step 3:** unit tests: `publish` is atomic (a torn temp file is never `newest`), retention unlinks, `newest(at_most)` picks correctly among {snap-100, snap-900}.
- [ ] **Step 4: verify** — `cargo test -p uc2_service -p uc2_log -p uc_protocol`, uc2_node regression, both clippy gates.
- [ ] **Step 5: commit** — `git add uc2_service/src/snapshots.rs uc2_service/src/builder_agent.rs uc2_service/src/lib.rs uc2_service/src/config.rs uc2_service/src/apply.rs uc_protocol/src/v2/cnc.rs uc2_log/src/cnc.rs uc2_service/tests/snapshot_build.rs && git commit -m "feat(uc2_service): snapshot builder — freeze on apply thread, stream off-lock, atomic position-tagged files, cnc marker"`

### Task 4: Node marker persistence + purge driver

**Files:**
- Modify: `uc2_log/src/state.rs` (NodeState + `snapshot.state`), `uc2_node/src/node.rs` (sampling, persist, mirror, purge command), `uc2_node/src/lib.rs` (config re-export)
- Tests: `uc2_node/tests/purge_safety.rs` (first scenarios) + inline

**Interfaces:**
- `NodeState` gains `snapshot: StableValue<u64>` (`state/snapshot.state`), accessor `snapshot_floor() -> u64`, durable `store_snapshot_floor(v)` — the exact `output_progress` pattern including the **increase-only guard** (the M5 review's marker-clobber lesson: a fresh page's 0 must never regress the durable value).
- `NodeConfig` gains `purge: PurgePolicy` — `#[derive(Clone, Copy, Default)] pub enum PurgePolicy { #[default] Disabled, BelowSnapshot { slack_bytes: u64 } }` (slack: purge below `marker - slack`, default suggestion 64 MiB in docs; env override `UC2_PURGE_SLACK_BYTES` NOT added — config only, gates configure it explicitly).
- Consensus duty cycle (new step 8, after `maybe_persist_output_progress`): sample `cnc.snapshots().service_snapshot_pos`; if it advanced AND `<= durable`: `state.store_snapshot_floor(v)` (durable) then mirror `cnc.snapshots().node_snapshot_floor.store_release(v)` (100 ms floor + increase-only, both copied from the output_progress persister). If `PurgePolicy::BelowSnapshot{slack}` and `floor.saturating_sub(slack) > archive_first_base_mirror`: send `ArchiveCmd::Purge { below: floor - slack }`.
- The archive command channel becomes `mpsc::sync_channel::<ArchiveCmd>(64)`:

```rust
pub(crate) enum ArchiveCmd {
    Truncate { epoch: u64, to: u64 },   // existing semantics verbatim
    Purge { below: u64 },               // archive.purge_below(below); NO ack needed
}
```

Archive agent: `Purge` runs `archive.purge_below(below)` (errors log-warn + drop — purge is best-effort by design; a failed purge retries next interval), then publishes the new floor into a node-internal `Arc<AtomicU64>` (`archive_first_base`) the consensus agent reads for the guard above and Task 9 mirrors to cnc.

- [ ] **Step 1: failing node test** (`uc2_node/tests/purge_safety.rs`):

```rust
#[test]
fn marker_persists_and_purge_advances_only_below_it() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node_with(dir.path(), "purge1", PurgePolicy::BelowSnapshot { slack_bytes: 0 },
                                      /*tiny segments so purge is observable*/ small_journal_cfg());
    wait_until(|| node.can_serve());
    drive_commits(&node, 6000); // helper: admission-paced submits, ~6000 frames
    let cnc = open_cnc(dir.path(), "purge1");
    // no service snapshot yet -> no purge ever
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(node.archive_first_base(), 0, "purge gated on the marker");
    // simulate the service publishing a snapshot at an applied position
    let s = pick_frame_boundary(&node, 4000); // helper via read_frame_validated walk
    write_fake_snapshot(dir.path(), s);       // SnapshotStore::publish with dummy bytes
    cnc.snapshots().service_snapshot_pos.store_release(s);
    wait_until(|| node.archive_first_base() > 0);
    assert!(node.archive_first_base() <= s, "never purged at/above the marker");
    // durable marker survives restart (increase-only vs fresh page 0)
    let floor0 = cnc.snapshots().node_snapshot_floor.load_acquire();
    let node = restart_node(node, dir.path());
    let cnc2 = open_cnc(dir.path(), "purge1");
    wait_until(|| cnc2.snapshots().node_snapshot_floor.load_acquire() == floor0);
    node.stop();
}
```

- [ ] **Step 2: run** → FAIL; implement per Interfaces (`Node::archive_first_base()` test accessor included). The restart leg is RED-first against a persister missing the increase-only guard — implement WITH the guard from the start but keep the assertion (it pins the M5 lesson).
- [ ] **Step 3:** marker-validation unit: `service_snapshot_pos > durable` is ignored (never persisted) — a torn/insane service value can't poison the floor.
- [ ] **Step 4: verify** — `cargo test -p uc2_node -p uc2_log`, failover ×2 (regression — purge default-off must change nothing), both clippy gates.
- [ ] **Step 5: commit** — `git add uc2_log/src/state.rs uc2_node/src/node.rs uc2_node/src/lib.rs uc2_node/tests/purge_safety.rs && git commit -m "feat(uc2_node): durable snapshot floor (increase-only) + policy-gated purge driver via ArchiveCmd (default OFF)"`

### Task 5: Service reconstruction below the floor — gap guard + snapshot install + tail replay

The `PositionPurged` class, closed end-to-end on one host.

**Files:**
- Modify: `uc2_service/src/replay.rs` (gap guard + install path), `uc2_service/src/apply.rs` (wiring), `uc2_service/src/config.rs` (`ServiceError::SnapshotRequired`), `uc2_service/src/attach.rs` (doc: drift check unchanged — durable is still the right bound)
- Tests: `uc2_service/tests/reconstruction.rs` (extend)

**Interfaces:**
- `replay_into` grows a pre-scan guard: `let first = TailReader::open(dir)?.first_meta()?;` — if `first.unwrap_or(0) > needed_start` (where `needed_start = sm.last_applied().map(|p| p + 1-frame… use the position itself — idempotent-skip tolerates overlap, so the guard is `first > sm.last_applied().unwrap_or(0)` compared as positions with the convention that a fresh SM needs 0):
  - SM implements `SnapshotStateMachine` → `SnapshotStore::newest(min(commit, durable))` and require the returned `S >= first_needed` (else no covering artifact → the SnapshotRequired arm); open file, `sm.install_snapshot(&mut file)?` (apply thread, SM lock held — the M5 lock story unchanged), assert returned `== S`, continue the normal block walk from S.
  - No snapshot capability or no covering file → `Err(ServiceError::SnapshotRequired { needed: u64, first_available: u64 })` → the apply agent FAIL-STOPS with a message naming the contract ("journal purged below the service frontier and the SM cannot install snapshots").
- The install is keyed to the artifact's tag S; after install `sm.last_applied() == Some(S)` and the tail walk's idempotent-skip makes any overlap harmless (same argument as M5 replay).

- [ ] **Step 1: failing test** (`uc2_service/tests/reconstruction.rs`):

```rust
#[test]
fn fresh_service_below_purge_floor_installs_snapshot_then_tail_replays() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node_with(dir.path(), "rec2", PurgePolicy::BelowSnapshot { slack_bytes: 0 },
                                      small_journal_cfg());
    wait_until(|| node.can_serve());
    // service #1 (snapshotting RegisterSm) runs, builds a snapshot, then more traffic commits
    let svc = ServiceBuilder::new(cfg_with_policy(dir.path(), "rec2", 4 * 1024), RegisterSm::default()).start().unwrap();
    let client = Client::connect(dir.path(), "rec2").unwrap();
    for i in 1..=800u64 { let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap(); }
    let cnc = open_cnc(dir.path(), "rec2");
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > 0);
    wait_until(|| node.archive_first_base() > 0); // purge actually happened
    svc.crash();
    for i in 801..=1000u64 { let _: CmdResp = write_submit_retrying(dir.path(), &Cmd::Write(i)); } // node-only commits
    // service #2: FRESH RegisterSm; journal no longer starts at 0
    let svc2 = ServiceBuilder::new(cfg(dir.path(), "rec2"), RegisterSm::default()).start().unwrap();
    wait_until(|| cnc.service().service_applied.load_acquire()
                   >= cnc.counters().commit.load_acquire().min(cnc.counters().durable.load_acquire()));
    let got: Option<u64> = Client::connect(dir.path(), "rec2").unwrap().query_linearizable(&()).unwrap();
    assert_eq!(got, Some(1000), "state == snapshot prefix + tail, exactly once");
    svc2.stop(); node.stop();
}

#[test]
fn gap_without_snapshot_capability_fails_stop_with_named_contract() {
    /* same setup but service #2 = CountSm (no SnapshotStateMachine); assert the
       apply agent dies with a panic whose message contains "SnapshotRequired"
       within 5s (catch via Service handle join error — crash()-free stop path). */
}
```

- [ ] **Step 2: run** → FAIL; implement guard + install + fail-stop per Interfaces.
- [ ] **Step 3:** the SILENT-GAP regression pin (the bug class this task exists for): construct the pre-guard behavior in a unit — journal whose first block base is 500, fresh SM, WITHOUT the guard replay would apply from 500 and "succeed" with wrong state; the test asserts the guard turns that into install-or-SnapshotRequired, never a silent partial replay.
- [ ] **Step 4: verify** — `cargo test -p uc2_service -p uc2_node`, both clippy gates.
- [ ] **Step 5: commit** — `git add uc2_service/src/replay.rs uc2_service/src/apply.rs uc2_service/src/config.rs uc2_service/src/attach.rs uc2_service/tests/reconstruction.rs && git commit -m "feat(uc2_service): below-floor reconstruction — gap guard, snapshot install on the apply thread, tail replay; SnapshotRequired fail-stop (kills the silent-gap class)"`

### Task 6: Wire snapshot session — kinds 12–15, sender session, receiver intake

One file-transfer mini-protocol, MDC-free (unicast to ONE peer), NAK-repaired like the main stream, strictly budget-bounded.

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (kinds + bodies + literal pins), `uc2_net/src/sender.rs`, `uc2_net/src/receiver.rs`, `uc2_node/src/node.rs` (orchestration + snapshot-file resolution)
- Tests: `uc2_net/tests/snapshot_session.rs` (loopback pair + fault layer) + inline

**Interfaces:**
- Produces (`uc_protocol::v2::datagram` — next free kind is 12; header `position` field carries the SNAPSHOT-FILE OFFSET for chunk kinds, session id rides the body):

```rust
pub const DGRAM_KIND_SNAP_BEGIN: u8 = 12;  // leader→peer; body below; position = 0
pub const DGRAM_KIND_SNAP_CHUNK: u8 = 13;  // leader→peer; position = file offset; payload = bytes
pub const DGRAM_KIND_SNAP_NAK:   u8 = 14;  // peer→leader; body = SnapNakBody
pub const DGRAM_KIND_SNAP_DONE:  u8 = 15;  // peer→leader; body = SnapBeginBody (echo = ack)
pub const SNAP_BEGIN_BODY_LEN: usize = 24;
pub struct SnapBeginBody { pub session: u32, pub snapshot_pos: u64, pub total_len: u64 }
// LE: session 0..4, 4..8 zero, snapshot_pos 8..16, total_len 16..24
pub const SNAP_NAK_BODY_LEN: usize = 16;
pub struct SnapNakBody { pub session: u32, pub offset: u64, pub length: u32 }
pub fn write_snap_begin_body(..); pub fn read_snap_begin_body(..) -> Option<..>; // + nak pair
```

- Sender session state (one active at a time — a second requester waits; sessions are rare by construction):

```rust
struct SnapSession { peer: SocketAddr, session: u32, snapshot_pos: u64,
                     file: std::fs::File, total_len: u64, cursor: u64,
                     naks: VecDeque<(u64, u32)>, done: bool, opened_ns: u64 }
```

Duty-cycle arbitration (Decision #1, the exact order inside `Sender::do_work`): live fan-out (existing) → NAK-replay (existing budget 8) → `SNAP_DGRAMS_PER_CYCLE = 4` chunk datagrams (serve session NAKs first, then the cursor). Chunk payload = `MTU - header` bytes from the file at the offset. `SNAP_DONE` (or a 30 s no-NAK-no-DONE timeout) closes the session. `SessionStats { snap_sessions, snap_bytes, snap_chunk_naks }` on `SenderStats`.
- Trigger seam: `serve_nak` — when `serve_nak_from_journal` cannot serve because `pos < archive first_base` (today's `overruns` bucket), the sender consults a new `snapshot_source: Option<Arc<dyn Fn() -> Option<(u64, PathBuf, u64)> + Send + Sync>>` (node-provided closure: newest durable snapshot `(pos, path, len)` with `pos >= needed`… any `pos > naked position` works — the peer's gap is below it either way) and opens a session INSTEAD of counting `overruns`. Node wires the closure to `SnapshotStore::newest` + the durable floor (only sessions from snapshots the node has PERSISTED as its floor marker — never a half-written file; `SnapshotStore::publish`'s rename-atomicity plus the marker check make this airtight).
- Receiver intake: kinds 12–15 handled in the receiver's datagram demux (NOT routed to consensus — data-plane, like DATA): `SNAP_BEGIN` opens `instance_dir/snapshots/incoming-<pos>.part` (pre-sized `set_len(total_len)`), tracks a contiguity cursor + gap list (same shape as the stream's `Rebuilt` tracking, reused if the type fits); gaps past a randomized ~1 RTT delay send `SNAP_NAK`; on completion: fsync, rename to `snap-<pos>.ultsnap`, send `SNAP_DONE`, and publish `cnc.snapshots().service_snapshot_pos`… **no** — the RECEIVER-side node writes a separate node-written slot: `incoming_snapshot_pos` (add to `SnapshotSlots` at 1280, writer: receiver agent) so the single-writer rule holds (`service_snapshot_pos` belongs to the service). The service's gap-guard path (Task 5) consults `SnapshotStore::newest` — the renamed file is simply THERE; no extra signaling needed beyond the file itself. `incoming_snapshot_pos` is observability.
- Receiver also primes: after rename, if `counters.durable < pos` the node's log/archive know nothing of `0..pos` — the receiving node's archive must adopt the floor: `ArchiveCmd::AdoptFloor { pos }` (new variant added to Task 4's enum here) → archive treats `pos` as its new base when EMPTY (prime `next_block_seq`/`durable_pos` = pos… exact rule: only legal when `durable_pos <= pos` and journal empty-or-below; the consensus agent issues it when it observes the completed file while its own durable < pos — the learner-join case; a mid-life follower that already has data ≥ pos ignores it). Document: adopting the floor advances `durable` WITHOUT bytes — safe because the snapshot file IS the state below pos, and the term map from the leader covers position provenance (reconcile runs as usual).

- [ ] **Step 1: wire pins** — literal-LE byte tests for both bodies + short-length rejection + kind-code stability extension (12–15), the kind-7/8/9 style. Run → FAIL → implement codecs.
- [ ] **Step 2: failing session test** (`uc2_net/tests/snapshot_session.rs`, loopback sender+receiver harness in the existing `common/mod.rs` style):

```rust
#[test]
fn below_floor_nak_upgrades_to_snapshot_session_and_file_transfers_exactly() {
    // sender with a journal whose first_base = 64 KiB (purged prefix) and a
    // 300 KiB snapshot file at pos 64 KiB; receiver NAKs position 0.
    let (mut sender, mut receiver, dirs) = session_pair_with_purged_prefix(300 * 1024);
    pump_until(&mut sender, &mut receiver, |r| r.completed_snapshot().is_some(), MAX_PUMPS);
    let (pos, path) = receiver_completed(&dirs);
    assert_eq!(pos, 64 * 1024);
    assert_eq!(sha256(&path), sha256(sender_snapshot_path(&dirs)), "byte-identical");
    assert_eq!(sender.stats().overruns.load(Relaxed), 0, "upgraded, not dropped");
    assert!(sender.stats().snap_sessions.load(Relaxed) == 1);
}

#[test]
fn snapshot_session_survives_chunk_loss_via_snap_nak() {
    // same, with FaultConfig{drop_per_million: 200_000} on the session direction;
    // completion still reached; snap_chunk_naks > 0 proves the repair path ran.
}

#[test]
fn live_stream_is_never_starved_by_a_session() {
    // run a session concurrently with live appends; assert live DATA datagrams
    // per pump-cycle never drop below the no-session baseline minus the fixed
    // snapshot budget (the arbitration order pins this structurally: live is
    // served first, sessions get at most SNAP_DGRAMS_PER_CYCLE).
}
```

- [ ] **Step 3: run** → FAIL; implement sender session + receiver intake + node closure wiring per Interfaces.
- [ ] **Step 4:** `AdoptFloor` unit on the archive (legal/illegal cases) + the receiver-side rename-atomicity unit (a torn `.part` is never adopted).
- [ ] **Step 5: verify** — `cargo test -p uc_protocol -p uc2_net -p uc2_log -p uc2_node`, failover ×2, both clippy gates.
- [ ] **Step 6: commit** — `git add uc_protocol/src/v2/datagram.rs uc2_net/src/sender.rs uc2_net/src/receiver.rs uc2_net/tests/snapshot_session.rs uc2_log/src/archive.rs uc_protocol/src/v2/cnc.rs uc2_log/src/cnc.rs uc2_node/src/node.rs && git commit -m "feat(uc2_net): snapshot sessions — kinds 12-15, budget-bounded unicast file ship, NAK-repaired, below-floor NAKs upgrade instead of dropping"`

### Task 7: Learner role — fan-out yes, safety quantities no

**Files:**
- Modify: `uc2_node/src/node.rs` (config, wiring, probe targeting), `uc2_net/src/flow.rs` (voting-only construction), `uc2_net/src/sender.rs` (constructor takes voters+learners), `uc2_consensus/src/election.rs` (only if a helper is needed — members stays voters-only BY CONSTRUCTION; document)
- Tests: `uc2_node/tests/learner.rs` + flow/sender units

**Interfaces:**
- `NodeConfig` gains `learners: Vec<(NodeId, SocketAddr)>` (default empty; ids must not collide with `members`). A node whose OWN id is in `learners` boots in learner mode: consensus agent runs with candidacy disabled (`ElectionConfig` gains `can_vote: bool`; a learner's SM never fires the election timeout and never answers RequestVote — grants require `can_vote`; everything else — term adoption, gossip intake, commit clamp, truncation, reconcile — is the ordinary follower path unchanged).
- Leader-side split (Decision #3):
  - `Sender::new(...)` fan-out list = voters-minus-self ++ learners (streamed identically).
  - `FlowControl::new(voting_followers: &[SocketAddr], cluster_size, initial_window)` — learner STATUS messages are accepted for observability but excluded from `limit()` (store them in a parallel list; `on_status` routes by membership). The as-built assert (`cluster_size > followers.len()`) moves to voters only.
  - `CommitTracker`/`ElectionSm.members`: untouched — learners are simply never in `members`, and `follower_slot` returns None for them (their AppendPosition reports are observability-only: route to a per-learner `learner_durable` cell, never to `tracker.on_durable`).
  - READ_PROBE targeting: the probe loop iterates VOTING peers only; ack counting re-checks `from ∈ voting members` (belt-and-suspenders — the constraint block).
- Learner liveness: the learner's own service works normally (applies to commit, snapshots, queries answer with `expected_epoch` semantics — linearizable reads on a learner host return NOT_LEADER, snapshot reads work).

- [ ] **Step 1: failing units** — `flow.rs`: learner STATUS never changes `limit()` (two voters + one learner; learner adverts the max — limit still the voters' quorum statistic). `commit.rs`-adjacent node unit: learner AppendPosition never advances commit (leader with 2 voters down + learner reporting sky-high durable → commit frozen — the phantom-commit-via-learner hole, pinned shut).
- [ ] **Step 2: run** → FAIL; implement the splits.
- [ ] **Step 3: failing cluster test** (`uc2_node/tests/learner.rs`, failover-harness style — 3 voters + 1 learner on loopback):

```rust
#[test]
fn learner_replicates_live_and_never_disturbs_quorum() {
    let mut c = spawn_cluster_with_learner(3, 1);
    let leader = c.await_single_leader(30);
    c.drive_commits(leader, 2000);
    c.await_learner_caught_up(5); // learner durable == cluster commit
    // kill the learner mid-load: commit keeps advancing (no quorum coupling)
    let commit0 = c.commit(leader);
    c.kill_learner(0);
    c.drive_commits(leader, 1000);
    assert!(c.commit(leader) > commit0);
    // learner restart rejoins via NAK-replay (journal) without leader config change
    c.restart_learner(0);
    c.await_learner_caught_up(10);
    // elections: kill the leader; the learner must never become a candidate
    c.crash_node(leader);
    let new_leader = c.await_single_leader(30);
    assert!(c.is_voter(new_leader));
    assert_eq!(c.learner_terms_requested(0), 0, "learner never candidacies");
}
```

- [ ] **Step 4:** linearizable-read guard test: partition both voters away from the leader, leave the learner connected — a linearizable read must RETRY (the learner's ack, if any arrived, must not complete the quorum). Reuse the query_barrier harness.
- [ ] **Step 5: verify** — full uc2 suite, failover ×2, both clippy gates.
- [ ] **Step 6: commit** — `git add uc2_node/src/node.rs uc2_net/src/flow.rs uc2_net/src/sender.rs uc2_consensus/src/election.rs uc2_node/tests/learner.rs && git commit -m "feat(uc2): learner role — replicated-to, never counted (fan-out yes; vote/quorum/flow/read-quorum no)"`

### Task 8: Learner join e2e + NoCommonPrefix wipe-and-rejoin

The two "start from nothing" paths, composed from Tasks 6+7.

**Files:**
- Modify: `uc2_consensus/src/election.rs` (+ sim plumbing in `uc2_sim/src/world.rs`, `uc2_sim/tests/scenarios.rs`), `uc2_node/src/node.rs`
- Tests: `uc2_node/tests/learner.rs` (join scenario), `uc2_node/tests/purge_safety.rs` (wipe scenario), sim pins

**Interfaces:**
- `Reconcile::NoCommonPrefix` handling in `reconcile_term_map` (election.rs:613) becomes `Action::WipeAndRejoin` (replacing `Action::Fatal` for THIS case only — other Fatal sites stay). SM-side: entering wipe sets the same data-plane latch as truncation (allow-list {RequestVote, Vote, Truncated}) and reuses the epoch machinery: wipe IS `Action::Truncate { epoch, to: 0, new_map: adopt-leader-prefix-empty }` in spirit — **implement it as exactly that**: emit `Action::Truncate { epoch, to: 0, new_map: vec![] }` plus a `wipes` counter action tag, with the wipe-safety comment (Global Constraints) at the emit site. The node's existing truncate path (persist-map-before-truncate → archive `truncate_to(0)` → first-block arm → `truncate_all` → prime(0) → epoch'd ack → gate reopen after clean reconcile) already does everything; `truncate_to(0)`'s first-block arm is the M5 Task-2 machinery. After the wipe the node is an empty follower: live stream NAKs from 0 → journal replay if the leader has it, snapshot session if purged (Task 6).
- Learner join e2e = fresh learner instance dir + purged leader → session → AdoptFloor → tail via NAK-replay → live; its SERVICE installs the shipped snapshot via Task 5's gap guard.

- [ ] **Step 1: failing sim pins** (`uc2_sim/tests/scenarios.rs`): (a) a no-common-prefix world (follower crashed pre-gossip with divergent-term-only data, leader far ahead — build on the crash_on_next_truncate-style scripting) reaches convergence with `wipes == 1` and the oracle green across 200 seeds; (b) the counterfactual — wipe disabled (old Fatal) — the same world FAIL-STOPs (documents what changed). Mechanical epoch/action plumbing for the sim mirrors Task 1 of M5.
- [ ] **Step 2: run** → FAIL; implement the SM change + sim arms.
- [ ] **Step 3: failing node test** (`purge_safety.rs`): 3-node cluster; isolate n2 BEFORE its first commit lands, let it accept uncommitted divergent bytes from a dying term (the M4 contested-election harness pattern), advance the majority far + purge the leader (policy on, snapshot built); heal → n2 must wipe (truncations/wipes counter ≥1), receive the session, converge to cluster commit within 10 s, and the whole run's submits stay linearizable (drive through the lincheck history recorder if cheap, else assert committed-value convergence).
- [ ] **Step 4: failing learner-join e2e** (`learner.rs`): 3 voters, purge-enabled leader with ≥1 snapshot + purged prefix; START a learner with a FRESH dir under sustained load (drive_commits running); assert: session completes, learner's service reaches cluster commit within a bounded window, live-stream handoff observed (`snap_sessions == 1`, then NAK-replay/live only), and the leader's commit rate never stalls (sample commit deltas during the join; no zero-delta window > 500 ms — "no quorum stall").
- [ ] **Step 5: verify** — full workspace suite + sim-heavy release tier + failover ×2 + both clippy gates.
- [ ] **Step 6: commit** — `git add uc2_consensus/src/election.rs uc2_sim/src/world.rs uc2_sim/tests/scenarios.rs uc2_node/src/node.rs uc2_node/tests/learner.rs uc2_node/tests/purge_safety.rs && git commit -m "feat(uc2): NoCommonPrefix = wipe-and-rejoin (truncate-to-zero reuse, safety argument in-code); learner join e2e under load"`

### Task 9: Ops — per-follower cnc observability, straddle hardening, runbook

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs`, `uc2_log/src/cnc.rs`, `uc2_node/src/node.rs`, `uc2_net/src/sender.rs`, `uc2_net/src/receiver.rs`, `uc2_log/src/archive.rs` (generation cell)
- Create: `docs/ops/uc2-runbook.md`
- Tests: inline layout pins + the straddle regression + the prefill-decision pin

**Interfaces:**
- cnc band final layout (offsets pinned in `uc_protocol`, layout test in `uc2_log` — the never-drift discipline). Already used by earlier tasks: 1152 `service_snapshot_pos` (T3, writer: service builder), 1216 `node_snapshot_floor` (T4 mirror, writer: consensus), 1280 `incoming_snapshot_pos` (T6, writer: receiver). This task adds: `CNC_OFF_ARCHIVE_FIRST_BASE = 1344` (writer: consensus, mirrored from the archive agent's atomic) and the per-peer slots `CNC_OFF_PEER_SLOTS = 1408`, `CNC_PEER_SLOT_STRIDE = 256`, `CNC_MAX_PEER_SLOTS = 8` (1408 + 8×256 = 3456 < 4096; const-assert the bound). Per slot, four cache lines: `peer_id_and_role` (u64: id<<8 | role bits voter/learner; writer: consensus, boot-once), `reported_durable` (writer: consensus, from Report intake incl. the learner cell), `advertised_limit` (writer: sender, from STATUS), `naks_served_plus_replay` (writer: sender; packed u32/u32). `CncPage::peer_slot(i) -> &PeerSlot`. One offset-pin test covers the WHOLE 1152..3456 band in one place.
- Straddle hardening (the named M5-final-review residual): `LogCounters` gains a `prime_generation` cell (or a node-internal `Arc<AtomicU64>` bumped by the archive agent on every `prime()` — choose the node-internal atomic: no layout change, the receiver already shares node internals via constructor). Receiver DATA arm: capture generation before the gate check, re-check after computing the new append value, BEFORE `append.store_release`; mismatch → drop the datagram (it will be NAK'd; correctness restored by the re-primed stream). Regression test: a test-only hook (`#[cfg(test)] pause_before_publish`) forcing the interleaving — prime between gate-check and publish — asserts the stale value is NOT published (red without the recheck).
- Prefill decision pin (Decision #6): `uc2_node/tests/smoke.rs` addition — restart a node, then a NAK below its boot frontier is served from the journal (assert `replay_datagrams > 0`, receiver converges); plus the doc note in the runbook ("prefill rejected — evidence").
- `docs/ops/uc2-runbook.md`: instance-dir layout table; the bind-concrete-IP footgun (symptom: `append_pos_unknown_source`/commit stall); systemd-run pattern + `TimeoutStopSec=1` (parked bins ignore SIGTERM); leader probe = cnc flags word at offset 768 == 0x03; purge enablement checklist (policy, snapshot capability, slack, watch `archive_first_base` vs `node_snapshot_floor`); learner add/remove-a-box procedure; per-follower slot decoding table; gate binaries exit 1 on FAIL.

- [ ] **Step 1:** layout pins first (offset tests both crates) → FAIL → land the band; wire consensus/sender writers (bounded: peer slots updated once per duty cycle, not per datagram).
- [ ] **Step 2:** straddle regression red (hook forces the interleaving, stale publish observed) → green with the generation recheck.
- [ ] **Step 3:** prefill pin + runbook.
- [ ] **Step 4: verify** — full uc2 suite + loom (`RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release`) + failover ×2 + both clippy gates.
- [ ] **Step 5: commit** — `git add uc_protocol/src/v2/cnc.rs uc2_log/src/cnc.rs uc2_log/src/archive.rs uc2_node/src/node.rs uc2_node/tests/smoke.rs uc2_net/src/sender.rs uc2_net/src/receiver.rs docs/ops/uc2-runbook.md && git commit -m "feat(uc2): ops — per-peer cnc observability band, prime-generation straddle recheck (M5 residual closed), runbook; prefill decision closed as rejected-with-evidence"`

### Task 10: M6 gates — L3 under purge, m6_gate binary, docs

**Files:**
- Modify: `uc2_node/tests/lin_v2.rs` + `uc2_node/tests/lincheck_v2/mod.rs` (purge-churn variant), `uc2_node/Cargo.toml`
- Create: `uc2_node/examples/m6_gate.rs`, `docs/benchmarks/uc2-m6-gate-2026-07-XX.md`
- Tests: the gate smoke itself

**Interfaces:**
- **L3 purge-churn capstone** (`lin_v2.rs`, new `#[test] linearizable_under_purge_and_snapshot_churn`): the existing capstone harness with — snapshotting `RegisterSm`, tiny journal segments + `SnapshotPolicy{interval_bytes: 64 * 1024}` + `PurgePolicy::BelowSnapshot{slack_bytes: 0}` on every node, fault mix extended with a third 1-in-3 arm: crash-and-restart a random FOLLOWER's service (forcing below-floor reconstruction via install, not just tail replay). Same bars: ≥80 % ok, `check_register == Linearizable`, ≤120 s, 3 seeds (default 0x1107 + 7 + 99). This is "purge safety, lincheck stays green" — the milestone's heart.
- **`m6_gate` binary**, m4_gate-shaped (journal_root arg + /tmp guard + honest exit(1)), two scenarios in one run:
  1. `learner-join`: 3 voters + sustained load (~60 % of the M5 fleet operating point via admission pacing), purge on; start a fresh learner; PASS iff join completes (learner durable ≥ commit-at-join-start) within `JOIN_BUDGET = 60 s` AND the leader's commit-rate dip during the session is < 10 % of the pre-join baseline (printed either way — "no quorum stall, bounded catch-up", measured honestly).
  2. `purge-cycle`: with load running, drive N snapshot→purge→follower-service-crash→reconstruct cycles; PASS iff every reconstruction converges (service_applied catches commit) within 10 s and zero committed-value divergence (spot-check via linearizable reads).
  Roles runnable in-process (`all`) for sandbox smoke AND as separate `node`/`service` procs for a fleet run (reuse the m5_gate role/CLI scaffolding). While shaping the shared load-driver, fold in the M4-ledger fleet-prep polish: `m4_gate`'s `drive_load` admission loop gets a deadline (a commit stall currently spins it forever) — same helper serves both gates.

- Gate doc: definition verbatim from spec §9 M6 row, sandbox smoke numbers (honest, core-starved), fleet placeholder + protocol (the m5 protocol amended: purge policy on, learner host = a 4th c6id or co-located — decide in the doc: **4th host**, `c6id.2xlarge`, same placement group, so join bandwidth is real), the loud "Task 10 complete ≠ M6 gate passed" banner. **v1 retirement checklist section** (spec §9: v1 retires only after M5+M6 hold the bar): enumerated criteria (M5 gate PASSED ✓ 2026-07-12; M6 gate pending; v1 test suite parity table; the retirement itself is a separate user decision — checklist only, no deletion in M6).

- [ ] **Step 1:** purge-churn capstone red→green (expect real debugging here — this is the task that finds M6's bugs; budget it as the largest. A Violation that survives your debugging = MAJOR finding: dump-preserve, analyze, report — never weaken the checker).
- [ ] **Step 2:** 3 seeds green ≤120 s each; partition-scenario regression (`lin_partition_v2.rs`) ×1; crashtest (`--features hard-crash-tests`) ×1.
- [ ] **Step 3:** m6_gate binary + `all --secs 30` sandbox smoke with honest verdict; paste numbers into the doc.
- [ ] **Step 4:** gate doc + retirement checklist.
- [ ] **Step 5: verify** — the whole-milestone block (below).
- [ ] **Step 6: commit** — `git add uc2_node/tests uc2_node/examples/m6_gate.rs uc2_node/Cargo.toml docs/benchmarks/ && git commit -m "test(uc2): M6 gates — lincheck under purge/snapshot churn, m6_gate learner-join + purge-cycle roles, gate doc + v1 retirement checklist"`

---

## Verification (whole-milestone, before the final review)

```bash
cargo test --workspace                                        # all suites
cargo test -p uc2_sim --features sim-heavy --release          # fuzz incl. wipe arms
RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release
cargo test -p uc2-crashtest --features hard-crash-tests       # regression
LIN_SEED=4359 cargo test -p uc2_node --test lin_v2 --release  # both capstones; + seeds 7, 99
cargo test -p uc2_node --test learner --test purge_safety --release
(cd /home/claude/ultima/ultima_db && cargo test -p ultima-db snapshot_stream)  # sibling change
cargo clippy --workspace -- -D warnings
cargo clippy -p uc2_consensus -p uc2_sim -p uc2_node -p uc2_net -p uc2_log -p uc2_service -p uc2_client --all-targets -- -D warnings
cargo run -p uc2_node --example m6_gate --release -- all --secs 30   # smoke, honest verdict
```

Milestone gate (spec §9): purge-safety lincheck + reconstruction-under-load green locally; **learner join under load** is claimable from the loopback gate for correctness but the doc records a fleet confirmation (3 voters + 1 learner host) as the official number — a separate user-approved run, M1–M5 precedent. **Be loud: "Task 10 complete" ≠ "M6 gate passed" until the doc's verdict section says so.**

## Deferred / non-goals (state them, don't do them)

- Joint-consensus membership reconfig (voter add/remove) → v2.x (spec §3). Learners cover replace-a-box.
- Live service/client re-attach across node restart (beyond the M5 fail-stop contract) → still M6-deferred territory; the supervisor-respawn contract stands.
- Snapshot session multiplexing (>1 concurrent), compression, resumable sessions → v2.x (one-at-a-time is enough for replace-a-box).
- `uc2_client`/`uc2_service` dep slimming; PROT_READ service mapping; PSK-MAC slot → v2.x.
- Cluster-wide purge-floor gossip (a node advertising its floor so peers can pre-warn) → v2.x observability nicety; the session mechanism makes it unnecessary for correctness.
- Per-node `output_progress` gossip (failover to a long-idle leader replays full history — at-least-once-safe) → v2.x, noted in the runbook.
- v1 retirement EXECUTION → separate user decision after the M6 gate holds (Task 10 ships the checklist only).
