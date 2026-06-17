# Design — journal fdatasync on the commit path (lower the leader fsync tail)

**Date:** 2026-06-17
**Status:** Design approved; pending spec review → implementation plan.
**Topic:** Reduce the leader journal fsync cost/tail (task13 §15: `submitted→persisted`
P50 ~1 ms but **P99 171 ms**) by switching the hot per-commit fsync from `sync_all`
(full fsync) to `sync_data` (fdatasync). No wire/format change, no new dependency.

## 1. Motivation

The RaftCore profile (task13 §15) localized the residual 3-node throughput cost to the
**leader journal append+fsync** stage (`submitted→persisted`): P50 ~1 ms, P90 ~1.2 ms,
**P99 171 ms** — a broad tail affecting ~10 % of appends. Segment rotation is ruled out
(64 MiB segments → <1 rotation per run, far too rare for a P99 tail). The cost lives in
the **group-commit fsync path** — either per-commit fsync cost or single-writer
saturation/queueing at the throughput knee. The RaftCore loop itself has spare capacity
(§15), and apply is already pipelined (§14), so the journal fsync is the next lever — and
it is in *our* code (`ultima_journal`), the same component whose group-commit gave a
+275 % microbench win earlier (task13 §9).

## 2. Goals / Non-goals

**Goals**
- Lower per-commit fsync cost on the journal's hot path, reducing `submitted→persisted`
  P99 and raising the writer's sustainable rate (helps both the fsync-cost and the
  saturation-queue hypotheses).
- Preserve `Durability::Consistent`'s power-loss guarantee.
- No `uc_protocol`/on-disk-format change; **no new dependency** (the journal is
  deliberately std-only).

**Non-goals**
- Not segment preallocation in v1 (gated follow-up — §5 — only if measurement shows
  fdatasync alone is insufficient).
- Not adding `libc`/`fallocate` (keep the journal dependency-free).
- Not touching the consensus/apply path (already optimized, §13/§14) or the RaftCore
  loop (has spare capacity, §15).
- Not changing `Durability::Eventual` semantics (its background idle fsync uses the
  same helper and benefits equally).

## 3. Architecture — fdatasync per commit

Single change site: `fsync_active_segment()` in `ultima_journal/src/journal/writer.rs`.
Today it grabs a dup'd fd of the active segment under the state lock, drops the lock,
then issues `f.sync_all()`. Change that to **`f.sync_data()`** (fdatasync).

- `sync_all` retained on **segment create** (`SegmentFile::create`, segment.rs) — the
  header write + new-file fsync + parent-directory fsync stay full fsyncs; these are
  rare (per 64 MiB segment) and need the directory entry durable.
- The dup'd-fd lock-release optimization (task13 §9) is unchanged — `sync_data` runs
  off the state lock exactly as `sync_all` did.
- The `SeqWatermark` barrier semantics are unchanged: `sync_data` on the dup'd fd
  flushes the same contiguous written prefix, so the high-water publish logic is
  identical.

**Why correct (durability preserved):** `fdatasync` flushes the file *data* plus the
metadata needed to *retrieve* it (the size growth from the append); it omits only inode
timestamps (mtime/atime), which are irrelevant to WAL durability. This is the standard
WAL commit primitive (Postgres et al.). `Durability::Consistent` keeps its power-loss
guarantee; the only skipped writes are timestamps.

**Why it helps the tail:** every commit currently pays `sync_all`'s inode write
(timestamps + i_size). Dropping the timestamp flush makes each commit cheaper, which
lowers `submitted→persisted` directly and raises the single writer's sustainable fsync
rate — so the saturation/queue tail at the knee recedes regardless of which cause
dominates. One line, no dep, no format change.

## 4. Testing & validation

**Correctness / durability (must stay green):**
- Full `ultima_journal` test suite — recovery (reopen → all records readable),
  truncation, rotation, and the `fail_next_fsync` fault path (now exercising
  `sync_data`).
- A targeted journal test: append N records → `sync_data` → reopen → assert all durable
  and the `SeqWatermark` reflects them.
- Cluster crash gates: the lincheck capstone + the hard-crash `kill -9`-service test —
  fdatasync'd entries must survive and replay correctly.
- **Honest caveat:** process-crash tests do **not** distinguish `fdatasync` from
  `fsync` (both survive a process crash via the page cache); they differ only under
  **power loss** (metadata), which is untestable in-process. fdatasync's WAL power-loss
  correctness rests on the data+size-flush property above, not on a new test.

**Performance validation (the point):**
- Fast local: the journal `group_commit_throughput` microbench (median-of-5) before/
  after — expected to rise; no fleet needed.
- Cluster confirmation: a **same-fleet A/B** using the `profile/raftcore-stats`
  instrument — compare `submitted→persisted` P50/P99 and overall `uc_throughput_msgs`
  with `sync_all` vs `sync_data`. Confirms whether the 171 ms tail receded and by how
  much.

## 5. Gated follow-up — segment preallocation (only if needed)

If the A/B shows fdatasync alone does **not** move the P99 (i.e., the tail is i_size
churn / lazy block allocation, not timestamps): add `set_len(segment_size)` at segment
create (std-only, no dep) so the file is fixed-size and fdatasync skips i_size flushes.
The unwritten tail reads as zeros → length-0 records → "uncommitted", compatible with
the reader's existing torn-record protection.

**Required audit before doing this** (not in v1): recovery must locate the write
frontier via the zero-length-record scan (not file size, which becomes a constant
64 MiB); confirm rotation uses the tracked write offset (it does, `writer.rs` `projected`)
and audit every `size()`/file-length caller. This adds format/recovery surface, so it is
deferred behind the measurement — YAGNI unless the profile demands it.

## 6. Files touched (v1)

- `ultima_journal/src/journal/writer.rs` — `fsync_active_segment`: `sync_all` →
  `sync_data` (one line). Optional: a brief comment on the WAL fdatasync rationale.
- `ultima_journal` tests — a targeted append→fdatasync→reopen durability assertion if
  not already covered.
- No `uc_*` source changes; no `uc_protocol`/format change; no new dependency.

## 7. Out of scope / future

- Segment preallocation (§5, gated) and real `fallocate` (would add `libc`).
- Pre-zeroed / background-created segments (further fsync-tail reduction if rotation
  ever becomes hot at smaller segment sizes).
- Replication round-trip (`persisted→committed`) — openraft/network territory
  (alpha.21 has no in-flight pipelining knob); a separate investigation.
