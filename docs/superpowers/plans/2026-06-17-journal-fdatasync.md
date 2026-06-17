# Journal fdatasync Commit Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch the journal's hot per-commit fsync from `sync_all` (full fsync) to `sync_data` (fdatasync) to lower the leader `submitted→persisted` P99 tail (task13 §15: P50 ~1 ms but P99 171 ms), with no wire/format change and no new dependency.

**Architecture:** A single change site — `fsync_active_segment()` in `ultima_journal/src/journal/writer.rs` — swaps `f.sync_all()` for `f.sync_data()` on the dup'd-fd commit path. `sync_all` is retained on the rare segment-create path (directory entry + header must be durable). fdatasync flushes file data plus the size growth needed to retrieve it, omitting only inode timestamps — the standard WAL commit primitive — so `Durability::Consistent`'s power-loss guarantee is preserved. Validated by a targeted durability test + the full journal suite + the cluster crash gates, then quantified by a local microbench and a same-fleet cloud A/B.

**Tech Stack:** Rust, `std`-only `ultima_journal` (segmented WAL, group-commit, `SeqWatermark`); `ultima_cluster` (openraft + uc_node/uc_service) which consumes the journal via a path dependency; `ultima-autobench` `journal-microbench`; `bench-infra` Terraform+Ansible cloud fleet.

---

## ⚠️ Cross-repo note — read before starting

The **code change and journal tests land in a different git repository** than this plan:

- This plan lives in `ultima_cluster` (`/home/claude/ultima/ultima_cluster`).
- The journal source is in the **`ultima_db` repo**, at `../ultima_db/ultima_journal/` (i.e. `/home/claude/ultima/ultima_db/ultima_journal/`). Tasks 1–3 commit **there**, on a branch in the `ultima_db` repo.
- `ultima_cluster` picks up the change automatically through its path dependency on `ultima_journal`. Tasks 4–5 build/test from `ultima_cluster` and need **no source edit** — they just exercise the new journal code through the cluster.

**Branch setup (do once, before Task 1):** create a feature branch in the `ultima_db` repo so the journal change is not committed straight to its `main`:

```bash
cd /home/claude/ultima/ultima_db
git checkout -b feat/journal-fdatasync
git status   # confirm clean working tree before starting
```

Do NOT branch/commit in `ultima_cluster` for Tasks 1–4 (no `ultima_cluster` source changes). Only this plan doc itself is an `ultima_cluster` artifact, already committed.

---

## File Structure

| File | Repo | Responsibility | Change |
|------|------|----------------|--------|
| `ultima_journal/src/journal/writer.rs` | ultima_db | Group-commit fsync of the active segment | `fsync_active_segment`: `sync_all` → `sync_data` (one line + rationale comment) |
| `ultima_journal/src/journal/mod.rs` | ultima_db | Journal public API + test module | Add `consistent_durability_survives_reopen` durability guard test |
| `ultima_journal/src/journal/segment.rs` | ultima_db | Per-segment file lifecycle | **Unchanged** — `SegmentFile::create` keeps full `sync_all` (verify only) |
| `ultima-autobench` `journal-microbench` | ultima_db | Local `group_commit_throughput` fitness | **Unchanged** — run before/after to quantify |
| `uc_node` lincheck capstone + `uc-crashtest` | ultima_cluster | Cluster crash-survival gates | **Unchanged** — run to prove fdatasync'd entries replay |
| `bench-infra` + `profile/raftcore-stats` | ultima_cluster | Same-fleet A/B on `submitted→persisted` P99 | **Unchanged** — operator-run validation |

---

### Task 1: Durability guard test (baseline-green against `sync_all`)

This test asserts that `Durability::Consistent` + group-commit fsync makes every record durable across a reopen. It is a **regression guard** for the refactor: it must pass against the *current* `sync_all` code first (establishing the durability baseline), then still pass after Task 2's switch to `sync_data`. That ordering is what proves the switch preserves durability.

**Files:**
- Test: `ultima_journal/src/journal/mod.rs` (in the existing `#[cfg(test)] mod tests` block, near `reopen_sees_appended_records`)

- [ ] **Step 1: Write the durability guard test**

Add this test alongside `reopen_sees_appended_records` in `ultima_journal/src/journal/mod.rs`. It mirrors that test's open/append/reopen shape but forces `Durability::Consistent` (so the fsync — soon fdatasync — actually runs per group commit) and waits the high-water seq before dropping:

```rust
#[test]
fn consistent_durability_survives_reopen() {
    // Under Consistent durability the group-commit fsync (sync_all today,
    // sync_data after the fdatasync switch) must make every acked record
    // durable, so a reopen recovers the full [1, N] range. This guards that
    // fdatasync preserves the same durable prefix as full fsync.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = crate::Durability::Consistent;
    {
        let j = Journal::open(cfg.clone()).unwrap();
        for i in 1..=64u64 {
            // append(seq, term, payload) — same call shape as
            // reopen_sees_appended_records.
            j.append(i, i, format!("rec-{i}").as_bytes()).unwrap();
        }
        // Wait the highest seq: the group-commit fsync barrier must make
        // every record at or below seq 64 durable before we drop.
        j.wait_durable(64).unwrap();
        assert_eq!(
            j.durable_seq(),
            64,
            "all 64 records durable after the commit-path fsync"
        );
    } // drop the Journal — clean process exit, no extra fsync on drop
    // Reopen: recovery must locate every fsync'd record.
    let j2 = Journal::open(cfg).unwrap();
    assert_eq!(j2.first_seq(), Some(1));
    assert_eq!(j2.last_seq(), Some(64));
}
```

- [ ] **Step 2: Run the test against the current (`sync_all`) code — expect PASS**

```bash
cd /home/claude/ultima/ultima_db/ultima_journal
cargo test consistent_durability_survives_reopen
```
Expected: `test journal::tests::consistent_durability_survives_reopen ... ok` (1 passed). This confirms the durability baseline holds with `sync_all` *before* the change. (Unlike a new-feature test, this guard is green now and must stay green — that is the point.)

- [ ] **Step 3: Commit the guard test**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/mod.rs
git commit -m "test(journal): durability guard — Consistent fsync survives reopen for [1,N]"
```

---

### Task 2: Switch the commit-path fsync to fdatasync

**Files:**
- Modify: `ultima_journal/src/journal/writer.rs` (`fsync_active_segment`, the `f.sync_all()` call at ~line 585)

- [ ] **Step 1: Read the change site for exact context**

```bash
cd /home/claude/ultima/ultima_db/ultima_journal
sed -n '565,590p' src/journal/writer.rs
```
Confirm: `fsync_active_segment` dup's the active segment fd, drops the state lock (the task13 §9 lock-release optimization), and then calls `f.sync_all()`. That `f.sync_all()` is the only line to change.

- [ ] **Step 2: Make the one-line change with a rationale comment**

In `ultima_journal/src/journal/writer.rs`, replace the commit-path fsync call:

```rust
        f.sync_all().map_err(JournalError::Io)?;
```

with:

```rust
        // WAL commit primitive: fdatasync, not full fsync. sync_data flushes
        // the appended data plus the i_size growth needed to read it back,
        // and skips only inode timestamps (mtime/atime) — irrelevant to
        // durability. This preserves Durability::Consistent's power-loss
        // guarantee while dropping the per-commit timestamp write, lowering
        // the submitted->persisted P99 tail (task13 §15). Full sync_all is
        // retained on segment create (segment.rs) where the new directory
        // entry must be made durable.
        f.sync_data().map_err(JournalError::Io)?;
```

- [ ] **Step 3: Verify segment-create still uses full fsync (no change needed)**

```bash
cd /home/claude/ultima/ultima_db/ultima_journal
grep -nE "sync_all|sync_data" src/journal/segment.rs src/journal/writer.rs
```
Expected: `segment.rs` `SegmentFile::create` still calls `sync_all` (file + parent-dir durability on rotation); `writer.rs` now shows `sync_data` on the commit path and no remaining commit-path `sync_all`.

- [ ] **Step 4: Run the durability guard + the full journal suite — expect PASS**

```bash
cd /home/claude/ultima/ultima_db/ultima_journal
cargo test consistent_durability_survives_reopen
cargo test
```
Expected: the guard still passes (fdatasync preserves the durable prefix); the whole suite passes — including recovery (`reopen_sees_appended_records`), rotation (`segment_rotates_when_size_exceeded`), idle fsync (`eventual_periodic_fsync_runs_on_idle`), `on_durable_fires_once_after_fsync`, and the fault path (`fsync_failure_halts_writer`, which now exercises the `sync_data` call site through `fail_next_fsync`). 0 failed.

- [ ] **Step 5: Lint (zero warnings)**

```bash
cd /home/claude/ultima/ultima_db/ultima_journal
cargo clippy --all-targets -- -D warnings
```
Expected: finishes with no warnings.

- [ ] **Step 6: Commit the change**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/writer.rs
git commit -m "perf(journal): fdatasync (sync_data) on the commit path; sync_all only on segment create

Lowers per-commit fsync cost by skipping inode-timestamp flushes; preserves
Durability::Consistent power-loss safety (data + i_size still flushed). Targets
the submitted->persisted P99 tail (task13 §15). One line, no dep, no format change."
```

---

### Task 3: Local microbench A/B (`group_commit_throughput`)

Quantify the local win with the `journal-microbench` fitness binary (real disk, median-of-5), comparing the pre-change commit (`sync_all`) against the post-change commit (`sync_data`). No fleet needed.

**Files:**
- Run-only: `ultima-autobench` `journal-microbench` (no edit)

- [ ] **Step 1: Measure the new (`sync_data`) code — 5 runs**

```bash
cd /home/claude/ultima/ultima_db
for i in 1 2 3 4 5; do
  cargo run -q -p ultima-autobench --bin journal-microbench --release -- --json
done
```
Each line is one JSON object; record the `group_commit_throughput` value from each (entries/sec). Take the median.

- [ ] **Step 2: Measure the old (`sync_all`) code — 5 runs on the parent commit**

```bash
cd /home/claude/ultima/ultima_db
git stash --include-untracked 2>/dev/null || true   # safety; tree should already be clean
git checkout HEAD~1 -- ultima_journal/src/journal/writer.rs   # restore sync_all only
for i in 1 2 3 4 5; do
  cargo run -q -p ultima-autobench --bin journal-microbench --release -- --json
done
git checkout HEAD -- ultima_journal/src/journal/writer.rs      # restore sync_data
```
Record the median `group_commit_throughput` for the `sync_all` baseline. (Checking out only `writer.rs` keeps the guard test from Task 1 in place for both measurements.)

- [ ] **Step 3: Record the delta**

Compute `(median_after - median_before) / median_before * 100`. fdatasync is expected to raise `group_commit_throughput`. If it is flat or worse, that is the signal that the §5 gated preallocation follow-up (out of scope here) may be needed — note the numbers either way. No commit (measurement only); the numbers feed Task 5's task13 write-up.

---

### Task 4: Cluster crash gates (lincheck capstone + hard-crash)

Prove fdatasync'd entries survive a real crash and replay linearizably. These run from `ultima_cluster`; the path dep means they build against the just-changed journal automatically (confirm with the cargo build line first).

**Files:**
- Run-only: `uc_node` lincheck capstone + `uc-crashtest` hard-crash (no edit)

- [ ] **Step 1: Confirm the cluster builds against the changed journal**

```bash
cd /home/claude/ultima/ultima_cluster
cargo build -p uc_node 2>&1 | tail -5
grep -nE "ultima.journal|ultima_journal" Cargo.toml uc_node/Cargo.toml
```
Expected: builds clean; the dependency resolves to the local `../ultima_db/ultima_journal` path (so the `sync_data` change is in the binary under test).

- [ ] **Step 2: Run the lincheck capstone (in-memory SM under faults + churn)**

```bash
cd /home/claude/ultima/ultima_cluster
cargo test -p uc_node --test lin_register -- --nocapture
```
Expected: PASS — history checks `Linearizable`. (This is the capstone referenced in CLAUDE.md; the non-persisting in-memory `RegisterSm` under service-crash + churn.)

- [ ] **Step 3: Run the hard-crash test (SIGKILL service mid-load)**

```bash
cd /home/claude/ultima/ultima_cluster
cargo test -p uc-crashtest --features hard-crash-tests -- --nocapture
```
Expected: PASS — `hard_crash` SIGKILLs the service mid-apply and the recovered cluster stays `Linearizable` (the fdatasync'd Raft log replays correctly). If it flakes on the known boot/convergence race, re-run; only a `Fatal`/non-linearizable result is a real failure.

- [ ] **Step 4: No commit**

These are run-only gates (no source change). If any gate fails on correctness, STOP and escalate — fdatasync must not change crash-recovery behavior.

---

### Task 5 (operator): Same-fleet cloud A/B + task13 record

Quantify the cluster-level effect on the `submitted→persisted` P99 tail and overall throughput using the `profile/raftcore-stats` instrument on a real 3-node fleet, then record the result in the canonical task13 doc. This is an **operator/manual** task — it provisions paid cloud hosts and must be run deliberately, not by an unattended subagent.

**Files:**
- Run-only: `bench-infra/` rig + `profile/raftcore-stats` branch instrument
- Record: `ultima_cluster/docs/tasks/task13_aeron_vs_uc_commit_path.md` (append a new section)

- [ ] **Step 1: Bring up one persistent UC fleet**

```bash
cd /home/claude/ultima/ultima_cluster/bench-infra
make up-uc            # provisions the 3-host topology-B fleet (AWS/GCP/Hetzner per .env)
make status
```

- [ ] **Step 2: A-side measurement — `sync_all` (pre-change journal)**

On the build host the rig uses, ensure the journal is at the pre-change commit (`sync_all`), then run the throughput sweep with the RaftCore-stats instrument. Record `submitted→persisted` P50/P99 (from the periodic `RAFT_RUNTIME_STATS` dump) and `uc_throughput_msgs`:

```bash
cd /home/claude/ultima/ultima_cluster/bench-infra
# build_uc rsyncs the sibling ../ultima_db (incl. ultima_journal) to the host;
# point it at the pre-change journal commit for the A side, then:
make bench-oneshot     # one sweep on the existing fleet
```
Record the A-side P50/P99 and throughput.

- [ ] **Step 3: B-side measurement — `sync_data` (post-change journal), same fleet**

Without destroying the fleet, rebuild with the post-change journal (`sync_data`, this branch's HEAD) and re-run the identical sweep:

```bash
cd /home/claude/ultima/ultima_cluster/bench-infra
make bench-oneshot
```
Record the B-side P50/P99 and throughput. Same fleet + same sweep = valid A/B (avoids the host-artifact problem from task13 §11).

- [ ] **Step 4: Destroy the fleet**

```bash
cd /home/claude/ultima/ultima_cluster/bench-infra
make destroy
make status   # confirm nothing left running (cost control)
```

- [ ] **Step 5: Record the result in task13**

Append a new section to `docs/tasks/task13_aeron_vs_uc_commit_path.md` (e.g. `§16 — journal fdatasync`) with: the local microbench delta (Task 3), the same-fleet `submitted→persisted` P50/P99 before/after and `uc_throughput_msgs` before/after (Task 3–4 wording: "fdatasync vs full fsync"), and a one-line verdict on whether the 171 ms P99 tail receded. Commit in `ultima_cluster`:

```bash
cd /home/claude/ultima/ultima_cluster
git add docs/tasks/task13_aeron_vs_uc_commit_path.md
git commit -m "docs(task13): journal fdatasync A/B — local microbench + same-fleet submitted->persisted P99"
```

- [ ] **Step 6: Decide on the gated follow-up**

If Steps 2–3 show fdatasync did **not** move the P99 (tail is i_size churn / lazy block allocation, not timestamps), open the §5 segment-preallocation follow-up (`set_len(segment_size)` at segment create, std-only, with the recovery/frontier audit the spec requires). If the P99 receded, mark §5 as not needed (YAGNI). Note the decision in the task13 section from Step 5.

---

## Notes on deferred / out-of-scope work

- **§5 segment preallocation** is explicitly deferred behind the Task 5 measurement (gated follow-up; would add format/recovery surface and needs the frontier-via-zero-scan audit). Not in this plan's v1.
- **No `libc`/`fallocate`** — the journal stays dependency-free (`set_len` is std).
- **`persisted→committed` replication round-trip** (openraft/network territory, no alpha.21 in-flight pipelining knob) is a separate investigation, not this change.

## Self-Review

- **Spec coverage:** §3 architecture → Task 2 (sync_all→sync_data, segment-create retains sync_all, dup'd-fd path unchanged). §4 correctness → Task 1 (targeted durability test) + Task 2 Step 4 (full suite incl. recovery/rotation/fail_next_fsync) + Task 4 (lincheck + hard-crash). §4 performance → Task 3 (local microbench median-of-5) + Task 5 Steps 1–4 (same-fleet A/B on submitted→persisted P50/P99). §5 gated preallocation → Task 5 Step 6 (decision gate, explicitly deferred). §6 files → writer.rs + journal tests, no uc_* / format / dep change (asserted in File Structure + Task 4 Step 1 path-dep check). Honest power-loss caveat → captured in the rationale comment (Task 2 Step 2) and the fact that crash gates don't distinguish fdatasync from fsync. All spec sections map to a task.
- **Placeholder scan:** no TBD/TODO; every code/command step shows the exact code or command and expected output.
- **Type consistency:** test uses `JournalConfig::new`, `cfg.durability`, `crate::Durability::Consistent`, `Journal::open`, `append(seq, term, payload)`, `wait_durable(u64)`, `durable_seq() -> u64`, `first_seq()/last_seq() -> Option<u64>` — all verified against the current `ultima_journal` source. Change site `f.sync_all()` → `f.sync_data()` verified at `writer.rs:585`.
