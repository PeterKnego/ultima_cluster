# Journal prealloc fill-contention fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the `SegmentPipeline`'s background segment preallocation from monopolizing the NVMe during fresh-segment commits (the cause of the ~2.9 ms `append_consistent_prealloc_p99` tail) by adding a selectable `PreallocFill` strategy — a reliable paced zero-write (B) and an empirically-validated `fallocate(ZERO_RANGE)` (A) — then A/B both on the fleet.

**Architecture:** Add a `PreallocFill` enum to `JournalConfig`; the background fill path (`SegmentFile::create_prealloc_temp`, reached via `SegmentPipeline`) dispatches on it. `ZeroWriteFull` keeps today's behavior (baseline + default); `ZeroWritePaced` breaks the 64 MiB flush into small spaced `sync_data` barriers; `FallocateZeroRange` uses one `rustix` syscall + `sync_data` (Linux-only, falls back to paced). Strategy is runtime-selectable via `UC_JOURNAL_PREALLOC_FILL` so one fleet session compares all three.

**Tech Stack:** Rust, `ultima_journal` (in-tree workspace member of `ultima_cluster`), `rustix` (new, Linux-only dep, for `fallocate`), `uc_node` (config wiring), `uc_autobench` (microbench strategy selection), Linux `perf`, AWS c6id fleet via `bench-infra`.

## Global Constraints

- **Default behavior is unchanged in this change.** `JournalConfig` default `prealloc_fill = PreallocFill::ZeroWriteFull` (the current 64 MiB zero-write + `sync_all` + parent-dir `sync_all`). The default flips to the A/B winner only in a follow-up commit after measurement.
- **Durability/recovery semantics must be preserved.** Every strategy produces a full-size segment file that reads back as all zeros, so tail-tolerant recovery (zeros = end-of-log) and torn-record CRC logic are untouched. `Durability::Consistent`'s per-commit guarantee on committed records is unchanged.
- **Only the prealloc TEMP's parent-dir `sync_all` may be dropped** (B and A) — justified because a temp is recreated at open on crash. The activated segment's rename + dir `sync_all` in `activate_prealloc_temp` stays.
- **`FallocateZeroRange` is Linux-only with a fallback:** on non-Linux, or on `fallocate` returning `OPNOTSUPP`/`NOSYS`, fall back to `ZeroWritePaced`. The journal must always work.
- **`rustix` is a Linux-only dependency** (target-gated) added to `ultima_journal` (currently has no libc/nix/rustix dep). Version `1`, feature `fs`.
- **`ultima_journal` is `runtime-agnostic`** — no tokio/async in this crate; pure blocking `std` + `rustix` syscalls only.
- **Defaults:** `prealloc_fill_chunk_bytes = 4 * 1024 * 1024` (4 MiB); `segment_size_bytes = 64 * 1024 * 1024`.
- **Env values:** `UC_JOURNAL_PREALLOC_FILL = full | paced | fallocate` (unset/unknown → `full`). This is orthogonal to the existing `UC_JOURNAL_PREALLOC` (on/off) toggle.
- **A's accept/reject is a fleet decision, not CI:** A is metadata-free ⟺ its `append_consistent_prealloc_p99` collapses like paced's AND `group_commit_throughput_prealloc` ≥ `ZeroWriteFull` baseline; else reject A, ship B.

---

## File Structure

**Phase 1 — implementation (local, TDD):**

- `ultima_journal/src/journal/mod.rs` (modify) — `PreallocFill` enum + two `JournalConfig` fields + defaults; thread strategy into the `SegmentPipeline::spawn` call and the recovery `preallocate_to` call.
- `ultima_journal/src/lib.rs` (modify) — export `PreallocFill`.
- `ultima_journal/src/journal/segment.rs` (modify) — `create_prealloc_temp` + `preallocate_to` take the strategy + chunk size and dispatch to `fill_zero_write_full` / `fill_zero_write_paced` / `fill_fallocate_zero_range`.
- `ultima_journal/src/journal/segment_pipeline.rs` (modify) — carry strategy + chunk in `Shared`; pass through `spawn` → `preallocator_loop` → `create_prealloc_temp`.
- `ultima_journal/Cargo.toml` (modify) — add target-gated `rustix`.
- `uc_node/src/raft/log_storage.rs` (modify) — keep the `JournalConfig` literal compiling (Task 1), then wire `UC_JOURNAL_PREALLOC_FILL` (Task 4).
- `uc_autobench/src/journal_bench.rs` (modify) — `fresh_cfg` selects the strategy from `UC_JOURNAL_PREALLOC_FILL` for the preallocated arms (Task 4).

**Phase 2 — measurement (runbook):**

- `docs/benchmarks/journal-prealloc-fill-ab-2026-06-21.md` (create) — the three-way A/B result + accept/reject verdict + default-flip recommendation.

---

## Phase 1 — Implementation

### Task 1: `PreallocFill` enum + `JournalConfig` fields + workspace compiles

**Files:**
- Modify: `ultima_journal/src/journal/mod.rs` (the `JournalConfig` struct at `mod.rs:14-34`)
- Modify: `ultima_journal/src/lib.rs:20` (export)
- Modify: `uc_node/src/raft/log_storage.rs:90-98` (keep the struct literal compiling)
- Test: `ultima_journal/src/journal/mod.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Produces: `pub enum PreallocFill { ZeroWriteFull, ZeroWritePaced, FallocateZeroRange }` (`Debug, Clone, Copy, PartialEq, Eq`); `JournalConfig.prealloc_fill: PreallocFill` (default `ZeroWriteFull`); `JournalConfig.prealloc_fill_chunk_bytes: u64` (default `4 * 1024 * 1024`). Re-exported as `ultima_journal::PreallocFill`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `ultima_journal/src/journal/mod.rs`:

```rust
#[test]
fn prealloc_fill_defaults() {
    let cfg = JournalConfig::new("/tmp/does-not-matter");
    assert_eq!(cfg.prealloc_fill, PreallocFill::ZeroWriteFull);
    assert_eq!(cfg.prealloc_fill_chunk_bytes, 4 * 1024 * 1024);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ultima-journal prealloc_fill_defaults`
Expected: FAIL to compile — `no variant or associated item named ... PreallocFill` / `no field prealloc_fill`.

- [ ] **Step 3: Add the enum and fields**

In `ultima_journal/src/journal/mod.rs`, add the enum above `JournalConfig` and the two fields to it:

```rust
/// How a preallocated segment's empty tail is laid down. All three produce a
/// full-size file that reads back as zeros (so recovery's zero-tail = end-of-log
/// logic is identical); they differ only in I/O shape, which governs whether the
/// background fill contends with foreground per-commit `fdatasync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreallocFill {
    /// Real zero-write of the whole segment + one `sync_all` + parent-dir
    /// `sync_all`. Original behavior; the A/B baseline and current default.
    ZeroWriteFull,
    /// Real zero-write in chunks, `sync_data` every `prealloc_fill_chunk_bytes`
    /// with a `yield_now` between, no per-temp dir sync. Same written extents as
    /// `ZeroWriteFull`, but the device flush is split into small spaced barriers
    /// so a foreground commit `fdatasync` can interleave. Pure `std`.
    ZeroWritePaced,
    /// `fallocate(FALLOC_FL_ZERO_RANGE)` + one `sync_data`, no per-temp dir sync.
    /// Linux-only; falls back to `ZeroWritePaced` on non-Linux or `OPNOTSUPP`.
    /// Near-zero background I/O *iff* the kernel yields initialized extents
    /// (validated by the fleet A/B, not assumed).
    FallocateZeroRange,
}
```

In `JournalConfig` add the fields (after `preallocate_segments`):

```rust
    /// Fill strategy for preallocated segments (only consulted when
    /// `preallocate_segments`). Default `ZeroWriteFull` (no behavior change).
    pub prealloc_fill: PreallocFill,
    /// Chunk granularity for `ZeroWritePaced` (and the paced fallback): issue a
    /// `sync_data` after roughly this many bytes. Default 4 MiB.
    pub prealloc_fill_chunk_bytes: u64,
```

In `JournalConfig::new`, set the defaults (after `preallocate_segments: false,`):

```rust
            prealloc_fill: PreallocFill::ZeroWriteFull,
            prealloc_fill_chunk_bytes: 4 * 1024 * 1024,
```

- [ ] **Step 4: Export the enum**

In `ultima_journal/src/lib.rs`, change line 20 to:

```rust
pub use journal::{Journal, JournalConfig, PreallocFill};
```

- [ ] **Step 5: Keep the uc_node struct literal compiling**

`uc_node/src/raft/log_storage.rs` builds `JournalConfig { ... }` as a struct literal (lines 90-98), so it must list the new fields. Add to that literal (after the `preallocate_segments: ...` line):

```rust
            prealloc_fill: ultima_journal::PreallocFill::ZeroWriteFull,
            prealloc_fill_chunk_bytes: 4 * 1024 * 1024,
```

(Task 4 replaces the `prealloc_fill` value with an env read; here it is the explicit default so the workspace compiles with unchanged behavior.)

- [ ] **Step 6: Run test to verify it passes + workspace builds**

Run: `cargo test -p ultima-journal prealloc_fill_defaults && cargo build -p uc_node`
Expected: test PASS (`1 passed`); `uc_node` builds clean.

- [ ] **Step 7: Commit**

```bash
git add ultima_journal/src/journal/mod.rs ultima_journal/src/lib.rs uc_node/src/raft/log_storage.rs
git commit -m "feat(journal): add PreallocFill strategy enum + JournalConfig fields (default ZeroWriteFull)"
```

---

### Task 2: Paced zero-write fill (B) + strategy dispatch end-to-end

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (`create_prealloc_temp` at `:232`, `preallocate_to` at `:428`)
- Modify: `ultima_journal/src/journal/segment_pipeline.rs` (`Shared`, `spawn`, `preallocator_loop`)
- Modify: `ultima_journal/src/journal/mod.rs` (the `SegmentPipeline::spawn` call at `:181` and the `preallocate_to` call at `:177`)
- Test: `ultima_journal/src/journal/segment.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Consumes: `PreallocFill` (Task 1).
- Produces: `SegmentFile::create_prealloc_temp(path: &Path, total_len: u64, fill: PreallocFill, chunk_bytes: u64) -> Result<(), JournalError>`; `SegmentFile::preallocate_to(&mut self, total_len: u64, fill: PreallocFill, chunk_bytes: u64) -> Result<(), JournalError>`; private `fn fill_zero_write_full(&File, u64)`, `fn fill_zero_write_paced(&File, u64, u64)` (free functions in `segment.rs` taking `&std::fs::File`). `SegmentPipeline::spawn(dir, segment_size, fill, fill_chunk_bytes)`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `ultima_journal/src/journal/segment.rs`:

```rust
#[test]
fn paced_fill_produces_full_size_all_zero_file() {
    use crate::PreallocFill;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("seg-prealloc.0.tmp");
    // A size that is NOT a multiple of the 4 MiB chunk to exercise the final partial sync.
    let total: u64 = 4 * 1024 * 1024 + 7;
    SegmentFile::create_prealloc_temp(&p, total, PreallocFill::ZeroWritePaced, 4 * 1024 * 1024).unwrap();
    let meta = std::fs::metadata(&p).unwrap();
    assert_eq!(meta.len(), total, "file must be exactly total_len");
    let bytes = std::fs::read(&p).unwrap();
    assert!(bytes.iter().all(|&b| b == 0), "every byte must be zero");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ultima-journal paced_fill_produces_full_size`
Expected: FAIL to compile — `create_prealloc_temp` takes 2 args, not 4.

- [ ] **Step 3: Refactor the fill into dispatchers + add the paced fill**

In `ultima_journal/src/journal/segment.rs`, add `use crate::PreallocFill;` near the top imports if not present. Replace `create_prealloc_temp` (`:232-250`) with:

```rust
    /// Create a NEW preallocated temp segment file filled per `fill`. All
    /// strategies leave the file `total_len` bytes long and readable as zeros.
    /// Writes NO header — `base_seq` is unknown until activation.
    pub(crate) fn create_prealloc_temp(
        path: &Path,
        total_len: u64,
        fill: PreallocFill,
        chunk_bytes: u64,
    ) -> Result<(), JournalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        match fill {
            PreallocFill::ZeroWriteFull => {
                fill_zero_write_full(&file, total_len)?;
                // Baseline parity: make the temp's dir entry crash-durable.
                if let Some(parent) = path.parent() {
                    std::fs::File::open(parent)?.sync_all()?;
                }
            }
            PreallocFill::ZeroWritePaced => fill_zero_write_paced(&file, total_len, chunk_bytes)?,
            PreallocFill::FallocateZeroRange => {
                fill_fallocate_zero_range(&file, total_len, chunk_bytes)?
            }
        }
        Ok(())
    }
```

Add these free functions in `segment.rs` (module scope, not in `impl`). `fill_fallocate_zero_range` is implemented in Task 3 — add a temporary body that delegates to paced so this task compiles, Task 3 replaces it:

```rust
/// Real zero-write of `[0, total_len)` + one `sync_all` (original strategy).
fn fill_zero_write_full(file: &std::fs::File, total_len: u64) -> Result<(), JournalError> {
    let zeros = vec![0u8; 1024 * 1024];
    let mut f = file;
    let mut remaining = total_len;
    use std::io::Write;
    while remaining > 0 {
        let n = remaining.min(zeros.len() as u64) as usize;
        f.write_all(&zeros[..n])?;
        remaining -= n as u64;
    }
    f.sync_all()?;
    Ok(())
}

/// Real zero-write in chunks: `sync_data` every ~`chunk_bytes` with a yield
/// between, so the device flush never queues as one large barrier ahead of a
/// foreground commit. No dir sync (temp is recreated on crash).
fn fill_zero_write_paced(
    file: &std::fs::File,
    total_len: u64,
    chunk_bytes: u64,
) -> Result<(), JournalError> {
    use std::io::Write;
    let zeros = vec![0u8; 1024 * 1024];
    let sync_every = chunk_bytes.max(1);
    let mut f = file;
    let mut written: u64 = 0;
    let mut since_sync: u64 = 0;
    while written < total_len {
        let n = (total_len - written).min(zeros.len() as u64) as usize;
        f.write_all(&zeros[..n])?;
        written += n as u64;
        since_sync += n as u64;
        if since_sync >= sync_every {
            f.sync_data()?;
            std::thread::yield_now();
            since_sync = 0;
        }
    }
    if since_sync > 0 {
        f.sync_data()?;
    }
    Ok(())
}

/// Placeholder until Task 3: behaves as paced so the dispatcher compiles.
fn fill_fallocate_zero_range(
    file: &std::fs::File,
    total_len: u64,
    chunk_bytes: u64,
) -> Result<(), JournalError> {
    fill_zero_write_paced(file, total_len, chunk_bytes)
}
```

(`let mut f = file;` then `f.write_all`/`f.sync_data` works because `&std::fs::File` implements `Write` and the sync methods take `&self`.)

- [ ] **Step 4: Thread the strategy through `preallocate_to`**

Replace `preallocate_to` (`segment.rs:428-442`) so it dispatches the same way (it fills the ACTIVE segment's tail in place on recovery):

```rust
    /// Physically fill from the logical cursor `self.size` out to `total_len`
    /// per `fill`, WITHOUT advancing the cursor. Leaves the tail readable as
    /// zeros so appends overwrite already-written blocks (metadata-free commit).
    pub(crate) fn preallocate_to(
        &mut self,
        total_len: u64,
        fill: PreallocFill,
        chunk_bytes: u64,
    ) -> Result<(), JournalError> {
        if total_len <= self.size {
            return Ok(());
        }
        self.file.seek(SeekFrom::Start(self.size))?;
        let span = total_len - self.size;
        match fill {
            PreallocFill::ZeroWriteFull => fill_zero_write_full(&self.file, span)?,
            PreallocFill::ZeroWritePaced => fill_zero_write_paced(&self.file, span, chunk_bytes)?,
            PreallocFill::FallocateZeroRange => {
                fill_fallocate_zero_range(&self.file, span, chunk_bytes)?
            }
        }
        Ok(())
    }
```

Note: the fill helpers write from the file's current position (`write_all` appends at the seek cursor); `preallocate_to` seeks to `self.size` first, so the helpers fill `[self.size, total_len)`. For `fill_fallocate_zero_range` the offset matters — Task 3's real implementation takes an explicit offset; here the placeholder delegates to paced which respects the seek. Task 3 updates `preallocate_to`'s fallocate branch to pass the offset.

- [ ] **Step 5: Carry the strategy through the pipeline**

In `ultima_journal/src/journal/segment_pipeline.rs`: add to `Shared` (after `segment_size: u64,`):

```rust
    fill: crate::PreallocFill,
    fill_chunk_bytes: u64,
```

Change `spawn` signature and the `Shared` initializer:

```rust
    pub(crate) fn spawn(
        dir: PathBuf,
        segment_size: u64,
        fill: crate::PreallocFill,
        fill_chunk_bytes: u64,
    ) -> Result<Arc<SegmentPipeline>, JournalError> {
        let shared = Arc::new(Shared {
            dir,
            segment_size,
            fill,
            fill_chunk_bytes,
            counter: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            slot: Mutex::new(Slot::default()),
            cv: Condvar::new(),
        });
```

In `preallocator_loop`, change the `create_prealloc_temp` call:

```rust
            match SegmentFile::create_prealloc_temp(
                &path,
                shared.segment_size,
                shared.fill,
                shared.fill_chunk_bytes,
            ) {
```

- [ ] **Step 6: Update the two call sites in `mod.rs`**

In `ultima_journal/src/journal/mod.rs`, the `SegmentPipeline::spawn` call (`:181-184`):

```rust
            Some(crate::journal::segment_pipeline::SegmentPipeline::spawn(
                config.dir.clone(),
                config.segment_size_bytes,
                config.prealloc_fill,
                config.prealloc_fill_chunk_bytes,
            )?)
```

And the recovery `preallocate_to` call (`:177`):

```rust
            active.preallocate_to(
                config.segment_size_bytes,
                config.prealloc_fill,
                config.prealloc_fill_chunk_bytes,
            )?;
```

- [ ] **Step 7: Run the new test + the existing journal suite**

Run: `cargo test -p ultima-journal`
Expected: PASS — `paced_fill_produces_full_size_all_zero_file` passes and all existing journal/preallocation/recovery tests still pass (they run under the default `ZeroWriteFull`, unchanged).

- [ ] **Step 8: Commit**

```bash
git add ultima_journal/src/journal/segment.rs ultima_journal/src/journal/segment_pipeline.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): ZeroWritePaced prealloc fill + strategy dispatch through pipeline"
```

---

### Task 3: `FallocateZeroRange` fill (A) via `rustix`, Linux-gated with fallback

**Files:**
- Modify: `ultima_journal/Cargo.toml` (add target-gated `rustix`)
- Modify: `ultima_journal/src/journal/segment.rs` (`fill_fallocate_zero_range`, and `preallocate_to`'s fallocate branch offset)
- Test: `ultima_journal/src/journal/segment.rs` (inline `#[cfg(test)]`, Linux-gated)

**Interfaces:**
- Consumes: `fill_zero_write_paced` (Task 2) as the fallback.
- Produces: real `fill_fallocate_zero_range(file: &std::fs::File, total_len: u64, chunk_bytes: u64)` filling `[0, total_len)`; plus an offset-aware path for `preallocate_to`.

- [ ] **Step 1: Add the dependency**

In `ultima_journal/Cargo.toml`, after the `[dependencies]` block add a target-gated section:

```toml
[target.'cfg(target_os = "linux")'.dependencies]
rustix = { version = "1", features = ["fs"] }
```

- [ ] **Step 2: Write the failing test (Linux-gated)**

Add to the `#[cfg(test)]` module in `ultima_journal/src/journal/segment.rs`:

```rust
#[cfg(target_os = "linux")]
#[test]
fn fallocate_zero_range_produces_full_size_all_zero_file() {
    use crate::PreallocFill;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("seg-prealloc.1.tmp");
    let total: u64 = 2 * 1024 * 1024 + 13;
    // On a filesystem without ZERO_RANGE support this falls back to paced;
    // either way the postcondition (full-size, all zeros) must hold.
    SegmentFile::create_prealloc_temp(&p, total, PreallocFill::FallocateZeroRange, 4 * 1024 * 1024).unwrap();
    let meta = std::fs::metadata(&p).unwrap();
    assert_eq!(meta.len(), total);
    let bytes = std::fs::read(&p).unwrap();
    assert!(bytes.iter().all(|&b| b == 0));
}
```

- [ ] **Step 3: Run test to verify it fails (or is a no-op pre-impl)**

Run: `cargo test -p ultima-journal fallocate_zero_range`
Expected: with the Task-2 placeholder, this test actually PASSES (placeholder delegates to paced). That is acceptable — the point of this task is to make the fallocate path *real*. Proceed; Step 5 verifies the real path runs.

- [ ] **Step 4: Implement the real fallocate fill**

Replace the placeholder `fill_fallocate_zero_range` in `segment.rs` with:

```rust
/// `fallocate(ZERO_RANGE)` over `[0, total_len)` + one `sync_data`. Linux only;
/// on non-Linux or `OPNOTSUPP`/`NOSYS` falls back to the paced zero-write.
fn fill_fallocate_zero_range(
    file: &std::fs::File,
    total_len: u64,
    chunk_bytes: u64,
) -> Result<(), JournalError> {
    fallocate_zero_range_at(file, 0, total_len, chunk_bytes)
}

/// Offset-aware ZERO_RANGE fill of `[offset, offset+len)`, used by both
/// `create_prealloc_temp` (offset 0) and `preallocate_to` (offset = cursor).
fn fallocate_zero_range_at(
    file: &std::fs::File,
    offset: u64,
    len: u64,
    chunk_bytes: u64,
) -> Result<(), JournalError> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{fallocate, FallocateFlags};
        use rustix::io::Errno;
        match fallocate(file, FallocateFlags::ZERO_RANGE, offset, len) {
            Ok(()) => {
                file.sync_data()?;
                return Ok(());
            }
            // Filesystem/kernel without ZERO_RANGE: fall back to paced.
            Err(Errno::OPNOTSUPP) | Err(Errno::NOSYS) => {}
            Err(e) => return Err(JournalError::Io(std::io::Error::from(e))),
        }
    }
    // Fallback (non-Linux, or unsupported): paced zero-write of [offset, offset+len).
    // The caller has already seeked to `offset` for the preallocate_to path; for
    // create_prealloc_temp offset is 0 and the fresh file is already at 0.
    let _ = offset;
    fill_zero_write_paced(file, len, chunk_bytes)
}
```

Then update `preallocate_to`'s `FallocateZeroRange` branch (added in Task 2) to use the offset-aware helper so it fills the correct tail range:

```rust
            PreallocFill::FallocateZeroRange => {
                fallocate_zero_range_at(&self.file, self.size, total_len - self.size, chunk_bytes)?
            }
```

(For the fallback inside `fallocate_zero_range_at`, the `preallocate_to` path has already `seek`ed to `self.size`, so `fill_zero_write_paced` writes the tail correctly; the `rustix` path is absolute-offset so it is correct regardless of the seek.)

- [ ] **Step 5: Verify the real path runs + full suite**

Run: `cargo test -p ultima-journal && cargo clippy -p ultima-journal -- -D warnings`
Expected: all tests PASS (including `fallocate_zero_range_produces_full_size_all_zero_file` exercising the real syscall on the Linux CI host); clippy clean.

- [ ] **Step 6: Commit**

```bash
git add ultima_journal/Cargo.toml ultima_journal/src/journal/segment.rs
git commit -m "feat(journal): FallocateZeroRange prealloc fill via rustix (Linux, falls back to paced)"
```

---

### Task 4: Wire `UC_JOURNAL_PREALLOC_FILL` (cluster + bench)

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs` (env parse + the `JournalConfig` literal)
- Modify: `uc_autobench/src/journal_bench.rs` (`fresh_cfg` selects strategy)
- Test: `uc_node/src/raft/log_storage.rs` (inline `#[cfg(test)]` for the parse fn)

**Interfaces:**
- Consumes: `ultima_journal::PreallocFill` (Task 1).
- Produces: `fn parse_prealloc_fill(s: Option<&str>) -> PreallocFill` (uc_node) mapping `"paced"`/`"fallocate"`/else; the cluster journal now honors `UC_JOURNAL_PREALLOC_FILL`; the journal microbench's preallocated arms honor it too.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `uc_node/src/raft/log_storage.rs`:

```rust
#[test]
fn parse_prealloc_fill_maps_values() {
    use ultima_journal::PreallocFill;
    assert_eq!(parse_prealloc_fill(Some("paced")), PreallocFill::ZeroWritePaced);
    assert_eq!(parse_prealloc_fill(Some("fallocate")), PreallocFill::FallocateZeroRange);
    assert_eq!(parse_prealloc_fill(Some("full")), PreallocFill::ZeroWriteFull);
    assert_eq!(parse_prealloc_fill(None), PreallocFill::ZeroWriteFull);
    assert_eq!(parse_prealloc_fill(Some("garbage")), PreallocFill::ZeroWriteFull);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_node parse_prealloc_fill_maps_values`
Expected: FAIL to compile — `parse_prealloc_fill` not found.

- [ ] **Step 3: Add the parse helpers**

In `uc_node/src/raft/log_storage.rs`, after `journal_prealloc_from_env` (`:36`), add:

```rust
/// Parse `UC_JOURNAL_PREALLOC_FILL` into the segment fill strategy. Default
/// `ZeroWriteFull` (unset/unknown); `"paced"`/`"fallocate"` select the
/// contention-reducing strategies. Orthogonal to `UC_JOURNAL_PREALLOC` (on/off).
/// Pure helper for unit testing without touching the process env.
fn parse_prealloc_fill(s: Option<&str>) -> ultima_journal::PreallocFill {
    use ultima_journal::PreallocFill;
    match s {
        Some("paced") => PreallocFill::ZeroWritePaced,
        Some("fallocate") => PreallocFill::FallocateZeroRange,
        _ => PreallocFill::ZeroWriteFull,
    }
}

fn journal_prealloc_fill_from_env() -> ultima_journal::PreallocFill {
    parse_prealloc_fill(std::env::var("UC_JOURNAL_PREALLOC_FILL").ok().as_deref())
}
```

- [ ] **Step 4: Use it in the config literal**

In the `JournalConfig { ... }` literal (the `prealloc_fill` field added in Task 1), replace the explicit default with the env read:

```rust
            prealloc_fill: journal_prealloc_fill_from_env(),
            prealloc_fill_chunk_bytes: 4 * 1024 * 1024,
```

- [ ] **Step 5: Wire the bench arms**

In `uc_autobench/src/journal_bench.rs`, in `fresh_cfg` (`:104-118`), set the strategy when preallocating. Replace the body's config setup with:

```rust
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = durability;
    cfg.preallocate_segments = preallocate;
    if preallocate {
        cfg.prealloc_fill = match std::env::var("UC_JOURNAL_PREALLOC_FILL").ok().as_deref() {
            Some("paced") => ultima_journal::PreallocFill::ZeroWritePaced,
            Some("fallocate") => ultima_journal::PreallocFill::FallocateZeroRange,
            _ => ultima_journal::PreallocFill::ZeroWriteFull,
        };
    }
    let j = Journal::open(cfg).unwrap();
    (dir, j)
```

(Confirm `ultima_journal` is a direct dependency of `uc_autobench` — it is, via the existing `use ultima_journal::{Durability, Journal, JournalConfig};` at `journal_bench.rs:11`. Add `PreallocFill` to that `use` if you prefer over the fully-qualified path.)

- [ ] **Step 6: Run tests + build**

Run: `cargo test -p uc_node parse_prealloc_fill_maps_values && cargo build -p uc_autobench`
Expected: test PASS; `uc_autobench` builds clean.

- [ ] **Step 7: Manual smoke — each strategy runs end-to-end (local tmp OK; this checks wiring, not perf)**

Run:
```bash
for f in full paced fallocate; do
  echo "== $f =="; AUTOBENCH_QUICK=1 UC_JOURNAL_PREALLOC_FILL=$f \
    cargo run -p uc_autobench --bin journal-microbench --release -- --json 2>/dev/null \
    | grep -o '"append_consistent_prealloc_p50_ns":[0-9.]*'
done
```
Expected: three lines, one per strategy, each emitting a p50 number (values are not meaningful on a non-NVMe dev box — this only confirms all three strategies open + commit + recover without error).

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_autobench/src/journal_bench.rs
git commit -m "feat(uc_node,journal-bench): wire UC_JOURNAL_PREALLOC_FILL strategy selector"
```

---

## Phase 2 — Fleet A/B (runbook)

> Runbook, not TDD. Deliverable = recorded numbers + accept/reject verdict. Requires the AWS c6id fleet (cost gate — operator confirmation to `make -C bench-infra up`). Reuses the investigation's `perf` instrumentation and `bench-infra` path (node0 build env: `sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup CARGO_TARGET_DIR=/opt/bench/target ...`; data lands on `/opt/bench` NVMe).

### Task 5: Three-way A/B + verdict + report

**Files:**
- Create: `docs/benchmarks/journal-prealloc-fill-ab-2026-06-21.md`

- [ ] **Step 1: Bring the fleet up (COST GATE — confirm with operator)**

Run: `make -C bench-infra up && make -C bench-infra inventory` ; note node0 IP; sync the repo to `/opt/bench/src` (same rsync as the investigation, excluding `target`).

- [ ] **Step 2: Run the microbench under each strategy on the c6id NVMe**

```bash
NODE0=<ip>
for f in full paced fallocate; do
  ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
   "cd /opt/bench/src/ultima_cluster && sudo env CARGO_TARGET_DIR=/opt/bench/target \
     UC_JOURNAL_PREALLOC_FILL=$f UC_JOURNAL_DUMP_PREALLOC=/opt/bench/src/dump-$f.txt \
     /opt/bench/target/release/journal-microbench --json" \
   | tee /tmp/fill-$f.json
done
```
(Build once first via `cargo run -p uc_autobench --bin journal-microbench --release -- --json`.) Record per strategy: `append_consistent_prealloc_p50_ns`, `append_consistent_prealloc_p99_ns`, `group_commit_throughput_prealloc`.

- [ ] **Step 3: Confirm the mechanism with a per-syscall trace for `paced` and `fallocate`**

```bash
for f in paced fallocate; do
  ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
   "cd /opt/bench/src/ultima_cluster && sudo env CARGO_TARGET_DIR=/opt/bench/target \
     UC_JOURNAL_PREALLOC_FILL=$f perf trace --duration 1.0 -e fdatasync,fsync,fallocate \
     /opt/bench/target/release/journal-microbench --json >/dev/null 2>/tmp/trace-$f.txt"
  echo "== $f: per-commit fdatasync >1ms count =="; ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 "grep -c fdatasync /tmp/trace-$f.txt"
done
```
Expected if the fix works: near-zero per-commit `fdatasync > 1 ms` for `paced` (vs `full`'s ~8-sample burst). For `fallocate`, also check whether `fallocate` appears and whether the per-commit fdatasync tail is absent.

- [ ] **Step 4: Apply the pre-registered accept/reject rule**

- `paced` is the shipped fix if its `append_consistent_prealloc_p99` collapses toward the ~57 µs floor and `group_commit_throughput_prealloc` ≥ `full` baseline.
- `fallocate` (A) is accepted as the winner **only if** its p99 collapses like `paced`'s **and** `group_commit_throughput_prealloc` ≥ `full` baseline (proves overwrites are metadata-free). Otherwise reject A → ship `paced`.

- [ ] **Step 5: Write the report + recommend the default flip**

Create `docs/benchmarks/journal-prealloc-fill-ab-2026-06-21.md` with the three-way table (p50/p99/throughput per strategy), the `perf trace` fdatasync counts, the verdict (which strategy wins), and a recommendation for the follow-up default-flip commit (change `JournalConfig::new`'s `prealloc_fill` default and, if A lost, optionally drop the `rustix` dep).

```bash
git add docs/benchmarks/journal-prealloc-fill-ab-2026-06-21.md
git commit -m "docs(bench): journal prealloc fill A/B results + default-strategy recommendation"
```

- [ ] **Step 6: Tear down (COST GATE)**

Run: `make -C bench-infra destroy` ; confirm no instances in `terraform -chdir=bench-infra/terraform state list`.

---

## Self-Review

**Spec coverage:**
- `PreallocFill` enum + config fields + default `ZeroWriteFull` → Task 1. ✓
- `ZeroWritePaced` (chunked `sync_data` + yield, drop temp dir sync) → Task 2. ✓
- `FallocateZeroRange` (rustix ZERO_RANGE + `sync_data`, Linux-gated, fallback to paced) → Task 3. ✓
- Strategy threaded through pipeline + `preallocate_to` consistency → Task 2 (+ offset fix Task 3). ✓
- `rustix` target-gated dep → Task 3. ✓
- `UC_JOURNAL_PREALLOC_FILL=full|paced|fallocate` wiring (cluster + bench) → Task 4. ✓
- Durability/recovery preserved (full-size all-zero file; existing suite under default) → Task 2 Step 7, plus per-strategy postcondition tests in Tasks 2–3. ✓
- A's accept/reject is a fleet decision; default flip deferred → Task 5 + Global Constraints. ✓
- Three-way A/B + report → Task 5. ✓

**Placeholder scan:** The only intentional placeholder is `fill_fallocate_zero_range` in Task 2, explicitly replaced in Task 3 — flagged in both tasks, with complete code in each. No `TODO`/vague steps.

**Type consistency:** `create_prealloc_temp(path, total_len, fill, chunk_bytes)` and `preallocate_to(total_len, fill, chunk_bytes)` are defined in Task 2 and consumed at the Task 2/3 call sites and pipeline (Task 2 Step 5/6) identically. `PreallocFill` variants (`ZeroWriteFull`/`ZeroWritePaced`/`FallocateZeroRange`) are spelled identically in Tasks 1–4. `parse_prealloc_fill(Option<&str>) -> PreallocFill` (Task 4) matches its test. `SegmentPipeline::spawn(dir, segment_size, fill, fill_chunk_bytes)` matches its mod.rs call site. Env var `UC_JOURNAL_PREALLOC_FILL` and values `full|paced|fallocate` consistent across Tasks 4–5.
