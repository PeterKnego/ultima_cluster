# M13a — MPSC ring per-record commit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the cross-producer publish convoy from `uc_protocol::ring::mpsc` — the defect that drops the ingress ring from 1.9 M/s to ~5 k/s as soon as producer threads outnumber free cores — by giving every producer its own per-record commit word, and make the dead-producer hole that this exposes *detectable and bounded* instead of a silent wedge.

**Architecture:** A producer CAS-claims a byte range on `claim_position` (unchanged, still bounded by `consumer_position`), stamps the slot's first word `CLAIMED | LAP | advance`, writes the record body, then Release-stores `LAP | total` — and waits for nobody. The single consumer walks records in claim order, deciding each slot from that one word alone (lap mismatch = not ours yet; CLAIMED = head-of-line on exactly that producer; committed = read it), so a preempted producer costs the consumer one thread's scheduling latency once, instead of stalling every other producer. `RingHeader.publish_position` keeps its name and its futex wake-word mechanics but is reinterpreted as `commit_count` on MPSC files; the file magic is bumped so a stale attach is refused rather than misread. SPSC and Broadcast keep the single-producer `publish_position` protocol untouched.

**Tech Stack:** Rust 2024 (workspace edition), stable 1.96.0 pinned / MSRV 1.89, `memmap2` file-backed shared memory, `crc32fast`, `loom` 0.7 under `cfg(loom)` (dev-dependency only), `cargo-fuzz` + `libfuzzer-sys` on nightly in the out-of-workspace `fuzz/` crate.

**Spec:** docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md (§4 and §4.2; background: `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`)

## Global Constraints

- MSRV is **1.89** (`cargo clippy --workspace --all-targets --locked -- -D warnings` must pass on 1.89 in CI's `msrv` job); local dev builds on the **1.96.0** pinned in `rust-toolchain.toml`. Nothing in this plan may use an API newer than 1.89 — `AtomicU32::from_ptr` (1.75) and `std::time::Instant` are the newest things touched.
- `cargo clippy --workspace --all-targets -- -D warnings` must be clean after **every** task; a warning is a failure.
- **Never write scratch, test or bench artifacts to `/tmp`** — it is RAM-backed tmpfs with no swap on this box. Ring test files go through `tempfile::NamedTempFile`/`tempdir()` seeded from `env!("CARGO_TARGET_TMPDIR")` exactly as the existing tests do; the hop-bench smoke writes under `/home/claude/`.
- cnc page offsets are pinned in **both** `uc_protocol::v2::cnc` and `uc_log::cnc`, each with its own offset-assertion test; the one new field goes in the reserved band at **3968** and both tests grow a line.
- **No consensus, node↔node wire-protocol or cnc-layout change** beyond that one reserved-band field. `version::CURRENT` and `CNC_V2_VERSION` are untouched. The *ring file* format changes (new magic), which is a same-host restart, not a wire flag day.
- The pinned public API — `MpscRing::{create, open, into_split, file_len}`, `MpscProducer::try_write`, `MpscConsumer::{try_read, wait_handle}`, `RingHeader`'s field names and `RING_HEADER_LEN` — does not change shape. Everything else in this plan is additive.
- Commit after every task with a conventional message (`feat(ring):`, `test(ring):`, `docs(ring):`, …). One task, one commit.
- SPSC and Broadcast must come out of this byte-identical in behaviour. Task 1 is the only task that touches their code path, and it is a pure refactor with the diff shown.

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `uc_protocol/src/magic.rs` | Modify | Adds `RING_MPSC_MAGIC` (`ULTRNG2\0`), the per-record-commit ring file magic. |
| `uc_protocol/src/ring/common.rs` | Modify | Commit-word encode/decode/classify, atomic word load/store helpers, the safe `decode_record_slice`, `RingError::Wedged`, `BadMagic` → `MagicMismatch`, magic-parameterised header init/validate, and the `write_record_at`/`write_padding_marker_at` body split. |
| `uc_protocol/src/ring/mpsc.rs` | Modify | The producer claim/commit split (no cross-producer wait, `PUBLISH_SPINS_BEFORE_YIELD` deleted), the lap-checking consumer, the hole timer, `holes_skipped`, `set_hole_timeout`/`hole_timeout`, the `RING_MPSC_MAGIC` create/open, and the new tests. |
| `uc_protocol/src/ring/mod.rs` | Modify | Re-exports the new public items. |
| `uc_protocol/Cargo.toml` | Modify | `[lints.rust] unexpected_cfgs` for `cfg(loom)` + the `cfg(loom)` dev-dependency on `loom = "0.7"`, mirroring `uc_log`. |
| `uc_protocol/tests/loom_mpsc.rs` | Create | The loom model of claim/commit/consume with two producers (the loom-on-rings item the M12d security package named). |
| `fuzz/fuzz_targets/ring_mpsc_record.rs` | Create | Fuzz target over the consumer's decision + decode path on arbitrary commit words and slot bytes. |
| `fuzz/src/seeds.rs` | Modify | `seeds::ring_mpsc_record()` — deterministic seeds built with the real record writer. |
| `fuzz/src/bin/seed_corpus.rs` | Modify | One `write_target` call for the new target. |
| `fuzz/Cargo.toml` | Modify | The `[[bin]]` block for `ring_mpsc_record`. |
| `fuzz/corpus/ring_mpsc_record/*` | Create (generated) | The committed seed corpus — exactly the generator's output. |
| `fuzz/README.md` | Modify | Target table row. |
| `.github/workflows/nightly.yml` | Modify | `FUZZ_GROUPS` gains the 15th target; the `loom` job gains the `uc_protocol` model. |
| `uc_protocol/src/v2/cnc.rs` | Modify | `CNC_OFF_INGRESS_HOLES_SKIPPED = 3968` + its offset assertion. |
| `uc_log/src/cnc.rs` | Modify | `ingress_holes_skipped()`/`store_ingress_holes_skipped()` accessors + offset assertion + round-trip test. |
| `uc_node/src/node.rs` | Modify | `Wedged` → named consensus fail-stop; publish `holes_skipped` to the cnc field each drain cycle; the log line. |
| `uc_node/src/obs/metrics.rs` | Modify | `uc2_ingress_holes_skipped_total` in `CONTRACT_SERIES` and the renderer, plus its test. |
| `docs/reference/instance-directory.md` | Modify | The ring-file format note (magic, restart-together rule). |
| `docs/ops/uc2-runbook.md` | Modify | One bullet under "Running a cluster" pointing at the restart-together rule. |
| `docs/how-to/upgrade-a-cluster.md` | Modify | "Ring format change in 2.7.0" section. |
| `docs/VERIFICATION.md` | Modify | §6 (loom now covers the MPSC ring protocol), §7 (fifteenth target), §11 (the "rings are covered by nothing" claim, corrected). |

---

### Task 1: Commit-word helpers, the record-body split, and the new error/magic surface

Pure additions plus one mechanical refactor, in `common.rs`. Nothing calls the new helpers yet, so SPSC/Broadcast/MPSC behaviour is unchanged at the end of this task — that is the point: the refactor lands green before any protocol changes.

**Files:**
- Modify `uc_protocol/src/magic.rs` (4 lines — append one const)
- Modify `uc_protocol/src/ring/common.rs` (lines 316–334 the `RingError` enum; 427–491 `init_ring_header`/`validate_ring_header`; 493–572 `write_record_at`/`write_padding_marker_at`; new code appended before `#[cfg(test)] mod tests` at line 680; new tests inside that module)
- Modify `uc_protocol/src/ring/mod.rs` (lines 21–24, the `pub use common::{…}` list)

**Interfaces:**

Consumes: `RECORD_ALIGN`, `align_record_size`, `FRAME_HEADER_LEN`, `FRAME_TRAILER_LEN`, `PADDING_MSG_TYPE`, `RecordHeader`, `crc32fast::hash`.

Produces:
```rust
pub const COMMIT_CLAIMED: u32 = 1 << 31;
pub const COMMIT_LAP_SHIFT: u32 = 18;
pub const COMMIT_LAP_MASK: u32 = 0x1FFF;
pub const COMMIT_LEN_MASK: u32 = 0x3_FFFF;
pub const MPSC_MAX_RECORD_BYTES: usize = COMMIT_LEN_MASK as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState { Empty, Claimed { advance: u32 }, Committed { length: u32 } }

pub const fn lap_of(pos: u64, capacity: usize) -> u32;
pub const fn encode_commit_word(lap: u32, len: u32, claimed: bool) -> u32;
pub const fn classify_commit_word(word: u32, expected_lap: u32) -> SlotState;
pub unsafe fn load_commit_word(slot_region: *const u8, slot_offset: usize) -> u32;
pub unsafe fn store_commit_word(slot_region: *mut u8, slot_offset: usize, word: u32, ord: Ordering);
pub unsafe fn write_record_body_at(slot_region: *mut u8, slot_offset: usize, msg_type: u16, flags: u16, header_extra: [u8; 8], payload: &[u8]);
pub unsafe fn write_padding_body_at(slot_region: *mut u8, slot_offset: usize);
pub fn decode_record_slice(slot: &[u8], payload_buf: &mut Vec<u8>) -> Result<(RecordHeader, usize), RingError>;
pub fn init_ring_header_with_magic(buf: &mut [u8], capacity_bytes: u64, max_msg_size: u32, msg_kind_filter: u32, magic: [u8; 8]) -> Result<(), RingError>;
pub fn validate_ring_header_with_magic(buf: &[u8], magic: [u8; 8]) -> Result<&RingHeader, RingError>;
// RingError gains:  MagicMismatch (renamed from BadMagic),  Wedged { position: u64 }
// magic.rs gains:   pub const RING_MPSC_MAGIC: [u8; 8] = *b"ULTRNG2\0";  // 8 bytes, like RING_MAGIC
```

### Steps

- [ ] **Write the failing tests first.** Append to `uc_protocol/src/ring/common.rs`'s `#[cfg(test)] mod tests` (after `init_rejects_undersized_buffer`, line 736):

```rust
    // ---- M13a: the MPSC commit word (spec §4.1) ---------------------------

    #[test]
    fn commit_word_round_trips_every_field() {
        // Max legal values in each field, so a shift/mask error cannot hide.
        let w = encode_commit_word(COMMIT_LAP_MASK, COMMIT_LEN_MASK, false);
        assert_eq!(classify_commit_word(w, COMMIT_LAP_MASK), SlotState::Committed {
            length: COMMIT_LEN_MASK
        });
        let c = encode_commit_word(COMMIT_LAP_MASK, COMMIT_LEN_MASK, true);
        assert_eq!(classify_commit_word(c, COMMIT_LAP_MASK), SlotState::Claimed {
            advance: COMMIT_LEN_MASK
        });
        // The claimed bit is bit 31 and nothing else.
        assert_eq!(c ^ w, COMMIT_CLAIMED);
        // A 64 KiB record — the real `max_msg_size` — fits the length field.
        assert_eq!(classify_commit_word(encode_commit_word(3, 65536, false), 3), SlotState::Committed {
            length: 65536
        });
    }

    #[test]
    fn a_zero_word_and_a_foreign_lap_both_read_as_empty() {
        // A freshly zeroed ring: every slot reads Empty at lap 0.
        assert_eq!(classify_commit_word(0, 0), SlotState::Empty);
        // The previous lap's COMMITTED record still sitting in the slot.
        let prev = encode_commit_word(4, 40, false);
        assert_eq!(classify_commit_word(prev, 5), SlotState::Empty);
        // The previous lap's CLAIMED word — also not ours.
        let prev_claim = encode_commit_word(4, 40, true);
        assert_eq!(classify_commit_word(prev_claim, 5), SlotState::Empty);
        // Lap matches but length is zero and nothing is claimed: impossible
        // from any producer, so the total classifier reads it as Empty
        // (the consumer then waits, and §4.2's wedge timer adjudicates).
        assert_eq!(classify_commit_word(encode_commit_word(5, 0, false), 5), SlotState::Empty);
    }

    #[test]
    fn lap_is_the_position_divided_by_capacity() {
        assert_eq!(lap_of(0, 4096), 0);
        assert_eq!(lap_of(4095, 4096), 0);
        assert_eq!(lap_of(4096, 4096), 1);
        assert_eq!(lap_of(4096 * 8192, 4096), 0, "13 bits wrap at 8192 laps");
        assert_eq!(lap_of(4096 * 8193, 4096), 1);
    }

    #[test]
    fn decode_record_slice_round_trips_the_real_writer() {
        let payload = b"hello ring";
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        let mut slot = vec![0u8; total];
        // SAFETY: `slot` is exactly `total` bytes, exclusively owned here.
        unsafe { write_record_body_at(slot.as_mut_ptr(), 0, 7, 3, [1; 8], payload) };
        slot[..4].copy_from_slice(&encode_commit_word(2, total as u32, false).to_le_bytes());

        let mut buf = Vec::new();
        let (rec, advance) = decode_record_slice(&slot, &mut buf).expect("decodes");
        assert_eq!(rec.msg_type, 7);
        assert_eq!(rec.flags, 3);
        assert_eq!(rec.header_extra, [1; 8]);
        assert_eq!(&buf[..], payload);
        assert_eq!(advance, align_record_size(total));
    }

    #[test]
    fn decode_record_slice_is_total_on_junk() {
        let mut buf = Vec::new();
        // Too short for even a msg_type.
        for n in 0..6usize {
            assert!(decode_record_slice(&vec![0xABu8; n], &mut buf).is_err(), "len {n}");
        }
        // Long enough for a padding marker but not a record: a non-padding
        // msg_type in a 6..20-byte slice is Corrupt, never a panic.
        let mut short = vec![0u8; 8];
        short[4..6].copy_from_slice(&9u16.to_le_bytes());
        assert!(matches!(decode_record_slice(&short, &mut buf), Err(RingError::Corrupt(_))));
        // A corrupt crc is BadCrc, not a panic.
        let payload = b"x";
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        let mut slot = vec![0u8; total];
        // SAFETY: `slot` is exactly `total` bytes, exclusively owned here.
        unsafe { write_record_body_at(slot.as_mut_ptr(), 0, 1, 0, [0; 8], payload) };
        slot[total - 1] ^= 0xFF;
        assert!(matches!(decode_record_slice(&slot, &mut buf), Err(RingError::BadCrc)));
    }

    #[test]
    fn decode_record_slice_reads_a_padding_marker() {
        let mut slot = vec![0u8; 24];
        // SAFETY: `slot` is 24 bytes >= the 6 the padding body writes.
        unsafe { write_padding_body_at(slot.as_mut_ptr(), 0) };
        slot[..4].copy_from_slice(&encode_commit_word(0, 24, false).to_le_bytes());
        let mut buf = Vec::new();
        let (rec, advance) = decode_record_slice(&slot, &mut buf).expect("padding decodes");
        assert_eq!(rec.msg_type, PADDING_MSG_TYPE);
        assert_eq!(advance, 24, "padding advances by its whole length");
    }

    #[test]
    fn a_ring_header_written_with_one_magic_is_refused_by_the_other() {
        let (mut mmap, _tmp) = mmap_buf(RING_HEADER_LEN * 2);
        init_ring_header_with_magic(&mut mmap[..], 4096, 1024, 0, crate::magic::RING_MPSC_MAGIC)
            .expect("init");
        assert!(matches!(
            validate_ring_header_with_magic(&mmap[..], crate::magic::RING_MAGIC),
            Err(RingError::MagicMismatch)
        ));
        assert!(
            validate_ring_header_with_magic(&mmap[..], crate::magic::RING_MPSC_MAGIC).is_ok()
        );
    }
```

- [ ] Run them and confirm the expected failure: `cargo test -p uc_protocol ring::common 2>&1 | tail -30`. Expected: **compile errors**, not assertion failures — `cannot find function `encode_commit_word` in this scope`, `cannot find type `SlotState``, `no variant or associated item named `MagicMismatch` found for enum `RingError``, `cannot find value `RING_MPSC_MAGIC` in module `crate::magic``, and the same for `lap_of`, `classify_commit_word`, `write_record_body_at`, `write_padding_body_at`, `decode_record_slice`, `init_ring_header_with_magic`, `validate_ring_header_with_magic`.

- [ ] **Add the magic.** Append to `uc_protocol/src/magic.rs`:

```rust
/// MPSC ring files (M13a). Distinct from [`RING_MAGIC`] because the MPSC
/// per-record-commit protocol reinterprets the slot's first word and the
/// header's `publish_position` — an old-format file mapped by a new binary
/// (or the reverse) would misread every slot, so the attach is refused
/// instead. SPSC and Broadcast keep [`RING_MAGIC`].
pub const RING_MPSC_MAGIC: [u8; 8] = *b"ULTRNG2\0";
```

- [ ] **Rename the magic error.** In `uc_protocol/src/ring/common.rs` line 328–329, `BadMagic` → `MagicMismatch` (the `#[error("magic mismatch")]` string is already right), and update the two other occurrences (line 488 `return Err(RingError::BadMagic);`, line 721 in the test). Confirm nothing else in the workspace names it: `grep -rn "BadMagic" --include=*.rs . | grep -v '^./target'` must print nothing after the edit. **Note:** the pinned-interface list calls this variant `MagicMismatch`; it is the same error the tree spells `BadMagic` today, and renaming (3 call sites, all in this file) is how the pinned name is honoured without adding a duplicate variant.

- [ ] **Add the `Wedged` variant.** In the same enum, after `Overwritten`:

```rust
    /// The consumer is head-of-line behind a claim whose claim word never
    /// appeared (spec §4.2: a producer killed between its CAS on
    /// `claim_position` and its claim-word store — a window of nanoseconds).
    /// The hole's length is unknowable, so the ring refuses to guess: the
    /// caller fail-stops. Strictly better than the pre-M13a behaviour, where
    /// the same death wedged every producer and the consumer silently.
    #[error("ingress ring wedged at position {position}: an unsized claim hole outlived the hole timeout")]
    Wedged { position: u64 },
```

- [ ] **Add the commit-word module section.** Insert into `common.rs` immediately before `#[cfg(test)] mod tests` (line 680):

```rust
// ---- MPSC per-record commit word (M13a; spec §4.1) ------------------------
//
// The first word of every MPSC slot. It replaces the pre-M13a "length, 0 =
// uncommitted" convention and carries three fields:
//
//   bit 31      CLAIMED   set between claim and commit
//   bits 18-30  LAP       (record_start_pos / capacity) & 0x1FFF
//   bits 0-17   LENGTH    total record bytes (claim word: the claimed advance)
//
// The lap is what makes the consumer's read of a stale slot unambiguous
// WITHOUT the consumer ever writing into the ring: the bounded claim means a
// producer only overwrites a slot the consumer has already consumed, so the
// only stale value the consumer can meet is an OLDER lap's committed word,
// which fails lap equality. 13 bits is unambiguous because the consumer can
// never be 8192 laps behind a claim — the bound is one lap.

/// Bit 31: the slot is claimed by a producer that has not committed yet.
pub const COMMIT_CLAIMED: u32 = 1 << 31;
/// Bits 18-30 hold the lap.
pub const COMMIT_LAP_SHIFT: u32 = 18;
/// 13-bit lap field.
pub const COMMIT_LAP_MASK: u32 = 0x1FFF;
/// 18-bit length field.
pub const COMMIT_LEN_MASK: u32 = 0x3_FFFF;
/// Largest record an MPSC ring can carry: the length field's ceiling.
/// `MpscRing::create` refuses a `max_msg_size` whose aligned size exceeds it.
pub const MPSC_MAX_RECORD_BYTES: usize = COMMIT_LEN_MASK as usize;

/// What the consumer found in a slot's commit word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Nothing of ours here: an untouched slot, an older lap's leftovers, or
    /// a claim whose word has not landed yet.
    Empty,
    /// A producer claimed `advance` bytes here and has not committed.
    Claimed { advance: u32 },
    /// A committed record of `length` bytes (unaligned; advance by
    /// [`align_record_size`]).
    Committed { length: u32 },
}

/// The lap a byte position belongs to.
#[inline]
pub const fn lap_of(pos: u64, capacity: usize) -> u32 {
    ((pos / capacity as u64) as u32) & COMMIT_LAP_MASK
}

/// Pack a commit word.
#[inline]
pub const fn encode_commit_word(lap: u32, len: u32, claimed: bool) -> u32 {
    let base = ((lap & COMMIT_LAP_MASK) << COMMIT_LAP_SHIFT) | (len & COMMIT_LEN_MASK);
    if claimed { base | COMMIT_CLAIMED } else { base }
}

/// Decide what a slot holds. Total on every `u32`.
#[inline]
pub const fn classify_commit_word(word: u32, expected_lap: u32) -> SlotState {
    if word == 0 {
        return SlotState::Empty;
    }
    if (word >> COMMIT_LAP_SHIFT) & COMMIT_LAP_MASK != expected_lap & COMMIT_LAP_MASK {
        return SlotState::Empty;
    }
    let len = word & COMMIT_LEN_MASK;
    if word & COMMIT_CLAIMED != 0 {
        SlotState::Claimed { advance: len }
    } else if len == 0 {
        // No producer can write this (a commit always carries a length).
        SlotState::Empty
    } else {
        SlotState::Committed { length: len }
    }
}

/// Acquire-load a slot's commit word.
///
/// # Safety
///
/// `slot_region + slot_offset` must be a mapped, 4-byte-aligned address
/// inside the slot region. Both hold by construction: the region is
/// page-aligned and every position advances in [`RECORD_ALIGN`] steps.
#[inline]
pub unsafe fn load_commit_word(slot_region: *const u8, slot_offset: usize) -> u32 {
    let p = unsafe { slot_region.add(slot_offset) }.cast::<AtomicU32>();
    unsafe { (*p).load(Ordering::Acquire) }
}

/// Store a slot's commit word. `Release` publishes the record; `Relaxed` is
/// for the claim stamp (nothing depends on it being ordered).
///
/// # Safety
///
/// Same as [`load_commit_word`], plus: the caller owns the claimed range.
#[inline]
pub unsafe fn store_commit_word(
    slot_region: *mut u8,
    slot_offset: usize,
    word: u32,
    ord: Ordering,
) {
    let p = unsafe { slot_region.add(slot_offset) }.cast::<AtomicU32>();
    unsafe { (*p).store(word, ord) }
}

/// Decode one record from `slot` — exactly the record's own bytes, commit
/// word included at `slot[0..4]`. Total on any input: every access is
/// bounds-checked and the crc32 is verified. Returns the header and the
/// number of bytes to advance the consumer position by.
///
/// Safe by construction (a slice, not a pointer), which is what makes the
/// `ring_mpsc_record` fuzz target possible.
pub fn decode_record_slice(
    slot: &[u8],
    payload_buf: &mut Vec<u8>,
) -> Result<(RecordHeader, usize), RingError> {
    if slot.len() < 6 {
        return Err(RingError::Corrupt(format!("record slice too short: {}", slot.len())));
    }
    let msg_type = u16::from_le_bytes([slot[4], slot[5]]);
    if msg_type == PADDING_MSG_TYPE {
        // Padding length is a multiple of RECORD_ALIGN by construction.
        return Ok((
            RecordHeader { msg_type, flags: 0, header_extra: [0; 8] },
            slot.len(),
        ));
    }
    if slot.len() < FRAME_HEADER_LEN + FRAME_TRAILER_LEN {
        return Err(RingError::Corrupt(format!("record length {} too small", slot.len())));
    }
    let flags = u16::from_le_bytes([slot[6], slot[7]]);
    let mut header_extra = [0u8; 8];
    header_extra.copy_from_slice(&slot[8..FRAME_HEADER_LEN]);
    let payload_end = slot.len() - FRAME_TRAILER_LEN;
    let crc_actual = u32::from_le_bytes(
        slot[payload_end..].try_into().expect("FRAME_TRAILER_LEN bytes remain"),
    );
    if crc32fast::hash(&slot[4..payload_end]) != crc_actual {
        return Err(RingError::BadCrc);
    }
    payload_buf.clear();
    payload_buf.extend_from_slice(&slot[FRAME_HEADER_LEN..payload_end]);
    Ok((
        RecordHeader { msg_type, flags, header_extra },
        align_record_size(slot.len()),
    ))
}
```

- [ ] **Split the record writers (pure refactor — behaviour identical).** Replace the body of `write_record_at` (lines 506–544) and `write_padding_marker_at` (553–572) so the length store is the only thing left in them, and add the two body writers. The diff, exactly:

```rust
// BEFORE (write_record_at's body):  one function doing header+payload+crc+length
// AFTER: `write_record_body_at` does header+payload+crc; `write_record_at`
//        calls it and then writes the length word, byte-for-byte as before.

/// Write everything in a record EXCEPT its first 4-byte word: `msg_type`,
/// `flags`, `header_extra`, the payload and the crc32 trailer. The caller
/// publishes the record by writing that word (a plain length for
/// SPSC/Broadcast, a commit word for MPSC).
///
/// # Safety
///
/// Same contract as [`write_record_at`].
pub unsafe fn write_record_body_at(
    slot_region: *mut u8,
    slot_offset: usize,
    msg_type: u16,
    flags: u16,
    header_extra: [u8; 8],
    payload: &[u8],
) {
    let dst = unsafe { slot_region.add(slot_offset) };
    unsafe {
        // bytes 4..6 — msg_type
        std::ptr::copy_nonoverlapping((&msg_type as *const u16).cast::<u8>(), dst.add(4), 2);
        // bytes 6..8 — flags
        std::ptr::copy_nonoverlapping((&flags as *const u16).cast::<u8>(), dst.add(6), 2);
        // bytes 8..16 — header_extra
        std::ptr::copy_nonoverlapping(header_extra.as_ptr(), dst.add(8), 8);
        // payload
        std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(FRAME_HEADER_LEN), payload.len());
        // crc32 over (msg_type..end-of-payload)
        let crc_input =
            std::slice::from_raw_parts(dst.add(4), FRAME_HEADER_LEN - 4 + payload.len());
        let crc = crc32fast::hash(crc_input);
        let crc_bytes = crc.to_le_bytes();
        std::ptr::copy_nonoverlapping(
            crc_bytes.as_ptr(),
            dst.add(FRAME_HEADER_LEN + payload.len()),
            4,
        );
    }
}

pub unsafe fn write_record_at(
    slot_region: *mut u8,
    slot_offset: usize,
    msg_type: u16,
    flags: u16,
    header_extra: [u8; 8],
    payload: &[u8],
    total_record_size: usize,
) {
    unsafe {
        write_record_body_at(slot_region, slot_offset, msg_type, flags, header_extra, payload);
        // Write length last (legacy "length != 0 means committed" guard). No
        // Release fence is needed here: the caller advances `publish_position`
        // with a Release store after this function returns. (SPSC/Broadcast
        // only — MPSC publishes with a commit word instead.)
        let len_bytes = (total_record_size as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), slot_region.add(slot_offset), 4);
    }
}

/// Write a tail-wrap padding marker's `msg_type` only (bytes 4..6). The
/// caller writes the first word.
///
/// # Safety
///
/// Same as [`write_record_at`]; the slot must have at least 6 bytes.
pub unsafe fn write_padding_body_at(slot_region: *mut u8, slot_offset: usize) {
    let dst = unsafe { slot_region.add(slot_offset) };
    unsafe {
        std::ptr::copy_nonoverlapping(
            (&PADDING_MSG_TYPE as *const u16).cast::<u8>(),
            dst.add(4),
            2,
        );
    }
}

pub unsafe fn write_padding_marker_at(
    slot_region: *mut u8,
    slot_offset: usize,
    padding_bytes: usize,
) {
    unsafe {
        write_padding_body_at(slot_region, slot_offset);
        let len_bytes = (padding_bytes as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), slot_region.add(slot_offset), 4);
    }
}
```

- [ ] **Parameterise the header magic (pure refactor).** Replace `init_ring_header`/`validate_ring_header` with wrappers:

```rust
pub fn init_ring_header(
    buf: &mut [u8],
    capacity_bytes: u64,
    max_msg_size: u32,
    msg_kind_filter: u32,
) -> Result<(), RingError> {
    init_ring_header_with_magic(
        buf,
        capacity_bytes,
        max_msg_size,
        msg_kind_filter,
        crate::magic::RING_MAGIC,
    )
}

/// As [`init_ring_header`], with the file magic chosen by the caller.
/// SPSC/Broadcast pass `RING_MAGIC`; MPSC passes `RING_MPSC_MAGIC` (M13a).
pub fn init_ring_header_with_magic(
    buf: &mut [u8],
    capacity_bytes: u64,
    max_msg_size: u32,
    msg_kind_filter: u32,
    magic: [u8; 8],
) -> Result<(), RingError> {
    /* body unchanged from today's init_ring_header, except `magic:
       crate::magic::RING_MAGIC` in the struct literal becomes `magic,` */
}

pub fn validate_ring_header(buf: &[u8]) -> Result<&RingHeader, RingError> {
    validate_ring_header_with_magic(buf, crate::magic::RING_MAGIC)
}

/// As [`validate_ring_header`], with the expected magic chosen by the caller.
pub fn validate_ring_header_with_magic(
    buf: &[u8],
    magic: [u8; 8],
) -> Result<&RingHeader, RingError> {
    /* body unchanged, except the comparison is against `magic` and the error
       is `RingError::MagicMismatch` */
}
```

- [ ] **Re-export.** In `uc_protocol/src/ring/mod.rs`, extend the `pub use common::{…}` list with `SlotState, classify_commit_word, decode_record_slice, encode_commit_word, lap_of, COMMIT_CLAIMED, COMMIT_LAP_MASK, COMMIT_LAP_SHIFT, COMMIT_LEN_MASK, MPSC_MAX_RECORD_BYTES` (keep the list alphabetised the way it is today).

- [ ] Run the tests: `cargo test -p uc_protocol 2>&1 | tail -20`. Expected: all 73 existing `uc_protocol` tests plus the 7 new ones pass — `test result: ok. 80 passed; 0 failed`. The SPSC/Broadcast/MPSC round-trip tests passing here is the evidence that the writer split is a pure refactor.

- [ ] Lint: `cargo clippy --workspace --all-targets -- -D warnings` → no output, exit 0.

- [ ] Commit:
```bash
git add uc_protocol/src/magic.rs uc_protocol/src/ring/common.rs uc_protocol/src/ring/mod.rs
git commit -m "feat(ring): commit-word helpers, record-body split, RING_MPSC_MAGIC, RingError::Wedged

Pure additions plus two mechanical refactors (write_record_at and the ring
header init/validate split by magic). No protocol behaviour changes yet:
SPSC, Broadcast and MPSC all still publish exactly as before. M13a spec §4.1."
```

---

### Task 2: The per-record-commit producer and lap-checking consumer

The protocol change itself. At the end of this task the convoy is gone, the four existing MPSC tests still pass, and an old-format ring file is refused.

**Files:**
- Modify `uc_protocol/src/ring/mpsc.rs` (module doc 1–19; delete `PUBLISH_SPINS_BEFORE_YIELD` 34–38; `MpscProducer::try_write` 106–252; `MpscConsumer` 100–104 and 254–293; `MpscRing::{create,open,into_split}` 299–353; tests 355–545)

**Interfaces:**

Consumes: everything Task 1 produced, plus `RingHeader::{signal, publish_position, claim_position, consumer_position}`.

Produces (public API — `try_write`/`try_read`/`into_split`/`create`/`open` signatures **unchanged**):
```rust
#[doc(hidden)]
pub struct PendingClaim { /* private fields */ }
impl MpscProducer {
    pub fn try_write(&self, msg_type: u16, flags: u16, header_extra: [u8; 8], payload: &[u8]) -> Result<(), RingError>;
    #[doc(hidden)] pub fn claim_without_commit(&self, msg_type: u16, flags: u16, header_extra: [u8; 8], payload: &[u8]) -> Result<PendingClaim, RingError>;
    #[doc(hidden)] pub fn commit_claim(&self, claim: PendingClaim);
}
impl MpscConsumer {
    pub fn try_read(&mut self, payload_buf: &mut Vec<u8>) -> Result<Option<RecordHeader>, RingError>;
    pub fn wait_handle(&self) -> RingWaitHandle;
}
```

### Steps

- [ ] **Write the failing test first** (the old-format refusal — the preemption and hole tests are Tasks 3 and 4). Add to `mpsc.rs`'s test module:

```rust
    /// M13a: an MPSC ring file written by a pre-M13a binary carries
    /// `RING_MAGIC`, and its slots use the old "length, 0 = uncommitted"
    /// word with publication ordered by `publish_position`. A new binary
    /// that mapped it would misread every slot, so the attach is refused.
    /// This is the whole reason the magic was bumped — the operator-visible
    /// consequence is "restart node, service, gateway and clients on a host
    /// together" (docs/how-to/upgrade-a-cluster.md).
    #[test]
    fn an_old_format_ring_file_is_refused_on_open() {
        let tmp = NamedTempFile::new().unwrap();
        // Build a file with the OLD magic, exactly as a pre-M13a
        // `MpscRing::create` would have.
        let file = crate::ring::common::create_shared_backing_file(
            tmp.path(),
            (RING_HEADER_LEN + 4096) as u64,
        )
        .unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        crate::ring::common::init_ring_header_with_magic(
            &mut mmap[..],
            4096,
            1024,
            0,
            crate::magic::RING_MAGIC,
        )
        .unwrap();
        drop(mmap);

        assert!(matches!(MpscRing::open(tmp.path()), Err(RingError::MagicMismatch)));

        // And the reverse direction is covered too: a file this binary
        // creates carries the new magic.
        let fresh = NamedTempFile::new().unwrap();
        MpscRing::create(fresh.path(), 4096, 1024).expect("create");
        let bytes = std::fs::read(fresh.path()).unwrap();
        assert_eq!(&bytes[..8], &crate::magic::RING_MPSC_MAGIC[..]);
    }

    /// M13a: the commit word's LENGTH field is 18 bits, so a ring whose
    /// `max_msg_size` cannot be expressed is refused at creation rather than
    /// silently truncating a length at runtime.
    #[test]
    fn create_refuses_a_max_msg_size_the_commit_word_cannot_hold() {
        let tmp = NamedTempFile::new().unwrap();
        let too_big = (MPSC_MAX_RECORD_BYTES + 1) as u32;
        assert!(matches!(
            MpscRing::create(tmp.path(), 1 << 20, too_big),
            Err(RingError::Corrupt(_))
        ));
        // The real node's 64 KiB is comfortably inside the field.
        let ok = NamedTempFile::new().unwrap();
        MpscRing::create(ok.path(), 1 << 20, 64 << 10).expect("64 KiB max_msg_size is legal");
    }
```
(Add `use memmap2::MmapMut;` and `use super::super::common::MPSC_MAX_RECORD_BYTES;` to the test module's imports as needed.)

- [ ] Run and confirm the failure: `cargo test -p uc_protocol ring::mpsc 2>&1 | tail -20`. Expected: `an_old_format_ring_file_is_refused_on_open` fails with `assertion failed: matches!(MpscRing::open(tmp.path()), Err(RingError::MagicMismatch))` (today's `open` accepts `RING_MAGIC`), and `create_refuses_a_max_msg_size_the_commit_word_cannot_hold` fails because `create` accepts any `max_msg_size`.

- [ ] **Rewrite the module doc** (lines 1–19) — the producer-panic invariant is now false and must not survive:

```rust
//! Many-producer single-consumer ring buffer.
//!
//! # Per-record commit (M13a, spec §4.1)
//!
//! A producer claims a byte range with `compare_exchange_weak` on
//! `claim_position` (bounded by an Acquire load of `consumer_position`, so a
//! claim only ever lands on a slot the consumer has finished with), stamps
//! the slot's first word `CLAIMED | LAP | advance`, writes the record, then
//! Release-stores `LAP | total`. **It waits for no other producer at any
//! step.** The single consumer walks records in claim order and decides each
//! slot from that one word: a foreign lap means nothing of ours is here yet,
//! `CLAIMED` means head-of-line behind exactly that producer (no spin, no
//! burn), and a committed word means read it.
//!
//! This replaces the pre-M13a protocol, where publication was serialized in
//! claim order by an unbounded spin on `publish_position`. That convoyed as
//! soon as producer threads outnumbered free cores: a producer preempted
//! between its CAS and its publish stalled every producer behind it, and the
//! spinners were what kept it off a core. Measured on the fleet: 1.9 M/s to
//! ~5 k/s at 8 gateway connections on 8 vCPU, every core busy
//! (`docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`).
//!
//! `publish_position` keeps its name in [`RingHeader`] (SPSC and Broadcast
//! still use it as a byte position) but on an MPSC file it is a
//! **`commit_count`**: a monotonically increasing count of committed
//! records, bumped once per commit purely so the futex wake word changes.
//! Nothing reads it as a position. The MPSC file magic is
//! [`RING_MPSC_MAGIC`](crate::magic::RING_MPSC_MAGIC) so an old-format file
//! cannot be mapped by mistake.
//!
//! ## Producer death
//!
//! A producer that dies between claim and commit leaves a hole. It is no
//! longer fatal to everyone else: the consumer stops at that one record,
//! and after `hole_timeout` (default 1 s) it skips the claimed range,
//! counts it in [`MpscConsumer::holes_skipped`] and carries on. The one
//! unrecoverable case is a death inside the nanoseconds between the CAS and
//! the claim-word store — the hole's length is then unknowable, and
//! `try_read` returns [`RingError::Wedged`] rather than guessing (spec
//! §4.2).
//!
//! The consumer reads with Relaxed loads on its own `consumer_position`
//! (single reader) and never writes into the slot region at all.
```

- [ ] **Delete `PUBLISH_SPINS_BEFORE_YIELD`** (lines 34–38) and the wait loop in `try_write` (lines 217–242). Nothing replaces the loop: there is no cross-producer wait.

- [ ] **Split the producer into claim + commit.** Replace `impl MpscProducer` with:

```rust
/// A claimed, written, not-yet-committed record. Produced by
/// [`MpscProducer::claim_without_commit`], consumed by
/// [`MpscProducer::commit_claim`].
///
/// **Not API** (`#[doc(hidden)]`): it exists so tests and harnesses can
/// reproduce a preempted or dead producer without killing a process — the
/// state a `SIGKILL` between claim and commit leaves behind. `try_write` is
/// exactly `claim` followed by `commit`, so the hook drives the production
/// path, not a copy of it.
#[doc(hidden)]
#[derive(Debug)]
#[must_use = "a claim that is never committed is a hole the consumer must time out"]
pub struct PendingClaim {
    pos: u64,
    total: usize,
    lap: u32,
}

impl MpscProducer {
    pub fn try_write(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<(), RingError> {
        let claim = self.claim(msg_type, flags, header_extra, payload)?;
        self.commit(claim);
        Ok(())
    }

    /// See [`PendingClaim`]. Test/harness hook.
    #[doc(hidden)]
    pub fn claim_without_commit(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<PendingClaim, RingError> {
        self.claim(msg_type, flags, header_extra, payload)
    }

    /// See [`PendingClaim`]. Test/harness hook.
    #[doc(hidden)]
    pub fn commit_claim(&self, claim: PendingClaim) {
        self.commit(claim)
    }

    /// Claim a slot and write the record into it, leaving the slot's word
    /// CLAIMED. Returns without waiting for any other producer.
    fn claim(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<PendingClaim, RingError> {
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        if total > self.inner.max_msg_size() {
            return Err(RingError::TooLarge { len: total, max: self.inner.max_msg_size() });
        }
        let advance = align_record_size(total);

        let header = self.inner.header();
        let capacity = self.inner.capacity();

        loop {
            let claim_pos = header.claim_position.load(Ordering::Acquire);

            let slot_offset = (claim_pos as usize) & (capacity - 1);
            let bytes_to_tail = capacity - slot_offset;
            let needed = if bytes_to_tail < advance { bytes_to_tail + advance } else { advance };

            // (free-space check unchanged from pre-M13a, including the
            // `saturating_sub` and the cached-consumer-position argument —
            // see the comments retained in place.)
            let mut consumer_pos = self.cached_consumer_pos.get();
            let mut free = capacity.saturating_sub(claim_pos.saturating_sub(consumer_pos) as usize);
            if free < needed {
                consumer_pos = header.consumer_position.load(Ordering::Acquire);
                self.cached_consumer_pos.set(consumer_pos);
                free = capacity.saturating_sub(claim_pos.saturating_sub(consumer_pos) as usize);
                if free < needed {
                    return Err(RingError::Full);
                }
            }

            let claim_size = if bytes_to_tail < advance { bytes_to_tail } else { advance };
            let target_pos = claim_pos + claim_size as u64;
            if header
                .claim_position
                .compare_exchange_weak(claim_pos, target_pos, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue; // raced with another producer; retry
            }

            // CAS succeeded: we own `[slot_offset, slot_offset + claim_size)`.
            let lap = lap_of(claim_pos, capacity);

            if claim_size != advance {
                // Tail straddle. The padding marker has no body to write, so
                // it is claimed, written and committed in one go; then loop
                // to claim the real record after the wrap. No commit_count
                // bump and no `signal`: padding carries nothing a parked
                // consumer needs to wake for, and the real record's commit
                // (one iteration later) does both.
                //
                // SAFETY: exclusive ownership of the claimed range;
                // claim_size == bytes_to_tail >= RECORD_ALIGN >= 6.
                unsafe {
                    store_commit_word(
                        self.inner.slot_region_mut(),
                        slot_offset,
                        encode_commit_word(lap, claim_size as u32, true),
                        Ordering::Relaxed,
                    );
                    write_padding_body_at(self.inner.slot_region_mut(), slot_offset);
                    store_commit_word(
                        self.inner.slot_region_mut(),
                        slot_offset,
                        encode_commit_word(lap, claim_size as u32, false),
                        Ordering::Release,
                    );
                }
                continue;
            }

            // Stamp the claim BEFORE writing the body: it is what lets a dead
            // producer's hole be sized (spec §4.2). Relaxed is enough — the
            // consumer that observes it only ever decides "not yet", and the
            // commit's Release is what orders the body.
            //
            // SAFETY: exclusive ownership of the claimed range.
            unsafe {
                store_commit_word(
                    self.inner.slot_region_mut(),
                    slot_offset,
                    encode_commit_word(lap, advance as u32, true),
                    Ordering::Relaxed,
                );
                write_record_body_at(
                    self.inner.slot_region_mut(),
                    slot_offset,
                    msg_type,
                    flags,
                    header_extra,
                    payload,
                );
            }
            return Ok(PendingClaim { pos: claim_pos, total, lap });
        }
    }

    /// Publish a claimed record: Release-store the commit word (which
    /// synchronizes-with the consumer's Acquire load and makes every byte of
    /// the record visible), bump the commit count (the futex wake word), and
    /// wake a parked consumer. No producer is waited on.
    fn commit(&self, claim: PendingClaim) {
        let header = self.inner.header();
        let slot_offset = (claim.pos as usize) & (self.inner.capacity() - 1);
        // SAFETY: we have owned this range since the CAS; this store hands it
        // to the consumer.
        unsafe {
            store_commit_word(
                self.inner.slot_region_mut(),
                slot_offset,
                encode_commit_word(claim.lap, claim.total as u32, false),
                Ordering::Release,
            );
        }
        // `publish_position` reinterpreted as `commit_count` (module doc):
        // the wake word must change on every commit.
        header.publish_position.fetch_add(1, Ordering::Release);
        header.signal(self.mode, false); // MPSC: single consumer -> wake one
    }
}
```

- [ ] **Rewrite the consumer.** Replace `MpscConsumer`'s struct and `try_read` (the hole timer and `holes_skipped` are wired in Task 4; here they are inert fields so the shape lands once):

```rust
pub struct MpscConsumer {
    inner: Arc<MpscInner>,
    /// Wakeup mechanism; must match the producer's `mode` (see `MpscProducer::mode`).
    pub mode: ParkMode,
    /// The position of the hole currently being timed, and when it was first
    /// observed. `None` when the consumer is not stalled behind one.
    hole: Option<(u64, std::time::Instant)>,
    /// How long a hole must persist before the consumer skips it (or, for an
    /// unsized hole, fail-stops). Spec §4.2's `hole_timeout`.
    hole_timeout: std::time::Duration,
    /// Cumulative count of dead-producer holes skipped.
    holes_skipped: u64,
}

/// Default `hole_timeout` (spec §4.2). The slowest legitimate claim-to-commit
/// is microseconds; a second is four orders of magnitude of headroom.
pub const DEFAULT_HOLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

impl MpscConsumer {
    /// Handle for a parker thread to block on this ring while the owner reads.
    pub fn wait_handle(&self) -> RingWaitHandle {
        RingWaitHandle::new(self.inner.clone(), self.inner.header(), self.mode)
    }

    pub fn try_read(
        &mut self,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Option<RecordHeader>, RingError> {
        loop {
            let header = self.inner.header();
            let capacity = self.inner.capacity();
            let consumer_pos = header.consumer_position.load(Ordering::Relaxed);
            let slot_offset = (consumer_pos as usize) & (capacity - 1);

            // SAFETY: `slot_offset < capacity` and is RECORD_ALIGN-aligned, so
            // the address is inside the mapping and 4-byte aligned.
            let word = unsafe { load_commit_word(self.inner.slot_region(), slot_offset) };

            match classify_commit_word(word, lap_of(consumer_pos, capacity)) {
                SlotState::Empty => {
                    // Nothing of ours here. Either the ring is genuinely
                    // empty, or a producer has CAS-claimed this range and its
                    // claim word has not landed yet (nanoseconds), or — the
                    // §4.2 residual — it died in exactly that window.
                    if header.claim_position.load(Ordering::Acquire) <= consumer_pos {
                        self.hole = None;
                        return Ok(None);
                    }
                    // NOTE: the clock is read only on this path and the
                    // Claimed path. An empty ring (claim == consumer) is the
                    // hot idle poll and never touches it.
                    if self.hole_elapsed(consumer_pos) {
                        return Err(RingError::Wedged { position: consumer_pos });
                    }
                    return Ok(None);
                }
                SlotState::Claimed { advance } => {
                    if !self.hole_elapsed(consumer_pos) {
                        // Head-of-line behind exactly this one producer. No
                        // spin, no burn — the caller polls or parks.
                        return Ok(None);
                    }
                    // Dead producer (spec §4.2): its claim is sized, so skip
                    // it. The client that died never gets an answer — correct,
                    // it is dead.
                    self.holes_skipped += 1;
                    self.hole = None;
                    header
                        .consumer_position
                        .store(consumer_pos + advance as u64, Ordering::Release);
                    continue;
                }
                SlotState::Committed { length } => {
                    self.hole = None;
                    let len = length as usize;
                    let bytes_to_tail = capacity - slot_offset;
                    let max_record = align_record_size(self.inner.max_msg_size())
                        .min(MPSC_MAX_RECORD_BYTES);
                    if len < 6 || len > bytes_to_tail || len > max_record {
                        return Err(RingError::Corrupt(format!(
                            "commit word length {len} out of range at position {consumer_pos} \
                             (tail {bytes_to_tail}, max {max_record})"
                        )));
                    }
                    // SAFETY: `[slot_offset, slot_offset + len)` is inside the
                    // slot region (len <= bytes_to_tail) and is fully written
                    // and stable: the Acquire load of the commit word above
                    // synchronizes-with the producer's Release commit store,
                    // made after the record bytes, and the bounded claim means
                    // no producer can reclaim this range until we advance
                    // `consumer_position` below.
                    let slot = unsafe {
                        std::slice::from_raw_parts(
                            self.inner.slot_region().add(slot_offset),
                            len,
                        )
                    };
                    let (rec, advance) = decode_record_slice(slot, payload_buf)?;
                    header
                        .consumer_position
                        .store(consumer_pos + advance as u64, Ordering::Release);
                    if rec.msg_type == PADDING_MSG_TYPE {
                        continue;
                    }
                    return Ok(Some(rec));
                }
            }
        }
    }

    /// First observation of a hole at `pos` starts its timer and reports
    /// `false`; a later observation of the SAME position reports whether
    /// `hole_timeout` has elapsed. Moving on clears the timer.
    fn hole_elapsed(&mut self, pos: u64) -> bool {
        match self.hole {
            Some((p, since)) if p == pos => since.elapsed() >= self.hole_timeout,
            _ => {
                self.hole = Some((pos, std::time::Instant::now()));
                false
            }
        }
    }
}
```

- [ ] **Create/open with the new magic, and the length guard.** In `MpscRing::create`, before building the file:

```rust
        if align_record_size(max_msg_size as usize) > MPSC_MAX_RECORD_BYTES {
            return Err(RingError::Corrupt(format!(
                "max_msg_size {max_msg_size} exceeds the commit word's {MPSC_MAX_RECORD_BYTES}-byte length field"
            )));
        }
```
then `init_ring_header_with_magic(&mut mmap[..], capacity_bytes, max_msg_size, 0, crate::magic::RING_MPSC_MAGIC)?;`, and in `open`, `validate_ring_header_with_magic(&mmap[..], crate::magic::RING_MPSC_MAGIC)?;`. In `into_split`, initialise the consumer's new fields:

```rust
            MpscConsumer {
                inner: self.inner,
                mode: ParkMode::default(),
                hole: None,
                hole_timeout: DEFAULT_HOLE_TIMEOUT,
                holes_skipped: 0,
            },
```

- [ ] **Fix the one existing test that pokes `publish_position`.** In `free_space_computation_does_not_underflow_on_stale_claim_snapshot`, the line `header.publish_position.store(120, Ordering::Release);` now sets a commit count, not a position. Replace it and its comment with:

```rust
        // `publish_position` is the commit COUNT on an MPSC file (M13a); the
        // free-space arithmetic under test never reads it. Left at 0.
```
The rest of the test is unchanged and still discriminates the `saturating_sub` fix.

- [ ] Run: `cargo test -p uc_protocol ring::mpsc 2>&1 | tail -20`. Expected: 6 passing — the four pre-existing (`single_producer_round_trip`, `many_producers_one_consumer_no_wrap`, `wrap_under_many_producers_no_torn_read`, `free_space_computation_does_not_underflow_on_stale_claim_snapshot`) plus the two new. Then the whole crate: `cargo test -p uc_protocol` → `test result: ok. 82 passed`.

- [ ] Run the **release** build of the wrap test, which is where the torn-read race is visible: `cargo test -p uc_protocol --release wrap_under_many_producers_no_torn_read -- --exact --nocapture` → `test result: ok. 1 passed`.

- [ ] Run the crates that attach to these rings: `cargo test -p uc_client -p uc_service -p uc_node 2>&1 | tail -30`. Expected: all green. (`uc_client`'s and `uc_service`'s tests create their own ring files, so they get the new magic automatically; nothing in-tree opens a ring file written by an older binary.)

- [ ] Lint: `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] Commit:
```bash
git add uc_protocol/src/ring/mpsc.rs
git commit -m "feat(ring): MPSC per-record commit — no cross-producer wait

Producers stamp CLAIMED|LAP|advance, write the record, then Release-store
LAP|total. Nobody waits for anybody: the publish-order spin (and its
PUBLISH_SPINS_BEFORE_YIELD mitigation) is deleted. The consumer decides each
slot from that one word and stops at a hole. publish_position becomes the
commit count on MPSC files; the file magic is RING_MPSC_MAGIC so an
old-format file is refused instead of misread. M13a spec §4.1."
```

---

### Task 3: The preemption test — a stopped producer stalls nobody

The regression test for the actual defect. It must fail against the pre-M13a protocol and pass now.

**Files:**
- Modify `uc_protocol/src/ring/mpsc.rs` (test module)

**Interfaces:** Consumes `MpscProducer::{claim_without_commit, commit_claim, try_write}`, `MpscConsumer::try_read`. Produces no new API.

### Steps

- [ ] **Write the test.** Add to `mpsc.rs`'s test module:

```rust
    /// M13a regression for the convoy (spec §4.3's preemption test).
    ///
    /// Producer A claims a slot and STOPS there — exactly the state a
    /// scheduler preemption (or a SIGKILL) between the CAS and the commit
    /// leaves behind. Producers B..H must each complete their own
    /// `try_write` while A is stopped; the consumer must return `None`
    /// (head-of-line behind A) and burn nothing; and once A commits, the
    /// records must come out in claim order, A first.
    ///
    /// Against the pre-M13a protocol this test HANGS: B's `try_write` spins
    /// forever on `publish_position != claim_pos`. That is the discrimination
    /// — run it against `git stash`ed old ring code and it never returns
    /// (bound the run with `timeout 30 cargo test …` if you check that).
    #[test]
    fn a_stopped_producer_blocks_nobody_and_order_is_preserved() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 65536, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();

        // A claims and stops.
        let a = producer
            .claim_without_commit(1, 0, [0; 8], b"A")
            .expect("A claims");

        // B..H each write a full record BEHIND A's hole, on their own
        // threads, and every one of them must finish. The join is the
        // assertion: with the old protocol these threads never return.
        const OTHERS: usize = 7;
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = (0..OTHERS)
            .map(|t| {
                let p = producer.clone();
                let done = Arc::clone(&done);
                thread::spawn(move || {
                    let payload = [b'B' + t as u8];
                    p.try_write(1, 0, [0; 8], &payload).expect("write behind the hole");
                    done.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            done.load(Ordering::Relaxed),
            OTHERS,
            "every producer behind a stopped one must complete"
        );

        // The consumer is head-of-line behind A: nothing readable yet, even
        // though seven records are committed behind it.
        let mut buf = Vec::new();
        for _ in 0..1000 {
            assert!(
                matches!(consumer.try_read(&mut buf), Ok(None)),
                "a claimed-but-uncommitted slot must read as None, never a record"
            );
        }
        assert_eq!(consumer.holes_skipped(), 0, "a 1 s hole timeout has not elapsed");

        // A commits. Now everything drains, A first.
        producer.commit_claim(a);
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while seen.len() < OTHERS + 1 {
            let mut buf = Vec::new();
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => seen.push(buf),
                Ok(None) => thread::yield_now(),
                Err(e) => panic!("read: {e}"),
            }
        }
        assert_eq!(seen[0], b"A".to_vec(), "claim order: A was claimed first");
        let mut rest: Vec<Vec<u8>> = seen[1..].to_vec();
        rest.sort();
        let mut want: Vec<Vec<u8>> =
            (0..OTHERS).map(|t| vec![b'B' + t as u8]).collect();
        want.sort();
        assert_eq!(rest, want, "every record behind the hole is delivered exactly once");
    }
```
(Add `use std::sync::atomic::AtomicUsize;` via the existing `Ordering` import path as needed — the test module already has `use super::*;` which brings `Arc` and `Ordering`.)

- [ ] `holes_skipped()` does not exist yet (it is Task 4's accessor) — add the one-line accessor now so this test compiles, with the doc it will keep:

```rust
    /// Cumulative count of dead-producer holes this consumer has skipped
    /// (spec §4.2). Mirrored to the cnc page and `/metrics` by the node.
    pub fn holes_skipped(&self) -> u64 {
        self.holes_skipped
    }
```

- [ ] Run: `cargo test -p uc_protocol a_stopped_producer_blocks_nobody -- --exact --nocapture` → `test result: ok. 1 passed`. Then `cargo test -p uc_protocol --release a_stopped_producer_blocks_nobody -- --exact` → also passes (the release build is where thread interleaving is real).

- [ ] **Prove the test discriminates.** Temporarily reinstate the old wait inside `commit` (`while header.publish_position.load(Ordering::Acquire) != claim.pos { std::hint::spin_loop(); }` plus a `publish_position.store(target, Release)`), run `timeout 30 cargo test -p uc_protocol a_stopped_producer_blocks_nobody -- --exact`, and confirm it **times out** (exit 124) instead of passing. Revert the temporary change immediately; record the observation in the test's doc comment if the wording needs sharpening.

- [ ] Lint: `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] Commit:
```bash
git add uc_protocol/src/ring/mpsc.rs
git commit -m "test(ring): preemption test — a stopped producer blocks nobody

Producer A claims and stops; B..H all complete their writes; the consumer
returns None until A commits, then drains in claim order. Verified to
discriminate: with the pre-M13a publish-order spin reinstated the test hangs
(timeout 124) instead of passing. M13a spec §4.3."
```

---

### Task 4: The dead-producer hole — skip, count, or fail-stop

Spec §4.2, in full: a sized hole is skipped after `hole_timeout` and counted; an unsized one (the producer died between the CAS and the claim-word store) is `RingError::Wedged`.

**Files:**
- Modify `uc_protocol/src/ring/mpsc.rs` (the `MpscConsumer` accessors; the test module)

**Interfaces:**

Produces:
```rust
impl MpscConsumer {
    pub fn holes_skipped(&self) -> u64;                      // added in Task 3
    pub fn set_hole_timeout(&mut self, d: std::time::Duration);
    pub fn hole_timeout(&self) -> std::time::Duration;
}
pub const DEFAULT_HOLE_TIMEOUT: std::time::Duration;         // added in Task 2
```

### Steps

- [ ] **Write the failing tests.** Add to `mpsc.rs`'s test module:

```rust
    /// Spec §4.2, first case: a producer died between claim and commit. Its
    /// claim word SIZES the hole, so after `hole_timeout` the consumer skips
    /// exactly that range, counts it, and delivers everything behind it.
    #[test]
    fn a_sized_hole_is_skipped_after_the_timeout_and_counted() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));
        assert_eq!(consumer.hole_timeout(), std::time::Duration::from_millis(0));

        // The dead producer: claimed, written, never committed. Dropping the
        // `PendingClaim` IS the death — nothing in the ring changes.
        let dead = producer.claim_without_commit(1, 0, [0; 8], b"lost").expect("claim");
        drop(dead);
        producer.try_write(1, 0, [0; 8], b"kept").expect("write behind the hole");

        // First poll starts the timer and reports nothing.
        let mut buf = Vec::new();
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        assert_eq!(consumer.holes_skipped(), 0);

        // Second poll finds the (zero) timeout elapsed: skip, count, deliver.
        let rec = consumer.try_read(&mut buf).expect("read").expect("the record behind the hole");
        assert_eq!(rec.msg_type, 1);
        assert_eq!(&buf[..], b"kept");
        assert_eq!(consumer.holes_skipped(), 1, "the hole is counted exactly once");

        // Nothing else is left, and the counter does not drift.
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        assert_eq!(consumer.holes_skipped(), 1);
    }

    /// A hole that resolves BEFORE the timeout is not a hole: no skip, no
    /// count, and the record is delivered normally.
    #[test]
    fn a_hole_that_commits_before_the_timeout_is_never_skipped() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        assert_eq!(consumer.hole_timeout(), DEFAULT_HOLE_TIMEOUT, "1 s by default");

        let slow = producer.claim_without_commit(1, 0, [0; 8], b"slow").expect("claim");
        let mut buf = Vec::new();
        for _ in 0..100 {
            assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        }
        producer.commit_claim(slow);
        let rec = consumer.try_read(&mut buf).expect("read").expect("record");
        assert_eq!(rec.msg_type, 1);
        assert_eq!(&buf[..], b"slow");
        assert_eq!(consumer.holes_skipped(), 0);
    }

    /// Spec §4.2, second case: the producer died in the nanoseconds between
    /// its CAS on `claim_position` and its claim-word store, so the slot's
    /// word is still the previous lap's (here: a fresh ring, so zero) while
    /// `claim_position > consumer_position`. The hole's length is unknowable
    /// — the ring refuses to guess and the caller fail-stops.
    ///
    /// Constructed by hand-writing the header atomic, the same technique
    /// `free_space_computation_does_not_underflow_on_stale_claim_snapshot`
    /// uses: `#[cfg(test)]` code in this module has field access to
    /// `MpscInner`, so no production accessor is added for a test.
    #[test]
    fn an_unsized_hole_wedges_after_the_timeout() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));

        // A claim that never stamped its word.
        producer.inner.header().claim_position.store(24, Ordering::Release);

        let mut buf = Vec::new();
        // First poll: the timer starts, nothing is decided yet.
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        // Second poll: the timeout has elapsed and the length is unknowable.
        assert!(matches!(
            consumer.try_read(&mut buf),
            Err(RingError::Wedged { position: 0 })
        ));
        assert_eq!(consumer.holes_skipped(), 0, "a wedge is not a skip");
    }

    /// An empty ring must never touch the clock or report a hole: this is the
    /// hot idle poll (the node's consensus agent runs it millions of times a
    /// second). Pinned by behaviour: an empty ring with `claim == consumer`
    /// reports `None` forever with a ZERO hole timeout, which is only
    /// possible if the hole path is never entered.
    #[test]
    fn an_empty_ring_never_reports_a_hole() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));
        let mut buf = Vec::new();
        for _ in 0..1000 {
            assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        }
        assert_eq!(consumer.holes_skipped(), 0);
        // And after a full round trip the ring is empty again, same story.
        producer.try_write(1, 0, [0; 8], b"x").unwrap();
        assert!(consumer.try_read(&mut buf).unwrap().is_some());
        for _ in 0..1000 {
            assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        }
        assert_eq!(consumer.holes_skipped(), 0);
    }
```

- [ ] Run and confirm the failure: `cargo test -p uc_protocol ring::mpsc 2>&1 | tail -20`. Expected: compile errors `no method named `set_hole_timeout` found for struct `MpscConsumer`` and `no method named `hole_timeout``.

- [ ] **Add the two accessors** next to `holes_skipped` in `impl MpscConsumer`:

```rust
    /// How long a claimed-but-uncommitted slot must persist before the
    /// consumer treats it as a dead producer's hole (spec §4.2). Default
    /// [`DEFAULT_HOLE_TIMEOUT`]. Lower it only in tests: the legitimate
    /// claim-to-commit window is microseconds, and shortening it trades a
    /// bounded stall for the risk of skipping a merely-descheduled
    /// producer's live record.
    pub fn set_hole_timeout(&mut self, d: std::time::Duration) {
        self.hole_timeout = d;
    }

    /// The current hole timeout.
    pub fn hole_timeout(&self) -> std::time::Duration {
        self.hole_timeout
    }
```

- [ ] Run: `cargo test -p uc_protocol ring::mpsc 2>&1 | tail -20` → `test result: ok. 11 passed; 0 failed` (4 pre-existing + 2 from Task 2 + 1 from Task 3 + 4 new).

- [ ] Run the whole crate in both profiles: `cargo test -p uc_protocol && cargo test -p uc_protocol --release` → both `ok`.

- [ ] Lint: `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] Commit:
```bash
git add uc_protocol/src/ring/mpsc.rs
git commit -m "feat(ring): bounded dead-producer holes — skip + count, or Wedged

A sized hole (claim word present) is skipped after hole_timeout (default 1 s)
and counted in holes_skipped; an unsized one — the producer died inside the
nanoseconds between its CAS and its claim-word store — is RingError::Wedged,
because its length is unknowable and guessing is worse than fail-stopping.
The clock is read only on the hole path, never on the empty-ring poll.
M13a spec §4.2."
```

---

### Task 5: The loom model

The loom-on-rings item the M12d security package named, cashed in here. The model is of the **protocol**, not the mapping: loom cannot see an mmap, so the model is a `Vec` of loom atomics with the same claim/commit/consume discipline — the same relationship `uc_log/tests/loom_frame.rs` has to `buffer.rs`.

**Files:**
- Modify `uc_protocol/Cargo.toml` (add `[lints.rust]` and the `cfg(loom)` dev-dependency, mirroring `uc_log/Cargo.toml` lines 14–15 and 29–30)
- Create `uc_protocol/tests/loom_mpsc.rs`
- Modify `.github/workflows/nightly.yml` (the `loom` job, lines 82–91)

**Interfaces:** Consumes nothing from the crate (the model is standalone, like `loom_frame.rs`). Produces no API.

### Steps

- [ ] **Wire the dependency.** In `uc_protocol/Cargo.toml`, after the `[package]` block:

```toml
[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = ['cfg(loom)'] }
```
and at the end of the file:
```toml
[target.'cfg(loom)'.dev-dependencies]
loom = "0.7"
```

- [ ] **Write the model** at `uc_protocol/tests/loom_mpsc.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the MPSC ring's per-record commit protocol (M13a, spec
//! §4.1), over a `Vec` of loom atomics rather than an mmap.
//!
//! What is modeled — the three properties the protocol rests on:
//!   1. `claims_are_disjoint` — two producers CAS-claiming concurrently
//!      never own the same slot.
//!   2. `a_committed_record_is_fully_visible` — a consumer that observes a
//!      commit word with the expected lap and CLAIMED clear also observes
//!      the record body written before it (the producer's Release commit
//!      store to the consumer's Acquire load).
//!   3. `a_claimed_slot_is_never_read_as_committed` — a producer that claims
//!      and never commits stops the consumer at exactly that slot; the
//!      consumer never reads it as a record and never reads past it.
//!
//! What is NOT modeled, deliberately: the mmap itself. Loom has no notion of
//! file-backed shared memory (the same wall Miri hits — see
//! `docs/VERIFICATION.md` §9), so the model covers the PROTOCOL and the
//! offset-pin tests cover the layout. The `../ultima_rings` `mpsc` loom
//! harness is the template; the difference here is byte records with a
//! lap-stamped commit word instead of fixed-size slots with a round stamp.
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::thread;

/// Two slots, one "byte" each — the smallest ring that can hold both
/// producers' records without either waiting for the consumer.
const SLOTS: u64 = 2;
const CLAIMED: u32 = 1 << 31;
const LAP_SHIFT: u32 = 18;
const LAP_MASK: u32 = 0x1FFF;
const LEN_MASK: u32 = 0x3_FFFF;

fn word(lap: u32, len: u32, claimed: bool) -> u32 {
    let base = ((lap & LAP_MASK) << LAP_SHIFT) | (len & LEN_MASK);
    if claimed { base | CLAIMED } else { base }
}

fn lap_of(pos: u64) -> u32 {
    ((pos / SLOTS) as u32) & LAP_MASK
}

struct Ring {
    words: Vec<AtomicU32>,
    bodies: Vec<AtomicU32>,
    claim: AtomicU64,
    consumer: AtomicU64,
}

impl Ring {
    fn new() -> Ring {
        Ring {
            words: (0..SLOTS).map(|_| AtomicU32::new(0)).collect(),
            bodies: (0..SLOTS).map(|_| AtomicU32::new(0)).collect(),
            claim: AtomicU64::new(0),
            consumer: AtomicU64::new(0),
        }
    }

    /// The producer's claim step: CAS one slot, bounded by `consumer` — the
    /// same bound the real `try_write` computes from the free-space check.
    /// Returns `None` if the ring is full (never happens with SLOTS=2 and
    /// two producers, but the branch is modeled anyway).
    fn claim_one(&self) -> Option<u64> {
        loop {
            let pos = self.claim.load(Ordering::Acquire);
            let consumer = self.consumer.load(Ordering::Acquire);
            if pos + 1 > consumer + SLOTS {
                return None;
            }
            if self
                .claim
                .compare_exchange(pos, pos + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(pos);
            }
        }
    }

    /// Claim, stamp CLAIMED (Relaxed), write the body (Relaxed — the real
    /// code's plain stores), and leave it uncommitted.
    fn claim_and_write(&self, body: u32) -> Option<u64> {
        let pos = self.claim_one()?;
        let slot = (pos % SLOTS) as usize;
        self.words[slot].store(word(lap_of(pos), 1, true), Ordering::Relaxed);
        self.bodies[slot].store(body, Ordering::Relaxed);
        Some(pos)
    }

    /// The commit: Release-store the commit word. Nothing else, and nobody
    /// waited.
    fn commit(&self, pos: u64) {
        let slot = (pos % SLOTS) as usize;
        self.words[slot].store(word(lap_of(pos), 1, false), Ordering::Release);
    }
}

/// 1 + 2: two producers claim disjoint slots, and every record the consumer
/// reads is fully visible.
#[test]
fn claims_are_disjoint_and_a_committed_record_is_fully_visible() {
    loom::model(|| {
        let ring = Arc::new(Ring::new());

        let p1 = Arc::clone(&ring);
        let t1 = thread::spawn(move || {
            let pos = p1.claim_and_write(0xA).expect("slot for producer 1");
            p1.commit(pos);
            pos
        });
        let p2 = Arc::clone(&ring);
        let t2 = thread::spawn(move || {
            let pos = p2.claim_and_write(0xB).expect("slot for producer 2");
            p2.commit(pos);
            pos
        });

        // The consumer runs concurrently, bounded: at most SLOTS reads, no
        // spinning (loom explores every interleaving; a spin would not
        // terminate).
        let mut read = 0u64;
        for _ in 0..SLOTS {
            let pos = ring.consumer.load(Ordering::Relaxed);
            let slot = (pos % SLOTS) as usize;
            let w = ring.words[slot].load(Ordering::Acquire);
            let lap_matches = (w >> LAP_SHIFT) & LAP_MASK == lap_of(pos);
            if w == 0 || !lap_matches || w & CLAIMED != 0 {
                break; // empty, foreign lap, or head-of-line behind a claim
            }
            let body = ring.bodies[slot].load(Ordering::Relaxed);
            assert!(
                body == 0xA || body == 0xB,
                "a committed record's body must be visible, saw {body:#x}"
            );
            ring.consumer.store(pos + 1, Ordering::Release);
            read += 1;
        }
        assert!(read <= SLOTS);

        let a = t1.join().unwrap();
        let b = t2.join().unwrap();
        assert_ne!(a, b, "two producers must never claim the same position");
    });
}

/// 3: a producer that claims and never commits stops the consumer at exactly
/// that slot. The consumer must not read it as a record, and must not read
/// the record the OTHER producer committed behind it.
#[test]
fn a_claimed_slot_is_never_read_as_committed() {
    loom::model(|| {
        let ring = Arc::new(Ring::new());

        // Producer A claims position 0 and stops (the preempted/dead case).
        let a = ring.claim_and_write(0xA).expect("slot 0");
        assert_eq!(a, 0);

        let p2 = Arc::clone(&ring);
        let t2 = thread::spawn(move || {
            let pos = p2.claim_and_write(0xB).expect("slot 1");
            p2.commit(pos);
        });

        for _ in 0..SLOTS {
            let pos = ring.consumer.load(Ordering::Relaxed);
            let slot = (pos % SLOTS) as usize;
            let w = ring.words[slot].load(Ordering::Acquire);
            let lap_matches = (w >> LAP_SHIFT) & LAP_MASK == lap_of(pos);
            assert!(
                w == 0 || !lap_matches || w & CLAIMED != 0,
                "the consumer must never see a committed word at position {pos} \
                 while position 0 is claimed-but-uncommitted"
            );
            break;
        }
        assert_eq!(
            ring.consumer.load(Ordering::Relaxed),
            0,
            "the consumer never advanced past the hole"
        );

        t2.join().unwrap();
    });
}
```

- [ ] Run: `RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release 2>&1 | tail -20`. Expected: `test result: ok. 2 passed; 0 failed`, after loom explores the interleavings (seconds, not minutes, at SLOTS=2).

- [ ] **Prove the model discriminates** (the mutation check `loom_frame.rs` documents for its own model): change `Ring::commit`'s store from `Ordering::Release` to `Ordering::Relaxed`, re-run, and confirm `claims_are_disjoint_and_a_committed_record_is_fully_visible` **fails** with `a committed record's body must be visible, saw 0x0`. Restore `Release` and re-run to green. Record the result in the file's doc comment (append to the module doc):

```rust
//! Mutation check (performed, not assumed): relaxing `commit`'s store to
//! `Ordering::Relaxed` makes property 2 FAIL under loom — the model finds an
//! interleaving where the consumer observes the commit word but not the
//! body. The Release is load-bearing, not decoration.
```

- [ ] Confirm the normal (non-loom) build is untouched: `cargo test -p uc_protocol` still passes and does not compile the loom file (`#![cfg(loom)]` empties it), and `cargo clippy --workspace --all-targets -- -D warnings` is clean — the `unexpected_cfgs` lint config is what keeps `cfg(loom)` from warning.

- [ ] **Add it to nightly CI.** In `.github/workflows/nightly.yml`, the `loom` job gains a second step, and the header comment on line 11–12 is updated:

```yaml
      - name: Frame-visibility loom model
        run: cargo test -p uc_log --test loom_frame --release
        env:
          RUSTFLAGS: --cfg loom
      - name: MPSC ring commit-protocol loom model
        run: cargo test -p uc_protocol --test loom_mpsc --release
        env:
          RUSTFLAGS: --cfg loom
```
```yaml
#   * loom       — the uc_log frame-visibility memory model AND the
#                  uc_protocol MPSC ring commit protocol (exhaustive
#                  interleaving exploration; needs `--cfg loom` + release).
```

- [ ] Commit:
```bash
git add uc_protocol/Cargo.toml uc_protocol/tests/loom_mpsc.rs .github/workflows/nightly.yml
git commit -m "test(ring): loom model of the MPSC commit protocol

Three properties over two producers and a bounded consumer: disjoint claims,
committed-record visibility (the Release/Acquire pair), and head-of-line at a
claimed slot. Verified to discriminate — relaxing the commit store fails the
visibility property. Runs in nightly's loom job. This is the loom-on-rings
item the M12d security package named. M13a spec §4.3."
```

---

### Task 6: The `ring_mpsc_record` fuzz target

The consumer's decision-and-decode path on bytes it did not write. It is reachable by any process with write access to the instance dir, and by a client with a torn write — the same class as `uc_protocol_cnc`.

**Files:**
- Create `fuzz/fuzz_targets/ring_mpsc_record.rs`
- Modify `fuzz/Cargo.toml` (a `[[bin]]` block before the `# one [[bin]] per target.` comment)
- Modify `fuzz/src/seeds.rs` (a `ring_mpsc_record()` function)
- Modify `fuzz/src/bin/seed_corpus.rs` (one `write_target` line)
- Create `fuzz/corpus/ring_mpsc_record/*` (generated, committed)
- Modify `fuzz/README.md` (target table row) and `.github/workflows/nightly.yml` (`FUZZ_GROUPS`)

**Interfaces:** Consumes `uc_protocol::ring::common::{classify_commit_word, decode_record_slice, encode_commit_word, write_record_body_at, SlotState, COMMIT_LAP_MASK, FRAME_HEADER_LEN, FRAME_TRAILER_LEN}`. Produces the target binary `ring_mpsc_record`.

### Steps

- [ ] **Write the target** at `fuzz/fuzz_targets/ring_mpsc_record.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::ring::common::{
    COMMIT_LAP_MASK, SlotState, classify_commit_word, decode_record_slice,
};

// The MPSC ingress ring is a writable mmap'd file that every client process
// on the host can write. Its consumer — the node's consensus agent, the
// single most safety-critical thread in the system — decides what to do with
// a slot from one 32-bit commit word and then decodes the bytes behind it.
// Both steps must be total on arbitrary input: a panic here is a node crash
// triggerable by a torn write or a hostile local process.
//
// Input layout: [0..4) commit word, [4..8) expected lap, [8..) slot bytes.
fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    let word = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let expected_lap = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) & COMMIT_LAP_MASK;
    let body = &data[8..];
    let mut buf = Vec::new();

    // 1. The classifier is total on every (word, lap) pair.
    match classify_commit_word(word, expected_lap) {
        SlotState::Committed { length } => {
            // The consumer decodes exactly `length` bytes. Clamp to what we
            // have so the committed path is exercised on EVERY input rather
            // than only when the fuzzer guesses a length that fits.
            let take = (length as usize).min(body.len());
            let _ = decode_record_slice(&body[..take], &mut buf);
        }
        SlotState::Claimed { .. } | SlotState::Empty => {}
    }

    // 2. The decoder is total on any slice, whatever the word claimed.
    let _ = decode_record_slice(body, &mut buf);
});
```

- [ ] **Declare the bin.** In `fuzz/Cargo.toml`, before the `# one [[bin]] per target.` comment:

```toml
[[bin]]
name = "ring_mpsc_record"
path = "fuzz_targets/ring_mpsc_record.rs"
test = false
doc = false
bench = false
```

- [ ] **Add the seeds.** In `fuzz/src/seeds.rs`, after `uc_protocol_cnc()`:

```rust
/// `ring_mpsc_record` — the MPSC slot decision + decode path. Every seed is
/// built with the REAL record writer (`write_record_body_at`) and the real
/// commit-word encoder, so the "valid" seeds are byte-exact what a producer
/// writes.
pub fn ring_mpsc_record() -> Vec<Seed> {
    use uc_protocol::ring::common::{
        FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, encode_commit_word,
        write_padding_body_at, write_record_body_at,
    };

    /// One fuzz input: commit word, expected lap, then the slot bytes.
    fn input(word: u32, expected_lap: u32, slot: &[u8]) -> Vec<u8> {
        let mut out = word.to_le_bytes().to_vec();
        out.extend_from_slice(&expected_lap.to_le_bytes());
        out.extend_from_slice(slot);
        out
    }

    fn record(msg_type: u16, flags: u16, extra: [u8; 8], payload: &[u8]) -> Vec<u8> {
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        let mut slot = vec![0u8; total];
        // SAFETY: `slot` is exactly `total` bytes and exclusively owned here;
        // `write_record_body_at` writes bytes 4..total.
        unsafe { write_record_body_at(slot.as_mut_ptr(), 0, msg_type, flags, extra, payload) };
        slot
    }

    let mut seeds = Vec::new();

    // A committed record, lap 3, decoding cleanly.
    let r = record(1, 0, [7; 8], b"submit-payload");
    seeds.push(Seed::fixed(
        "01-committed-record",
        input(encode_commit_word(3, r.len() as u32, false), 3, &r),
    ));

    // The same record with a CLAIMED word: the consumer stops, decodes nothing.
    seeds.push(Seed::fixed(
        "02-claimed",
        input(encode_commit_word(3, r.len() as u32, true), 3, &r),
    ));

    // The same record under the WRONG lap: reads as Empty.
    seeds.push(Seed::fixed(
        "03-foreign-lap",
        input(encode_commit_word(2, r.len() as u32, false), 3, &r),
    ));

    // A tail-wrap padding marker.
    let mut pad = vec![0u8; 24];
    // SAFETY: 24 bytes >= the 6 the padding body writes.
    unsafe { write_padding_body_at(pad.as_mut_ptr(), 0) };
    seeds.push(Seed::fixed("04-padding", input(encode_commit_word(0, 24, false), 0, &pad)));

    // A length far beyond the bytes present.
    seeds.push(Seed::fixed("05-overlong-length", input(encode_commit_word(3, 0x3FFFF, false), 3, &r)));

    // A record whose crc has been flipped.
    let mut bad = r.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    seeds.push(Seed::fixed(
        "06-bad-crc",
        input(encode_commit_word(3, bad.len() as u32, false), 3, &bad),
    ));

    // A record header claiming a payload shorter than the frame header.
    seeds.push(Seed::fixed("07-truncated", input(encode_commit_word(3, 8, false), 3, &r[..8])));

    // An all-zero slot: the fresh-ring state.
    seeds.push(Seed::fixed("08-zero", input(0, 0, &[0u8; 24])));

    // A padding msg_type in a slot too short to hold one.
    let mut tiny = vec![0u8; 6];
    tiny[4..6].copy_from_slice(&PADDING_MSG_TYPE.to_le_bytes());
    seeds.push(Seed::fixed("09-min-padding", input(encode_commit_word(0, 6, false), 0, &tiny)));

    seeds
}
```

- [ ] **Generate the corpus.** Add `write_target(root, "ring_mpsc_record", seeds::ring_mpsc_record())?;` to `fuzz/src/bin/seed_corpus.rs`'s `main`, then:
```bash
cd /home/claude/ultima/ultima_cluster/fuzz && cargo +nightly run --bin seed-corpus
```
Expected output line: `ring_mpsc_record: 9 seeds in /home/claude/ultima/ultima_cluster/fuzz/corpus/ring_mpsc_record`. Re-run it once and confirm `git status` shows no churn (the generator is idempotent).

- [ ] **Smoke the target:**
```bash
scripts/fuzz_smoke.sh --min-runs 10000 60 ring_mpsc_record
```
Expected: `== fuzz ring_mpsc_record (60s) ==`, then `-- ring_mpsc_record: <N> runs in 60s (floor 10000)` with N in the millions (this target does no per-input setup), then `fuzz smoke: all targets clean`. A run count near the floor means the symbolizer stall described in `fuzz_smoke.sh` — investigate before moving on.

- [ ] **Wire CI.** In `.github/workflows/nightly.yml`, add the target to the third `FUZZ_GROUPS` leg (which has three, keeping the documented four-per-leg ceiling):
```yaml
            uc_journal_record uc_journal_stable_value uc_service_session ring_mpsc_record
```
Then add the README row (in `fuzz/README.md`'s target table):
```markdown
| `ring_mpsc_record` | `uc_protocol::ring::common`'s MPSC slot decision (`classify_commit_word`) and record decoder (`decode_record_slice`) — what the node's consensus agent meets in a shared-memory ring any local process can write. |
```

- [ ] Commit:
```bash
git add fuzz/fuzz_targets/ring_mpsc_record.rs fuzz/Cargo.toml fuzz/src/seeds.rs fuzz/src/bin/seed_corpus.rs fuzz/corpus/ring_mpsc_record fuzz/README.md .github/workflows/nightly.yml
git commit -m "test(fuzz): ring_mpsc_record — the MPSC slot decision and record decoder

Fifteenth target: arbitrary commit words and slot bytes through
classify_commit_word + decode_record_slice, the path the consensus agent
takes over a ring file any local process can write. Nine deterministic seeds
built with the real writer; assigned to nightly's third fuzz leg. M13a spec §4.3."
```

---

### Task 7: Node wiring — `Wedged` fail-stop, the cnc field, and `/metrics`

**Files:**
- Modify `uc_protocol/src/v2/cnc.rs` (after `CNC_OFF_ADMIN_AUTH`, line 175–176; the module-doc reserved-band line 30; the offset test at line 502)
- Modify `uc_log/src/cnc.rs` (the import list lines 22–28; accessors next to `free_disk_bytes` at lines 500–514; the offset test at line 832; a new round-trip test after `free_disk_bytes_roundtrip_and_offset_pin` at line 1107)
- Modify `uc_node/src/node.rs` (the `Consensus` struct near line 1726; both constructors — the production one near line 1214 and the test harness near line 5669; `drain_ingress_ring`'s error arm line 3128–3133; `drain_query_ring`'s error arm; `publish_status` line 2969; a new `ring_error_fail_stop` + `publish_ring_holes`; a new test)
- Modify `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` after `"uc2_wipes_total"` line 70; the renderer after the `uc2_wipes_total` `push_counter` at line 412; a new test)

**Interfaces:**

Produces:
```rust
// uc_protocol::v2::cnc
pub const CNC_OFF_INGRESS_HOLES_SKIPPED: usize = 3968;
// uc_log::cnc::CncPage
pub fn ingress_holes_skipped(&self) -> u64;
pub fn store_ingress_holes_skipped(&self, v: u64);
// uc_node::node::Consensus (private)
fn ring_error_fail_stop(&self, e: &RingError, ring: &'static str);
fn publish_ring_holes(&mut self);
// /metrics
// uc2_ingress_holes_skipped_total  (counter)
```

### Steps

- [ ] **Write the failing tests first.**

In `uc_protocol/src/v2/cnc.rs`'s offset test (after line 502's `assert_eq!(CNC_OFF_ADMIN_AUTH, 3904);`):
```rust
        // M13a: ingress_holes_skipped.
        assert_eq!(CNC_OFF_INGRESS_HOLES_SKIPPED, 3968);
        assert_eq!(CNC_OFF_INGRESS_HOLES_SKIPPED - CNC_OFF_ADMIN_AUTH, 64);
        assert!(CNC_OFF_INGRESS_HOLES_SKIPPED + 64 <= CNC_PAGE_LEN);
```

In `uc_log/src/cnc.rs`'s `layout_offsets_are_pinned` test (after line 832):
```rust
        // M13a: ingress_holes_skipped.
        assert_eq!(CNC_OFF_INGRESS_HOLES_SKIPPED, 3968);
```
and a new test after `free_disk_bytes_roundtrip_and_offset_pin`:
```rust
    #[test]
    fn ingress_holes_skipped_roundtrip_and_offset_pin() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.ingress_holes_skipped(), 0, "fresh page reads 0 (no holes)");
        page.store_ingress_holes_skipped(3);
        assert_eq!(page.ingress_holes_skipped(), 3);
        let raw = page.page();
        assert_eq!(
            u64::from_le_bytes(raw[3968..3976].try_into().unwrap()),
            3,
            "offset pin: the value must live at 3968 exactly"
        );
    }
```

In `uc_node/src/obs/metrics.rs`, add `"uc2_ingress_holes_skipped_total",` to `CONTRACT_SERIES` after `"uc2_wipes_total"` (`every_contract_series_is_present` now fails until the renderer emits it), plus:
```rust
    #[test]
    fn ingress_holes_skipped_is_exported() {
        let s = synthetic_sources();
        assert!(render_prometheus(&s).contains("uc2_ingress_holes_skipped_total 0"));
        s.cnc.store_ingress_holes_skipped(2);
        assert!(render_prometheus(&s).contains("uc2_ingress_holes_skipped_total 2"));
    }
```

In `uc_node/src/node.rs`'s test module:
```rust
    /// M13a: a client killed between its ring claim and its commit leaves a
    /// sized hole. The consensus agent must skip it (never wedge on it),
    /// count it, and publish the count to the cnc page for `/metrics`.
    #[test]
    fn a_dead_clients_ring_hole_is_skipped_counted_and_published() {
        let mut h = harness();
        // A second producer on the harness's own ingress ring: claim, then
        // "die" (drop the claim), then write a live record behind it.
        let (producer, _c) = MpscRing::open(&h._dir.path().join("ingress.ring"))
            .unwrap()
            .into_split();
        let dead = producer
            .claim_without_commit(MSG_V2_SUBMIT, 0, extra_client(9, 1), b"lost")
            .unwrap();
        drop(dead);
        producer.try_write(MSG_V2_SUBMIT, 0, extra_client(9, 2), b"kept").unwrap();
        h.cons.ingress_ring.set_hole_timeout(std::time::Duration::from_millis(0));

        // `serving = false`: every drained record is answered NOT_LEADER, so
        // no appender is needed. First drain starts the hole timer; the
        // second finds it elapsed, skips, and delivers the live record.
        h.cons.drain_ingress_ring(false);
        assert_eq!(h.cons.ingress_ring.holes_skipped(), 0);
        assert!(h.cons.drain_ingress_ring(false), "the record behind the hole is drained");
        assert_eq!(h.cons.ingress_ring.holes_skipped(), 1);

        h.cons.publish_ring_holes();
        assert_eq!(h.cons.cnc.ingress_holes_skipped(), 1);
    }

    /// M13a: the unsized hole (`RingError::Wedged`) is not recoverable — the
    /// consensus agent fail-stops with a named error, which the daemon turns
    /// into `agent_failstopped` + exit 1 (the same chain the ENOSPC path
    /// uses). Asserted on the message, because that name is what an operator
    /// greps for.
    #[test]
    #[should_panic(expected = "IngressRingWedged")]
    fn a_wedged_ingress_ring_fail_stops_the_consensus_agent() {
        let h = harness();
        h.cons.ring_error_fail_stop(&RingError::Wedged { position: 4096 }, "ingress");
    }

    /// Every other ring error stays what it was: a bounded, non-fatal end to
    /// this drain cycle.
    #[test]
    fn an_ordinary_ring_error_does_not_fail_stop() {
        let h = harness();
        h.cons.ring_error_fail_stop(&RingError::BadCrc, "ingress");
        h.cons.ring_error_fail_stop(&RingError::Full, "query");
    }
```
(`MSG_V2_SUBMIT` and `RingError` need adding to `node.rs`'s imports: extend the `uc_protocol::v2::ipc::{…}` list with `MSG_V2_SUBMIT` and the `uc_protocol::ring::{…}` list with `RingError`.)

- [ ] Run all four and confirm the expected failures:
  - `cargo test -p uc_protocol v2::cnc 2>&1 | tail -10` → `cannot find value `CNC_OFF_INGRESS_HOLES_SKIPPED` in this scope`
  - `cargo test -p uc_log cnc 2>&1 | tail -10` → the same, plus `no method named `ingress_holes_skipped``
  - `cargo test -p uc_node --lib obs::metrics 2>&1 | tail -10` → `missing series uc2_ingress_holes_skipped_total`
  - `cargo test -p uc_node --lib node::tests 2>&1 | tail -10` → `no method named `ring_error_fail_stop``

- [ ] **Add the cnc offset.** In `uc_protocol/src/v2/cnc.rs`, after `CNC_OFF_ADMIN_AUTH`:

```rust
/// M13a: cumulative count of dead-producer holes the node has skipped on the
/// two client-facing MPSC rings (`ingress.ring` + `query.ring`) — a client
/// process killed between its ring claim and its commit (spec §4.2). Writer:
/// the consensus agent, published only when the value CHANGES (it moves
/// approximately never, and this is a shared cache line). 0 = no client has
/// ever died mid-write, which is the expected steady state; a nonzero value
/// means at least one client's submit was silently dropped, which is exactly
/// the kind of thing an operator must be able to see from outside the
/// process — same motivation as `seal_failures` and `free_disk_bytes` above.
/// Next free reserved-band offset after this line: 4032.
pub const CNC_OFF_INGRESS_HOLES_SKIPPED: usize = 3968;
const _: () = assert!(CNC_OFF_INGRESS_HOLES_SKIPPED + 64 <= CNC_PAGE_LEN);
```
and update the module-doc reserved line (line 30) from `//! 3904..4096  reserved (zero)` to `//! 4032..4096  reserved (zero)`.

- [ ] **Add the `uc_log` accessors.** Add `CNC_OFF_INGRESS_HOLES_SKIPPED` to the import list, and after `store_free_disk_bytes`:

```rust
    /// M13a: cumulative dead-producer ring holes skipped — see
    /// `CNC_OFF_INGRESS_HOLES_SKIPPED`'s doc.
    pub fn ingress_holes_skipped(&self) -> u64 {
        // SAFETY: offset 3968, size 8.
        let ptr =
            unsafe { self.region.ptr_at(CNC_OFF_INGRESS_HOLES_SKIPPED) as *const PaddedAtomicU64 };
        unsafe { (*ptr).load_acquire() }
    }

    /// M13a: store the cumulative skipped-hole count. Writer: the consensus
    /// agent, on change only.
    pub fn store_ingress_holes_skipped(&self, v: u64) {
        // SAFETY: offset 3968, size 8.
        let ptr =
            unsafe { self.region.ptr_at(CNC_OFF_INGRESS_HOLES_SKIPPED) as *const PaddedAtomicU64 };
        unsafe { (*ptr).store_release(v) }
    }
```

- [ ] **Wire the node.** Add the field to `Consensus` (next to `pending_ring_ingress`):

```rust
    /// M13a: the last `holes_skipped` sum published to the cnc page. The
    /// cnc store and the log line fire only when the sum moves.
    last_holes_published: u64,
```
initialise it to `0` in both constructors, and add the two methods next to `drain_ingress_ring`:

```rust
    /// M13a: a ring error from a client-facing MPSC ring. Everything except
    /// `Wedged` ends this drain cycle and is retried next cycle (the record
    /// was not consumed). `Wedged` cannot be retried — the consumer is stuck
    /// behind a claim whose length is unknowable (spec §4.2) — so the
    /// consensus agent fail-stops the same way `Action::Fatal` does: a panic
    /// on the agent thread, which the `AgentRunner` drop-guard turns into a
    /// finished flag, which `uc2-node`'s main loop reports as
    /// `agent_failstopped` before exiting 1 for systemd to restart.
    fn ring_error_fail_stop(&self, e: &RingError, ring: &'static str) {
        if let RingError::Wedged { position } = e {
            crate::obs_event!(
                Error,
                "ingress_ring_wedged",
                node = self.id as u64,
                ring = ring,
                position = *position
            );
            panic!(
                "consensus fatal (fail-stop): IngressRingWedged ring={ring} position={position} \
                 — a producer died between its claim and its claim word; the hole's length is \
                 unknowable. Restart the node; every attached client must reattach."
            );
        }
    }

    /// M13a: mirror the two client-facing rings' skipped-hole counts onto the
    /// cnc page for `/metrics`, and log the first observation of each new
    /// hole. Called once per duty cycle from `publish_status`; the store and
    /// the log fire only when the sum changes.
    fn publish_ring_holes(&mut self) {
        let holes = self.ingress_ring.holes_skipped() + self.query_ring.holes_skipped();
        if holes != self.last_holes_published {
            self.cnc.store_ingress_holes_skipped(holes);
            crate::obs_event!(
                Warn,
                "ingress_hole_skipped",
                node = self.id as u64,
                holes_skipped = holes
            );
            self.last_holes_published = holes;
        }
    }
```
Call `self.publish_ring_holes();` as the last statement of `publish_status`, and change both drain loops' error arms:
```rust
                // Corrupt record (bad crc/magic — the wire has no per-record
                // recovery once framing is suspect): stop this cycle rather
                // than risk misreading a subsequent slot; the next cycle
                // re-tries at the same (unread) consumer position. A
                // `Wedged` ring never returns from the call below.
                Err(e) => {
                    self.ring_error_fail_stop(&e, "ingress");   // "query" in drain_query_ring
                    break;
                }
```

- [ ] **Render the metric.** In `uc_node/src/obs/metrics.rs`, after the `uc2_wipes_total` `push_counter`:

```rust
    push_counter(
        &mut out,
        "uc2_ingress_holes_skipped_total",
        "Client ring records skipped because the producing process died between its claim and its commit.",
        s.cnc.ingress_holes_skipped(),
    );
```

- [ ] Run everything this task touched:
```bash
cargo test -p uc_protocol v2::cnc
cargo test -p uc_log cnc
cargo test -p uc_node --lib obs::metrics
cargo test -p uc_node --lib node::tests
```
Expected: all `ok`, including `a_dead_clients_ring_hole_is_skipped_counted_and_published`, `a_wedged_ingress_ring_fail_stops_the_consensus_agent` (a passing `should_panic`), `an_ordinary_ring_error_does_not_fail_stop`, `ingress_holes_skipped_roundtrip_and_offset_pin`, `every_contract_series_is_present`, `ingress_holes_skipped_is_exported`.

- [ ] Lint: `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] Commit:
```bash
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs uc_node/src/node.rs uc_node/src/obs/metrics.rs
git commit -m "feat(node): IngressRingWedged fail-stop + ingress_holes_skipped on cnc/metrics

cnc reserved-band field at 3968 (pinned in both uc_protocol and uc_log with
offset tests), published by the consensus agent on change only, exported as
uc2_ingress_holes_skipped_total. RingError::Wedged fail-stops the consensus
agent with a named error through the existing panic -> agent_failstopped ->
exit 1 chain; every other ring error still just ends the drain cycle.
M13a spec §4.2."
```

---

### Task 8: Documentation — the format change, the operator rule, and the honest verification claim

**Files:**
- Modify `docs/reference/instance-directory.md` (the `ingress.ring`/`query.ring` rows in the Files table, line 18–19; a new note after the table at line 26)
- Modify `docs/ops/uc2-runbook.md` ("Running a cluster", after the `run-a-cluster.md` bullet at line 41–43)
- Modify `docs/how-to/upgrade-a-cluster.md` (a new section after "Config choices added in v2.6.0", line ~188)
- Modify `docs/VERIFICATION.md` (§6 line 306–316; the §7 count at line 30 and the target table; §9's ring paragraph at line 457–464; §11's ring bullet at line 554–557)

**Interfaces:** none (prose).

### Steps

- [ ] **Instance directory.** Change the two ring rows and add the note:

```markdown
| `ingress.ring` | clients → node | MPSC submit ring. Per-record commit format (`ULTRNG2` magic) since 2.7.0. |
| `query.ring` | clients → node | Query submissions, both linearizable and snapshot reads. Same format as `ingress.ring`. |
```
after the table:
```markdown
**Ring file format (2.7.0).** The two client-facing MPSC rings changed
format: each record now carries its own commit word (a lap stamp plus a
length) instead of being published in claim order through a shared cursor,
which is what removed the producer convoy documented in
[the convoy explainer](../notes/uc2-m13-mpsc-publish-convoy-explained.md).
The file magic changed with it (`ULTRNG2`), so a process built before 2.7.0
and one built after **cannot share an instance directory**: the older one's
ring file is refused with a magic mismatch rather than misread. The node,
the service, the gateway and every shmem client on a host therefore restart
together on this upgrade — see
[Upgrade a cluster](../how-to/upgrade-a-cluster.md). The rings are volatile
(recreated on boot), so there is nothing to migrate.
```

- [ ] **Runbook.** Add a bullet under "Running a cluster":

```markdown
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — the flag day, and (as
  of 2.7.0) the rule that a host's node, service, gateway and shmem clients
  restart *together*, because the ring file format changed.
```

- [ ] **Upgrade how-to.** New section before "Where to go next":

```markdown
## Ring format change in 2.7.0: restart a host's processes together

2.7.0 changes the format of the two client-facing shared-memory rings
(`ingress.ring`, `query.ring`) — per-record commit, new file magic
`ULTRNG2`. This is **not** a wire flag day: nothing about node-to-node
traffic, the cnc page layout or the journal changes, and a 2.7.0 node
replicates with a 2.7.0 peer exactly as before.

What it does change is the *same-host* contract. A pre-2.7.0 service,
gateway or client that attaches to a 2.7.0 node's instance directory is
refused with a ring magic mismatch (and the reverse), because the two
binaries disagree about what a slot's first word means. So on each host,
stop and restart **all** of them together:

```bash
sudo systemctl stop uc2-gateway uc2-service uc2-node
# swap binaries
sudo systemctl start uc2-node uc2-service uc2-gateway
```

`scripts/uc2_flag_day.sh` already stops and starts whole hosts, so a flag
day run covers this by construction — the rule matters for anything that
attaches *outside* those units, most often a long-lived embedded client.
The rings are volatile (recreated at boot), so there is nothing to migrate
and no rollback step beyond restarting the old binaries together.
```

- [ ] **VERIFICATION.md — the honest part.** Four edits:
  - The summary table row for loom (line 29): `| **loom** | Exhaustive over interleavings | The frame-visibility memory protocol **and the MPSC ring's per-record commit protocol** |`
  - The fuzzing row (line 30): "fourteen decoders" → "fifteen decoders".
  - §6: add the second command and a sentence:
    ```markdown
    ```bash
    RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
    RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
    ```

    The second model is the MPSC ring's per-record commit protocol (M13a):
    disjoint concurrent claims, the commit word's Release/Acquire visibility
    pair, and head-of-line behaviour at a claimed slot. Both models are of
    protocols, not of mappings — loom cannot see an mmap, so the ring's
    *layout* stays frozen by offset-pin tests.
    ```
  - §9 and §11: the standing claim that `uc_protocol/src/ring` is covered by "nothing" is now false for MPSC and must be narrowed, not deleted:
    ```markdown
    and their interleavings and UB are covered by **nothing** — except the
    MPSC ring, whose commit protocol has had a loom model and whose slot
    decoder has had a fuzz target (`ring_mpsc_record`) since 2.7.0. SPSC,
    Broadcast, the futex layer and the mapping itself remain uncovered.
    ```
  - The `fuzz` target table in §7 gains the same row added to `fuzz/README.md`.

- [ ] Verify the three link targets the new prose introduces actually exist:
```bash
ls docs/notes/uc2-m13-mpsc-publish-convoy-explained.md \
   docs/how-to/upgrade-a-cluster.md \
   docs/reference/instance-directory.md
```
Expected: all three listed, no `No such file`.

- [ ] Commit:
```bash
git add docs/reference/instance-directory.md docs/ops/uc2-runbook.md docs/how-to/upgrade-a-cluster.md docs/VERIFICATION.md
git commit -m "docs: ring format change, restart-together rule, corrected verification claims

instance-directory + runbook + upgrade how-to get the ULTRNG2 note (same-host
restart, not a wire flag day). VERIFICATION.md's 'the rings are covered by
nothing' is narrowed honestly: the MPSC commit protocol now has a loom model
and its decoder a fuzz target; SPSC, Broadcast and the mapping still do not."
```

---

### Task 9: The regression smoke and the full local proof stack

The dev-box convoy reproduction is the gate for this milestone's row d (the fleet run is a separate, user-approved step — never quote a dev-box number as a gate result; see `docs/notes/dev-box-not-a-bench.md`'s standing rule).

**Files:** none modified. This task runs things and records what it saw.

**Interfaces:** Consumes `uc_gateway`'s `hop_bench` example (`dummy-node` + `engine-load` roles).

### Steps

- [ ] Build the bench:
```bash
cd /home/claude/ultima/ultima_cluster
cargo build --release -p uc_gateway --example hop_bench
```

- [ ] Run the convoy reproduction. **The instance dir goes on real disk, never `/tmp`** (`/tmp` is RAM-backed with no swap on this box):
```bash
mkdir -p /home/claude/hop-smoke
target/release/examples/hop_bench dummy-node --instance-dir /home/claude/hop-smoke &
SINK=$!
sleep 1
target/release/examples/hop_bench engine-load --instance-dir /home/claude/hop-smoke \
  --engines 1 --inflight 256 --secs 4
target/release/examples/hop_bench engine-load --instance-dir /home/claude/hop-smoke \
  --engines 4 --inflight 256 --secs 4
kill $SINK
```

- [ ] Read the two `RESULT {...}` lines' `responses_per_sec`. The 4-engine rung is the one under test, on a 4-vCPU box:
  - pre-M13a pure spin: **5,589 resp/s** (the collapse)
  - pre-M13a with the `PUBLISH_SPINS_BEFORE_YIELD` mitigation: **160,604 resp/s**
  - **expected now: > 500,000 resp/s**, i.e. within the same order of magnitude as the 1-engine rung rather than an order of magnitude below it.
  A 4-engine number below 500k is a failure of this task, not a flaky bench: re-run once to rule out a noisy box, then treat it as a defect and go back to Task 2 (`superpowers:systematic-debugging`) — the most likely cause is the consumer entering the hole path on every poll (check `holes_skipped` stays 0 and that the `Empty` fast path really returns before the clock read).

- [ ] Clean up the artifacts so they do not sit on disk: `rm -rf /home/claude/hop-smoke`.

- [ ] Run the full local proof stack:
```bash
cargo build --workspace
cargo test --workspace 2>&1 | tail -40
cargo test -p uc_protocol --release 2>&1 | tail -5
cargo clippy --workspace --all-targets -- -D warnings
RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
```
Expected: every suite `ok`, clippy silent. `cargo test --workspace` covers `uc_client`'s synthetic/pipelined/torn-header suites and `uc_service`'s apply/reconstruction suites — all of which drive real ring files, and all of which are the cross-crate evidence that the format change did not break an attacher.

- [ ] Run the two lincheck capstones, which exercise the ring under failover and churn:
```bash
cargo test -p uc_node --test lin_v2
cargo test -p uc_node --test lin_partition_v2
```
Expected: `ok` on both.

- [ ] Run the multi-process crashtest — the one tier that actually `SIGKILL`s a process holding ring state, i.e. the real dead-producer path this milestone made survivable:
```bash
cargo test -p uc_crashtest --features hard-crash-tests 2>&1 | tail -20
```
Expected: `ok`. If a run reports a skipped hole in the node's log (`ingress_hole_skipped`), that is the new behaviour working as designed — record it in the commit message rather than treating it as a failure.

- [ ] Commit the record (no code):
```bash
git commit --allow-empty -m "chore(m13a): regression smoke — 4-engine convoy rung <N> resp/s

hop_bench dummy-node + engine-load --engines 4 --inflight 256 --secs 4 on the
4-vCPU dev box: <N> resp/s, against 5,589 with the pre-M13a spin and 160,604
with the yield mitigation. DEV-BOX SMOKE ONLY — the gate number is fleet row
d (bench-infra/scripts/m13_hop_bench.py), never this.

Full local proof stack green: cargo test --workspace, --release uc_protocol,
clippy -D warnings, both loom models, lin_v2, lin_partition_v2, crashtest."
```

---

## Self-review

Performed against the spec and the tree, not asserted.

**Spec §4.1 coverage** — every clause has a home:

| §4.1 clause | Where |
|---|---|
| Commit word: bit 31 CLAIMED, bits 18–30 LAP, bits 0–17 LENGTH | Task 1 (`encode_commit_word`/`classify_commit_word` + `commit_word_round_trips_every_field`) |
| Producer step 1 — CAS claim bounded by `consumer_position` | Task 2 (`claim`, free-space check retained verbatim including `saturating_sub` and its regression test) |
| Producer step 2 — claim word `CLAIMED|LAP|advance` (Relaxed) | Task 2 (`store_commit_word(..., Relaxed)` before the body write) |
| Producer step 3 — body + crc | Task 1 (`write_record_body_at`) |
| Producer step 4 — commit `LAP|total` (Release) | Task 2 (`commit`) |
| Producer step 5 — `commit_count.fetch_add(Release)` + `signal()` | Task 2 (`publish_position.fetch_add` — the field name is pinned, the reinterpretation is documented in the module doc and the cnc/ring docs) |
| "No producer waits for any other producer at any step" | Task 2 (the wait loop and `PUBLISH_SPINS_BEFORE_YIELD` are deleted) + Task 3 (the preemption test, verified to hang against the old code) |
| `Full` still returned, never spun | Task 2 (`return Err(RingError::Full)` unchanged) |
| Consumer: lap mismatch → `Ok(None)` | Task 2 (`SlotState::Empty` arm) + Task 1 test `a_zero_word_and_a_foreign_lap_both_read_as_empty` |
| Consumer: CLAIMED → `Ok(None)`, no spin, hole timer | Task 2 + Task 3 + Task 4 |
| Consumer: committed → read, crc failure is fail-stop `Corrupt`/`BadCrc` | Task 1 (`decode_record_slice` returns `BadCrc`) + Task 2 (the node's drain arm ends the cycle) |
| Padding markers consumed the same way | Task 1 (`decode_record_slice`'s padding branch) + Task 2 (`claim`'s padding path uses the same word) |
| Consumer makes **no** writes into the slot region | Task 2 (`try_read` only stores `consumer_position`) — stated in the module doc |
| Memory ordering: producer Acquire on `consumer_position`; consumer Acquire on the commit word | Task 2 + Task 5 (loom model, mutation-checked) |
| Ring file magic bumped, `RING_HEADER_LEN` unchanged | Task 1 (`RING_MPSC_MAGIC`) + Task 2 (`create`/`open`) + `an_old_format_ring_file_is_refused_on_open` |

**Spec §4.2 coverage:** sized hole skipped after `hole_timeout` (default 1 s) → Task 4 `a_sized_hole_is_skipped_after_the_timeout_and_counted`; counter exported on the cnc reserved band and `/metrics` → Task 7; logged once per hole → Task 7's `publish_ring_holes` (on change only); unsized hole → `RingError::Wedged` → Task 4 `an_unsized_hole_wedges_after_the_timeout` and Task 7's named `IngressRingWedged` fail-stop through the existing panic → `agent_failstopped` → exit 1 chain.

**Spec §4.3 coverage:** existing tests kept green (Task 2 runs them, and the one test that poked `publish_position` is corrected rather than deleted); preemption test (Task 3, with its discrimination check); hole-skip and zero-hole fail-stop (Task 4); loom model (Task 5, with its mutation check); `ring_mpsc_record` fuzz target and `uc_protocol_cnc` untouched (Task 6); dev-box convoy reproduction as the regression smoke (Task 9).

**Pinned-interface audit:** `MpscRing::{create, open, into_split, file_len}`, `MpscProducer::try_write`, `MpscConsumer::{try_read, wait_handle}` keep their exact signatures (Task 2's code shows them). `RingHeader` keeps every field name and `RING_HEADER_LEN`; `wake_word`/`arm`/`disarm`/`signal`/`park` are untouched. SPSC and Broadcast keep `RING_MAGIC` and the `publish_position` protocol — the only shared code they touch is `write_record_at`/`write_padding_marker_at`/`init_ring_header`/`validate_ring_header`, all of which become thin wrappers with byte-identical behaviour (Task 1 shows the diff, and their own round-trip tests are the check). Additions only: `RING_MPSC_MAGIC`, `RingError::Wedged`, `holes_skipped`, `set_hole_timeout`, `hole_timeout`, `DEFAULT_HOLE_TIMEOUT`, the commit-word helpers, `PendingClaim` + its two `#[doc(hidden)]` hooks. **One deviation, deliberate and flagged:** the pinned list names `RingError::MagicMismatch`; the tree spells that variant `BadMagic` today with the Display string "magic mismatch", so Task 1 renames it (3 call sites, all inside `common.rs`, nothing outside the crate matches on it) rather than adding a second variant meaning the same thing.

**Placeholder scan:** grepped the plan for `TBD`, `similar to`, `add error handling`, `...` as a stand-in, and `<name>` outside the `write_target`/docs contexts where it is literal shell/regex syntax. Every step carries either real code, a real command with its expected output, or a real prose edit. One value is deliberately left to the run rather than guessed: `<N>` in Task 9's commit message, which is the measurement being recorded. Test counts are pinned (73 existing `uc_protocol` tests + 7 in Task 1 = 80; +2 in Task 2 = 82; the `ring::mpsc` module goes 4 → 6 → 7 → 11) — if a run disagrees, a test was silently dropped and that is a finding, not a rounding error.

**Type consistency:** `holes_skipped` is `u64` in `MpscConsumer`, `u64` through `CncPage::store_ingress_holes_skipped`, and `u64` into `push_counter` — no casts. `hole_timeout` is `std::time::Duration` on both accessors and in `DEFAULT_HOLE_TIMEOUT`. `RingError::Wedged { position: u64 }` matches `consumer_position`'s type and `drain_ingress_ring`'s `&e` borrow in `ring_error_fail_stop(&self, e: &RingError, …)`. Commit-word fields are `u32` throughout (`lap_of` returns `u32`, `SlotState::{Claimed{advance:u32}, Committed{length:u32}}`), widened to `usize`/`u64` only at the two arithmetic sites, both bounds-checked first. `decode_record_slice` returns `(RecordHeader, usize)` — the same shape `try_read_record_at` returns, so the consumer's advance arithmetic is unchanged.

**Two facts worth re-checking during execution, because they are load-bearing and cheap to get wrong:**
1. A padding marker's length is bounded by `max_msg_size`, not by `capacity` — the padding path only runs when `bytes_to_tail < advance ≤ align(max_msg_size)` — which is why 18 bits suffice for it. If a future change lets padding cover more than `max_msg_size` bytes, the length field overflows silently.
2. The consumer's clock is read **only** on the hole path. An empty ring returns before it. Losing that turns a `clock_gettime` into the node's hottest idle instruction.
