# task08 — shmem ring buffer optimization

Status: **complete** (branch `autoresearch/shmem-may29`). First optimization run
driven by the `uc_autobench` Claude-Code autoresearch loop (the framework itself
is task07). Target: the `uc_protocol` lock-free shared-memory ring buffers
(SPSC / MPSC / Broadcast) used for all same-host inter-process traffic.

Platform: Apple Silicon / arm64. The wins below are arm64-relevant; on x86 the
fence removals are no-ops (x86 loads/stores are already acquire/release) but the
cached-cursor changes still help under contention.

## Result

| metric | before | after | change |
|--------|--------|-------|--------|
| `spsc_p99_ns` (round-trip latency) | ~29 (noisy) | **~15** | ~2× faster |
| `spsc_throughput_msgs` (saturated) | ~25M/s | **~44M/s** | ~1.75× |
| `mpsc_4p_throughput` (4 producers) | ~5.9M/s | **~6.9M/s** | ~1.17× |
| e2e `submit_to_resp_p99` (Goodhart gate) | 41.4ms | ~39.9–40.1ms | no regression |
| `ring_torture` (frozen behavioral suite) | 7/7 | 7/7 | unchanged |

Six changes kept, all on the ring hot paths. No public wire-format or layout
change; `RingHeader` and the on-wire frame are byte-identical. One API-surface
caveat: `MpscProducer` is now `!Sync` (see below).

## The single invariant that makes every win safe

The whole cross-process protocol's memory visibility rests on **one**
happens-before edge:

> The producer advances `publish_position` with a **Release** store *after*
> writing the full record. Every reader loads `publish_position` with **Acquire**
> and only ever touches slots that lie fully *below* the value it observed.

Given that edge, a reader that has seen `publish_position == X` is guaranteed to
see all slot bytes written below X. Everything kept here is a consequence of
recognizing that this one edge already provides the ordering that some
*additional* barrier or *redundant* atomic was separately paying for.

## Kept optimizations

### 1. Drop the dead `claim_position` store in the SPSC producer — `541dc6d`
`uc_protocol/src/ring/spsc.rs`. SPSC has a single producer whose sole cursor is
`publish_position`; `claim_position` was written on every record but read by
nobody (only MPSC needs a distinct claim phase). The producer now sources its
position from a Relaxed load of `publish_position` (it is the sole writer). One
fewer atomic store per write. `spsc_p99` 29 → ~18.7.

### 2. Drop the redundant Acquire fence in `try_read_record_at` — `c868c9b`
`uc_protocol/src/ring/common.rs`. Every reader (SPSC/MPSC/Broadcast `try_read`)
already does `publish_position.load(Acquire)` before touching the slot, which
orders the subsequent slot reads. The extra `fence(Acquire)` inside the read
helper was a per-read `dmb ishld` on arm64 with no purpose. `spsc_p99` → ~16.8.

### 3. Drop the redundant Release fences in the write helpers — `45807e5`
`uc_protocol/src/ring/common.rs` (`write_record_at` + `write_padding_marker_at`).
The caller advances `publish_position` with a Release store *after* these return,
which orders all slot writes (including the length-last store) before any reader
can observe the slot. The internal `fence(Release)` (a per-write `dmb ish`) was
redundant. The MPSC multi-producer publish chain still holds: each producer's
publish-turn wait-loop does an Acquire load of `publish_position`, so
release/acquire transitivity makes every producer's slot writes visible.
`spsc_p99` → ~15.5.

### 4. Cache `consumer_position` on the SPSC producer — `6d3593e`
`uc_protocol/src/ring/spsc.rs`. The producer loaded `consumer_position` (Acquire,
a cross-core `ldar`) on every write just to check free space. The consumer only
ever *advances* `consumer_position`, so a locally cached copy is a safe **lower
bound**: free space computed from it is never over-estimated, so a write admitted
by the cached check can never claim into unread territory. Refresh from shared
memory (one Acquire) only when the cache reports the ring full, then re-check.
Latency flat (~15ns, the line is L1-hot in a ping-pong bench) but **saturated
throughput ~25M → ~44M/s** because the shared line stops bouncing between cores
on every write.

### 5. Drop the dead `claim_position` store in the Broadcast producer — `d41b8c3`
`uc_protocol/src/ring/broadcast.rs`. Mirror of #1: single producer, sole cursor
is `publish_position`, consumers track their own in-memory `head`. Removes two
atomic stores per write. `broadcast_4sub_p99` flat (fan-out reads dominate that
metric); kept as a code-deletion wash.

### 6. Cache `consumer_position` on the MPSC producer via `Cell<u64>` — `45ca8c7`
`uc_protocol/src/ring/mpsc.rs`. Same lower-bound trick as #4, applied to the
multi-producer claim loop, which loaded `consumer_position` on every CAS attempt.
With N producers that load bounced the consumer's cache line to every producer
core on every attempt. `mpsc_4p_throughput` ~5.9M → ~6.9M/s (modest — the
CAS-retry loop and publish-order spin dominate MPSC cost, not this load).

**API caveat:** `MpscProducer::try_write` takes `&self` (the producer is `Clone`
and fanned out across threads), so the cache uses `Cell<u64>`. This makes
`MpscProducer` **`!Sync`** (still `Send`). The supported usage — already what the
torture suite and all call sites do — is **clone the producer per thread**, never
share one `&MpscProducer` across threads. If a future caller needs a shared
`&MpscProducer: Sync`, revert this commit or switch the cache to a relaxed
`AtomicU64` (it is only a hint, so a torn/raced read is harmless).

## The Goodhart guardrail — do NOT remove the frame CRC

`write_record_at` computes a `crc32fast` checksum over `msg_type..end-of-payload`
and `try_read_record_at` validates it. It never fires in the happy-path
microbench or e2e, so deleting it would "win" the benchmark — but it is the real
integrity check across the shmem process boundary, and **the e2e Goodhart gate
would not catch its removal**. It must stay. This is the canonical example of why
the task keeps a frozen behavioral suite + an e2e gate alongside the microbench.

## What was tried and rejected (don't re-attempt without new evidence)

All measured median-of-5+ and reverted as within-noise / no gain:

- Coalesce the 16-byte frame-header read (and 12-byte write tail) into single
  `copy_nonoverlapping` calls — flat.
- `#[inline]` on the hot-path helpers — flat; the release profile already has
  `lto = "thin"`, `codegen-units = 1`, which inlines across the crate boundary.
- Compute the producer CRC from source registers + payload to avoid reading the
  just-written mmap bytes back — flat; the written bytes are L1-hot in a
  single-thread bench.
- Cache `capacity` / `max_msg_size` in the Producer/Consumer structs — flat; the
  header config line is already L1-resident.
- Cache `publish_position` on the *consumer* (mirror of #4/#6) — flat; the
  round-trip bench re-advances `publish_position` before every read, so the cache
  never saves an Acquire, and throughput is already saturated by the
  producer-side cache.

## Remaining headroom (intentionally not pursued)

The SPSC round-trip is near its structural floor at ~15ns: the two remaining
`ldar` + two `stlr` per round-trip and the payload copy + CRC are all required by
the cross-process safety the torture suite enforces and cannot be weakened.
Further gains would need a different design (e.g. batched claim, eliminating the
CRC on trusted same-host paths behind a feature flag), not micro-tuning.

## Reproducing / measurement notes

- Fitness: `cargo run -p uc_autobench --bin run-iter --release -- --task shmem
  --json --baseline-spsc-p99-ns <n> --baseline-e2e-p99-ns <n>` (baselines are
  **integer nanoseconds**).
- `spsc_p99_ns` has large between-process variance (~14%); a single run is
  unreliable. Every keep/discard decision compared **median-of-5** (often A/B
  against the prior commit back-to-back), not single samples.
- Keep rule for this run: a variant is kept if it clearly beats the current best
  on **either** `spsc_p99_ns` **or** `spsc_throughput_msgs` (beyond noise)
  without regressing the other, passes the e2e gate, and keeps `ring_torture`
  green. A code-deletion wash (no metric change) is also a keep.
- Full per-iteration history (keeps + discards, with the reasoning): see
  `uc_autobench/tasks/shmem/results.tsv` and the matching commit messages.
