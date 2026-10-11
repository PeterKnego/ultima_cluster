# UC2 Backup Tier (3a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Copy the cluster's agreed snapshot sets and committed journal into an off-node directory, record how far it reaches as a replicated watermark, and restore a cluster, a node, a fork or a diff-replay corpus from it.

**Architecture:** A new `uc_node::tier` module owns the tier format (zstd-compressed, CRC-checked, write-once objects), the `BackupTarget` trait with a filesystem implementation, chain/verify logic, retention, the `uc2-backup` writer thread and the offline `materialize` builder. The cluster FSM gains `CLUSTER` kind 7 `BackupWatermark` (reported over pairwise datagram 27, appended by the leader), Settings v5 gains two retention fields, and `uc2ctl` gains a `tier` verb group. The writer's own node holds its purge floor at the writer's progress.

**Tech Stack:** Rust (MSRV 1.89, pinned 1.96), `zstd` (new), `crc32fast`, `sha2` (via `artifact_hash_of`), `clap`, `serde`/`toml`, existing `uc_journal`, `uc_log`, `uc_protocol`, `uc_service`.

**Spec:** `docs/superpowers/specs/2026-10-10-uc2-backup-tier-design.md` (read it whole, including D1–D11). Its parents: `2026-10-01-uc2-snapshot-catalog-design.md` and `2026-10-09-uc2-snapshot-lifecycle-design.md` — read their errata blocks.

## Global Constraints

- Wire stays `0.11.0` (unreleased; spec §9). Extend the `0.11.0` comment block in `uc_protocol/src/version.rs`; do not bump `CURRENT`.
- `CLUSTER` kind `7` = `BackupWatermark`; pairwise datagram `DGRAM_KIND_BACKUP_REPORT = 27`; admin op `11` = `tier_forget`; refusal `62` = `tier_unknown`; `MAX_BACKUP_TIERS = 4`.
- `SETTINGS_VERSION` 4 → 5, `SETTINGS_LEN` 36 → 46 (`backup_retain_sets: u16` @36, `backup_retain_age_secs: u64` @38); older versions decode both as `0`. `backup_retain_age_secs == u64::MAX` refused with `47 settings_bounds`.
- `CLUSTER_IMAGE_VERSION` 5 → 6, one trailing `backups` blob (`u32 len ‖ bytes`) after `catalog`; v≤5 images load with no tiers.
- Tier object: 48-byte header, magic `ULTTOBJ1` (artifact) / `ULTJCHK1` (chunk), codec `0 none` / `1 zstd`; block = `stored_len u32 ‖ raw_len u32 ‖ crc32(raw) u32 ‖ bytes`, `raw_len ≤ 1 MiB`; CRC-32/IEEE via `crc32fast`. Every check is over raw bytes.
- Tier layout and names exactly as spec §4.1: `TIER`, `sets/<P:020>/row-<id>.ultsnap`, `sets/<P:020>/cluster.ultcluster`, `sets/<P:020>/SET`, `journal/<from:020>-<to:020>.ultj`, `.tmp/`. `TIER` `format=uc2-tier-v1`.
- `[backup]` keys: `target` (required), `chunk_bytes` (default `64MiB`), `quiet_interval` (default `60s`), `compression` (`"zstd"` default | `"none"`), `compression_level` (default `3`, `1..=19`). Parsed into `StartupOptions`, passed as `StartOpts::backup: Option<BackupConfig>` — never a `NodeConfig` field.
- Writer thread name `uc2-backup`, `IdleStrategy::Sleep(Duration::from_millis(100))`, appended LAST to the node's `agents` vec (the `/healthz` indices 0–4 are fixed).
- New dependency `zstd` (BSD-3-Clause/MIT — `deny.toml` already allows BSD-3-Clause); `crc32fast` moves from dev- to regular dependency of `uc_node`.
- Scratch and test artifacts never under `/tmp`: tests use `CARGO_TARGET_TMPDIR` via the existing `tempdir()` helpers; manual runs use `$HOME/scratch/`.
- Every commit passes `cargo fmt --all -- --check`. Before any push: `cargo clippy --workspace --all-targets -- -D warnings`, the MSRV gate `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`, and every feature-gated clippy line in `.github/workflows/ci.yml` (`uc_service --features apply-profile`, `uc_gateway test-util`, `uc_lincheck replay-bin`, `uc_crashtest hard-crash-tests`).
- No `Co-Authored-By` or any attribution trailer in commits.
- Use a private `CARGO_TARGET_DIR` (e.g. `$HOME/.cache/cargo-target-tier`) for proof-stack runs.
- `docs/superpowers/` artifacts stay in place; never delete them.

## Review Focus

1. A failover truncating bytes above commit while the writer cuts a chunk — the chunk must end at or below the `min(commit, durable)` the writer read. Pinned by Task 8's `a_chunk_never_holds_bytes_a_failover_truncated`.
2. Two writers into one target, one behind a purged journal — the watermark follows the extending chain, never reports reach across a gap. Pinned by Task 3's `reach_stops_at_a_gap_and_a_new_chain_starts_at_its_set` and Task 8's `two_writers_share_a_target_and_the_watermark_never_crosses_a_gap`.
3. Retention deleting the set a concurrent restore chose — the restore refuses by name. Pinned by Task 11's `a_restore_whose_set_vanishes_mid_build_refuses_and_leaves_no_instance_dir`.
4. A pin made then completed under tight tier retention — the rollback set is protected until completion, then ages out. Pinned by Task 10's `pin_protection_holds_until_completion_then_lifts`.
5. A whole-cluster restore with more than `fsm_lag` of replay between S and Q — the restored cluster elects and commits (spec §13). Pinned by Task 14's capstone (`Q` chosen ≥ 4 × `fsm_lag` past S); Task 0 establishes the current behaviour first.

---

## File structure

| File | Responsibility |
|---|---|
| `uc_node/src/tier/mod.rs` | module root, `TierError`, re-exports |
| `uc_node/src/tier/object.rs` | tier-object codec: header, blocks, zstd/none, streaming read/write, CRC |
| `uc_node/src/tier/layout.rs` | path naming, `TIER` and `SET` text formats, parsing names |
| `uc_node/src/tier/target.rs` | `BackupTarget` trait, `FsTarget`, `.tmp` cleanup, `TIER` create/check |
| `uc_node/src/tier/chain.rs` | listing sets/chunks, chain + reach computation, overlap check, `show`/`verify` |
| `uc_node/src/tier/retention.rs` | the deletable-set rule (spec §7.2), pin protection, deletion order |
| `uc_node/src/tier/writer.rs` | `BackupConfig`, the `uc2-backup` duty cycle, gap handling, claims |
| `uc_node/src/tier/materialize.rs` | the offline builder (spec §8.1) and its four modes |
| `uc_protocol/src/v2/backup.rs` | `BackupWatermark` + list codecs, `BackupReportBody` |
| `uc_node/src/cluster_fsm.rs` | kind 7 apply/validate/query, `ClusterState::backups`, view, image v6 |
| `uc_node/src/node.rs` | report send/receive, leader coalescing + append, op 11, purge/keep holds, writer spawn |
| `uc_ctl/src/tier.rs` | `uc2ctl tier show/verify/restore/fork/export-corpus/forget` |
| `uc_node/tests/backup_tier.rs` | in-process writer/watermark/hold/retention tests |
| `uc_node/tests/lin_backup_tier.rs` | the restore capstone |
| `docs/how-to/back-up-to-a-tier.md` | new how-to |

---

### Task 0: Pre-flight — establish the two unverified risks

**Files:**
- Create: `uc_node/tests/preflight_tier_risks.rs` (throwaway; deleted at the end of this task)
- Modify: the SDD ledger (`<workspace>/progress.md`) only

**Interfaces:** Consumes the `uc_node/tests/catalog.rs` fixture shape (copy what you need; there is no shared helper crate). Produces two ledger rulings that Tasks 11, 12 and 14 read.

Spec §13 names two facts nobody has checked. This task measures them before code depends on them.

- [ ] **Step 1: Cold restart past `fsm_lag`.** Write a test that starts a 3-voter cluster with `SumSm` services (copy `catalog.rs`'s `spawn`, `opts`, `start_sums`, `submit_frames`, `await_single_leader`), sets `Settings { fsm_lag_bytes: 64 * 1024, ..Settings::genesis_default() }`, submits ≥ 4 × 64 KiB of frames, commands an instant early (`command_instant`) so the newest set S is far below the log end, stops every node and service, restarts every node and service, and asserts a new submit commits within 20 s (`node.counters().commit` passes the pre-stop `append`).

```rust
#[test]
fn a_whole_cluster_cold_restart_past_fsm_lag_commits() {
    let _g = serialize();
    let mut o = opts("tier-pf-cold", false);
    o.settings = Settings { fsm_lag_bytes: 64 * 1024, ..Settings::genesis_default() };
    let mut c = spawn(3, 0, o, |_| true);
    start_sums(&mut c);
    let l = await_single_leader(&c, 20);
    let _s = instant_until_complete(&c, l, &[0, 1, 2]);
    submit_frames(c.node(l), 4 * 64 * 1024 / 64);
    let end = c.node(l).counters().append.load_acquire();
    c.stop_all_services();
    for i in 0..3 { c.nodes[i].stop(); }
    for i in 0..3 { c.nodes[i].start(); }
    start_sums(&mut c);
    let l = await_single_leader(&c, 20);
    submit_frames(c.node(l), 1);
    let deadline = Instant::now() + Duration::from_secs(20);
    while c.node(l).counters().commit.load_acquire() <= end {
        assert!(Instant::now() < deadline, "cold restart never committed past {end}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
```

(`stop_all_services` = drop `c.svcs` in order; add it to your copy of the fixture.)

- [ ] **Step 2: Run it 5 times.** `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-tier cargo test -p uc_node --test preflight_tier_risks a_whole_cluster_cold -- --nocapture` ×5. Record pass/fail counts in the ledger as `Ruling PF-cold: <n>/5 committed — <consequence>`. If any run fails, file a GitHub issue ("whole-cluster cold restart past fsm_lag never commits", body = the test and the failure output) and record that Task 14's capstone depends on that issue's fix; STOP and report to the controller — the plan's whole-cluster restore is blocked on it (spec §13).

- [ ] **Step 3: Fork membership after `force-single-member`.** Write a test that runs a 3-voter cluster to a complete instant, stops all three, calls `uc_node::recovery::force_single_member(dir(0), id0)`, restarts node 0 alone, and asserts (a) it becomes leader and commits a submit within 20 s, and (b) after one more `command_instant`, the cluster FSM's committed membership (`c.node(0).cluster_view().membership()`) has exactly one voter. Record `Ruling PF-fork: (a) <yes/no>, (b) <yes/no>` in the ledger. If (b) is no (the FSM still lists three voters), Task 12's fork step must append a `Membership` command — the ledger ruling says so and Task 12's Step 5 applies.

- [ ] **Step 4: Delete the throwaway test file and commit nothing but the ledger.** `git rm -q --cached uc_node/tests/preflight_tier_risks.rs 2>/dev/null; rm uc_node/tests/preflight_tier_risks.rs`. The ledger is git-ignored scratch; there is no commit in this task.

---

### Task 1: Tier object codec

**Files:**
- Create: `uc_node/src/tier/mod.rs`, `uc_node/src/tier/object.rs`
- Modify: `uc_node/src/lib.rs` (add `pub mod tier;`), `uc_node/Cargo.toml` (add `zstd = "0.13"` and move `crc32fast` to `[dependencies]`), root `Cargo.toml` `[workspace.dependencies]` (`zstd = { version = "0.13", default-features = false }`), `Cargo.lock`
- Test: in-module `#[cfg(test)] mod tests` in `object.rs`

**Interfaces:**
- Produces:
  - `pub const OBJECT_HEADER_LEN: usize = 48; pub const BLOCK_HEADER_LEN: usize = 12; pub const MAX_BLOCK_RAW: usize = 1 << 20;`
  - `pub const MAGIC_ARTIFACT: [u8; 8] = *b"ULTTOBJ1"; pub const MAGIC_CHUNK: [u8; 8] = *b"ULTJCHK1";`
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum Codec { None = 0, Zstd = 1 }`
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct ObjectHeader { pub magic: [u8; 8], pub codec: Codec, pub raw_len: u64, pub crc: u32, pub from: u64, pub to: u64 }`
  - `pub fn write_object<W: Write + Seek>(dst: &mut W, magic: [u8; 8], codec: Codec, level: i32, from: u64, to: u64, raw: &mut dyn Read, raw_len: u64) -> Result<u64 /*stored bytes*/, TierError>` — writes a zeroed header, streams the blocks, then seeks back and writes the final header (the whole-object CRC is known only at the end). `std::fs::File` and `Cursor<Vec<u8>>` are both `Write + Seek`.
  - `pub struct ObjectReader<R: Read> { .. }` with `pub fn open(src: R) -> Result<Self, TierError>`, `pub fn header(&self) -> &ObjectHeader`, `impl<R: Read> Read for ObjectReader<R>` (yields raw bytes; verifies each block's CRC as it is consumed and the whole-object CRC + length at EOF).
  - `pub enum TierError` in `mod.rs` (see Step 3).

- [ ] **Step 1: Write the failing tests** (in `object.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    fn roundtrip(codec: Codec, raw: &[u8]) -> (ObjectHeader, Vec<u8>, usize) {
        let mut sink = Cursor::new(Vec::new());
        let stored = write_object(&mut sink, MAGIC_CHUNK, codec, 3, 64, 64 + raw.len() as u64,
            &mut Cursor::new(raw), raw.len() as u64).unwrap();
        let out = sink.into_inner();
        assert_eq!(stored as usize, out.len());
        let mut r = ObjectReader::open(Cursor::new(out.clone())).unwrap();
        let mut back = Vec::new();
        r.read_to_end(&mut back).unwrap();
        (*r.header(), back, out.len())
    }

    #[test]
    fn both_codecs_round_trip_and_header_is_exact() {
        let raw: Vec<u8> = (0..3 * MAX_BLOCK_RAW + 17).map(|i| (i % 32) as u8).collect();
        for codec in [Codec::None, Codec::Zstd] {
            let (h, back, _) = roundtrip(codec, &raw);
            assert_eq!(back, raw);
            assert_eq!(h.codec, codec);
            assert_eq!(h.raw_len, raw.len() as u64);
            assert_eq!(h.crc, crc32fast::hash(&raw));
            assert_eq!((h.from, h.to), (64, 64 + raw.len() as u64));
        }
    }

    #[test]
    fn zstd_shrinks_repetitive_data_and_stores_incompressible_blocks_raw() {
        let rep = vec![7u8; MAX_BLOCK_RAW];
        let (_, _, z) = roundtrip(Codec::Zstd, &rep);
        assert!(z < MAX_BLOCK_RAW / 10, "repetitive block compressed: {z}");
        // Incompressible: SHA-256 chained bytes.
        let mut noise = Vec::with_capacity(MAX_BLOCK_RAW);
        let mut h = [0u8; 32];
        while noise.len() < MAX_BLOCK_RAW { h = sha2::Sha256::digest(h).into(); noise.extend_from_slice(&h); }
        let (_, back, z) = roundtrip(Codec::Zstd, &noise);
        assert_eq!(back, noise);
        assert_eq!(z, OBJECT_HEADER_LEN + BLOCK_HEADER_LEN + noise.len(), "stored raw");
    }

    #[test]
    fn empty_object_round_trips() {
        let (h, back, len) = roundtrip(Codec::Zstd, &[]);
        assert!(back.is_empty());
        assert_eq!(h.raw_len, 0);
        assert_eq!(len, OBJECT_HEADER_LEN);
    }

    #[test]
    fn a_corrupted_block_fails_by_name_and_a_partial_read_stops_at_a_block() {
        let raw: Vec<u8> = (0..2 * MAX_BLOCK_RAW).map(|i| (i % 251) as u8).collect();
        let mut sink = Cursor::new(Vec::new());
        write_object(&mut sink, MAGIC_ARTIFACT, Codec::None, 0, 0, 0,
            &mut Cursor::new(&raw), raw.len() as u64).unwrap();
        let mut out = sink.into_inner();
        // Flip a byte inside the SECOND block's payload.
        let second = OBJECT_HEADER_LEN + BLOCK_HEADER_LEN + MAX_BLOCK_RAW + BLOCK_HEADER_LEN + 5;
        out[second] ^= 1;
        let mut r = ObjectReader::open(Cursor::new(out)).unwrap();
        let mut first = vec![0u8; MAX_BLOCK_RAW];
        r.read_exact(&mut first).unwrap();          // block 1 intact
        assert_eq!(first, raw[..MAX_BLOCK_RAW]);
        let err = r.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("block 1 crc"), "{err}");
    }

    #[test]
    fn decode_refuses_bad_magic_codec_reserved_and_truncation() {
        let mut sink = Cursor::new(Vec::new());
        write_object(&mut sink, MAGIC_CHUNK, Codec::None, 0, 0, 32,
            &mut Cursor::new(vec![0u8; 32]), 32).unwrap();
        let out = sink.into_inner();
        for (i, v) in [(0usize, b'X'), (8, 9u8), (9, 1u8)] {
            let mut bad = out.clone(); bad[i] = v;
            assert!(ObjectReader::open(Cursor::new(bad)).is_err(), "byte {i}");
        }
        let mut short = out.clone(); short.truncate(out.len() - 1);
        let mut r = ObjectReader::open(Cursor::new(short)).unwrap();
        assert!(r.read_to_end(&mut Vec::new()).is_err());
    }
}
```

- [ ] **Step 2: Run to verify failure.** `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-tier cargo test -p uc_node --lib tier::object` → FAIL to compile (module missing).

- [ ] **Step 3: Implement.** `mod.rs`:

```rust
//! Backup tier (spec `2026-10-10-uc2-backup-tier-design.md`): the off-node
//! copy of the cluster's agreed history — format, target, chain, retention,
//! the `uc2-backup` writer and the offline restore builder.
pub mod object;

#[derive(Debug, thiserror::Error)]
pub enum TierError {
    #[error("tier io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tier object {what}: {detail}")]
    Corrupt { what: String, detail: String },
    #[error("tier target {0} belongs to app_id {1}, not {2} (backup_target_foreign)")]
    Foreign(String, String, String),
    #[error("tier overlap mismatch at {0} (tier_overlap_mismatch)")]
    OverlapMismatch(String),
    #[error("{0}")]
    Refused(String),
}
```

`object.rs` — header layout `magic(8) ‖ codec(1) ‖ reserved(7)=0 ‖ raw_len u64 ‖ crc u32 ‖ reserved u32=0 ‖ from u64 ‖ to u64` (LE). `write_object` writes a zeroed 48-byte header, then reads `raw` in `MAX_BLOCK_RAW` pieces; for each piece it computes `crc32fast::hash(piece)`, compresses with `zstd::bulk::compress(piece, level)` when `codec == Zstd`, keeps the compressed bytes only if strictly shorter (else stores raw — `stored_len == raw_len` is the flag), and writes `stored_len ‖ raw_len ‖ crc ‖ bytes`. A running `crc32fast::Hasher` over all raw bytes gives the header's `crc`; after the last block it `seek(SeekFrom::Start(0))`s, writes the final header, and seeks back to the end. Bytes read from `raw` must equal `raw_len`, else `Corrupt { what: "source", detail: "length" }`. Returns the total bytes written (header + blocks).

`ObjectReader::open` reads and validates the header (magic is one of the two, codec 0/1, reserved bytes zero). `Read::read` refills an internal raw buffer one block at a time: read the 12-byte block header, `raw_len ≤ MAX_BLOCK_RAW`, `stored_len ≤ raw_len` when stored raw or any length for zstd, read `stored_len` bytes, decompress with `zstd::bulk::decompress(&stored, raw_len)` when `stored_len != raw_len` and codec is zstd, check `crc32fast::hash(raw) == crc` else `io::Error::new(InvalidData, format!("block {n} crc"))`, feed the running hasher. At EOF (no more bytes where a block header would start) check total raw bytes == `raw_len` and the running CRC == header `crc`, else `InvalidData` ("object length"/"object crc"). A short read inside a block is `UnexpectedEof`.

- [ ] **Step 4: Run tests.** `cargo test -p uc_node --lib tier::object` → 5 passed.

- [ ] **Step 5: `cargo deny check licenses` and commit.**

```bash
cargo deny check licenses
git add Cargo.toml Cargo.lock uc_node/Cargo.toml uc_node/src/lib.rs uc_node/src/tier
git commit -m "tier: object codec — 48-byte header, CRC-checked blocks, zstd or raw"
```

---

### Task 2: Layout, `TIER`/`SET`, and `FsTarget`

**Files:**
- Create: `uc_node/src/tier/layout.rs`, `uc_node/src/tier/target.rs`
- Modify: `uc_node/src/tier/mod.rs` (`pub mod layout; pub mod target;`)

**Interfaces:**
- Consumes: `TierError` (Task 1).
- Produces:
  - `layout.rs`: `pub const TIER_FILE: &str = "TIER"; pub const SET_FILE: &str = "SET"; pub const TIER_FORMAT: &str = "uc2-tier-v1";`
    `pub fn set_dir(p: u64) -> String` (`format!("sets/{p:020}")`), `pub fn row_object(p: u64, row: u8) -> String` (`"sets/{p:020}/row-{row}.ultsnap"`), `pub fn cluster_object(p: u64) -> String`, `pub fn set_label(p: u64) -> String`, `pub fn chunk_object(from: u64, to: u64) -> String` (`"journal/{from:020}-{to:020}.ultj"`), `pub fn parse_set_dir(name: &str) -> Option<u64>`, `pub fn parse_chunk_name(name: &str) -> Option<(u64, u64)>`.
    `#[derive(Clone, Debug, PartialEq, Eq)] pub struct TierInfo { pub app_id: String, pub tier_id: u64, pub created_ns: u64 }` with `pub fn render(&self) -> String` / `pub fn parse(text: &str) -> Result<TierInfo, TierError>`.
    `#[derive(Clone, Debug, PartialEq, Eq)] pub struct SetLabel { pub position: u64, pub kind: SetKind, pub time_ns: u64, pub rows: Vec<RowLabel>, pub cluster_hash: u64, pub cluster_size: u64 }`, `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct RowLabel { pub row: u8, pub hash: u64, pub version: u32, pub size: u64 }`, `render`/`parse`, and `pub fn from_catalog(e: &SetEntry) -> SetLabel` (rows whose verdict is `Agreed`, in row order).
  - `target.rs`:
    ```rust
    pub enum Put { Written(u64 /*stored*/), Exists }
    pub trait BackupTarget: Send + Sync {
        fn put_if_absent(&self, path: &str, write: &mut dyn FnMut(&mut std::fs::File) -> Result<u64, TierError>) -> Result<Put, TierError>;
        fn list(&self, dir: &str) -> Result<Vec<String>, TierError>;   // names only; absent dir → empty
        fn open(&self, path: &str) -> Result<Option<std::fs::File>, TierError>;  // absent → None
        fn delete(&self, path: &str) -> Result<(), TierError>;         // absent → Ok
        fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError>;
    }
    pub struct FsTarget { root: PathBuf }
    impl FsTarget { pub fn new(root: impl Into<PathBuf>) -> FsTarget; pub fn root(&self) -> &Path; pub fn clean_tmp(&self, older_than: Duration) -> Result<usize, TierError>; }
    pub fn open_or_create_tier(t: &dyn BackupTarget, app_id: &str, now_ns: u64) -> Result<TierInfo, TierError>;
    pub fn read_tier(t: &dyn BackupTarget) -> Result<Option<TierInfo>, TierError>;
    ```

The `put_if_absent` callback receives a `.tmp/<random>` file opened read-write; `FsTarget` then `sync_all`s it, creates the parent dirs, `rename`s with `std::fs::hard_link` + `remove_file` (link fails with `AlreadyExists` when the final name exists → `Exists`, the tmp is removed; this is the atomic "iff absent" on POSIX filesystems, where `rename` would silently replace), and fsyncs the parent directory.

- [ ] **Step 1: Failing tests** (in-module, using `tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR"))` — `uc_node` is a lib crate, so use `std::env::var("CARGO_TARGET_TMPDIR")` fallback `target/tmp` via the existing `crate::test_util::tempdir()` if present; otherwise create `tempfile::Builder::new().tempdir_in(concat!(env!("CARGO_MANIFEST_DIR"), "/../target/tmp"))`):

```rust
#[test]
fn names_round_trip_and_sort_by_position() {
    assert_eq!(set_dir(7), "sets/00000000000000000007");
    assert_eq!(parse_set_dir("00000000000000000007"), Some(7));
    assert_eq!(parse_chunk_name("00000000000000000064-00000000000000000128.ultj"), Some((64, 128)));
    assert_eq!(parse_chunk_name("64-128.ultj"), None);
    assert_eq!(parse_chunk_name("00000000000000000128-00000000000000000064.ultj"), None, "to > from");
}

#[test]
fn tier_and_set_labels_round_trip_and_refuse_garbage() {
    let t = TierInfo { app_id: "kv".into(), tier_id: 0xabc, created_ns: 5 };
    assert_eq!(TierInfo::parse(&t.render()).unwrap(), t);
    assert!(TierInfo::parse("format=uc2-tier-v9\napp_id=kv\ntier_id=1\ncreated_ns=0\n").is_err());
    let s = SetLabel { position: 4096, kind: SetKind::Full, time_ns: 9,
        rows: vec![RowLabel { row: 0, hash: 1, version: 0x10000, size: 100 }],
        cluster_hash: 2, cluster_size: 50 };
    assert_eq!(SetLabel::parse(&s.render()).unwrap(), s);
}

#[test]
fn put_if_absent_is_write_once_and_crash_residue_is_cleaned() {
    let d = tmp();
    let t = FsTarget::new(d.path());
    let w = |b: &'static [u8]| move |f: &mut File| { f.write_all(b)?; Ok(b.len() as u64) };
    assert!(matches!(t.put_if_absent("sets/1/x", &mut w(b"one")).unwrap(), Put::Written(3)));
    assert!(matches!(t.put_if_absent("sets/1/x", &mut w(b"two")).unwrap(), Put::Exists));
    assert_eq!(std::fs::read(d.path().join("sets/1/x")).unwrap(), b"one");
    assert!(t.list(".tmp").unwrap().is_empty(), "Exists removes its tmp");
    std::fs::write(d.path().join(".tmp/stale"), b"x").unwrap();
    assert_eq!(t.clean_tmp(Duration::ZERO).unwrap(), 1);
}

#[test]
fn a_foreign_tier_is_refused_by_name() {
    let d = tmp();
    let t = FsTarget::new(d.path());
    let a = open_or_create_tier(&t, "kv", 1).unwrap();
    assert_eq!(open_or_create_tier(&t, "kv", 2).unwrap(), a, "second writer reads it");
    let e = open_or_create_tier(&t, "other", 3).unwrap_err();
    assert!(e.to_string().contains("backup_target_foreign"), "{e}");
}
```

- [ ] **Step 2: Run → fail** (`cargo test -p uc_node --lib tier::`).
- [ ] **Step 3: Implement** both files. `tier_id` = `rand::random::<u64>() | 1` (non-zero). `TIER` and `SET` are `key=value\n` lines; unknown keys are refused (`Refused`), missing keys are refused, `format` must equal `TIER_FORMAT`. `SET` keys: `position`, `kind` (`full`/`standby`), `time_ns`, per row `row.<id>.hash` (hex `{:#018x}`), `row.<id>.version` (hex), `row.<id>.size`, then `cluster.hash`, `cluster.size`; render rows in ascending id.
- [ ] **Step 4: Run → pass.**
- [ ] **Step 5: Commit** `git commit -m "tier: layout names, TIER/SET labels, write-once FsTarget"`.

---

### Task 3: Chains, reach, show and verify (library)

**Files:**
- Create: `uc_node/src/tier/chain.rs`
- Modify: `uc_node/src/tier/mod.rs`

**Interfaces:**
- Consumes: Tasks 1–2.
- Produces:
  ```rust
  #[derive(Clone, Debug, PartialEq, Eq)] pub struct TierListing { pub sets: Vec<SetLabel>, pub incomplete_sets: Vec<u64>, pub chunks: Vec<(u64, u64)> }
  pub fn list_tier(t: &dyn BackupTarget) -> Result<TierListing, TierError>;
  /// Longest gap-free run of chunks from `start` (spec §4.5.4). Returns `start` when none begins there.
  pub fn reach_from(chunks: &[(u64, u64)], start: u64) -> u64;
  /// The chain the writer extends and the watermark reports: the newest set S whose reach is maximal;
  /// `(oldest_set, newest_set, through)` where oldest_set = the oldest complete set from which the
  /// same maximal reach is gap-free, newest_set = the newest complete set ≤ through. None when no complete set.
  pub fn watermark_of(l: &TierListing) -> Option<(u64, u64, u64)>;
  /// Chunks covering [from, to) in order, choosing for each step the chunk starting at the cursor
  /// with the greatest `to`; errors on a gap.
  pub fn plan_chain(chunks: &[(u64, u64)], from: u64, to: u64) -> Result<Vec<(u64, u64)>, TierError>;
  /// Streams the raw log bytes [from, to) from the plan into `sink`, verifying every block CRC,
  /// that every chunk's raw length equals to − from, that every frame walks (`read_header`,
  /// `align_frame_len`) inside the chunk, and that bytes covered by two overlapping chunks are equal.
  pub fn read_range(t: &dyn BackupTarget, chunks: &[(u64, u64)], from: u64, to: u64, sink: &mut dyn FnMut(u64, &[u8]) -> Result<(), TierError>) -> Result<(), TierError>;
  #[derive(Debug, Default)] pub struct VerifyReport { pub sets: usize, pub chunks: usize, pub findings: Vec<String> }
  pub fn verify(t: &dyn BackupTarget, deep: bool) -> Result<VerifyReport, TierError>;
  pub fn render_show(info: &TierInfo, l: &TierListing) -> String;
  ```

Overlap rule: when two chunks overlap, `read_range` reads the overlapping bytes from both and compares; `verify` does this for every overlapping pair. `deep` re-hashes each artifact: row objects with `artifact_hash_of(&raw[SNAPSHOT_ENVELOPE_LEN..])` and checks the envelope position equals the set's P (`decode_snapshot_envelope`); the cluster object with `artifact_hash_of(&raw)`. Non-deep checks presence, header `raw_len` == `SET` size, and block CRCs by streaming.

- [ ] **Step 1: Failing tests** — use a helper `fn put_chunk(t, from, to)` that writes a chunk whose raw bytes are valid 32-byte-aligned frames: `(to-from)/32` header-only `FRAME_TYPE_PADDING` frames each with `length = 32` (build with a 32-byte array: `length` u32 LE @0 = 32, type @4 = 2).

```rust
#[test]
fn reach_stops_at_a_gap_and_a_new_chain_starts_at_its_set() {
    let chunks = vec![(0, 64), (64, 128), (192, 256)];
    assert_eq!(reach_from(&chunks, 0), 128);
    assert_eq!(reach_from(&chunks, 192), 256);
    assert_eq!(reach_from(&chunks, 100), 100);
    let l = TierListing { sets: vec![label(0), label(192)], incomplete_sets: vec![], chunks };
    assert_eq!(watermark_of(&l), Some((192, 192, 256)), "the newer chain reaches further");
}

#[test]
fn overlapping_chunks_are_planned_greedily_and_must_match() {
    let d = tmp(); let t = FsTarget::new(d.path());
    put_chunk(&t, 0, 128); put_chunk(&t, 64, 256);
    let l = list_tier(&t).unwrap();
    assert_eq!(plan_chain(&l.chunks, 0, 256).unwrap(), vec![(0, 128), (64, 256)]);
    let mut got = 0u64;
    read_range(&t, &l.chunks, 32, 224, &mut |_, b| { got += b.len() as u64; Ok(()) }).unwrap();
    assert_eq!(got, 192);
    assert!(verify(&t, false).unwrap().findings.is_empty());
}

#[test]
fn an_overlap_that_differs_is_a_named_finding() {
    let d = tmp(); let t = FsTarget::new(d.path());
    put_chunk(&t, 0, 128); put_chunk_with_term(&t, 64, 256, 9); // same frames, different term id
    let r = verify(&t, false).unwrap();
    assert!(r.findings.iter().any(|f| f.contains("tier_overlap_mismatch")), "{:?}", r.findings);
}

#[test]
fn a_set_without_its_label_is_incomplete_and_never_a_start() {
    let d = tmp(); let t = FsTarget::new(d.path());
    put_set_files_without_label(&t, 64);
    let l = list_tier(&t).unwrap();
    assert!(l.sets.is_empty()); assert_eq!(l.incomplete_sets, vec![64]);
    assert_eq!(watermark_of(&l), None);
}
```

- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run → pass.**
- [ ] **Step 5: Commit** `git commit -m "tier: chains, reach, verify and show"`.

---

### Task 4: Protocol — `BackupWatermark`, kind 7, datagram 27

**Files:**
- Create: `uc_protocol/src/v2/backup.rs`
- Modify: `uc_protocol/src/v2/mod.rs` (`pub mod backup;`), `uc_protocol/src/v2/frame.rs` (kind 7 + doc comment at 43–44 + tests at 371/383), `uc_protocol/src/v2/datagram.rs` (kind 27 const + doc), `uc_protocol/src/version.rs` (0.11.0 block), `uc_crypto/src/transport.rs` (`scope_of` arm + test at ~1841), `fuzz/fuzz_targets/uc_protocol_cluster_frame.rs` (exhaustive match arm), `fuzz/fuzz_targets/uc_protocol_datagram.rs` (kind 27), `fuzz/src/seeds.rs` (two seeds), `docs/reference/wire-protocol.md` (datagram table row 27, a body table after `SNAP_REPORT body`, CLUSTER kind 7 row)

**Interfaces:**
- Produces:
  ```rust
  pub const MAX_BACKUP_TIERS: usize = 4;
  pub const BACKUP_WATERMARK_LEN: usize = 48;   // op u8 ‖ reserved [u8;7] ‖ tier_id ‖ oldest_set ‖ newest_set ‖ through ‖ reporter (u64 each)
  pub const BACKUP_REPORT_BODY_LEN: usize = 40; // tier_id ‖ oldest_set ‖ newest_set ‖ through ‖ reporter
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum WatermarkOp { Report = 0, Forget = 1 }
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct BackupWatermark { pub op: WatermarkOp, pub tier_id: u64, pub oldest_set: u64, pub newest_set: u64, pub through: u64, pub reporter: u64 }
  pub fn encode_backup_watermark(w: &BackupWatermark, out: &mut Vec<u8>);
  pub fn decode_backup_watermark(buf: &[u8]) -> Option<BackupWatermark>; // exact length; reserved zero; tier_id != 0; for Report: oldest_set <= newest_set <= through
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct BackupTierEntry { pub tier_id: u64, pub oldest_set: u64, pub newest_set: u64, pub through: u64, pub reporter: u64, pub updated_pos: u64 }
  pub const BACKUP_TIER_ENTRY_LEN: usize = 48;
  pub fn encode_backup_list(l: &[BackupTierEntry], out: &mut Vec<u8>);   // u8 count ‖ entries
  pub fn decode_backup_list(buf: &[u8]) -> Option<Vec<BackupTierEntry>>; // count <= MAX_BACKUP_TIERS, unique tier ids, exact
  pub fn write_backup_report_body(buf: &mut [u8], w: &BackupWatermark);  // Report op only; 40 bytes
  pub fn read_backup_report_body(buf: &[u8]) -> Option<BackupWatermark>; // same validity as decode, op = Report
  // frame.rs
  ClusterKind::BackupWatermark = 7   // from_u8(7) => Some
  // datagram.rs
  pub const DGRAM_KIND_BACKUP_REPORT: u8 = 27;
  ```

- [ ] **Step 1: Failing tests** in `backup.rs`: golden bytes for one watermark (write the expected 48-byte array by hand: `[0,0,0,0,0,0,0,0, tier LE…]`), strict decode (wrong length, non-zero reserved, `tier_id == 0`, `op == 2`, `oldest > newest`, `newest > through` all `None`), list round trip, list refuses 5 entries and duplicate tier ids, report body round trip and 40-byte length. In `frame.rs` change `cluster_kind_six_is_row_genesis`'s `from_u8(7) == None` assertion to `from_u8(8) == None` and add `cluster_kind_seven_is_backup_watermark`. In `datagram.rs` add `backup_report_kind_is_27_and_pairwise_docs_hold` asserting the constant.
- [ ] **Step 2: Run → fail** (`cargo test -p uc_protocol`).
- [ ] **Step 3: Implement**; add the `uc_crypto` `scope_of` arm (`| DGRAM_KIND_BACKUP_REPORT => Scope::Pairwise`) and its test line; add the fuzz arms and seeds `11-backup-watermark` / `23-backup-report` following the neighbouring seed functions; append to the `0.11.0` comment in `version.rs`: "+ backup tier 3a: `CLUSTER` kind 7 `BackupWatermark`, pairwise datagram 27 `BACKUP_REPORT`, Settings v5, cluster image v6 — a 0.10.0 peer drops kind 27 and refuses kind 7 as undecodable; same flag day."
- [ ] **Step 4: Run** `cargo test -p uc_protocol -p uc_crypto` → pass; `(cd fuzz && cargo +nightly build)` → builds.
- [ ] **Step 5: Commit** `git commit -m "protocol: CLUSTER kind 7 BackupWatermark and pairwise BACKUP_REPORT 27"`.

---

### Task 5: Cluster FSM — the watermark record, image v6, view, query, forget

**Files:**
- Modify: `uc_node/src/cluster_fsm.rs`, `uc_protocol/src/v2/cluster_image.rs`, `uc_node/src/cluster_agent.rs` (`note_applied` arm; `read_committed_backups`), `uc_node/src/node.rs` (`REASON_TIER_UNKNOWN = 62` beside `REASON_PIN_ORIGIN_NOT_AGREED`; extend the reason-freeze test at ~20709), `uc_node/src/lib.rs` (re-export), every `ClusterImageParts { .. }` literal (cluster_image.rs 487/535/562/635/690/710/762/787/853/909, cluster_fsm.rs 953/2263/3247/3594, `uc_node/tests/catalog.rs:1513`, `fuzz/src/seeds.rs` 1495/1524/1538/1572), fuzz corpora regenerated (`fuzz/corpus/uc_node_cluster_artifact`, `uc_protocol_cluster_image`, `uc_protocol_cluster_frame`) via the `seed-corpus` bin

**Interfaces:**
- Consumes: Task 4's codec.
- Produces:
  - `ClusterState::backups: Vec<BackupTierEntry>` (genesis empty), `ClusterCommand::BackupWatermark(BackupWatermark)`, `ClusterRefusal::TierUnknown` → 62.
  - `ClusterImageParts::backups: &'a [u8]`; `CLUSTER_IMAGE_VERSION = 6`; decode gates `backups` on `version >= 6` (else empty).
  - `ClusterViewInner::backups: Vec<BackupTierEntry>`; `ClusterView::backups(&self) -> Vec<BackupTierEntry>`.
  - `query` code `7` → `encode_backup_list`.
  - `pub fn read_committed_backups(instance_dir: &Path) -> io::Result<Option<(u64 /*artifact pos*/, Vec<BackupTierEntry>)>>` in `cluster_agent.rs`, modelled on `read_committed_catalog`.

Apply rules (spec §6.1), all deterministic:
- `Report`, known tier: `through' = max(through, w.through)`; when `w.through < through` it is a no-op and the apply still returns accepted `0` (stale news is not a refusal) — count it on the node via `note_applied` (`backup_report_stale`). Otherwise `newest_set' = max(newest_set, w.newest_set)`, `oldest_set' = min(max(oldest_set, w.oldest_set), newest_set')`, `reporter = w.reporter`, `updated_pos = ctx.position`.
- `Report`, unknown tier: insert if `backups.len() < MAX_BACKUP_TIERS`, else no-op (`backup_tiers_full`, counted the same way).
- `Forget`: `validate_replicated` refuses `TierUnknown` when no entry has the id; else remove.

- [ ] **Step 1: Failing tests** (in `cluster_fsm.rs`'s test module, using its `body()` helper):

```rust
#[test]
fn a_watermark_moves_forward_only_and_oldest_stays_below_newest() {
    let mut f = fsm();
    apply(&mut f, 10, ClusterCommand::BackupWatermark(wm(7, 64, 128, 4096, 2)));
    apply(&mut f, 11, ClusterCommand::BackupWatermark(wm(7, 64, 128, 2048, 3))); // stale
    let b = &f.state().backups;
    assert_eq!((b[0].through, b[0].reporter, b[0].updated_pos), (4096, 2, 10));
    apply(&mut f, 12, ClusterCommand::BackupWatermark(wm(7, 9999, 256, 8192, 3)));
    let b = &f.state().backups[0];
    assert_eq!((b.oldest_set, b.newest_set, b.through), (256, 256, 8192), "oldest clamps to newest");
}

#[test]
fn a_fifth_tier_is_ignored_and_forget_refuses_an_unknown_tier_with_62() {
    let mut f = fsm();
    for id in 1..=5 { apply(&mut f, id, ClusterCommand::BackupWatermark(wm(id, 0, 0, 64, 1))); }
    assert_eq!(f.state().backups.len(), MAX_BACKUP_TIERS);
    assert_eq!(apply_verdict(&mut f, 20, ClusterCommand::BackupWatermark(forget(99))), 62);
    assert_eq!(apply_verdict(&mut f, 21, ClusterCommand::BackupWatermark(forget(1))), 0);
    assert_eq!(f.state().backups.len(), 3);
}

#[test]
fn backups_survive_freeze_and_install_and_a_v5_image_loads_with_none() {
    let mut f = fsm();
    apply(&mut f, 10, ClusterCommand::BackupWatermark(wm(7, 64, 128, 4096, 2)));
    let img = freeze(&f);
    let g = install(&img);
    assert_eq!(g.state().backups, f.state().backups);
    let v5 = image_with_version(&f, 5); // build ClusterImageParts by hand, re-encode, patch version + crc
    assert!(install(&v5).state().backups.is_empty());
}
```

plus in `cluster_image.rs` `v6_image_round_trips_a_backups_blob_and_v5_reads_empty` and a version assertion test `images_are_v6` (rename the existing v5 assertion test).

- [ ] **Step 2: Run → fail.** **Step 3: Implement** — follow the RowGenesis (kind 6) wiring exactly: `decode_command`/`encode_command`/`validate_replicated`/`apply` arms, `freeze` adds `backups: &backups_bytes`, `install_snapshot` decodes it (empty → `Vec::new()`), `ClusterView::publish` clones `backups` into inner, `note_applied` logs `backup_watermark_applied{tier, through}`. Update the image doc comments (cluster_image.rs 1–27 and 56–70; cluster_fsm.rs ~905–930).
- [ ] **Step 4: Run** `cargo test -p uc_protocol -p uc_node --lib` → pass. Regenerate fuzz corpora: `(cd fuzz && cargo +nightly run --bin seed-corpus)`.
- [ ] **Step 5: Commit** `git commit -m "cluster FSM: BackupWatermark record, image v6, view and query"`.

---

### Task 6: Settings v5 — tier retention

**Files:**
- Modify: `uc_protocol/src/v2/settings.rs`, `uc_protocol/src/v2/cluster_image.rs` (settings-length match at ~244 AND ~262), `uc_node/src/cluster_fsm.rs` (`validate_replicated` Settings arm; `to_state` ~1455; view atomics `backup_retain_sets: AtomicU16`, `backup_retain_age_secs: AtomicU64` set in `publish`), `uc_node/src/config_file.rs` (`SettingsSection` + genesis literal ~845 + tests), `uc_ctl/src/settings.rs` (`SettingsFile`, `parse_settings`, `render_settings_line` — new keys BEFORE the `datagram_mtu` suffix), every full `Settings { .. }` literal (fuzz/src/seeds.rs:1800, uc_ctl/src/settings.rs ~118/~291, config_file.rs ~845, cluster_fsm.rs 1455/1711/1719/1744/1857/1895/1930/1989, settings.rs tests), `docs/reference/configuration.md`, `docs/reference/uc2ctl.md`

**Interfaces:**
- Produces: `Settings::{backup_retain_sets: u16, backup_retain_age_secs: u64}`, `SETTINGS_VERSION = 5`, `SETTINGS_LEN = 46`, `SETTINGS_LEN_V4 = 36`; TOML keys in both `[settings]` (node.toml) and the `settings apply` file: `backup_retain_sets = <u16>`, `backup_retain_age = "<n>(s|m|h|d)"`. `settings show` prints `backup_retain_sets=<n> backup_retain_age_secs=<n>`.

- [ ] **Step 1: Failing tests**: `settings_v5_round_trips_backup_retention_and_v1_to_v4_read_zero`; bump `decode_is_total_and_refuses_unknown_version_and_target`'s unknown version from 5 to 6; `settings_layout_is_frozen` asserts `SETTINGS_LEN == 46` and byte offsets 36/38; a `validate_replicated` test that `backup_retain_age_secs = u64::MAX` refuses 47; config_file `settings_backup_retention_seeds_genesis_and_defaults_to_zero`; uc_ctl `backup_retention_parses_and_shows` (`backup_retain_age = "30d"` → 2_592_000). Add a small duration parser `pub fn parse_duration_secs(field: &str, s: &str) -> Result<u64, String>` to `uc_node::config_file` (accepts `<n>(s|m|h|d)`; error text leads with `field`), used by both node.toml and uc_ctl.
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_protocol -p uc_node --lib -p uc_ctl` → pass.
- [ ] **Step 5: Commit** `git commit -m "settings v5: backup_retain_sets and backup_retain_age_secs"`.

---

### Task 7: The report path, leader append, and `tier forget` (op 11)

**Files:**
- Modify: `uc_net/src/receiver.rs` (`NetEvent::BackupReport { from: u32, w: BackupWatermark }`, idx 12, `NET_EVENT_KINDS = 13`, decode before the stale-term drop exactly like kind 26, test), `uc_node/src/node.rs`, `uc_node/src/obs/mod.rs` + `metrics.rs` + every `ObsSources` literal (counters `backup_reports_sent/unsent/appended`), `uc_protocol/src/v2/cnc.rs` (`ADMIN_OP_TIER_FORGET: u32 = 11` + op-number test), `uc_node/src/audit.rs` (`11 => "tier_forget"`, fix test at 710), `uc_ctl/src/main.rs` (`62 => "tier_unknown"` in `reason_str`; `Tier(TierArgs)` group with `Forget` only for now), `uc_ctl/src/tier.rs` (new, `forget`), `docs/reference/uc2ctl.md` (refusal 62 row, op 11)

**Interfaces:**
- Consumes: Tasks 4–5.
- Produces (node.rs, on `Consensus`):
  - `backup_claim: Arc<Mutex<Option<BackupWatermark>>>` — written by the writer thread (Task 8), taken by the consensus pass.
  - `fn deliver_backup_report(&mut self, w: BackupWatermark)` — leader: `self.on_backup_report(self.id, w)`; follower: send `DGRAM_KIND_BACKUP_REPORT` to the leader (`cnc.status().leader_hint` → `id_to_addr`), header position 0; no leader → `backup_reports_unsent += 1` and put the claim back for the next pass.
  - `fn on_backup_report(&mut self, from: NodeId, w: BackupWatermark)` — leader only, `from` must be a member; coalesce into `pending_backup_reports: BTreeMap<u64 /*tier*/, BackupWatermark>` keeping the greatest `through` (ties: greatest `newest_set`).
  - `fn maybe_append_backup_watermark(&mut self) -> bool` — after `maybe_append_row_genesis` in `do_work` (~4502), same `serving && !hold_clients` gate and the `last_cluster_append > view_position` single-in-flight guard; appends ONE pending tier per pass (lowest tier id), dropping an entry whose `through` ≤ the committed entry's (`cluster_view.backups()`).
  - Admin op 11: in `handle_admin`, its own block beside ops 8/9: leader → validate `ClusterCommand::BackupWatermark(Forget{tier_id = req.id as u64 …})` through `validate_cluster_command` (→ 62) then `append_cluster_frame`; follower → retry (2). Audited `tier_forget` with the tier id as detail. `uc2ctl tier forget --tier-id <hex>` carries the u64 tier id split across `AdminReq` (`uc_log/src/cnc.rs:617`, `id: u32`, `ip: u32`): low 32 bits in `id`, high 32 in `ip`, `port = 0`; the node rebuilds `((ip as u64) << 32) | id as u64`. Document it in `audit.rs`'s per-op field paragraph (~135–157).

- [ ] **Step 1: Failing tests**: receiver routes a valid kind-27 body and drops a malformed one; node unit test `a_follower_report_reaches_the_leader_and_commits_one_watermark` (in `uc_node/tests/backup_tier.rs`: 3 voters, put a claim into node 2's `backup_claim` via a `#[doc(hidden)] pub fn offer_backup_claim_for_test(&self, w)`, await `cluster_view().backups()` on all three); `two_claims_for_one_tier_coalesce_to_the_furthest`; `tier_forget_removes_a_tier_and_refuses_62_for_an_unknown_one` (uc_ctl integration via the admin path, as `uc_ctl/tests/*_bin.rs` do for settings).
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_net -p uc_node --test backup_tier -p uc_ctl` → pass.
- [ ] **Step 5: Commit** `git commit -m "node: BACKUP_REPORT path, leader-appended watermark, tier forget (op 11, refusal 62)"`.

---

### Task 8: The writer

**Files:**
- Create: `uc_node/src/tier/writer.rs`
- Modify: `uc_node/src/tier/mod.rs`, `uc_node/src/config_file.rs` (`[backup]` section → `StartupOptions::backup: Option<BackupConfig>`; byte-size parser `parse_bytes(field, s)` for `"64MiB"`), `uc_node/src/preflight.rs` (`StartupOptions` field), `uc_node/src/node.rs` (`StartOpts::backup`; spawn `uc2-backup` after the holdings runner and push it LAST onto `agents`), `uc_node/src/bin/uc2-node.rs` (pass `opts.backup` into `StartOpts`), `docs/reference/configuration.md` (`[backup]`)
- Test: `uc_node/tests/backup_tier.rs`

**Interfaces:**
- Consumes: Tasks 1–3, 7 (`backup_claim`).
- Produces:
  ```rust
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct BackupConfig { pub target: PathBuf, pub chunk_bytes: u64, pub quiet_interval: Duration, pub compression: Codec, pub compression_level: i32 }
  impl Default for BackupConfig { /* target empty, 64 MiB, 60 s, Zstd, 3 */ }
  pub struct WriterShared {            // Arc'd, owned by the node
      pub progress: AtomicU64,         // end of the chain this writer last confirmed in the tier (0 = none) — Task 9's purge hold
      pub set_hold: AtomicU64,         // newest agreed set it still needs to copy (0 = none) — Task 9's keep hold
      pub hold_released: AtomicBool,   // Task 9
      pub stats: WriterStats,          // bytes_written{set,journal}, errors{reason}
  }
  pub struct Writer { .. }
  impl Writer {
      pub fn new(cfg: BackupConfig, app_id: String, node_id: u32, instance_dir: PathBuf,
                 journal: Arc<uc_journal::Journal>, cnc: Arc<CncPage>, first_base: Arc<AtomicU64>,
                 view: Arc<ClusterView>, claim: Arc<Mutex<Option<BackupWatermark>>>,
                 shared: Arc<WriterShared>, target: Box<dyn BackupTarget>) -> Writer;
      /// One bounded duty cycle; true = progress.
      pub fn step(&mut self, now_ns: u64) -> bool;
  }
  ```

Duty cycle (`step`), each a bounded amount of work (one set OR one chunk per call):
1. First call: `clean_tmp(1 h)`, `open_or_create_tier` (foreign → log `backup_target_foreign` once, stop the writer: `step` returns false forever and `stats.errors{foreign}` = 1). Unavailable target → `backup_target_unavailable` once per episode, retry ladder 1 s → 60 s.
2. `list_tier` (cached; refreshed after every write and every 10 s).
3. **Sets:** `view.snapshot_inner().catalog` entries with `is_agreed()` whose position the tier lacks and this node holds complete on disk (row artifacts `snapshots/<row>/snap-<P>.ultsnap` for every row whose verdict is Agreed + `snapshots/cluster/snap-<P>.ultcluster`). Newest first. For each file: hash (`artifact_hash_of` over the payload after the 24-byte envelope for rows, over the whole image for the cluster); mismatch → `backup_set_hash_mismatch` once, skip the set. Then `put_if_absent` each object (`write_object(file, MAGIC_ARTIFACT, codec, level, 0, 0, src, len)`); `Exists` → open the tier object, compare envelope position and payload hash (rows) / hash (cluster), mismatch → `TierError::OverlapMismatch` → writer stops (`Uc2BackupWriterError`). Last, `put_if_absent(SET)`. `set_hold` = the newest agreed set not yet complete in the tier (0 when none).
4. **Journal:** `(oldest, newest, through)` = `watermark_of(listing)`; if none, nothing to extend (wait for a set). E = `through`. C = `min(cnc.counters().commit, cnc.counters().durable)`. If `first_base > E` → gap: emit `backup_chain_gap{from: E, to: first_base}` and the next chain starts at the newest set this node holds with position ≥ `first_base` (step 3 copies it; reach then starts there). Else cut when `C − E ≥ chunk_bytes` or (`C > E` and `now − last_cut ≥ quiet_interval`): read `[E, min(C, E + chunk_bytes))` with `uc_log::archive::replay_journal_from(&journal, E)` → walk `ReplayFrame`s, re-serialising each frame's header + payload padded to `align_frame_len` (PADDING frames: write the 32-byte header with its `length`, then zero-fill to `align_frame_len(length)`) until the next frame would pass the end; the end is therefore a frame end ≤ C. Write the chunk with `MAGIC_CHUNK` and `(from, to)`; name `chunk_object(from, to)`.
5. `progress` = new `through`. When anything advanced: `*claim.lock() = Some(BackupWatermark { op: Report, tier_id, oldest_set, newest_set, through, reporter: node_id })`.

- [ ] **Step 1: Failing tests** (`uc_node/tests/backup_tier.rs`, fixture copied from `catalog.rs` with `StartOpts { socket, backup: Some(cfg), ..Default::default() }`):

```rust
#[test]
fn a_writer_copies_the_agreed_set_and_committed_journal_and_reports_it() {
    let _g = serialize();
    let tier = tmp_tier();
    let mut c = spawn_with_writer(3, 1, opts("tier-w1", false), 3 /* the learner writes */, &tier, small_chunks());
    start_sums(&mut c);
    let l = await_single_leader(&c, 20);
    submit_frames(c.node(l), 2000);
    let p = instant_until_complete(&c, l, &[0, 1, 2, 3]);
    await_agreed(&c, &[0, 1, 2, 3], p);
    submit_frames(c.node(l), 2000);
    let w = await_watermark(&c, |w| w.newest_set == p && w.through > p, 30);
    let listing = list_tier(&FsTarget::new(tier.path())).unwrap();
    assert!(listing.sets.iter().any(|s| s.position == p));
    assert!(w.through <= min_commit_durable(&c));
    assert!(verify(&FsTarget::new(tier.path()), true).unwrap().findings.is_empty());
}

#[test]
fn a_quiet_cluster_is_backed_up_after_the_quiet_interval() { /* quiet_interval = 500 ms, chunk 64 MiB; one submit; watermark through reaches it within 5 s */ }

#[test]
fn a_chunk_never_holds_bytes_a_failover_truncated() {
    // Writer on a follower F. Partition the leader L from the others with the fault table
    // (copy lin_partition_v2's FaultConfig use), let L append uncommitted frames, heal after
    // a new leader commits different bytes at those positions; then read every chunk and assert
    // each frame's (position, term) matches the surviving log (replay from a voter's journal).
}

#[test]
fn two_writers_share_a_target_and_the_watermark_never_crosses_a_gap() {
    // Writers on two learners into one target; purge ON with tiny segments; stop writer B's node
    // long enough that its journal purges past the tier end; restart; assert every reported
    // watermark's [oldest_set, through] is gap-free per `reach_from` on the final listing.
}

#[test]
fn a_foreign_target_is_refused_and_nothing_is_written() { /* pre-create TIER with app_id "other" */ }
```

- [ ] **Step 2: Run → fail.** **Step 3: Implement** writer + config plumbing (refuse a `[backup]` without `target` by name: `ConfigError::Invalid { field: "backup.target", .. }`; `compression_level` outside 1..=19 refused; `chunk_bytes` < 1 MiB refused). **Step 4: Run** `cargo test -p uc_node --test backup_tier` → pass; `cargo test -p uc_node --lib config_file` → pass.
- [ ] **Step 5: Commit** `git commit -m "tier: the uc2-backup writer — agreed sets, committed chunks, gaps, claims"`.

---

### Task 9: The purge hold and the set keep-hold

**Files:**
- Modify: `uc_node/src/node.rs` (`maybe_persist_snapshot_floor` ~5660–5760; archive Purge handler ~2136; `prune_snapshots_below_in` keep list ~8007), `uc_node/src/obs/*` (`uc2_backup_hold_released_total`)
- Test: `uc_node/tests/backup_tier.rs`

**Interfaces:**
- Consumes: `WriterShared::{progress, set_hold, hold_released}` (Task 8).
- Produces: `Consensus::backup_shared: Option<Arc<WriterShared>>`; `fn backup_purge_cap(&mut self) -> Option<u64>`.

Rule (spec §5.4): when `backup_shared` is `Some` and `hold_released` is false, the purge target is `min(target, progress)` (`progress == 0` holds everything — no purge until the writer has a chain). Released when `!auto_fetch::fits(holdings.free_bytes, writer.chunk_bytes)` with `free_bytes != FREE_BYTES_UNKNOWN`: set `hold_released = true`, emit `backup_hold_released{progress, free_bytes}` once per episode; re-armed when `fits` holds again. The archive's Purge handler applies the same cap defensively (`below.min(cap)`). The keep list in `prune_snapshots_below_in` gains `set_hold` when non-zero and not released.

- [ ] **Step 1: Failing tests**:

```rust
#[test]
fn the_writers_node_does_not_purge_journal_the_tier_lacks() {
    // purge ON, SEG small; writer target made unwritable (chmod 0o500 on the target dir);
    // submit far past several instants; assert the writer node's archive_first_base stays ≤ its
    // progress (0 here → stays 0) while a non-writer voter's first_base advances.
}

#[test]
fn the_hold_lets_go_when_space_runs_low_and_records_a_gap() {
    // as above, then node.set_free_bytes_for_test(1); assert first_base advances, the obs event
    // backup_hold_released is counted once, and after making the target writable again and
    // restoring free bytes, the tier gains a NEW chain at a set ≥ first_base.
}
```

- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run → pass.**
- [ ] **Step 5: Commit** `git commit -m "node: hold purge and set retention for the backup writer, release on low space"`.

---

### Task 10: Tier retention

**Files:**
- Create: `uc_node/src/tier/retention.rs`
- Modify: `uc_node/src/tier/writer.rs` (run retention once per 60 s after the copy work)

**Interfaces:**
- Consumes: `ClusterView` settings atomics (Task 6), `ClusterViewInner::{pins, running, catalog}`, `uc_node::catalog::{pin_gate, pin_complete}`.
- Produces:
  ```rust
  pub struct RetentionPolicy { pub keep_sets: u16, pub keep_age_secs: u64 }
  /// Positions of protected sets and the protected journal spans [from, to).
  pub struct Protection { pub sets: Vec<u64>, pub spans: Vec<(u64, u64)> }
  pub fn protection(inner: &ClusterViewInner, tier_sets: &[u64]) -> Protection;
  pub fn deletable_sets(sets: &[SetLabel], p: &RetentionPolicy, prot: &Protection) -> Vec<u64>;
  pub fn deletable_chunks(chunks: &[(u64, u64)], oldest_kept: u64, prot: &Protection) -> Vec<(u64, u64)>;
  pub fn apply(t: &dyn BackupTarget, sets: &[u64], chunks: &[(u64, u64)]) -> Result<(), TierError>; // SET first, files, dir, then chunks
  ```

`protection`: for each row whose `pin_gate(...)` is `Some(g)` and `!pin_complete(catalog, row, &g)`: protect the pin's origin, the newest tier set strictly below the pin's record position (the rollback set), and the span `[rollback_set, record_pos)`.

`deletable_sets`: if both limits are 0 → empty. Sort complete sets ascending; newest = last. A set is deletable iff not newest, not protected, and outside every non-zero limit: (`keep_sets == 0` or not among the `keep_sets` newest) AND (`keep_age_secs == 0` or `newest.time_ns − s.time_ns > keep_age_secs × 1e9`).

- [ ] **Step 1: Failing tests** (pure, in-module): defaults keep all; count only; age only; both (union keep); newest never; `pin_protection_holds_until_completion_then_lifts` (build a `ClusterViewInner` with a pin whose `to` line has no agreed set above the record → protected; add one → lifted); `deletable_chunks` never removes a chunk inside a protected span or reaching above the oldest kept set; `apply` removes `SET` before the files (assert with a recording fake target).
- [ ] **Step 2: Run → fail.** **Step 3: Implement**, wire into the writer, and add an integration test `retention_deletes_old_sets_and_reports_the_new_oldest` in `backup_tier.rs` (`backup_retain_sets = 1` via `settings apply` path or genesis settings, three instants, await `oldest_set == newest_set`).
- [ ] **Step 4: Run → pass.** **Step 5: Commit** `git commit -m "tier: replicated retention, pin protection, SET-first deletion"`.

---

### Task 11: The builder and `tier show` / `verify` / `restore`

**Files:**
- Create: `uc_node/src/tier/materialize.rs`
- Modify: `uc_ctl/src/tier.rs`, `uc_ctl/src/main.rs` (`TierCmd::{Show, Verify, Restore, Forget}`), `docs/reference/uc2ctl.md`
- Test: in-module tests + `uc_node/tests/backup_tier.rs`

**Interfaces:**
- Consumes: Tasks 1–3, `uc_log::state::NodeState`, `uc_node::cluster_agent::recover`, `uc_node::ipc::{probe_instance_lock, InstanceLock}`, `uc_journal::{Journal, JournalConfig}`.
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum Mode { Recovery, NewNode { node_id: u32 }, Fork }
  #[derive(Debug)] pub struct Materialized { pub start_set: u64, pub q: u64, pub mode: Mode }
  pub fn materialize(t: &dyn BackupTarget, q: Option<u64>, out_dir: &Path, mode: Mode, now_ns: u64) -> Result<Materialized, TierError>;
  ```

Steps (spec §8.1), assembling under `out_dir/.restore-tmp/` and renaming `journal/`, `state/`, `snapshots/` into place last:
1. Refuse `out_dir` whose `instance.lock` probe is `Held`, or whose `journal/`/`state/`/`snapshots/` is non-empty (`TierError::Refused("TargetNotEmpty …")`).
2. `listing`; S = newest complete set ≤ Q whose `reach_from(chunks, S) ≥ Q` (Q default: the max reach over complete sets). Explicit Q must be ≥ S and ≤ reach, and a frame end — checked while streaming (the last frame must end exactly at Q) — else `Refused("restore_point_unreachable: restorable [a, b]")`.
3. Copy every artifact object of S out of the tier (streaming `ObjectReader`), verifying length and hash against `SET`, into `snapshots/<row>/snap-S.ultsnap` and `snapshots/cluster/snap-S.ultcluster`. A missing object → `Refused("restore set <S> incomplete: <path> missing")`.
4. Journal: `Journal::open(JournalConfig { dir: journal/, preallocate_segments: false, ..JournalConfig::new(journal/) })`; `read_range(S, Q)` into blocks of ≤ 1 MiB cut on frame boundaries; `journal.append(seq, base, &block)?.wait()?` with `seq` from 0 and `base` from S. Collect term changes while walking: `TermMapEntry { term, base: frame_position }` whenever a frame's `leadership_term_id` exceeds the last seen.
5. State via `NodeState::open(state/)`: `store_snapshot_floor(S)`; `store_term_map(&terms)`; `store_config_record(&ConfigRecord { position: S, config: stored(membership_of_cluster_artifact(S)), prev_position: S, prev: same })` where the membership comes from `cluster_agent::recover(snapshots/cluster, ClusterState::genesis_empty(), vec![])?.0.state().membership`; vote left absent; `store_output_progress(match mode { Fork => Q, _ => S })`.
6. `NewNode { node_id }`: refuse `restore_id_reused` when `node_id` is a voter, learner or tombstone in that membership.
7. Write `RESTORED` (`tier_id`, `start_set`, `q`, `mode`, `created_ns`).
8. Finally run `uc_node::backup::verify_artifact`'s checks on the assembled dir through its public entry point and refuse on any finding.

`uc2ctl tier show --target T [--instance-dir D]` prints `render_show` (plus `read_committed_backups` when D given); `tier verify --target T [--deep]` exits 1 on findings, printing each; `tier restore --target T [--at Q] --instance-dir D --confirm-cluster <app_id>` refuses unless `--confirm-cluster` equals `TIER.app_id`, prints a data-loss line `restoring as of Q; history after Q is discarded on every node restored this way` before building; `--new-node --node-id N` replaces `--confirm-cluster`.

- [ ] **Step 1: Failing tests**: `materialize_builds_a_dir_verify_accepts_and_a_node_boots_from` (in `backup_tier.rs`: writer backs up a 3-voter cluster, stop all, materialize at `through` into three fresh dirs, start three nodes on those dirs, await leader, `instant_until_complete` succeeds, `counters().commit ≥ Q`); `a_q_outside_the_chain_is_refused_with_the_restorable_range`; `a_restore_whose_set_vanishes_mid_build_refuses_and_leaves_no_instance_dir` (fake `BackupTarget` wrapping `FsTarget` that deletes `SET`'s second object after the first `open`); `a_running_nodes_dir_is_refused`; `new_node_refuses_a_reused_id`.
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run → pass.**
- [ ] **Step 5: Commit** `git commit -m "tier: materialize builder and uc2ctl tier show/verify/restore"`.

---

### Task 12: Fork, new node, corpus export

**Files:**
- Modify: `uc_node/src/tier/materialize.rs`, `uc_ctl/src/tier.rs`, `uc_ctl/src/main.rs`, `uc_ctl/Cargo.toml` (depend on `uc_diffreplay` with `default-features = false` for `CorpusManifest`)
- Test: `uc_node/tests/backup_tier.rs`, `uc_ctl/tests/tier_bin.rs`

**Interfaces:**
- Consumes: Task 11; Task 0's `Ruling PF-fork`; `uc_node::recovery::force_single_member(dir, node_id)`; `uc_diffreplay::corpus::CorpusManifest::write`.
- Produces: `uc2ctl tier fork --target T [--at Q] --instance-dir D --app-id NEW --node-id N`, `uc2ctl tier export-corpus --target T --row R [--from S] --to Q --out D`, `uc2ctl tier restore --new-node --node-id N`.

- [ ] **Step 1: Failing tests**: `a_fork_boots_alone_under_its_new_app_id_and_replays_no_outputs` (a service with an `OutputHandler` that counts calls; fork at Q; start node under `app_id = "fork"` with members = [itself]; attach the service; submit one command; assert it commits and the handler saw exactly that one, nothing ≤ Q); `fork_refuses_the_same_app_id`; `export_corpus_passes_determinism_mode` (export from a kv-example tier, run `uc2-diffreplay determinism --corpus D` as `uc_diffreplay`'s own e2e tests do — copy their invocation; assert PASS); `a_new_node_seeded_from_the_tier_joins_and_catches_up` (materialize `NewNode { node_id: 9 }`, `uc2ctl add` it as learner via the admin path, await its `durable ≥ leader commit`).
- [ ] **Step 2: Run → fail.** **Step 3: Implement**: fork = `materialize(Mode::Fork)` then `force_single_member(out_dir, node_id)`; when `Ruling PF-fork (b)` was **no**, the fork additionally stages the single-voter membership so the node appends a `Membership` command at its first leadership — implement as the existing reconfiguration path offers (record the exact mechanism chosen in the ledger). Corpus = `materialize(Mode::Recovery)` into `D` then `CorpusManifest { app_id, row, origin: S, end: Q, version: <row version from SET> }.write(D)`.
- [ ] **Step 4: Run → pass.** **Step 5: Commit** `git commit -m "tier: fork, seeded new node, diff-replay corpus export"`.

---

### Task 13: Metrics and alerts

**Files:**
- Modify: `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` + rendering + family-count test), `uc_node/src/obs/mod.rs`, every `ObsSources` literal (node.rs `observability()`, metrics.rs fixtures, `uc_node/tests/obs_http.rs`, `uc_node/examples/m10_alerts.rs`), `packaging/prometheus/uc2-alerts.yml`, `scripts/m10_alert_fire.sh` (`RULE_META`, builder, `RULE_BUILDERS`), `uc_node/examples/m10_alerts.rs` (three scenarios), `docs/how-to/monitor-a-cluster.md` (family count line ~70)

**Interfaces:**
- Produces series: `uc2_backup_through{tier}`, `uc2_backup_oldest_set{tier}`, `uc2_backup_lag_bytes{tier}` (gauges, leader's view: `commit − through`, 0 on followers), `uc2_backup_bytes_written_total{kind}`, `uc2_backup_errors_total{reason}`, `uc2_backup_hold_released_total`, `uc2_backup_reports_sent_total`, `uc2_backup_reports_unsent_total`, `uc2_backup_reports_appended_total`. Alerts: `Uc2BackupBehind` (`uc2_backup_lag_bytes > 4294967296 or (changes(uc2_backup_through[15m]) == 0 and increase(uc2_commit_position[15m]) > 0)`, `for: 15m`, warning), `Uc2BackupHoldReleased` (`increase(uc2_backup_hold_released_total[10m]) > 0`, warning), `Uc2BackupWriterError` (`increase(uc2_backup_errors_total[10m]) > 0`, warning). (Use the existing commit-position series name — check `CONTRACT_SERIES` for its exact name and substitute.)

- [ ] **Step 1: Failing tests**: the family-count test with the new count; `obs_http` scrape contains every new family; the alerts-yml rule test (precedent `the_wont_fit_rule_keys_on_the_verdict_gauge`) for each new rule.
- [ ] **Step 2: Run → fail.** **Step 3: Implement** and add the three `m10_alert_fire.sh` builders + scenarios. **Step 4: Run** `cargo test -p uc_node --lib obs && cargo test -p uc_node --test obs_http && scripts/m10_alert_fire.sh` (needs promtool; if absent, record "not run" in the ledger and run the script's completeness cross-check alone: `python3 scripts/... --check-only` if it has one, else grep `RULE_BUILDERS` for the three names).
- [ ] **Step 5: Commit** `git commit -m "obs: backup tier series and three alerts"`.

---

### Task 14: Restore capstone under the linearizability checker

**Files:**
- Create: `uc_node/tests/lin_backup_tier.rs`
- Modify: `uc_node/tests/lincheck_v2/mod.rs` only if a hook is missing (add `ClusterCfg::backup: Option<(usize /*node idx*/, BackupConfig)>` threaded into `StartOpts`)

**Interfaces:** Consumes everything above; Task 0's `Ruling PF-cold`.

- [ ] **Step 1: Write the capstone** — `LinClusterV2::start_cfg` with 3 voters + 1 learner writer, `fsm_lag_bytes = 64 KiB`, register load from `lin_v2`'s generator for 8 s with one `kill_and_restart_leader` in the middle; record the history; `command_instant` early; keep loading until the tier's `through` passes the start set by ≥ 4 × 64 KiB; read the tier's `through` as Q; stop everything; for each node `materialize(Recovery, Some(Q))` into a fresh dir; restart the cluster on those dirs with the register service; read every register key linearizably; assert: (1) the pre-stop history truncated to ops whose commit position ≤ Q, plus the post-restore reads, is linearizable under `uc_lincheck::checker`; (2) no op with commit position > Q is visible; (3) a new write commits.

Op commit positions: the register response carries `ctx.position` (check `RegisterSm`'s response type; if absent, record positions from the client completion's position field, as `lin_v2` does for its truncation checks).

- [ ] **Step 2: Run 3×** `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-tier cargo test -p uc_node --release --test lin_backup_tier -- --nocapture` → 3/3 pass. If `Ruling PF-cold` recorded failures and its issue is not fixed, this task is BLOCKED — report it.
- [ ] **Step 3: Commit** `git commit -m "lin: backup-tier restore capstone — linearizable up to Q, nothing after"`.

---

### Task 15: Fuzz target, docs, size smoke

**Files:**
- Create: `fuzz/fuzz_targets/uc_node_tier_object.rs`, `docs/how-to/back-up-to-a-tier.md`, `docs/notes/uc2-backup-tier-explained.md`
- Modify: `fuzz/Cargo.toml` (`[[bin]] uc_node_tier_object`), `fuzz/README.md` + `docs/VERIFICATION.md` (target count 25 → 26), `docs/how-to/back-up-a-cluster.md` (pointer to the new how-to as the recommended path), `docs/how-to/upgrade-an-application.md` (rollback = `tier restore --at <record position>` from the protected rollback set), `docs/reference/instance-directory.md` (`RESTORED`), `docs/reference/wire-protocol.md` (verify Task 4 rows), `docs/ops/uc2-runbook.md` (the three alerts), `docs/BACKLOG.md` (Project 3 entry → "3a implemented on design/backup-tier; 3b open" and the spec's §14 questions)

- [ ] **Step 1: Fuzz target**:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Read;

fuzz_target!(|data: &[u8]| {
    if let Ok(mut r) = uc_node::tier::object::ObjectReader::open(std::io::Cursor::new(data)) {
        let mut sink = Vec::new();
        let _ = r.by_ref().take(64 << 20).read_to_end(&mut sink);
    }
    let _ = uc_node::tier::layout::SetLabel::parse(&String::from_utf8_lossy(data));
    let _ = uc_node::tier::layout::TierInfo::parse(&String::from_utf8_lossy(data));
});
```

Run `scripts/fuzz_smoke.sh 60 --min-runs 10000 uc_node_tier_object` → passes (needs nightly + cargo-fuzz; if unavailable record "not run").

- [ ] **Step 2: Size smoke (not a gate)** — run Task 8's first test with `RUST_LOG`-free `--nocapture`, then `uc2ctl tier show` on its target and record raw vs stored bytes for chunks and sets in the ledger (`du -b` on `journal/` and `sets/` vs the `raw_len` sum from `tier show`). One line in the how-to cites it as dev-box smoke, never a performance claim.
- [ ] **Step 3: Docs** — the how-to covers: choose a writer (a learner), `[backup]` keys, retention settings, reading `tier show` and the metrics, `tier verify [--deep]`, the four restores with their guards and the data-loss boundary, the fork's no-outputs rule, and what 3a does not do (nodes never delete because of the tier). The explainer covers why the tier is the cluster's agreed history, not a node's disk (spec §1, D1), the write-once format, and the watermark.
- [ ] **Step 4: Commit** `git commit -m "docs + fuzz: backup tier how-to, explainer, tier-object fuzz target"`.

---

### Task 16: Full proof stack

- [ ] **Step 1:** With `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-tier`: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; the four feature-gated clippy lines from `ci.yml`; the MSRV gate (`cargo +1.89.0 clippy ... --locked`); `cargo test` (workspace); `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay`; `cargo test -p uc_node --test lin_v2`; `cargo test -p uc_node --test lin_partition_v2`; `cargo test -p uc_crashtest --features hard-crash-tests`; `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`; `cargo test -p uc_node --release --test lin_backup_tier`; `cargo deny check`.
- [ ] **Step 2:** Record each command and its result line in the ledger. Any red → fix in the owning task's area, re-run that line and everything after it.
- [ ] **Step 3:** Add the spec's `#### Errata (as built, <date>)` block listing every ruling that changed the spec.
- [ ] **Step 4: Commit** `git commit -m "spec(backup tier): errata as built"`.
