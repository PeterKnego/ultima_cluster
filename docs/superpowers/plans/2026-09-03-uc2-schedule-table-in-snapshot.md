# The schedule table in the snapshot session — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A below-floor joiner (a fresh learner, a cold-started node, a crashed-and-restarted service host under `PurgePolicy::BelowSnapshot`) adopts the leader's current schedule table as part of the snapshot session it installs, so a node can never become a voter, or lead, without the table the cluster has — closing plan 2's ruling R13 and the "every scheduled recurrence stops if that node later leads" limitation.

**Architecture:** The table cannot ride `SNAP_BEGIN`: that body is `SNAP_BEGIN_FIXED_LEN = 122` plus up to 1024 B of config against a 1392 B datagram body budget, and an encoded table is up to 1064 B. So the leader sends one new pairwise datagram, `SNAP_TABLE` (kind 21), immediately after every `SNAP_BEGIN` it sends or re-sends (same session id, same 20 ms cadence, same sealing). The receiver records it on the session's intake, refuses to complete the session until it has one, and on completion publishes `(position, time_ns, table bytes)` to a cell the consensus agent reads **before** the position signal — the exact discipline the carried config already uses (`incoming_snapshot_config`). The consensus agent installs it **by fiat** (a wholesale replace with `prev = None`, like `adopt_snapshot_config`), because below the floor a joiner's own record carries nothing genuine. The leader's side mirrors `config_bytes`: a cache the consensus agent refreshes on every adoption/revert and the sender's `SnapshotSource` closure reads at ship time.

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89); `uc_protocol` (core-only leaf), `uc_net` (sender/receiver agents), `uc_node`, `uc_node/tests/learner.rs` + `timers.rs`, `fuzz/` (nightly + cargo-fuzz), docs.

**Spec:** `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` §5 (as amended by plan 2's errata; Task 0 here records this change), §6–§7. Plan 2 (`docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan2.md`) is on `main` (2ef1b47); this plan builds on its as-built surfaces, listed under Global Constraints.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals --test timers --test services`; after Task 1, `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`.
- **Still the unreleased 2.11.0 flag day**: `uc_protocol::version::CURRENT` stays `0.7.0`, `CNC_V2_VERSION` stays 3.1. A new datagram kind is an addition to an unreleased wire; no shipped node speaks it, so there is no mixed-version case to design for — but the docs say so explicitly.
- **Frozen after it ships**: `DGRAM_KIND_SNAP_TABLE = 21`; `SNAP_TABLE_FIXED_LEN = 22`; the body layout (`session u32 @0 ‖ position u64 @4 ‖ time_ns u64 @12 ‖ table_len u16 @20 ‖ table @22`); pinned by a test whose comment says so.
- **Budget**: `SNAP_TABLE_FIXED_LEN + SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES × SCHEDULE_ENTRY_LEN = 22 + 1064 = 1086 ≤ MTU_DEFAULT − DATAGRAM_HEADER_LEN − CRYPTO_OVERHEAD = 1408 − 16 − 24 = 1368` (a `const` assert, like `SNAP_BEGIN`'s).
- **Plan-2 surfaces this builds on (as built)**: `uc_protocol::v2::schedule::{decode_schedule_table, encode_schedule_table, ScheduleTable, MAX_SCHEDULE_ENTRIES, SCHEDULE_HEADER_LEN, SCHEDULE_ENTRY_LEN}`; `uc_node::schedule_state::{ScheduleRecord { position, time_ns, table, prev }, open, load, store, read_record}`; `Consensus::{adopt_table_frame(position, time_ns, payload), install_table(position, &ScheduleTable, log_time_ns) -> entries, revert_schedule_below(to), arm_schedule_at_boot}` in `uc_node/src/node.rs` (~4126–4300), `schedule_position`, the `schedule_pos_pub`/entries gauges, obs records `schedule_table_adopted`/`schedule_table_reverted`; the snapshot path: `SnapshotSource`/`SnapshotSet` (`uc_net/src/sender.rs:137–215`), `snapshot_set_for` (`uc_node/src/node.rs:7337`), `send_snap_begin` (`sender.rs:1424`), `assemble_snap` (`:1491`), the BEGIN resend at `SNAP_BEGIN_RESEND_NS` (`:76`, `:1273`, `:3877`); receiver: `SnapIntake` (`receiver.rs:705`), `snap_begin` (`:1910`), the intake creation (`:2079`), `snap_complete` (`:2279`) publishing `incoming_snapshot_config` then `incoming_snapshot_pos` (`:2324–2330`), `set_snapshot_intake` (`:1193`) with `IncomingSnapshotSignal = (Arc<AtomicU64>, Arc<Mutex<Vec<u8>>>)` (`:668`); the consensus install handler that reads the config cell and calls `adopt_snapshot_config` (`node.rs:3555–3605`, emits `snapshot_installed`); the config cache `config_bytes: Arc<Mutex<Vec<u8>>>` (`node.rs:544`, `:829`, `:2280`) refreshed by `rebuild_net_for_config`.
- **Determinism**: the table a joiner installs is byte-identical to the leader's record; `time_ns` is the leader's record's `time_ns` (the adopting frame's stamp), never a clock; arming uses the joiner's cnc log clock as `install_table` already does.
- **Fleet spend is user-gated. Never write scratch to `/tmp`.**
- Commit subjects: `type(scope): imperative summary`. Every new or changed test is **watched red first**.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` §5 | errata: the table rides the snapshot session (`SNAP_TABLE`), replacing the "not carried" limitation | 0 |
| `uc_protocol/src/v2/datagram.rs` | `DGRAM_KIND_SNAP_TABLE`, `SnapTableBody`, `write_snap_table_body`/`read_snap_table_body` (total), the budget const, the pin test; `fuzz/src/seeds.rs` seeds for the datagram target | 1 |
| `uc_net/src/sender.rs`, `uc_net/src/receiver.rs` | `SnapshotSet.table`; `send_snap_table` after every `send_snap_begin`; `SnapIntake.table`; `SNAP_TABLE` arm; completion withheld until the table arrived; `incoming_snapshot_table` cell published before the position signal | 2 |
| `uc_node/src/node.rs` | the `schedule_bytes` cache (refreshed by `install_table`/`revert_schedule_below`/boot), read by `snapshot_set_for`; the fiat install `install_snapshot_table(position, time_ns, bytes)` in the snapshot-install handler; `snapshot_installed` gains `table_position` | 3 |
| `uc_node/tests/learner.rs`, `uc_node/tests/timers.rs` | the below-floor joiner adopts the table (record equality); the "no table" case; the capstone: a promoted below-floor joiner that becomes leader keeps the schedule ticking | 4 |
| docs, `RELEASES.md`, `docs/releases.md`, alert annotation, `CLAUDE.md`, `docs/BACKLOG.md`, `docs/VERIFICATION.md` | the limitation becomes a closed item; kind 21 in the wire reference; runbook/explainer/limits/attack-surface updates | 5 |

---

### Task 0: Spec errata — the table rides the snapshot session

**Files:**
- Modify: `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` §5 (the as-built errata block plan 2's Task 8 appended)

- [ ] **Step 1**: In §5's errata block, append:

```markdown
- **Errata (plan 3, snapshot carry).** The table IS carried by the snapshot
  session: the leader sends a `SNAP_TABLE` datagram (kind 21, body `session ‖
  position ‖ time_ns ‖ table_len ‖ table`, ≤ 1086 B) after every `SNAP_BEGIN`
  of a session; the receiver withholds `SNAP_DONE` until it has one and
  publishes it to the consensus agent before the floor signal, which installs
  it by fiat (a wholesale replace, `prev = None`, like the carried config). A
  below-floor joiner therefore holds the cluster's table before it can serve
  or lead. Position `0` with an empty table means "the leader has none" and
  is installed as such. This supersedes the "not carried in the snapshot
  stream" limitation recorded above.
```

- [ ] **Step 2**: Strike the "not carried" sentence in the limitations list (keep it, struck through, with "→ closed by plan 3") so the history stays legible.
- [ ] **Step 3: Commit** `docs(spec): time and timers §5 — the schedule table rides the snapshot session (SNAP_TABLE)`.

---

### Task 1: `uc_protocol` — the `SNAP_TABLE` datagram body

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (constants after `DGRAM_KIND_CONFIG_REPLY`; body struct + codec beside `SnapBeginBody`; `mod tests`), `fuzz/src/seeds.rs` (`uc_protocol_datagram` seeds)
- Test: `uc_protocol/src/v2/datagram.rs` `mod tests`

**Interfaces:**
- Produces:
  - `pub const DGRAM_KIND_SNAP_TABLE: u8 = 21;` (pairwise scope — add it to whatever `Transport::scope_of` table in `uc_crypto` maps kinds 12/13 to `Scope::Pairwise`; grep `DGRAM_KIND_SNAP_BEGIN` in `uc_crypto/src`).
  - `pub const SNAP_TABLE_FIXED_LEN: usize = 22;`
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct SnapTableBody { pub session: u32, pub position: u64, pub time_ns: u64, pub table: Vec<u8> }`
  - `pub fn write_snap_table_body(buf: &mut [u8], b: &SnapTableBody)` (caller sizes `buf` to `SNAP_TABLE_FIXED_LEN + b.table.len()`).
  - `pub fn read_snap_table_body(buf: &[u8]) -> Option<SnapTableBody>` — total: `None` on a short buffer, `table_len > SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES * SCHEDULE_ENTRY_LEN`, `buf.len() != FIXED + table_len`, or `position == 0 && table_len != 0` / `position != 0 && table_len == 0` (a table without a position, or a position without a table, is malformed). It does **not** decode the table (the node does, and fail-stops like CONFIG).
- Consumed by: Tasks 2, 3.

- [ ] **Step 1: Write the failing test** in `datagram.rs`'s `mod tests`:

```rust
    /// FROZEN: kind 21 and the SNAP_TABLE body layout. Never change these bytes.
    #[test]
    fn snap_table_body_pins_bytes_and_is_total() {
        use crate::v2::schedule::{
            MAX_SCHEDULE_ENTRIES, SCHEDULE_ENTRY_LEN, SCHEDULE_HEADER_LEN,
        };
        assert_eq!(DGRAM_KIND_SNAP_TABLE, 21);
        assert_eq!(SNAP_TABLE_FIXED_LEN, 22);
        let b = SnapTableBody { session: 7, position: 4096, time_ns: 99, table: vec![1, 2, 3] };
        let mut buf = vec![0u8; SNAP_TABLE_FIXED_LEN + 3];
        write_snap_table_body(&mut buf, &b);
        assert_eq!(&buf[0..4], &7u32.to_le_bytes());
        assert_eq!(&buf[4..12], &4096u64.to_le_bytes());
        assert_eq!(&buf[12..20], &99u64.to_le_bytes());
        assert_eq!(&buf[20..22], &3u16.to_le_bytes());
        assert_eq!(&buf[22..], &[1, 2, 3]);
        assert_eq!(read_snap_table_body(&buf), Some(b.clone()));
        // no table at all: position 0, len 0
        let none = SnapTableBody { session: 7, position: 0, time_ns: 0, table: vec![] };
        let mut nb = vec![0u8; SNAP_TABLE_FIXED_LEN];
        write_snap_table_body(&mut nb, &none);
        assert_eq!(read_snap_table_body(&nb), Some(none));
        // totality
        assert_eq!(read_snap_table_body(&buf[..21]), None, "short");
        assert_eq!(read_snap_table_body(&buf[..buf.len() - 1]), None, "length mismatch");
        let mut z = buf.clone(); z[4..12].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(read_snap_table_body(&z), None, "position 0 with a table");
        let mut p = nb.clone(); p[4..12].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(read_snap_table_body(&p), None, "position without a table");
        let max = SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES * SCHEDULE_ENTRY_LEN;
        let mut big = vec![0u8; SNAP_TABLE_FIXED_LEN + max + 1];
        big[4..12].copy_from_slice(&1u64.to_le_bytes());
        big[20..22].copy_from_slice(&((max + 1) as u16).to_le_bytes());
        assert_eq!(read_snap_table_body(&big), None, "over the table ceiling");
        assert!(SNAP_TABLE_FIXED_LEN + max <= MTU_DEFAULT - DATAGRAM_HEADER_LEN - 24, "fits crypto-on");
    }
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc_protocol snap_table` → compile errors.

- [ ] **Step 3: Implement** beside `SnapBeginBody`:

```rust
/// Time-and-timers plan 3: the leader's current schedule table, sent once
/// after every `SNAP_BEGIN` of a session so a below-floor joiner installs
/// the table it could not read from the purged log. Pairwise scope.
pub const DGRAM_KIND_SNAP_TABLE: u8 = 21;
/// `session u32 ‖ position u64 ‖ time_ns u64 ‖ table_len u16`, then the
/// encoded table (`v2::schedule::encode_schedule_table` bytes).
pub const SNAP_TABLE_FIXED_LEN: usize = 22;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapTableBody {
    pub session: u32,
    /// The adopted table frame's END position on the leader; `0` = no table
    /// (then `table` is empty).
    pub position: u64,
    /// The adopting frame's stamp — what the joiner's record carries.
    pub time_ns: u64,
    pub table: Vec<u8>,
}

pub fn write_snap_table_body(buf: &mut [u8], b: &SnapTableBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4..12].copy_from_slice(&b.position.to_le_bytes());
    buf[12..20].copy_from_slice(&b.time_ns.to_le_bytes());
    buf[20..22].copy_from_slice(&(b.table.len() as u16).to_le_bytes());
    buf[22..22 + b.table.len()].copy_from_slice(&b.table);
}

/// Total on any input; does NOT decode the table (the node does, fail-stop).
pub fn read_snap_table_body(buf: &[u8]) -> Option<SnapTableBody> {
    use crate::v2::schedule::{MAX_SCHEDULE_ENTRIES, SCHEDULE_ENTRY_LEN, SCHEDULE_HEADER_LEN};
    if buf.len() < SNAP_TABLE_FIXED_LEN {
        return None;
    }
    let session = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    let position = u64::from_le_bytes(buf[4..12].try_into().ok()?);
    let time_ns = u64::from_le_bytes(buf[12..20].try_into().ok()?);
    let table_len = u16::from_le_bytes(buf[20..22].try_into().ok()?) as usize;
    if table_len > SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES * SCHEDULE_ENTRY_LEN {
        return None;
    }
    if buf.len() != SNAP_TABLE_FIXED_LEN + table_len {
        return None;
    }
    if (position == 0) != (table_len == 0) {
        return None;
    }
    Some(SnapTableBody { session, position, time_ns, table: buf[22..].to_vec() })
}

const _: () = assert!(
    SNAP_TABLE_FIXED_LEN
        + crate::v2::schedule::SCHEDULE_HEADER_LEN
        + crate::v2::schedule::MAX_SCHEDULE_ENTRIES * crate::v2::schedule::SCHEDULE_ENTRY_LEN
        <= MTU_DEFAULT - DATAGRAM_HEADER_LEN - 24
);
```

(`24` is `CRYPTO_OVERHEAD`; import it from `v2::crypto` if that module is reachable from `datagram.rs` without a cycle — it is a sibling in `v2`, so `use crate::v2::crypto::CRYPTO_OVERHEAD` should work; if it does not, keep the literal with a comment naming the constant.) Add kind 21 to `uc_crypto`'s pairwise-scope table beside 12/13 and to its scope test. Add two seeds to `fuzz/src/seeds.rs::uc_protocol_datagram`: `"11-snap-table"` (kind 21, a three-entry table encoded with `encode_schedule_table`) and `"12-snap-table-bad-len"` (table_len one over the ceiling). Regenerate the corpus (`cd fuzz && cargo +nightly run --bin seed-corpus`, per `fuzz/README.md`).

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_protocol && cargo test -p uc_crypto && (cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)` → PASS.

- [ ] **Step 5: Commit** `feat(uc_protocol): SNAP_TABLE datagram (kind 21) — the leader's schedule table on the snapshot session (test: snap_table_body_pins_bytes_and_is_total)`.

---

### Task 2: `uc_net` — send it after every `SNAP_BEGIN`; require it before `SNAP_DONE`

**Files:**
- Modify: `uc_net/src/sender.rs` (`SnapshotSet` gains `table: (u64, u64, Vec<u8>)` = `(position, time_ns, bytes)`; `send_snap_table`; call it after every `send_snap_begin` — both the initial send and the `SNAP_BEGIN_RESEND_NS` resend), `uc_net/src/receiver.rs` (`SnapIntake.table: Option<(u64, u64, Vec<u8>)>`; the `DGRAM_KIND_SNAP_TABLE` arm; `snap_complete` withholds until `table.is_some()`; `IncomingSnapshotSignal` gains a third cell `Arc<Mutex<(u64, u64, Vec<u8>)>>` published BEFORE the position signal)
- Test: both files' `mod tests` (the sender has a datagram-capturing test harness — the `#[cfg(test)] use read_snap_begin_body` at `sender.rs:12` is its trace; the receiver has `snap_begin_wire` at `:5391` as the forging helper)

**Interfaces:**
- Produces: `SnapshotSet { .., table: (u64, u64, Vec<u8>) }`; `pub type IncomingSnapshotSignal = (Arc<AtomicU64>, Arc<Mutex<Vec<u8>>>, Arc<Mutex<(u64, u64, Vec<u8>)>>)`; the receiver drops a `SNAP_TABLE` whose `session` does not match the open intake's, counts it in a new `snap_table_stray` stat (`ReceiverStats`, exported by Task 3's metrics if a per-stat series exists for the snap counters — check `uc_node/src/obs/metrics.rs` for `snap_refused_legacy_peer`; if it is exported, export this one the same way, else leave it internal and say so).
- Consumed by: Task 3.

- [ ] **Step 1: Write the failing tests.**

Sender (`sender.rs` `mod tests`, using the existing harness that captures what `send_snap_begin` puts on the socket — find the test that asserts a `SNAP_BEGIN` body's `config` round-trips and copy its setup):

```rust
    #[test]
    fn a_snap_table_follows_every_snap_begin_with_the_same_session() {
        // setup: a sender whose SnapshotSource yields one artifact and
        // table = (4096, 99, b"tbl".to_vec()); drive one snapshot cycle
        // and capture the datagrams sent to the peer, in order.
        // assert: [0] kind 12 (SNAP_BEGIN) with session S; [1] kind 21 whose
        // read_snap_table_body == Some(SnapTableBody { session: S, position: 4096, time_ns: 99, table: b"tbl" });
        // advance the clock past SNAP_BEGIN_RESEND_NS with no SNAP_DONE and
        // assert the resend is again BEGIN then TABLE, same session.
        // a table of (0, 0, vec![]) yields a kind-18 body with position 0 / len 0.
    }
```

Receiver (`receiver.rs` `mod tests`, beside the session tests that use `snap_begin_wire`):

```rust
    #[test]
    fn a_session_does_not_complete_until_its_snap_table_arrives_and_publishes_it_first() {
        // setup: an intake-enabled receiver with an IncomingSnapshotSignal
        // (pos cell, config cell, table cell); feed SNAP_BEGIN + all chunks
        // for a one-artifact session so that, pre-change, snap_complete fires.
        // assert: no SNAP_DONE sent and pos cell still 0 (withheld);
        // feed a SNAP_TABLE with the WRONG session → dropped, stat snap_table_stray == 1, still withheld;
        // feed the right SNAP_TABLE → SNAP_DONE sent, table cell == (position, time_ns, bytes)
        //   and — order — the table cell was written before the pos cell (assert by reading
        //   both after: pos cell == floor AND table cell set; the ordering itself is by construction,
        //   name it in a comment and mirror the config cell's placement);
        // feed the same SNAP_TABLE again → ignored (no second DONE, no change).
    }
```

Fill both in against the real harness (the plan gives the assertions; the harness's constructor names are in the neighbouring tests).

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_net snap_table` → compile errors.

- [ ] **Step 3: Implement.** Sender: `fn send_snap_table(&mut self, peer, session, table: &(u64, u64, Vec<u8>)) -> bool` builds `SnapTableBody { session, position: t.0, time_ns: t.1, table: t.2.clone() }`, `assemble_snap(peer, 0, DGRAM_KIND_SNAP_TABLE, &body)` (sealed exactly like BEGIN, pairwise), sends; call it right after each successful `send_snap_begin` (grep the two call sites, `:1273` region and `:3877` region). Receiver: in the kind match add `DGRAM_KIND_SNAP_TABLE => if let Some(b) = read_snap_table_body(..) { self.snap_table(from, b) }` where `snap_table` checks `snap_intake` is `Some` with `peer == from && session == b.session` (else `snap_table_stray += 1`, drop), sets `intake.table = Some((b.position, b.time_ns, b.table))` if `None`, then calls the same "maybe complete" check the last chunk calls; `snap_complete` gains `if intake.table.is_none() { return; }` at its top (with a comment: the leader re-sends BEGIN+TABLE every 20 ms until DONE, so a lost TABLE is retried, never stuck), and publishes the table cell right before the config cell. Update `set_snapshot_intake`'s signature for the third cell and every caller (`uc_node/src/node.rs:1122` — Task 3 wires the node; here pass a fresh cell in tests).

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_net` → PASS (the `uc_node` build breaks on the signature until Task 3 — if so, make the minimal `node.rs` call-site change here (a fresh `Arc<Mutex<..>>` the node does not read yet) so the workspace stays green, and say so).

- [ ] **Step 5: Commit** `feat(uc_net): SNAP_TABLE after every SNAP_BEGIN; a session completes only once its table arrived; published before the floor signal (tests: a_snap_table_follows_every_snap_begin_with_the_same_session, a_session_does_not_complete_until_its_snap_table_arrives_and_publishes_it_first)`.

---

### Task 3: `uc_node` — ship the record, install it by fiat

**Files:**
- Modify: `uc_node/src/node.rs` (a `schedule_bytes: Arc<Mutex<(u64, u64, Vec<u8>)>>` cache beside `config_bytes` (`:544`, `:829`, `:2280`), refreshed at the end of `install_table` (leader adopt and follower adopt both go through it), in `revert_schedule_below`, and after `arm_schedule_at_boot`; `snapshot_set_for` (`:7337`) reads it into `SnapshotSet.table`; `set_snapshot_intake` gets the third cell (`:1122`); the snapshot-install handler (`:3555–3605`) reads the table cell after the config cell and calls `install_snapshot_table`; `snapshot_installed` gains `table_position`), `uc_node/src/obs/metrics.rs` only if Task 2 exported `snap_table_stray`
- Test: `uc_node/src/node.rs` `mod tests` (beside `fiat_snapshot_install_clears_config_pending_mirror` at `:9455`)

**Interfaces:**
- `Consensus::install_snapshot_table(&mut self, position: u64, time_ns: u64, bytes: &[u8])`: decode (`None` → fail-stop `panic!("corrupt snapshot-carried SCHEDULE_TABLE at floor {pos}")`, the CONFIG rule); store `ScheduleRecord { position, time_ns, table: bytes.to_vec(), prev: None }` (persist BEFORE effect); `install_table(position, &table, self.cnc.log_time_ns())`; set `schedule_position = position`; refresh `schedule_bytes`; emit `schedule_table_adopted { position, entries, source: "snapshot" }` (add the `source` field to the existing record with value `"log"` on the frame path, so the two are distinguishable). Position `0` + empty table installs "no table": `install_table(0, &ScheduleTable { entries: vec![] }, ..)`, record stored with `position: 0`, `schedule_position = 0`. **By fiat means no `position <= schedule_position` idempotence check** — the joiner's own record is not genuine below the floor (the same argument `adopt_snapshot_config` makes in its comment).
- Ordering in the install handler: config first (it may rebuild the net layer), then the table, then `AdoptFloor` — the table must be armed before the node can be promoted/serve, and `AdoptFloor` is what re-primes `append` and lets the node catch up.

- [ ] **Step 1: Write the failing test** in `node.rs`'s `mod tests`, modelled on `fiat_snapshot_install_clears_config_pending_mirror` (`:9455`) and `a_boot_record_above_durable_reverts_the_record` (`~:7864`):

```rust
    #[test]
    fn a_fiat_snapshot_install_replaces_the_schedule_record_and_arms_it() {
        // harness with a declared row; pre-store a record at 8192 with prev 4096
        // (the joiner's stale, non-genuine table); put (floor=6016, time_ns=77,
        // encode(table with one Every{100, 0} entry for the row's hash)) into
        // the table cell and drive the snapshot-install handler for floor 6016.
        // assert: read back record == { position: 6016, time_ns: 77, table: bytes, prev: None };
        // schedule_position == 6016; gauge == 6016; the row's timers.table_len() == 1;
        // the snapshot_installed event carries table_position = 6016 (if the test
        // harness captures obs — else assert the gauge only and say so).
        // Then drive a second install with (0, 0, vec![]) → record position 0, table empty,
        // table_len() == 0, schedule_position == 0.
    }
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc_node --lib fiat_snapshot_install_replaces` → compile errors.

- [ ] **Step 3: Implement** per the interfaces. `schedule_bytes` is refreshed in ONE helper `fn refresh_schedule_bytes(&self)` called from `install_table`'s tail, `revert_schedule_below`'s tail, and `arm_schedule_at_boot`'s tail, reading `(self.schedule_position, rec.time_ns, rec.table)` from the in-memory record you already hold (add a `current_schedule: Option<ScheduleRecord>` field if the record is not kept in memory today — check; the persisted `StableValue` has `load()` returning the cached value, which is cheap, so `load()` is fine).

- [ ] **Step 4: Run** the Global-Constraints set → PASS (existing snapshot tests must stay green: a leader with no table now ships `(0, 0, [])` and every joiner installs "no table", which is what it had).

- [ ] **Step 5: Commit** `feat(uc_node): ship the schedule record on the snapshot session and install it by fiat at the floor (test: a_fiat_snapshot_install_replaces_the_schedule_record_and_arms_it)`.

---

### Task 4: End-to-end — the joiner adopts the table, and keeps the schedule ticking when it leads

**Files:**
- Modify: `uc_node/tests/learner.rs` (extend `fresh_learner_joins_a_purged_leader_via_snapshot_session` at `:416`, or add a sibling), `uc_node/tests/timers.rs` (the capstone)

**Tests (each watched red — say against what):**

1. `learner.rs::a_fresh_learner_below_the_floor_installs_the_leaders_schedule_table` — the `:416` scenario, plus: before the purge, apply a two-entry table on the voter through the staged file + admin request (copy `timers.rs`'s `apply_table` helper; `AdminPolicy::Filesystem`); after the learner's snapshot session completes, `uc_node::schedule_state::read_record(learner_dir)` equals the voter's record in `position`, `time_ns` and `table` bytes, `prev == None`; the learner's `uc2_timers_pending` cnc word for the row == 2 (both entries armed). Red: with `install_snapshot_table` stubbed to a no-op.
2. `learner.rs::a_leader_without_a_table_ships_none_and_the_joiner_installs_none` — the unchanged `:416` scenario asserting `read_record(learner_dir)` is `None` or has `position == 0` with an empty table, and the session still completes (SNAP_TABLE with position 0). Red: with the receiver's "withhold until table" check present but the sender's `send_snap_table` stubbed out (the session never completes → the existing convergence wait times out).
3. Capstone, `timers.rs::a_promoted_below_floor_joiner_keeps_the_schedule_ticking_when_it_leads` — 2 voters + `Timed<ClockSm>` services with `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` and a small ring; apply a `every 200ms` table; churn enough commands + a snapshot so the floor moves; start a fresh learner (below the floor → snapshot session, which now carries the table); promote it (`uc2ctl`'s op 2 shape — `admin_auth.rs`/`reconfig.rs` show the in-process request); then stop BOTH original voters' services and nodes… — no: stop only the current leader and make the promoted joiner win (kill the leader; with 2 voters + 1 promoted = 3, the joiner + the survivor form a quorum; if the survivor wins instead, kill it too and restart it as a follower — say what the harness needs); assert that AFTER the joiner becomes leader, its `Timed<ClockSm>` service records table fires with strictly increasing deadlines continuing the rule (`latest_at_or_before(time_ns) == deadline`), ≥ 5 fires, no gap longer than one period plus the election time. Red: with `install_snapshot_table` stubbed (the joiner leads with an empty table → zero fires → the ≥ 5 assertion fails by timeout).

- [ ] **Step 1–3**: write, run red, implement per the list; record wall times and fire counts.
- [ ] **Step 4: Run** the Global-Constraints set + `cargo test -p uc_node --test learner -- --test-threads=1` + `cargo test -p uc_node --test timers -- --test-threads=1`.
- [ ] **Step 5: Commit** `test: a below-floor joiner installs the leader's schedule table and keeps the schedule ticking when it leads`.

---

### Task 5: Docs, release, alert, backlog

**Files:**
- Modify: `docs/reference/wire-protocol.md` (kind 21 in the snapshot-session table + a `SNAP_TABLE` body subsection beside `SNAP_BEGIN`'s), `docs/reference/limits.md` (the "not in the snapshot stream" row → removed, replaced by a one-line "carried on the snapshot session since plan 3" note in the table's rationale column), `docs/ops/uc2-runbook.md` (the below-floor-joiner paragraph: what a joiner now installs; the `snapshot_installed` record's `table_position` field), `docs/notes/uc2-log-time-and-timers-explained.md` (the known-limits section: the limitation becomes "closed — how"; the "Every replica gets the wake-up by playing the tape" section gains one sentence on the snapshot carry), `docs/security/attack-surface.md` (a `SNAP_TABLE` row: pairwise-sealed under crypto, total decoder, node-side decode is fail-stop like CONFIG, ≤ 1086 B), `packaging/prometheus/uc2-alerts.yml` (`Uc2ScheduleTableDiverged`'s comment/annotation: a below-floor join is no longer a cause; the remaining causes are the record-vs-persist crash window and a wipe), `RELEASES.md` + `docs/releases.md` (fold into the schedule-table bullet: "carried on the snapshot session"), `CLAUDE.md` (the standing-facts sub-bullet: one sentence), `docs/BACKLOG.md` (2a's R13 line → closed, pointer to this plan), `docs/VERIFICATION.md` (the datagram fuzz row mentions kind 21's seeds; the learner/timers rows name the three new tests), `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (row e's rationale unaffected — check and say so)
- The sweep check: every constant/name in the docs exists in the tree; every link resolves; `promtool check rules` green.

- [ ] **Step 1–3**: write; `ls` every linked path; grep every named constant.
- [ ] **Step 4: Commit** `docs: the schedule table rides the snapshot session — wire reference, limits, runbook, explainer, attack surface, alert, release note`.

---

## Self-review

**Spec coverage (§5 as amended by Task 0).** The table reaches a below-floor joiner → Tasks 1–3; before it can serve or lead → Task 3's ordering (table before `AdoptFloor`) + Task 4's capstone; position 0 = no table → Tasks 1 (codec rule), 3 (install "none"), 4 (test 2); fiat with `prev = None` → Task 3; determinism (the leader's `time_ns`, the joiner's log clock for arming) → Task 3; observability → Task 3 (`source` field, `table_position`) + Task 5; the limitation closed → Task 5.

**Placeholders.** Task 2's and Task 4's tests are given as assertion lists against named harnesses rather than full code, because both harnesses' constructors are long-lived test infrastructure the executor must read (`sender.rs`/`receiver.rs` `mod tests`, `learner.rs:416`); every assertion and red condition is stated. Task 1 and Task 3 carry code.

**Type consistency.** `SnapTableBody { session, position, time_ns, table }` (Task 1) is what `send_snap_table` writes and `snap_table` reads (Task 2); `SnapshotSet.table: (u64, u64, Vec<u8>)` (Task 2) is what `snapshot_set_for` fills from `schedule_bytes: Arc<Mutex<(u64, u64, Vec<u8>)>>` (Task 3); the third `IncomingSnapshotSignal` cell (Task 2) is what the install handler reads before calling `install_snapshot_table(position, time_ns, &bytes)` (Task 3); `read_record` (plan 2) is what Task 4 asserts with.

**Budget check.** 22 + 8 + 32 × 33 = 1086 ≤ 1368 (crypto-on datagram body). A `const` assert pins it.
