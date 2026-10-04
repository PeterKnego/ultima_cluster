# UC2 Snapshot Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A replicated catalog of snapshot sets (position, kind, log time, state, per-row version/hash/verdict, cluster-artifact hash) plus a replicated `retain_sets` policy in the cluster FSM, a soft per-node advertisement of holdings in `STATUS`, and a pure query interface over both — so the purge floor and every install source become the newest **agreed** set, with per-node effective floors.

**Architecture:** The catalog is a new field of `ClusterState`, fed by two existing streams: the `SNAPSHOT` frame (→ a *commanded* entry, derived in the cluster agent before any role check) and `SnapshotReport` records (→ per-row verdicts; row 255 carries the cluster artifact's hash). Retention is derived from `Settings.retain_sets`. Soft state rides a versioned `STATUS` body from every follower to the leader, which keeps a per-node table with staleness. Eight pure query functions sit in a new `uc_node::catalog` module. The node's purge driver and pruner read the catalog through the published `ClusterView`, bounded by what the node itself holds.

**Tech Stack:** Rust 1.96 (MSRV 1.89), existing `uc_protocol` LE codecs, `crc32fast`, the `uc2-cluster` agent, `uc_sim`, `cargo-fuzz`, `promtool`.

**Spec:** `docs/superpowers/specs/2026-10-01-uc2-snapshot-catalog-design.md` (read its §3 decisions and §4.6 before any task).

## Global Constraints

- MSRV 1.89: before every push run `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`.
- Private target dir for every build in this plan: `export CARGO_TARGET_DIR=$HOME/.cache/cargo-target-rv-catalog`.
- Scratch under `$HOME/scratch/`, never `/tmp`.
- **No attribution trailer** on any commit.
- Wire bump `0.10.0 → 0.11.0` (spec §7); cnc page **unchanged**; `CLUSTER_IMAGE_VERSION` `3 → 4`; a v3 image loads with an empty catalog; **nothing on disk is cleared** on the flag day.
- Every replicated record must fit one `CLUSTER` frame body: `SETTINGS_LEN + CLUSTER_BODY_PREFIX_LEN <= 1344` is a compile-time assert in `settings.rs:137`; keep the pattern.
- `retain_sets`: `u16`, `1..=MAX_CATALOG_SETS (64)`; `0` or `> 64` refused at the door with `ClusterRefusal::SettingsBounds("retain_sets")` → reason 47.
- D4/§4.4: a node's **effective floor** is the newest agreed set **it holds on disk**; `newest_agreed()` is a ceiling, never a licence to purge.
- D6: no directory listing on the consensus pass; the soft advertisement is a cached struct.
- D8: no rate bar. Record `STATUS` body size before/after as a number.
- `uc_sim` does not run `ClusterFsm`; its invariant is written abstractly (spec §10.3).
- The soft table's staleness timeout is `SOFT_STALE_NS = 3 × election_timeout_max_ns` (default 900 ms). Ruling recorded here: the spec says "the liveness timeout the node already uses for heartbeats"; no such per-peer timestamp exists (dossier §6), so this plan defines one from the election timer.

## Review Focus

Spec-implied inputs no task's tests exercise unless listed here; each line names the owning task's added test.

1. **A `SNAPSHOT` frame replayed from the journal (restart) must not duplicate a catalog entry.** The agent re-walks committed frames after a restart; the FSM must treat a second frame at an existing P as a no-op. → Task 5 `a_replayed_snapshot_frame_does_not_duplicate_the_entry`.
2. **A report for a row the cluster no longer declares.** Reports are filtered by membership, not by the declared row set; a late report for a row dropped from `[services] names` must not flip a set to Complete. → Task 5 `a_report_for_an_undeclared_row_is_ignored`.
3. **`retain_sets` lowered while the only agreed set is a pinned origin.** It must stay (never retire the pin's origin), and `newest_agreed` must still answer it. → Task 5 `lowering_retention_never_retires_a_pinned_origin`.
4. **A learner-only cluster where the catalog says agreed but this voter holds nothing.** The voter's effective floor must not move and the pruner must delete nothing. → Task 8 unit test `effective_floor_is_what_this_node_holds` and Task 12 e2e `learner_only_voters_do_not_purge_until_they_fetch`.
5. **A `STATUS` body from a `0.10.0` peer (reserved word 0).** Must decode to `None`, never as a v2 body with garbage holdings. → Task 4 `a_v1_status_body_is_refused`.

---

## File structure

| File | Responsibility | Task |
|---|---|---|
| `uc_protocol/src/v2/upgrade.rs` | admit row `255` in `SnapshotReport` encode/decode | 1 |
| `uc_protocol/src/v2/datagram.rs` | admit row `255` in `SnapReportBody`; `StatusBody` v2 (`Holdings`) | 1, 4 |
| `uc_protocol/src/v2/settings.rs` | `Settings` v3: `retain_sets` | 2 |
| `uc_protocol/src/v2/catalog.rs` (new) | `SetKind`, `SetState`, `RowVerdict`, `RowEntry`, `SetEntry`, list codec, `MAX_CATALOG_SETS`, `CLUSTER_ROW` | 3 |
| `uc_protocol/src/v2/cluster_image.rs` | image v4: trailing `catalog` blob | 5 |
| `uc_protocol/src/version.rs` | `CURRENT = 0.11.0` | 4 |
| `uc_node/src/cluster_fsm.rs` | `ClusterState.catalog`, transitions, retention, `Empty`, `retain_sets` bounds, view publish | 5 |
| `uc_node/src/catalog.rs` (new) | `SoftTable`, the eight query functions | 6 |
| `uc_node/src/cluster_agent.rs` | commanded entry from the `SNAPSHOT` frame; `read_committed_catalog` | 7 |
| `uc_node/src/node.rs` | row-255 report; effective floor; pruner keep-set; `Holdings` cache; soft table owner | 8, 9 |
| `uc_net/src/receiver.rs`, `uc_net/src/sender.rs`, `uc_net/src/flow.rs` | `STATUS` v2 send/receive, `CtrlMsg::Status` carries holdings | 9 |
| `uc_node/src/obs/metrics.rs` | five gauges | 10 |
| `packaging/prometheus/uc2-alerts.yml`, `scripts/m10_alert_fire.sh` | `Uc2SnapshotSetDiverged` re-sourced | 10 |
| `uc_sim/src/invariants.rs`, `uc_sim/src/world.rs`, `uc_sim/tests/scenarios.rs` | inv13 | 11 |
| `fuzz/fuzz_targets/uc_protocol_status_body.rs` (new), `fuzz/Cargo.toml` | fuzz target | 11 |
| `uc_node/tests/catalog.rs` (new) | six e2e scenarios | 12 |
| docs (see Task 13) | sweep + spec errata | 13 |

---

### Task 1: Row 255 in the snapshot-report codecs

**Files:**
- Modify: `uc_protocol/src/v2/upgrade.rs:93-147` (`encode_snapshot_report`, `decode_snapshot_report`)
- Modify: `uc_protocol/src/v2/datagram.rs:557-586` (`write_snap_report_body`, `read_snap_report_body`)
- Test: both files' `mod tests`

**Interfaces:**
- Produces: `pub const CLUSTER_ROW: u8 = 255;` in `uc_protocol/src/v2/upgrade.rs` (re-exported by Task 3's module). A row is valid iff `row < CNC_MAX_SERVICES || row == CLUSTER_ROW`.

- [ ] **Step 1: Write the failing tests** in `upgrade.rs` `mod tests`:

```rust
#[test]
fn row_255_is_the_cluster_artifact_and_round_trips() {
    let r = SnapshotReport { row: CLUSTER_ROW, position: 4096, hashes: vec![(0, 7), (1, 7)] };
    let mut b = Vec::new();
    assert_eq!(encode_snapshot_report(&r, &mut b), Some(()));
    assert_eq!(b[0], 255);
    assert_eq!(decode_snapshot_report(&b), Some(r));
}

#[test]
fn rows_8_to_254_are_still_refused() {
    for row in [8u8, 9, 100, 254] {
        let r = SnapshotReport { row, position: 4096, hashes: vec![(0, 7)] };
        assert_eq!(encode_snapshot_report(&r, &mut Vec::new()), None, "row {row}");
        let mut b = Vec::new();
        encode_snapshot_report(&SnapshotReport { row: 0, ..r.clone() }, &mut b).unwrap();
        b[0] = row;
        assert_eq!(decode_snapshot_report(&b), None, "row {row}");
    }
}
```

and in `datagram.rs` `mod tests`:

```rust
#[test]
fn snap_report_body_admits_row_255_only_beyond_the_declared_rows() {
    let mut buf = [0u8; SNAP_REPORT_BODY_LEN];
    write_snap_report_body(&mut buf, &SnapReportBody { row: 255, node_id: 1, position: 64, hash: 9 });
    assert_eq!(read_snap_report_body(&buf).map(|b| b.row), Some(255));
    buf[0] = 8;
    assert_eq!(read_snap_report_body(&buf), None);
}
```

- [ ] **Step 2: Run, expect FAIL** (`CLUSTER_ROW` undefined; row 255 refused):
`cargo test -p uc_protocol row_255 snap_report_body_admits`

- [ ] **Step 3: Implement.** In `upgrade.rs` add beside the constants:

```rust
/// The cluster artifact's "row" in a snapshot report (catalog spec §5.1):
/// the same `service_id = 255` the snapshot session ships it under. Not a
/// declared row — `CNC_MAX_SERVICES` is 8 — so every row check is
/// `row < CNC_MAX_SERVICES || row == CLUSTER_ROW`.
pub const CLUSTER_ROW: u8 = 255;

#[inline]
pub const fn is_report_row(row: u8) -> bool {
    (row as usize) < CNC_MAX_SERVICES || row == CLUSTER_ROW
}
```

Replace the two guards `r.row as usize >= CNC_MAX_SERVICES` (line 98) and `row as usize >= CNC_MAX_SERVICES` (line 122) with `!is_report_row(r.row)` / `!is_report_row(row)`. Leave the `UpgradePin` (line 46) and `RowGenesis` (line 222) guards unchanged: a pin or genesis names a declared row only. In `datagram.rs:574` replace `if row >= 8 { return None; }` with `if !super::upgrade::is_report_row(row) { return None; }` and update the doc comment above it.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_protocol`

- [ ] **Step 5: Commit**: `git commit -am "protocol(catalog): row 255 names the cluster artifact in snapshot reports"`

---

### Task 2: `Settings` v3 — `retain_sets`

**Files:**
- Modify: `uc_protocol/src/v2/settings.rs` (constants 13–17, struct 63–82, `encode_settings` 96–103, `decode_settings` 110–135, the compile-time asserts 137–140)
- Test: its `mod tests`

**Interfaces:**
- Produces: `Settings.retain_sets: u16` (`0` on a v1/v2 record = "unset", which the FSM treats as `1`, today's behaviour); `SETTINGS_VERSION = 3`; `SETTINGS_LEN = 35`; `SETTINGS_LEN_V2 = 33` (the old `SETTINGS_LEN`).

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn settings_v3_round_trips_retain_sets_and_v2_reads_as_unset() {
    let s = Settings { fsm_lag_bytes: 0, admission_bytes: 0, snapshot_interval_bytes: 0,
                       snapshot_target: Target::All, datagram_mtu: 0, retain_sets: 3 };
    let mut b = Vec::new();
    encode_settings(&s, &mut b);
    assert_eq!(b.len(), SETTINGS_LEN);
    assert_eq!(&b[0..4], &3u32.to_le_bytes(), "version 3");
    assert_eq!(&b[33..35], &3u16.to_le_bytes(), "retain_sets @33");
    assert_eq!(decode_settings(&b), Some(s));
    // a v2 record (33 B, version 2) decodes with retain_sets = 0 (unset)
    let mut v2 = b[..33].to_vec();
    v2[0..4].copy_from_slice(&2u32.to_le_bytes());
    assert_eq!(decode_settings(&v2).map(|s| s.retain_sets), Some(0));
    // a v3 header on a v2 length is refused
    let mut bad = b[..33].to_vec();
    bad[0..4].copy_from_slice(&3u32.to_le_bytes());
    assert_eq!(decode_settings(&bad), None);
}
```

- [ ] **Step 2: Run, expect FAIL** (no field `retain_sets`).

- [ ] **Step 3: Implement.** Constants:

```rust
pub const SETTINGS_VERSION: u32 = 3;
/// v3: `+ retain_sets u16 @33` (catalog spec §7).
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2; // 35
pub const SETTINGS_LEN_V2: usize = 4 + 8 + 8 + 8 + 1 + 4;   // 33
pub const SETTINGS_LEN_V1: usize = 4 + 8 + 8 + 8 + 1;       // 29
```

Add to `Settings`:

```rust
    /// Catalog spec §4.4: how many AGREED snapshot sets the cluster keeps.
    /// `0` = unset (a v1/v2 record), read as `1` at use — today's
    /// newest-only retention. `1..=MAX_CATALOG_SETS` otherwise; the FSM's
    /// door refuses anything else (47).
    pub retain_sets: u16,
```

`encode_settings` appends `out.extend_from_slice(&s.retain_sets.to_le_bytes());`. `decode_settings` becomes a three-arm match:

```rust
    let (datagram_mtu, retain_sets) = match (version, buf.len()) {
        (1, SETTINGS_LEN_V1) => (0, 0),
        (2, SETTINGS_LEN_V2) => (u32::from_le_bytes(buf[29..33].try_into().unwrap()), 0),
        (3, SETTINGS_LEN) => (
            u32::from_le_bytes(buf[29..33].try_into().unwrap()),
            u16::from_le_bytes(buf[33..35].try_into().unwrap()),
        ),
        _ => return None,
    };
```

Rename the old `SETTINGS_LEN` uses: `cluster_image.rs:237-239` matches settings version words `1 => SETTINGS_LEN_V1, 2 => SETTINGS_LEN` — change the `2` arm to `SETTINGS_LEN_V2` and add `3 => SETTINGS_LEN` (both branches, lines ~215 and ~237). Fix every `Settings { .. }` literal in the workspace (`grep -rn "datagram_mtu:" --include=*.rs` finds them) by adding `retain_sets: 0`. Update the doc comment at `settings.rs:9-12` with the v3 line.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_protocol && cargo build --workspace`

- [ ] **Step 5: Commit**: `git commit -am "protocol(catalog): Settings v3 carries retain_sets"`

---

### Task 3: The catalog types and list codec

**Files:**
- Create: `uc_protocol/src/v2/catalog.rs`
- Modify: `uc_protocol/src/v2/mod.rs` (add `pub mod catalog;`)
- Test: in the new file

**Interfaces:**
- Produces (all `pub`, LE, `core`-friendly except `Vec`):

```rust
pub use super::upgrade::{CLUSTER_ROW, is_report_row};
pub const MAX_CATALOG_SETS: usize = 64;
pub const SET_ENTRY_LEN: usize = 8 + 1 + 1 + 8 + 9 * ROW_ENTRY_LEN; // 135
pub const ROW_ENTRY_LEN: usize = 4 + 8 + 1;                        // 13

#[repr(u8)] pub enum SetKind { Full = 0, Standby = 1 }
#[repr(u8)] pub enum SetState { Commanded = 0, Complete = 1 }   // a retired set leaves the list (spec errata)
#[repr(u8)] pub enum RowVerdict { Unreported = 0, Agreed = 1, Diverged = 2, NoMajority = 3 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowEntry { pub version: u32, pub hash: u64, pub verdict: RowVerdict }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetEntry {
    pub position: u64, pub kind: SetKind, pub time_ns: u64, pub state: SetState,
    pub rows: [RowEntry; CNC_MAX_SERVICES], pub cluster: RowEntry,
}
impl SetEntry {
    pub fn commanded(position: u64, kind: SetKind, time_ns: u64) -> Self;
    /// Every DECLARED row (bit r of `declared`) and `cluster` are `Agreed`.
    pub fn is_agreed(&self, declared: u64) -> bool;
}
pub fn encode_set_list(sets: &[SetEntry], out: &mut Vec<u8>) -> Option<()>; // None if > MAX_CATALOG_SETS
pub fn decode_set_list(buf: &[u8]) -> Option<Vec<SetEntry>>;             // exact framing: u16 count ‖ entries
```

`RowVerdict::default()` is `Unreported`. Entry layout: `position u64 @0 ‖ kind u8 @8 ‖ state u8 @9 ‖ time_ns u64 @10 ‖ rows[0..8] × (version u32 ‖ hash u64 ‖ verdict u8) @18 ‖ cluster (same 13 B) @122` = 135 B. List: `count u16 ‖ count × 135 B`, nothing after.

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn set_entry_layout_is_frozen() {
    let mut e = SetEntry::commanded(4096, SetKind::Standby, 77);
    e.rows[2] = RowEntry { version: 0x0102_0003, hash: 0xAB, verdict: RowVerdict::Agreed };
    e.cluster = RowEntry { version: 0, hash: 0xCD, verdict: RowVerdict::Diverged };
    let mut b = Vec::new();
    encode_set_list(&[e.clone()], &mut b).unwrap();
    assert_eq!(b.len(), 2 + SET_ENTRY_LEN);
    assert_eq!(&b[0..2], &1u16.to_le_bytes());
    assert_eq!(&b[2..10], &4096u64.to_le_bytes(), "position");
    assert_eq!(b[10], 1, "kind standby"); assert_eq!(b[11], 0, "state commanded");
    assert_eq!(&b[12..20], &77u64.to_le_bytes(), "time_ns");
    let row2 = 20 + 2 * ROW_ENTRY_LEN;
    assert_eq!(&b[row2..row2 + 4], &0x0102_0003u32.to_le_bytes());
    assert_eq!(b[2 + SET_ENTRY_LEN - 1], 2, "cluster verdict diverged is the last byte");
    assert_eq!(decode_set_list(&b), Some(vec![e]));
}

#[test]
fn the_list_is_bounded_and_exact() {
    let e = SetEntry::commanded(1, SetKind::Full, 0);
    let too_many = vec![e.clone(); MAX_CATALOG_SETS + 1];
    assert_eq!(encode_set_list(&too_many, &mut Vec::new()), None);
    let mut b = Vec::new();
    encode_set_list(&[e], &mut b).unwrap();
    b.push(0);
    assert_eq!(decode_set_list(&b), None, "trailing byte");
    assert_eq!(decode_set_list(&b[..b.len() - 2]), None, "short");
    let mut bad = b.clone(); bad.pop(); bad[11] = 9;
    assert_eq!(decode_set_list(&bad), None, "unknown state byte");
    assert_eq!(decode_set_list(&0u16.to_le_bytes()), Some(vec![]));
}

#[test]
fn is_agreed_needs_every_declared_row_and_the_cluster() {
    let mut e = SetEntry::commanded(1, SetKind::Full, 0);
    let ok = RowEntry { version: 1, hash: 1, verdict: RowVerdict::Agreed };
    e.rows[0] = ok; e.rows[1] = ok;
    assert!(!e.is_agreed(0b11), "cluster unreported");
    e.cluster = ok;
    assert!(e.is_agreed(0b11));
    assert!(!e.is_agreed(0b111), "row 2 unreported");
    e.rows[1].verdict = RowVerdict::Diverged;
    assert!(!e.is_agreed(0b11));
}
```

- [ ] **Step 2: Run, expect FAIL** (module missing).

- [ ] **Step 3: Implement** the module exactly per the interface. Decode every byte with `.get(..)` and refuse unknown enum bytes (`kind > 1`, `state > 1`, `verdict > 3`). `is_agreed`:

```rust
    pub fn is_agreed(&self, declared: u64) -> bool {
        self.cluster.verdict == RowVerdict::Agreed
            && (0..CNC_MAX_SERVICES).all(|r| {
                declared & (1 << r) == 0 || self.rows[r].verdict == RowVerdict::Agreed
            })
    }
```

Add `const _: () = assert!(SET_ENTRY_LEN == 135);` and a module doc stating the layout and that the list lives in the cluster IMAGE, not in a `CLUSTER` frame, so `MAX_CATALOG_SETS` is a retention bound, not a datagram bound (spec §4 "Sizing").

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_protocol catalog`

- [ ] **Step 5: Commit**: `git commit -am "protocol(catalog): SetEntry/RowEntry and the set-list codec"`

---

### Task 4: `STATUS` body v2 (`Holdings`) and the wire bump

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs:873-900`
- Modify: `uc_protocol/src/version.rs:84-87`, test at `:122-125`
- Test: `datagram.rs` and `version.rs` `mod tests`

**Interfaces:**
- Produces:

```rust
pub const STATUS_BODY_LEN_V1: usize = 16;   // the 0.10.0 body
pub const STATUS_BODY_LEN: usize = 144;     // v2
pub const STATUS_LAYOUT_V2: u32 = 2;        // lives in the old reserved word @12

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Holdings {
    pub journal_first: u64, pub durable: u64, pub commit: u64,
    pub applied: [u64; CNC_MAX_SERVICES],
    pub free_bytes: u64, pub journal_bytes: u64, pub snapshots_bytes: u64,
    /// The published catalog position `sets_held` was computed against.
    pub catalog_position: u64,
    /// Bit i ⇔ this node holds the catalog's i-th listed set (oldest first) complete on disk.
    pub sets_held: u64,
}
pub struct StatusBody { pub contiguous_position: u64, pub receive_window: u32, pub holdings: Holdings }
pub fn write_status_body(buf: &mut [u8], b: &StatusBody);          // buf.len() >= STATUS_BODY_LEN
pub fn read_status_body(buf: &[u8]) -> Option<StatusBody>;         // None unless len >= 144 AND word@12 == 2
```

Layout: `contiguous u64 @0 ‖ window u32 @8 ‖ layout u32 @12 (=2) ‖ journal_first @16 ‖ durable @24 ‖ commit @32 ‖ applied[8] @40 ‖ free @104 ‖ journal_bytes @112 ‖ snapshots_bytes @120 ‖ catalog_position @128 ‖ sets_held @136` = 144.

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn status_body_v2_round_trips_and_pins_its_layout() {
    let mut h = Holdings::default();
    h.journal_first = 10; h.durable = 20; h.commit = 15; h.applied[3] = 13;
    h.free_bytes = 1; h.journal_bytes = 2; h.snapshots_bytes = 3; h.catalog_position = 99; h.sets_held = 0b101;
    let b = StatusBody { contiguous_position: 20, receive_window: 7, holdings: h };
    let mut buf = [0u8; STATUS_BODY_LEN];
    write_status_body(&mut buf, &b);
    assert_eq!(&buf[12..16], &2u32.to_le_bytes(), "layout word @12");
    assert_eq!(&buf[40 + 3 * 8..40 + 4 * 8], &13u64.to_le_bytes(), "applied[3]");
    assert_eq!(&buf[136..144], &0b101u64.to_le_bytes(), "sets_held last");
    assert_eq!(read_status_body(&buf), Some(b));
}

#[test]
fn a_v1_status_body_is_refused() {
    // a 0.10.0 peer: 16 bytes, reserved word zero
    let mut v1 = [0u8; 16];
    v1[0..8].copy_from_slice(&20u64.to_le_bytes());
    assert_eq!(read_status_body(&v1), None);
    // a 144-byte body whose layout word is 0 is refused too
    let zero = [0u8; STATUS_BODY_LEN];
    assert_eq!(read_status_body(&zero), None);
    // and a short v2
    let mut ok = [0u8; STATUS_BODY_LEN];
    write_status_body(&mut ok, &StatusBody { contiguous_position: 1, receive_window: 1, holdings: Holdings::default() });
    assert_eq!(read_status_body(&ok[..143]), None);
}
```

and in `version.rs` replace `current_is_the_upgrade_lifecycle_wire` with:

```rust
#[test]
fn current_is_the_snapshot_catalog_wire() {
    assert_eq!(CURRENT, ProtocolVersion::new(0, 11, 0));
}
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** the body per the layout; `read_status_body` checks `buf.len() >= STATUS_BODY_LEN` **and** `u32 @12 == STATUS_LAYOUT_V2`, else `None`. Bump `version.rs`: add a doc block above `CURRENT`:

```rust
// 0.11.0 (snapshot catalog): STATUS body 16 B → 144 B (layout word 2 in the
// old reserved slot; a 0.10.0 body reads as `None`), SNAP_REPORT/report row
// 255 = the cluster artifact, Settings v3 `retain_sets`, cluster image v4.
// No layout change on the replication path — which is exactly why mixing is
// unsound: a 0.10.0 peer drops the row-255 report as undecodable and its
// catalog never completes a set, in silence.
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 11, 0);
```

Fix the two existing call sites now broken by the new field: `uc_net/src/receiver.rs:3362-3392` (send) and `:2199-2213` (receive) — for THIS task only, send `Holdings::default()` and ignore `b.holdings` on receive, with a `// Task 9 wires the real holdings` comment; Task 9 replaces both. Use `STATUS_BODY_LEN` for the buffer.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_protocol && cargo test -p uc_net`

- [ ] **Step 5: Commit**: `git commit -am "protocol(catalog): STATUS body v2 carries Holdings; wire 0.11.0"`

---

### Task 5: The catalog in the cluster FSM

**Files:**
- Modify: `uc_protocol/src/v2/cluster_image.rs` (`CLUSTER_IMAGE_VERSION = 4`, `ClusterImageParts.catalog: &'a [u8]`, encode appends a length-prefixed blob, decode reads it when `version >= 4`, else empty)
- Modify: `uc_node/src/cluster_fsm.rs` (`ClusterState.catalog`, transitions, retention, bounds, `ClusterView`/`ClusterViewInner`/`publish`/`snapshot_inner`/`to_state`, `freeze`/`install_snapshot`)
- Test: `cluster_fsm.rs` `mod tests`, `cluster_image.rs` `mod tests`

**Interfaces:**
- Consumes: Task 3's types; Task 2's `retain_sets`; Task 1's `CLUSTER_ROW`.
- Produces, on `ClusterState`:

```rust
pub catalog: Vec<SetEntry>,                       // oldest first; Task 3's SetEntry
pub fn declared_mask(&self) -> u64;               // bit r ⇔ row r in `running` or in any pin — see Step 3
pub fn on_snapshot_frame(&mut self, end: u64, standby: bool, time_ns: u64); // D3; ALSO on ClusterFsm, forwarding (there is no `state_mut`)
pub fn catalog_empty(&self) -> bool;              // no agreed set
pub fn newest_agreed_at_most(&self, at_most: u64) -> Option<u64>;
pub fn retain_sets(&self) -> u16;                 // settings.retain_sets.max(1)
```

and on `ClusterView`: `pub catalog_agreed_position: AtomicU64` (0 = Empty), `pub catalog_len: AtomicU64`, `pub catalog_stalled: AtomicU64`, `pub catalog_diverged: AtomicU64`, and `ClusterViewInner.catalog: Vec<SetEntry>`.

**The declared mask.** `is_agreed(declared)` needs "every declared row". The FSM does not hold `[services] names`; the node does. The replicated proxy is `running`: since #33 every declared row has a committed running version (genesis or pin) before it serves. Define `declared_mask()` as the bits of `running` rows. Record this in the spec's errata (Task 13).

- [ ] **Step 1: Failing tests** (append to `cluster_fsm.rs` `mod tests`; use the existing `fsm()`, `body()`, `apply_at` helpers):

```rust
use uc_protocol::v2::catalog::*;
use uc_protocol::identity::pack_version;

fn report(row: u8, position: u64, hashes: &[(u32, u64)]) -> ClusterCommand {
    ClusterCommand::SnapshotReport(SnapshotReport { row, position, hashes: hashes.to_vec() })
}
fn genesis_row(f: &mut ClusterFsm, row: u8, pos: u64) {
    assert_eq!(apply_at(f, pos, &ClusterCommand::RowGenesis(RowGenesis { row, version: pack_version(1, 0, 0) })), 0);
}
/// One agreed set at `p`: the SNAPSHOT frame, then row 0 and the cluster row report one hash each.
fn agreed_set(f: &mut ClusterFsm, p: u64, at: u64) {
    f.on_snapshot_frame(p, false, p);
    assert_eq!(apply_at(f, at, &report(0, p, &[(0, 1)])), 0);
    assert_eq!(apply_at(f, at + 10, &report(CLUSTER_ROW, p, &[(0, 1)])), 0);
}
fn positions(f: &ClusterFsm) -> Vec<u64> { f.state().catalog.iter().map(|e| e.position).collect() }
fn settings_with_retain(f: &ClusterFsm, retain_sets: u16) -> ClusterCommand {
    let mut s = f.state().settings; s.retain_sets = retain_sets; ClusterCommand::Settings(s)
}

#[test]
fn a_snapshot_frame_records_a_commanded_set() {
    let mut f = fsm();
    f.on_snapshot_frame(4096, true, 77);
    let e = &f.state().catalog[0];
    assert_eq!((e.position, e.kind, e.state, e.time_ns), (4096, SetKind::Standby, SetState::Commanded, 77));
    assert!(f.state().catalog_empty(), "commanded is not agreed");
}

#[test]
fn a_replayed_snapshot_frame_does_not_duplicate_the_entry() {
    let mut f = fsm();
    f.on_snapshot_frame(4096, false, 1);
    f.on_snapshot_frame(4096, false, 1);
    assert_eq!(f.state().catalog.len(), 1);
}

#[test]
fn reports_complete_a_set_and_the_cluster_row_is_required() {
    let mut f = fsm();
    genesis_row(&mut f, 0, 100);
    f.on_snapshot_frame(4096, false, 1);
    assert_eq!(apply_at(&mut f, 4200, &report(0, 4096, &[(0, 7), (1, 7)])), 0);
    assert_eq!(f.state().catalog[0].state, SetState::Commanded, "cluster row still unreported");
    assert_eq!(apply_at(&mut f, 4300, &report(CLUSTER_ROW, 4096, &[(0, 9), (1, 9)])), 0);
    let e = &f.state().catalog[0];
    assert_eq!(e.state, SetState::Complete);
    assert_eq!(e.rows[0], RowEntry { version: pack_version(1, 0, 0), hash: 7, verdict: RowVerdict::Agreed });
    assert_eq!((e.cluster.hash, e.cluster.verdict), (9, RowVerdict::Agreed));
    assert!(!f.state().catalog_empty());
    assert_eq!(f.state().newest_agreed_at_most(u64::MAX), Some(4096));
}

#[test]
fn a_diverged_row_completes_but_never_agrees() {
    let mut f = fsm();
    genesis_row(&mut f, 0, 100);
    f.on_snapshot_frame(4096, false, 1);
    assert_eq!(apply_at(&mut f, 4200, &report(0, 4096, &[(0, 7), (1, 8), (2, 7)])), 0);
    assert_eq!(apply_at(&mut f, 4300, &report(CLUSTER_ROW, 4096, &[(0, 9), (1, 9), (2, 9)])), 0);
    let e = &f.state().catalog[0];
    assert_eq!((e.state, e.rows[0].verdict, e.rows[0].hash), (SetState::Complete, RowVerdict::Diverged, 7));
    assert_eq!(f.state().newest_agreed_at_most(u64::MAX), None);
    assert!(f.state().catalog_empty());
}

#[test]
fn a_report_for_an_undeclared_row_is_ignored() {
    let mut f = fsm();
    genesis_row(&mut f, 0, 100);
    f.on_snapshot_frame(4096, false, 1);
    assert_eq!(apply_at(&mut f, 4200, &report(0, 4096, &[(0, 7)])), 0);
    assert_eq!(apply_at(&mut f, 4250, &report(5, 4096, &[(0, 7)])), 0); // row 5 never declared
    assert_eq!(apply_at(&mut f, 4300, &report(CLUSTER_ROW, 4096, &[(0, 9)])), 0);
    let e = &f.state().catalog[0];
    assert_eq!(e.rows[5].verdict, RowVerdict::Unreported);
    assert_eq!(e.state, SetState::Complete, "row 5 is not required");
}

#[test]
fn retention_keeps_retain_sets_agreed_sets_and_drops_the_rest() {
    let mut f = fsm();
    genesis_row(&mut f, 0, 100);
    assert_eq!(apply_at(&mut f, 200, &settings_with_retain(&f, 2)), 0);
    agreed_set(&mut f, 1000, 1100);
    agreed_set(&mut f, 2000, 2100);
    agreed_set(&mut f, 3000, 3100);
    assert_eq!(positions(&f), vec![2000, 3000], "1000 retired: beyond retain_sets = 2");
    agreed_set(&mut f, 4000, 4100);
    assert_eq!(positions(&f), vec![3000, 4000]);
    assert_eq!(f.state().newest_agreed_at_most(3500), Some(3000));
}

#[test]
fn lowering_retention_never_retires_a_pinned_origin() {
    let mut f = fsm();
    genesis_row(&mut f, 0, 100);
    assert_eq!(apply_at(&mut f, 200, &settings_with_retain(&f, 4)), 0);
    agreed_set(&mut f, 1000, 1100);
    agreed_set(&mut f, 2000, 2100);
    let pin = UpgradePin { row: 0, origin: 1000, from: pack_version(1, 0, 0), to: pack_version(1, 1, 0) };
    assert_eq!(apply_at(&mut f, 2500, &ClusterCommand::UpgradePin(pin)), 0);
    assert_eq!(apply_at(&mut f, 2600, &settings_with_retain(&f, 1)), 0);
    assert_eq!(positions(&f), vec![1000, 2000], "the pinned origin stays");
    assert_eq!(f.state().newest_agreed_at_most(1500), Some(1000));
}

#[test]
fn retain_sets_zero_and_above_the_bound_are_refused_with_47() {
    let f = fsm();
    for v in [0u16, (MAX_CATALOG_SETS + 1) as u16] {
        assert_eq!(
            f.validate_replicated(&settings_with_retain(&f, v)),
            Err(ClusterRefusal::SettingsBounds("retain_sets")),
            "retain_sets = {v}"
        );
    }
    assert_eq!(ClusterRefusal::SettingsBounds("retain_sets").reason_code(), 47);
}

#[test]
fn a_commanded_entry_older_than_the_oldest_kept_agreed_set_is_dropped() {
    let mut f = fsm();
    genesis_row(&mut f, 0, 100);
    f.on_snapshot_frame(500, false, 1); // never completes
    assert_eq!(positions(&f), vec![500]);
    agreed_set(&mut f, 1000, 1100);
    assert_eq!(positions(&f), vec![1000], "500 is older than the oldest kept agreed set");
    f.on_snapshot_frame(1500, false, 2); // a stall ABOVE the floor stays visible
    agreed_set(&mut f, 2000, 2100);      // retain_sets unset → 1: 1000 goes, and 1500 with it
    assert_eq!(positions(&f), vec![2000]);
}

#[test]
fn two_fsms_fed_the_same_sequence_freeze_byte_equal_images() {
    let mut a = fsm();
    let mut b = fsm();
    for f in [&mut a, &mut b] {
        genesis_row(f, 0, 100);
        f.on_snapshot_frame(1000, true, 5);
        assert_eq!(apply_at(f, 1100, &report(0, 1000, &[(2, 1)])), 0);
        assert_eq!(apply_at(f, 1110, &report(CLUSTER_ROW, 1000, &[(2, 1)])), 0);
        f.set_consumed(1200);
    }
    let (ia, _) = a.freeze().unwrap();
    let (ib, _) = b.freeze().unwrap();
    assert_eq!(ia, ib);
    let mut c = fsm();
    assert_eq!(c.install_snapshot(1200, &mut &ia[..]).unwrap(), 1200);
    assert_eq!(c.state().catalog, a.state().catalog);
}

#[test]
fn a_v3_image_installs_with_an_empty_catalog() {
    // A v3 image = a v4 image minus the trailing empty-catalog prefix, with
    // the version word rewritten and the CRC recomputed.
    let mut f = fsm();
    f.set_consumed(300);
    let (img, _) = f.freeze().unwrap();
    let body_end = img.len() - 4;
    let mut v3 = img[..body_end - 4].to_vec(); // drop the 4-byte empty-catalog length prefix
    v3[8..12].copy_from_slice(&3u32.to_le_bytes());
    let crc = crc32fast::hash(&v3);
    v3.extend_from_slice(&crc.to_le_bytes());
    let mut g = fsm();
    assert_eq!(g.install_snapshot(300, &mut &v3[..]).unwrap(), 300);
    assert!(g.state().catalog.is_empty());
    assert!(g.state().catalog_empty());
    assert_eq!(g.state().retain_sets(), 1, "an unset retain_sets reads as 1");
}
```

- [ ] **Step 2: Run, expect FAIL** (no `catalog` field, no `on_snapshot_frame`, image v4 absent).

- [ ] **Step 3: Implement.**

*`cluster_image.rs`*: `CLUSTER_IMAGE_VERSION = 4`; add `pub catalog: &'a [u8]` to `ClusterImageParts`; in `encode_cluster_image` append `catalog_len u32 ‖ catalog` after `running`; in `decode_cluster_image` read it when `version >= 4` (same `.get(..)` shape as `running`), else `&body[body.len()..]`. Extend the doc comment at line 50–57 with the v4 line ("a v1–v3 image is still ACCEPTED on read, with `catalog` empty — the `Empty` state of catalog spec §4.5"). Update the frozen-bytes test's expectations only where the version word changes; add a test that a v4 image round-trips a non-empty catalog blob.

*`cluster_fsm.rs`*:

```rust
// ClusterState
pub catalog: Vec<SetEntry>,

pub fn retain_sets(&self) -> u16 { self.settings.retain_sets.max(1) }

pub fn declared_mask(&self) -> u64 {
    self.running.iter().enumerate()
        .filter(|(_, r)| r.is_some())
        .fold(0, |m, (i, _)| m | (1 << i))
}

/// D3: a `SNAPSHOT` frame ending at `end`. Idempotent on `end`.
pub fn on_snapshot_frame(&mut self, end: u64, standby: bool, time_ns: u64) {
    if self.catalog.iter().any(|e| e.position == end) { return; }
    let kind = if standby { SetKind::Standby } else { SetKind::Full };
    self.catalog.push(SetEntry::commanded(end, kind, time_ns));
    self.catalog.sort_by_key(|e| e.position);
    self.cap_catalog();
}

fn put_report(&mut self, r: SnapshotReport) {
    // existing per-row replace stays for rows < 8 (the diagnostic matrix)
    if r.row != CLUSTER_ROW { /* existing body */ }
    // catalog fold
    let v = uc_protocol::v2::upgrade::verdict(&r);
    let verdict = if v.agreed { RowVerdict::Agreed }
        else if v.majority_hash.is_some() { RowVerdict::Diverged }
        else { RowVerdict::NoMajority };
    let entry = RowEntry { version: self.version_for_report(&r), hash: v.majority_hash.unwrap_or(0), verdict };
    let declared = self.declared_mask();
    if let Some(e) = self.catalog.iter_mut().find(|e| e.position == r.position) {
        if r.row == CLUSTER_ROW { e.cluster = entry; }
        else if declared & (1 << r.row) != 0 { e.rows[r.row as usize] = entry; }
        else { return; }                                   // undeclared row: ignored
        let complete = e.cluster.verdict != RowVerdict::Unreported
            && (0..CNC_MAX_SERVICES).all(|i| declared & (1 << i) == 0 || e.rows[i].verdict != RowVerdict::Unreported);
        if complete && e.state == SetState::Commanded { e.state = SetState::Complete; }
        if e.is_agreed(declared) { self.retire(); }
    }
}
```

`version_for_report`: the row's `running` version at the time of the report for rows `< 8` (`self.running[row].map(|r| r.version).unwrap_or(0)`); `0` for the cluster row. `retire()` implements §4.4 (as amended by the errata): let `A` = agreed entries in position order and `keep_pins` = every row's `pin_for(row).origin`; while `|A| > retain_sets()`, remove the oldest agreed entry that is not a pinned origin (stop if only pinned ones remain); then remove every entry older than the oldest remaining agreed entry. `cap_catalog()` drops the oldest **non-agreed** entries when `catalog.len() > MAX_CATALOG_SETS`, so a stall storm cannot grow the image without bound. `newest_agreed_at_most(x)`: `self.catalog.iter().rev().find(|e| e.position <= x && e.is_agreed(self.declared_mask())).map(|e| e.position)`. `catalog_empty()`: `newest_agreed_at_most(u64::MAX).is_none()`. In `apply`'s `Settings` arm, after `self.state.settings = s;`, call `self.state.retire()`. In `validate_replicated`'s `Settings` arm add:

```rust
if !(1..=MAX_CATALOG_SETS as u16).contains(&s.retain_sets) {
    return Err(ClusterRefusal::SettingsBounds("retain_sets"));
}
```

`0` is refused unconditionally at apply; `ClusterState::genesis()` seeds `retain_sets = 1`. A `0` reaches the state only through an installed v1–v3 image, which `retain_sets()`'s `.max(1)` reads as today's newest-only retention (spec errata).

`freeze`: encode `catalog` with `encode_set_list` into the new blob (map `None` to `SnapshotError::Codec("cluster image: catalog exceeds MAX_CATALOG_SETS")`). `install_snapshot`: `decode_set_list(parts.catalog)` (an empty slice → `Vec::new()`), else `SnapshotError::Codec("cluster image: catalog")`.

`ClusterView`: add the four atomics and `inner.catalog`; `publish` stores `catalog_agreed_position = newest_agreed_at_most(u64::MAX).unwrap_or(0)`, `catalog_len`, `catalog_stalled` (count of `Commanded` entries — the timeout judgement is Task 6's `stalled()`), `catalog_diverged` (count of rows with `Diverged|NoMajority` across listed sets), all BEFORE `position`. `snapshot_inner`/`to_state` carry `catalog`. Add `pub fn on_snapshot_frame(&mut self, end: u64, standby: bool, time_ns: u64)` on `ClusterFsm`, forwarding to the state (the agent and the tests call it; there is no `state_mut`).

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_protocol cluster_image && cargo test -p uc_node --lib cluster_fsm`

- [ ] **Step 5: Commit**: `git commit -am "cluster_fsm(catalog): sets from SNAPSHOT frames and reports; retain_sets retention; image v4"`

---

### Task 6: The query module and the soft table

**Files:**
- Create: `uc_node/src/catalog.rs`
- Modify: `uc_node/src/lib.rs` (`pub mod catalog;`, re-export `SoftTable`, `Holdings`)
- Test: in the new file

**Interfaces:**
- Consumes: `ClusterViewInner.catalog`, `SetEntry`, `Holdings` (Task 4).
- Produces:

```rust
pub struct SoftEntry { pub holdings: Holdings, pub last_seen_ns: u64 }
#[derive(Default)]
pub struct SoftTable { pub by_node: BTreeMap<NodeId, SoftEntry> }
impl SoftTable {
    pub fn record(&mut self, node: NodeId, h: Holdings, now_ns: u64);
    pub fn live(&self, now_ns: u64, stale_ns: u64) -> impl Iterator<Item = (NodeId, &Holdings)>;
}

pub struct CatalogQuery<'a> {
    pub sets: &'a [SetEntry], pub declared: u64, pub catalog_position: u64,
    pub soft: &'a SoftTable, pub now_ns: u64, pub stale_ns: u64,
}
impl CatalogQuery<'_> {
    pub fn newest_agreed(&self, at_most: u64) -> Option<u64>;
    pub fn agreed_for(&self, row: u8, version: u32) -> Vec<u64>;        // same_line
    pub fn holders(&self, p: u64) -> Vec<NodeId>;
    pub fn journal_covers(&self, p: u64, q: u64) -> Vec<NodeId>;
    pub fn stalled(&self, timeout_ns: u64) -> Vec<u64>;
    pub fn diverged(&self) -> Vec<(u64, u8)>;                            // (position, row; 255 = cluster)
    pub fn coverage_gaps(&self) -> Vec<(u64, u64)>;
    /// §4.4: the newest agreed set among those `node` advertises holding.
    pub fn effective_floor(&self, node: NodeId) -> Option<u64>;
}
```

`holders(p)`: a node counts iff its entry is live, `holdings.catalog_position == self.catalog_position`, and bit `i` of `sets_held` is set where `i` is `p`'s index in `sets`. `effective_floor(node)` = the newest agreed `p` held by `node` (`None` when Empty or nothing held — the caller falls back). "What may be deleted" is not a query: the pruner deletes every on-disk position the catalog does not list (spec errata).

- [ ] **Step 1: Failing tests**

```rust
use super::*;
use uc_protocol::identity::pack_version;
use uc_protocol::v2::catalog::*;

const OK: RowEntry = RowEntry { version: 0x0102_0000, hash: 1, verdict: RowVerdict::Agreed };
fn agreed(p: u64) -> SetEntry {
    let mut e = SetEntry::commanded(p, SetKind::Full, p);
    e.rows[0] = OK; e.cluster = OK; e.state = SetState::Complete; e
}
fn diverged(p: u64) -> SetEntry {
    let mut e = agreed(p); e.rows[0].verdict = RowVerdict::Diverged; e
}
fn holding(catalog_position: u64, sets_held: u64, first: u64, durable: u64) -> Holdings {
    Holdings { catalog_position, sets_held, journal_first: first, durable, commit: durable, ..Default::default() }
}
fn q<'a>(sets: &'a [SetEntry], soft: &'a SoftTable) -> CatalogQuery<'a> {
    CatalogQuery { sets, declared: 0b1, catalog_position: 42, soft, now_ns: 10_000, stale_ns: 900 }
}

#[test]
fn newest_agreed_skips_diverged_and_commanded_sets() {
    let sets = [agreed(1000), diverged(2000), SetEntry::commanded(2500, SetKind::Full, 0), agreed(3000)];
    let soft = SoftTable::default();
    let c = q(&sets, &soft);
    assert_eq!(c.newest_agreed(u64::MAX), Some(3000));
    assert_eq!(c.newest_agreed(2999), Some(1000), "2000 diverged, 2500 commanded");
    assert_eq!(c.newest_agreed(999), None);
}

#[test]
fn holders_requires_live_and_matching_catalog_position() {
    let sets = [agreed(1000), agreed(2000)];
    let mut soft = SoftTable::default();
    soft.record(1, holding(42, 0b10, 0, 5000), 9_500);   // live, holds 2000
    soft.record(2, holding(42, 0b10, 0, 5000), 9_000);   // stale: 10_000 - 9_000 > 900
    soft.record(3, holding(41, 0b10, 0, 5000), 9_900);   // wrong catalog position
    soft.record(4, holding(42, 0b01, 0, 5000), 9_900);   // holds 1000 only
    let c = q(&sets, &soft);
    assert_eq!(c.holders(2000), vec![1]);
    assert_eq!(c.holders(1000), vec![4]);
    assert_eq!(c.holders(3000), Vec::<NodeId>::new(), "not a listed set");
}

#[test]
fn journal_covers_is_first_le_p_and_durable_ge_q() {
    let sets = [agreed(1000)];
    let mut soft = SoftTable::default();
    soft.record(1, holding(42, 0, 1000, 2000), 9_900);
    soft.record(2, holding(42, 0, 1001, 2000), 9_900);
    soft.record(3, holding(42, 0, 1000, 1999), 9_900);
    let c = q(&sets, &soft);
    assert_eq!(c.journal_covers(1000, 2000), vec![1]);
}

#[test]
fn stalled_names_commanded_sets_past_the_timeout() {
    let sets = [SetEntry::commanded(100, SetKind::Full, 1_000), SetEntry::commanded(200, SetKind::Full, 9_800), agreed(300)];
    let soft = SoftTable::default();
    let c = q(&sets, &soft);                       // now_ns = 10_000
    assert_eq!(c.stalled(500), vec![100], "200 is only 200 ns old; 300 is complete");
}

#[test]
fn diverged_lists_every_non_agreed_row_including_the_cluster() {
    let mut e = agreed(1000);
    e.cluster.verdict = RowVerdict::NoMajority;
    let sets = [e, diverged(2000), agreed(3000)];
    let soft = SoftTable::default();
    assert_eq!(q(&sets, &soft).diverged(), vec![(1000, CLUSTER_ROW), (2000, 0)]);
}

#[test]
fn coverage_gaps_are_spans_nobody_can_rebuild() {
    let sets = [agreed(1000), agreed(2000), agreed(3000)];
    let mut soft = SoftTable::default();
    soft.record(1, holding(42, 0b100, 2500, 3500), 9_900);   // holds 3000; journal [2500, 3500]
    let c = q(&sets, &soft);
    // (1000, 2000]: nobody holds 2000 and no journal covers it → gap
    // (2000, 3000]: node 1 holds 3000 → covered
    assert_eq!(c.coverage_gaps(), vec![(1000, 2000)]);
}

#[test]
fn effective_floor_is_what_this_node_holds() {
    let sets = [agreed(1000), agreed(2000)];
    let mut soft = SoftTable::default();
    soft.record(1, holding(42, 0b01, 0, 9000), 9_900);
    soft.record(2, holding(42, 0b00, 0, 9000), 9_900);
    let c = q(&sets, &soft);
    assert_eq!(c.newest_agreed(u64::MAX), Some(2000), "the cluster floor");
    assert_eq!(c.effective_floor(1), Some(1000), "node 1 holds only 1000");
    assert_eq!(c.effective_floor(2), None, "a voter on a learner-only cluster");
}

#[test]
fn agreed_for_matches_the_line_not_the_patch() {
    let sets = [agreed(1000), agreed(2000)];
    let soft = SoftTable::default();
    let c = q(&sets, &soft);
    assert_eq!(c.agreed_for(0, pack_version(1, 2, 7)), vec![1000, 2000]);
    assert_eq!(c.agreed_for(0, pack_version(1, 3, 0)), Vec::<u64>::new());
}
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** per the interface. `coverage_gaps`: for each consecutive pair of agreed `(a, b)` in position order, the span `(a, b]` is a gap iff `holders(b).is_empty() && journal_covers(a, b).is_empty()`.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_node --lib catalog`

- [ ] **Step 5: Commit**: `git commit -am "node(catalog): SoftTable and the eight query functions"`

---

### Task 7: The cluster agent records *commanded* and exposes the catalog offline

**Files:**
- Modify: `uc_node/src/cluster_agent.rs:503-531` (the `FRAME_TYPE_SNAPSHOT` arm), `:136-163` (offline readers)
- Test: `uc_node/tests/cluster_agent.rs` or the agent's in-file tests (follow whichever exists: `grep -n "mod tests" uc_node/src/cluster_agent.rs`)

**Interfaces:**
- Produces: `pub fn read_committed_catalog(instance_dir: &Path) -> io::Result<(u64, Vec<SetEntry>, u64 /*declared_mask*/)>`.

- [ ] **Step 1: Failing test**: a unit/integration test that appends a standby `SNAPSHOT` frame to a log buffer walked by the agent on a **voter** (`node_flags` without `NODE_FLAG_LEARNER`) and asserts `fsm.state().catalog[0].kind == SetKind::Standby` — i.e. the entry exists even though the voter paid no freeze. Model it on the agent's existing freeze test.

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.** In the `FRAME_TYPE_SNAPSHOT` arm, **before** `if standby && node_flags & NODE_FLAG_LEARNER == 0 { continue; }`, insert:

```rust
// Catalog spec D3: every node records the instant, whatever its role —
// a voter that skips a standby freeze still catalogs the set, or voters
// and learners would hold different catalogs.
self.fsm.state_mut().on_snapshot_frame(end, standby, hdr.time_ns);
applied_any = true;
```

(`applied_any = true` makes `publish_view` run at the end of the batch.) Add `read_committed_catalog` beside `read_committed_upgrade`, returning `(st.applied, st.catalog.clone(), st.declared_mask())` from `recover(...)`.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_node cluster_agent`

- [ ] **Step 5: Commit**: `git commit -am "cluster_agent(catalog): record the commanded set before the role check; offline catalog reader"`

---

### Task 8: The node — row-255 report and the effective floor

**Files:**
- Modify: `uc_node/src/node.rs:6360-6469` (`send_snapshot_reports`), `:6527` (`on_snap_report` row guard), `:6652` (`maybe_append_snapshot_reports`), `:5272-5354` (`maybe_persist_snapshot_floor`), `:6763-6825` (`prune_snapshots_below`)
- Test: `node.rs` `mod tests` (the retention tests near `:12811` and `:17243` are the shape)

**Interfaces:**
- Consumes: `CatalogQuery::effective_floor`, `ClusterView.catalog_agreed_position`, `inner.catalog`.
- Produces: `fn effective_floor_candidate(&self) -> u64` on `Consensus`: `if catalog is Empty → snapshot_set_position; else → min(snapshot_set_position, newest agreed set this node holds)`. Holding is read from the node's own `Holdings` cache (Task 9) — until Task 9 lands, compute it from `snapshot_set_position` alone (`newest_agreed_at_most(snapshot_set_position)`).

- [ ] **Step 1: Failing tests**

Follow the shape of the existing retention tests near `node.rs:12811` (`retention_keeps_every_pinned_origin`) and the floor test at `:17243`, which build a `Consensus` fixture with a temp instance dir and a published `ClusterView`.

```rust
#[test]
fn the_completeness_report_includes_the_cluster_artifact_as_row_255() {
    let (mut c, dir) = leader_fixture_with_rows(&[0]);          // the helper the :12811 tests use
    let p = 4096;
    write_row_artifact(&dir, 0, p, b"row-zero");                // snap-4096.ultsnap with a real envelope
    let cluster_payload = write_cluster_artifact(&dir, p, b"cluster");
    c.cnc.service_slot(0).snapshot_pos.store_release(p);
    c.cnc.service_slot(0).identity.store_artifact_hash(uc_service::snapshots::artifact_hash_of(b"row-zero"));
    c.cluster_snapshot_pos.store(p, Ordering::Release);
    c.check_set_completeness();
    let rows: Vec<u8> = c.pending_snapshot_reports.keys().copied().collect();
    assert_eq!(rows, vec![0, CLUSTER_ROW]);
    assert_eq!(
        c.pending_snapshot_reports[&CLUSTER_ROW].hashes[&c.id],
        uc_service::snapshots::artifact_hash_of(&cluster_payload),
        "row 255's hash is the SAME function over the cluster artifact's payload"
    );
}

#[test]
fn effective_floor_candidate_follows_what_this_node_holds() {
    let (mut c, _dir) = leader_fixture_with_rows(&[0]);
    let mut st = c.cluster_view.to_state();
    st.catalog = vec![agreed_entry(1000), agreed_entry(2000)];   // row 0 + cluster Agreed
    st.running[0] = Some(RowRunning { row: 0, version: 1, record_pos: 1 });
    c.cluster_view.publish(&st);
    c.snapshot_set_position.store(1000, Ordering::Release);
    assert_eq!(c.effective_floor_candidate(), 1000);
    c.snapshot_set_position.store(2000, Ordering::Release);
    assert_eq!(c.effective_floor_candidate(), 2000);
    c.snapshot_set_position.store(1500, Ordering::Release);
    assert_eq!(c.effective_floor_candidate(), 1000, "holds 1500 on disk? no — 1500 is not a listed set; the newest agreed ≤ 1500 is 1000");
    c.snapshot_set_position.store(500, Ordering::Release);
    assert_eq!(c.effective_floor_candidate(), 0, "nothing agreed at or below what this node holds");
    st.catalog.clear();
    c.cluster_view.publish(&st);
    c.snapshot_set_position.store(3000, Ordering::Release);
    assert_eq!(c.effective_floor_candidate(), 3000, "Empty: today's behaviour");
}

#[test]
fn the_pruner_keeps_every_catalogued_position_and_every_pin() {
    let (c, dir) = leader_fixture_with_rows(&[0]);
    for p in [500u64, 1000, 2000, 3000] { write_row_artifact(&dir, 0, p, b"x"); write_cluster_artifact(&dir, p, b"c"); }
    let mut st = c.cluster_view.to_state();
    st.catalog = vec![agreed_entry(2000), agreed_entry(3000)];
    st.pins.push(UpgradePin { row: 0, origin: 1000, from: 1, to: 2 });
    c.cluster_view.publish(&st);
    c.prune_snapshots_below(3000);
    let left: Vec<u64> = list_row_artifacts(&dir, 0);
    assert_eq!(left, vec![1000, 2000, 3000], "500 deleted; 1000 is a pin; 2000/3000 are listed");
}
```

`agreed_entry(p)`, `write_row_artifact`, `write_cluster_artifact`, `list_row_artifacts` are small test helpers to add beside `leader_fixture_with_rows` if they do not exist (the `:12811` test already writes artifacts by name; reuse its code).

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.**
  - `send_snapshot_reports(p)`: after the row loop, hash the cluster artifact: `let path = self.cluster_snapshot_dir.join(format!("snap-{p}{CLUSTER_SNAP_SUFFIX}"));` read it, strip the envelope with `uc_service::snapshots::decode_snapshot_envelope`'s length (`SNAPSHOT_ENVELOPE_LEN`) and hash the payload with `uc_service::snapshots::artifact_hash_of` — **the same function the service builder uses** (check its call in `uc_service/src/lib.rs` builder thread and match its input exactly: envelope-stripped payload or whole file). Report it as row `CLUSTER_ROW` through the same leader/follower branch. If the file is missing, skip the row (the set is not complete here, and the completeness check will re-run).
  - `on_snap_report`: replace `row as usize >= CNC_MAX_SERVICES` with `!is_report_row(row)`; `held_report_position(row)` must index safely for 255 (use the view's `report_position_for`, which looks up by row value, not by array index — verify at `cluster_fsm.rs:922-932`).
  - `maybe_append_snapshot_reports`: walk rows ascending **then** `CLUSTER_ROW`.
  - `maybe_persist_snapshot_floor`: replace `let service_pos = self.snapshot_set_position.load(..)` with `let service_pos = self.effective_floor_candidate();` where:

```rust
fn effective_floor_candidate(&self) -> u64 {
    let own = self.snapshot_set_position.load(Ordering::Acquire);
    let agreed = self.cluster_view.catalog_agreed_position.load(Ordering::Acquire);
    if agreed == 0 { return own; }                       // Empty: today's behaviour
    let inner = self.cluster_view.snapshot_inner();
    let declared = self.services.ids().fold(0u64, |m, r| m | (1 << r));
    inner.catalog.iter().rev()
        .find(|e| e.position <= own && e.is_agreed(declared))
        .map(|e| e.position).unwrap_or(0)
}
```

    `0` means "no agreed set I hold": `have_new_floor` is then false and nothing moves — a voter on a learner-only cluster (§4.6).
  - `prune_snapshots_below(p)`: extend `keep` with every `e.position` in `inner.catalog` (listed = kept, whatever its state). Keep `below = p`.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_node --lib`

- [ ] **Step 5: Commit**: `git commit -am "node(catalog): row-255 report; the effective floor; the pruner keeps what the catalog lists"`

---

### Task 9: The soft side end to end

**Files:**
- Modify: `uc_net/src/receiver.rs:416-434` (`FollowerConfig` gains `holdings: Option<Arc<Mutex<Holdings>>>`), `:3362-3392` (send the real body), `:2199-2213` (receive)
- Modify: `uc_net/src/sender.rs` (`CtrlMsg::Status` gains `holdings: Holdings`; `Sender` gains `soft: Option<Arc<Mutex<SoftTableWire>>>`)
- Modify: `uc_node/src/node.rs` (`Holdings` cache writers: `check_set_completeness`, pruner, archive first-base mirror, a 1 s filesystem probe in `do_work`; the leader's `SoftTable` fed from the sender's shared map; `SOFT_STALE_NS`)
- Test: `uc_net` unit test for the round trip; `node.rs` unit test for the probe cadence

`SoftTableWire` is `HashMap<SocketAddr, (Holdings, u64 /*now_ns*/)>` in `uc_net` (it knows addresses, not node ids); the node maps `SocketAddr → NodeId` via membership when it builds `SoftTable`.

- [ ] **Step 1: Failing tests**
  - `uc_net`: a follower configured with a `holdings` cell whose `sets_held = 0b10` sends a `STATUS`; the leader's shared map shows that value for the follower's address.
  - `node.rs`: `holdings_probe_runs_at_most_once_per_second` — call the pass twice within 1 ms, assert the filesystem probe counter is 1; after advancing the clock 1 s, 2. And `holdings_are_not_rebuilt_on_the_pass` — the directory-listing counter does not increase across 1000 passes.

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.**
  - `uc_net`: `FollowerConfig.holdings`; at the send site, `let holdings = self.cfg.holdings.as_ref().map(|h| *h.lock().unwrap()).unwrap_or_default();` and write the v2 body. On receive, forward `b.holdings` in `CtrlMsg::Status`; the sender inserts `(holdings, now_ns)` into `soft` when present. Keep `FlowControl` untouched.
  - `node.rs`: a `holdings: Arc<Mutex<Holdings>>` created at start and handed to the follower config; writers: `check_set_completeness` recomputes `sets_held`/`catalog_position` from the view (it already walks the rows); `prune_snapshots_below` clears bits it deleted; the archive first-base mirror updates `journal_first`; the consensus pass copies `durable`/`commit`/`applied[..]` (cheap atomic loads) and, at most once per second (`last_holdings_probe_ns`), runs `std::fs` for the three byte counts. Define `pub const SOFT_STALE_FACTOR: u64 = 3;` and `fn soft_stale_ns(&self) -> u64 { SOFT_STALE_FACTOR * self.cfg.election_timeout_max_ns }`. Expose `pub fn soft_table(&self) -> SoftTable` on `Node` (map addresses to ids via `cluster_view.snapshot_inner().membership`).
  - Record in the task report the `STATUS` body size before (16) and after (144) — D8.

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_net && cargo test -p uc_node --lib`

- [ ] **Step 5: Commit**: `git commit -am "net+node(catalog): STATUS carries Holdings; the leader keeps a soft table with staleness"`

---

### Task 10: Metrics and alerts

**Files:**
- Modify: `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` +5; render block beside `uc2_snapshot_set_position` at `:874`; the count test `~:1915` 116 → 121; `every_contract_series_is_present`)
- Modify: `docs/how-to/monitor-a-cluster.md:70` (116 → 121), `:409-421` (qualify "must agree cluster-wide"), `:457-485`
- Modify: `packaging/prometheus/uc2-alerts.yml:347-360` (`Uc2SnapshotSetDiverged` → `expr: max(uc2_catalog_diverged) > 0`, `for: 60s`)
- Modify: `scripts/m10_alert_fire.sh` (`build_Uc2SnapshotSetDiverged` selects `uc2_catalog_diverged` from the scenario; regenerate or hand-edit the captured `snapshot_set_diverged` scenario to carry the new series)

Gauges: `uc2_catalog_sets` (listed), `uc2_catalog_agreed_position` (the cluster floor, 0 = Empty), `uc2_catalog_empty` (1 while Empty), `uc2_catalog_stalled` (commanded-not-complete count), `uc2_catalog_diverged` (rows Diverged|NoMajority across listed sets). All read from the `ClusterView` atomics, no lock at scrape.

- [ ] **Step 1: Failing tests**: bump the count assertion to 121 and add the five names to `synthetic_sources()`; run `cargo test -p uc_node --lib metrics` → FAIL until rendered.
- [ ] **Step 2–4**: render the five with `push_gauge`; run until PASS; run `promtool test rules` through `scripts/m10_alert_fire.sh` for the two snapshot rules (the script fails fast if a shipped alert lacks a builder — keep names unchanged).
- [ ] **Step 5: Commit**: `git commit -am "obs(catalog): five catalog gauges; Uc2SnapshotSetDiverged keys on the catalog verdict"`

---

### Task 11: Sim invariant inv13 and the fuzz target

**Files:**
- Modify: `uc_sim/src/invariants.rs` (after `check_set_alignment`, `:727-770`), `uc_sim/src/world.rs` (the sweep at `~:1370-1400`; a `check_catalog_determinism` wrapper beside `check_set_alignment`; a `stat_catalog_checks` counter + accessor)
- Modify: `uc_sim/tests/scenarios.rs` (a test beside `inv12_…` at `:2140`)
- Create: `fuzz/fuzz_targets/uc_protocol_status_body.rs`; Modify: `fuzz/Cargo.toml` (one `[[bin]]`)

inv13, abstractly: for every node `n` with frontier `f = min(commit, durable)`, derive `catalog(n) = { (P, kind) : SNAPSHOT frame at END P ≤ f }` from the world's `snapshot_frames`, and `agreed(n) = { P ∈ catalog(n) : the set at P is complete on every node that lists it }` from `complete_snapshot_sets()`. Check (a) any two nodes with equal `f` have equal `catalog(n)`; (b) no node's purge floor (the world's per-node floor, as inv11 reads it) exceeds `max(agreed(n) ∩ sets n holds)`.

- [ ] **Step 1: Failing test** in `scenarios.rs`:

```rust
#[test]
fn inv13_the_catalog_is_a_function_of_the_committed_prefix() {
    let mut w = World::new(two_readers_cfg());
    churn_membership(&mut w, 2).expect("invariants");
    let n = w.command_instants(3).expect("invariants (instants)");   // existing helper or add one beside churn_membership
    assert!(n >= 3);
    assert!(w.catalog_checks() > 0, "inv13 never asserted anything");
}
```

- [ ] **Step 2: Run, expect FAIL** (`catalog_checks` missing).
- [ ] **Step 3: Implement** `InvariantChecker::check_catalog_determinism(&self, a: NodeId, b: NodeId, frontier: u64, missing: &[(u64, u8)], step) -> Result<(), InvariantViolation>` and the floor check, the `World::check_catalog_determinism(&self, step) -> Result<u64, InvariantViolation>` wrapper (pairs of nodes with equal frontiers), the counter, and the sweep call next to `check_set_alignment`. The fuzz target:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::datagram::{read_status_body, write_status_body, STATUS_BODY_LEN};
fuzz_target!(|data: &[u8]| {
    if let Some(b) = read_status_body(data) {
        let mut buf = [0u8; STATUS_BODY_LEN];
        write_status_body(&mut buf, &b);
        assert_eq!(read_status_body(&buf), Some(b), "re-encode must round-trip");
    }
});
```

- [ ] **Step 4: Run, expect PASS**: `cargo test -p uc_sim inv13`; `(cd fuzz && cargo +nightly fuzz run uc_protocol_status_body -- -max_total_time=30)`; also 30 s of `uc_node_cluster_artifact` (the v4 layout).
- [ ] **Step 5: Commit**: `git commit -am "sim+fuzz(catalog): inv13 catalog determinism; STATUS body fuzz target"`

---

### Task 12: End-to-end tests

**Files:**
- Create: `uc_node/tests/catalog.rs` (reuse `learner.rs`'s fixtures by copying the small helpers it needs: `make_config`, `spawn_cluster_with_learner_services`, `instant_until_complete`, `start_sum_service`, `submit_frames`, `await_until`, `command_standby_instant`; do not `#[path]`-include `learner.rs`)

Six tests (spec §10.5), each driving a real 2-voter + 1-learner cluster with `SumSm` rows and asserting through `node.cluster_view()`/`CatalogQuery` and `uc2_catalog_*` readings:

1. `a_diverged_row_completes_the_set_but_never_moves_the_floor`: a second row implemented by a `DivergingSumSm` whose `freeze` encodes `node_id` into the image on one node; assert `rows[1].verdict == Diverged`, `catalog_agreed_position` unchanged, a joiner below the floor installs the previous agreed set, `uc2_catalog_diverged >= 1`.
2. `a_stalled_set_stays_commanded_and_the_next_instant_completes`: one row's slot without `CNC_SVC_STATUS_SNAPSHOT_CAPABLE` (faked row, as `learner.rs` does); command an instant → the FSM's entry stays `Commanded`; restore the bit; next instant → `Complete`; `stalled(0)` names the first.
3. `retain_sets_2_retires_the_oldest_and_keeps_the_pinned_origin`: purge on, `settings apply` with `retain_sets = 2`, three instants; assert the catalog lists the two newest, the oldest set's files are gone from every node, `archive_first_base` follows the effective floor, and a pinned origin survives a fourth instant.
4. `the_flag_day_window_is_empty_and_deletes_nothing`: write a v3 cluster artifact and pre-existing row artifacts into a fresh instance dir (reuse Task 5's v3 construction), start the node; assert `uc2_catalog_empty == 1`, a joiner is served, the pruner deletes nothing; command an instant; assert `Empty` ends and `catalog_agreed_position == P`.
5. `a_killed_node_leaves_holders_after_the_stale_timeout`: stop a learner; after `soft_stale_ns` it is absent from `holders(P)`; `uc2ctl snapshot fetch` from the other holder lands.
6. `learner_only_voters_do_not_purge_until_they_fetch`: `settings apply` `snapshot_target = learners`, purge on; a cadence instant completes on the learner; assert the set is agreed, `holders(P) == [learner]`, both voters' `archive_first_base == 0` after 10 s; `uc2ctl snapshot fetch --from learner` on one voter; assert that voter's floor moves and the other's does not.

Test 6, written in full (the others follow its shape; the standby instant is the learner-only driver the existing fixtures already use, `command_standby_instant`, so no `settings apply` plumbing is needed in a test):

```rust
#[test]
fn learner_only_voters_do_not_purge_until_they_fetch() {
    let _g = serialize();
    let app = "catalog-learner-only";
    let c = spawn_cluster_with_learner_services_purging(2, 1, ServicesConfig::single(SumSm::NAME), 64 * 1024);
    let svcs: Vec<_> = c.nodes.iter().map(|n| start_sum_service(n.dir(), app)).collect();
    let leader = await_single_leader(&c.nodes, 30);
    let learner = 2usize;
    submit_frames(&c.nodes[leader].node, 6000);
    let p = command_standby_instant(&c.nodes[leader].node);
    await_until(60, "the learner completed the standby set", || c.nodes[learner].node.snapshot_set_position() >= p);
    await_until(60, "the catalog agreed the standby set", || {
        c.nodes[leader].node.cluster_view().catalog_agreed_position.load(Ordering::Acquire) == p
    });
    let soft = c.nodes[leader].node.soft_table();
    let view = c.nodes[leader].node.cluster_view().snapshot_inner();
    let q = CatalogQuery { sets: &view.catalog, declared: 0b1, catalog_position: c.nodes[leader].node.cluster_view().position.load(Ordering::Acquire),
                           soft: &soft, now_ns: unix_ns(), stale_ns: c.nodes[leader].node.soft_stale_ns() };
    assert_eq!(q.holders(p), vec![c.nodes[learner].id], "only the learner holds the standby set");
    submit_frames(&c.nodes[leader].node, 6000);
    std::thread::sleep(Duration::from_secs(10));
    for v in [0usize, 1] {
        assert_eq!(c.nodes[v].node.archive_first_base(), 0, "voter {v} must not purge below a set it does not hold");
        assert_eq!(c.nodes[v].node.cluster_view().catalog_agreed_position.load(Ordering::Acquire), p, "…though it knows the cluster floor");
    }
    c.nodes[0].node.request_fetch(c.nodes[learner].id, 0).expect("fetch from the learner");
    await_until(60, "voter 0 landed the set", || c.nodes[0].node.snapshot_set_position() >= p);
    await_until(60, "voter 0's floor moved", || c.nodes[0].node.archive_first_base() > 0);
    assert_eq!(c.nodes[1].node.archive_first_base(), 0, "voter 1 still holds nothing");
    drop(svcs);
}
```

`spawn_cluster_with_learner_services_purging` is `spawn_cluster_with_learner_services` with `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` and the given ring size in `make_config`; `request_fetch` is the node-side entry of admin op 9 (`node.rs`, `fn request_fetch`) — if it is not `pub`, expose a `pub fn fetch_snapshot_from(&self, learner: NodeId, position: u64)` test seam.

- [ ] For each of the six: write it, run it to see it fail against a stubbed assertion (e.g. assert the catalog is non-empty before any instant — fails), then make it pass. Run the whole file with `--test-threads=1`.
- [ ] **Commit**: `git commit -am "test(catalog): six end-to-end scenarios"`

---

### Task 13: Docs and spec errata

**Files (each line names the statement to change):**
- `docs/how-to/upgrade-a-cluster.md`: new `## Wire change after 0.10.0: the snapshot catalog (0.11.0)` before `## Where to go next` (~`:1001`), modelled on the `0.10.0` section at `:916-999`: stop-all/start-all, no cnc change, **nothing cleared on disk**, the `Empty` window and its gauge, `retain_sets` default 1.
- `docs/reference/semver-policy.md:218-224` (current wire → `0.11.0`), new `###` flag-day block after `:364`.
- `docs/reference/wire-protocol.md:13` (bump list), `:233` (`SNAP_REPORT` row 255), `:337-346` (kind 7 reserved), the `STATUS` body row.
- `docs/reference/configuration.md:134-135`: `retain_sets` row under `[settings]`; `[purge]` text unchanged.
- `docs/reference/uc2ctl.md:255-287` (`settings apply/show` samples gain `retain_sets`), `:856` (47 names `retain_sets`), `:378-406` (`snapshot show`: add "this is the node-local reading; the cluster's agreed view is the catalog, project 4").
- `docs/how-to/bound-journal-growth.md:78-104`: the floor follows the newest **agreed** set the node **holds**; `set=` is on-disk, not agreed.
- `docs/notes/uc2-cluster-fsm-explained.md:656-659`: the cluster artifact IS reported now (row 255); `:428-474` cross-reference to the catalog.
- `docs/ops/uc2-runbook.md`: a "reading the catalog" paragraph (gauges + `read_committed_catalog` via `uc2ctl` is project 4; today: `/metrics`).
- `docs/BACKLOG.md`: strike the "cluster artifact has no determinism check" line (closed by row 255); add project 2/3/4 lines pointing at the spec.
- The spec's errata block (append `#### Errata (as built)`): `declared_mask` is derived from `running`; `retain_sets = 0` refused unconditionally at apply, `.max(1)` only covers installed v1–v3 images; `SOFT_STALE_NS = 3 × election_timeout_max_ns`; `uc2_catalog_stalled` counts commanded-not-complete without a timeout (the timeout judgement is `stalled()`); anything else that diverged.
- `RELEASES.md` / `docs/releases.md`: **not** in this task — owed at the release cut.

- [ ] Run `scripts/check_doc_links.sh` (or the repo's link checker: `grep -rn "doc-links\|lychee" scripts .github | head`) → 0 errors.
- [ ] **Commit**: `git commit -am "docs(catalog): flag day 0.11.0, retain_sets, the catalog's floor rule; spec errata as built"`

---

### Task 14: Proof stack (evidence only, no commit)

Run, with the private target dir, logging each to `$HOME/scratch/rv-catalog/NN-*.log`:

1. `cargo build --workspace`
2. `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay`
3. `cargo test --workspace`
4. `cargo test -p uc_node --test lin_v2`
5. `cargo test -p uc_node --test lin_partition_v2`
6. `cargo test -p uc_crashtest --features hard-crash-tests`
7. `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`
8. `cargo test -p uc_node --test catalog -- --test-threads=1`
9. `cargo clippy --workspace --all-targets -- -D warnings` and `cargo clippy -p uc_crashtest --all-targets --features hard-crash-tests -- -D warnings`
10. `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`
11. `cargo fmt --all -- --check`
12. `scripts/fuzz_smoke.sh 30 --min-runs 1000 uc_protocol_status_body uc_node_cluster_artifact uc_protocol_settings`
13. `cargo test -p uc_sim` (inv13 on) and the doc-link check.

Report every command's exit code and the pass/fail counts verbatim. Any red stops the plan: it is not "flaky" until it has been reproduced and named.
