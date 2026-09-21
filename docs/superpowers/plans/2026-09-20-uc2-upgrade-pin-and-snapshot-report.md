# UpgradePin and SnapshotReport — the cluster records (plan B1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the two records the FSM upgrade lifecycle needs into the cluster FSM — `UpgradePin` (CLUSTER kind 4: "at position `origin`, row `r` went from `from` to `to`") and `SnapshotReport` (CLUSTER kind 5: the vector of per-node artifact hashes for one `(row, P)`, with a deterministic verdict) — with the operator path (`uc2ctl upgrade pin/show`, admin op 10, refusals, audit), the cnc words a service will read at attach, the gauges, the obs event and the alert. One UC flag day: wire `0.8.0` → `0.9.0`, cnc `3.2` → `3.3`, cluster image `1` → `2`.

**Architecture:** Both records are `CLUSTER` frame kinds applied at COMMIT by the `uc2-cluster` agent into `uc_node::cluster_fsm` (the internal state machine that already holds membership, the schedule table and settings), snapshotted into the cluster artifact under `service_id = 255`, so a below-floor joiner holds the pin history before its service attaches. The pin's operator path is the staged-file admin pipeline `settings apply` already uses (`<instance_dir>/upgrade.pending`, SHA-256 digest signed in the admin line's `id ‖ ip ‖ port`), leader-only and single-in-flight with the other kinds. The cluster agent writes each row's newest pin into two new words on the row's cnc status line; the SnapshotReport's verdict is a pure function of the record, computed at read time from FSM state (the state holds the input, never a derived verdict). This plan lands the records, the FSM, the operator path for pins, and the observability; **B2** (service-side install-at-attach + attach refusal + `ULTSNAP2`) and **B3** (node-side hash at stream time, the follower→leader datagram, the leader's report append) build on it. Until B3, a `SnapshotReport` can only be appended by a test.

**Tech Stack:** Rust 1.96 (MSRV 1.89), workspace crates `uc_protocol` / `uc_node` / `uc_log` / `uc_ctl`, `cargo-fuzz` (nightly, outside the workspace), promtool via `scripts/m10_alert_fire.sh`.

**Spec:** `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` — §2.5 (UpgradePin, SnapshotReport), §3 S4 (steps 2–3), §6.5.2 (live nondeterminism: items 4–5), §10 Q7, §11 items 5 and 13 (the FSM/record half of 13 only).

## Global Constraints

Every task's requirements include this section. Values are copied from the spec, `CLAUDE.md` and the code as read on 2026-09-20 at `f63d293`.

- **Flag day, one release (`2.13.0`).** `uc_protocol::version::CURRENT` `0.8.0` → **`0.9.0`**; `uc_protocol::v2::cnc::CNC_V2_VERSION` `(3 << 24) | (2 << 16)` → **`(3 << 24) | (3 << 16)`**; `uc_protocol::v2::cluster_image::CLUSTER_IMAGE_VERSION` `1` → **`2`**, and the decoder ACCEPTS a version-1 image (empty pin and report lists) — the settings v1/v2 precedent: a restarting `2.12.0` node reads its own artifact off disk. A `0.8.0` peer applies a kind-4/5 frame as "undecodable" (`out.push(42)`, `applied` still advances) and silently diverges, which is why this is a wire bump. Stop every node before starting any node.
- **`CLUSTER kind = 4` UpgradePin**, payload **exactly 20 B**: `row u8 @0 ‖ reserved [u8; 3] @1 ‖ from u32 @4 ‖ to u32 @8 ‖ origin u64 @12`, little-endian. Reserved bytes are written as zero and a non-zero one is undecodable (the `read_cluster_prefix` posture). `row < CNC_MAX_SERVICES (8)`; `origin > 0`.
- **`CLUSTER kind = 5` SnapshotReport**, payload `row u8 @0 ‖ count u8 @1 ‖ reserved [u8; 6] @2 ‖ position u64 @8 ‖ count × (node_id u32 ‖ hash u64)`, `1 ≤ count ≤ MAX_MEMBERS (8)` → **16–112 B**, node ids strictly increasing (a canonical encoding: the same set of reports always produces the same bytes). `position > 0`.
- **Refusal codes (plan erratum, see "Errata" below): 52 `pin_row_undeclared`, 53 `pin_from_mismatch`, 54 `pin_no_set`, 55 `pin_not_monotone`, 56 `pin_digest`, 57 `pin_missing`, 58 `pin_decode`, 59 `report_stale`.** The spec said 51–54; 51 is `REASON_SCHEDULE_TOO_LARGE` (`uc_node/src/node.rs:522`), so the band shifts by one, and the three staged-file outcomes 44–46 have (`digest`/`missing`/`decode`) get their own numbers rather than reusing settings'.
- **Replicated vs. door (cluster-FSM spec §4.4, Ruling R24, verbatim in `ClusterFsm::validate_replicated`'s doc):** what decides acceptance at APPLY reads FSM state and constants only. So: **replicated** = 55 (`origin` ≤ the row's newest pin's `origin`), 53 when a pin for the row already exists (`from` ≠ that pin's `to`), 59 (a report whose `position` is below the row's held report). **Door-only, on the leader** = 52 (row not in `[services] names`, read as `self.timers[row].is_some()` — the same source `declared_hashes` reads), 53 when NO pin exists yet (`from` ≠ the row's attached version word `slot.status.version()`), 54 (`origin` ≠ this node's newest COMPLETE set, `self.snapshot_set_position`).
- **Admin op `10 = ADMIN_OP_UPGRADE_PIN`**, staged at `<instance_dir>/upgrade.pending` (`UPGRADE_PENDING_FILE`), `MAX_UPGRADE_PIN_BYTES = 20`, digest via the existing `uc_node::staged_digest` in `(id, ip, port)`, audited as `upgrade_pin`, **leader-only and node-local** (a follower answers `retry`, never forwards), **single-in-flight** on `last_cluster_append > view_position` across all five kinds.
- **cnc words (cnc 3.3):** on the service slot's STATUS line, `+16 upgrade_origin u64` and `+24 pinned_version u64` (low 32 bits = the packed version), **node-written by the cluster agent**, republished from FSM state on every view publish; `0` = no pin. Written version FIRST, origin LAST with `Release`, so a reader that `Acquire`-loads a non-zero origin sees the version that goes with it. Offsets pinned in the existing offset tests in BOTH `uc_protocol/src/v2/cnc.rs` and `uc_log/src/cnc.rs` (the `const _` assertions).
- **Pin history bound: `MAX_PINS_PER_ROW = 4`** — the oldest entry for that row is dropped when a fifth lands. Reports: **the newest per row only** (one entry per row, replaced by a report at a higher-or-equal position).
- **Verdict (pure, deterministic, `uc_protocol::v2::upgrade::verdict`):** `agreed` = every hash equal; `majority_hash` = the hash held by strictly more than `count / 2` reporters, else `None`; `minority` = node ids whose hash ≠ the majority, empty when there is no majority (N = 2 disagreeing: `agreed = false`, no majority, nobody named — the spec's "with N ≥ 3").
- **Retention keeps a pinned origin's artifacts.** `prune_snapshot_dir` never removes `snap-<origin>.*` for any row's current pin — in every row dir and the cluster dir — or B2's attach-time install would find nothing.
- **Apply is sync, deterministic, no I/O; `validate_replicated` reads FSM state only.** No `SystemTime`, no RNG, no filesystem in `ClusterFsm`.
- **Frozen numbers get a test** (the `..._are_frozen` pattern in `uc_protocol/src/v2/cnc.rs` and `frame.rs`): kinds 4/5, op 10, the two cnc offsets, the cnc version, the wire version, `UPGRADE_PIN_LEN`, `SNAPSHOT_REPORT_MAX_LEN`.
- **Metric names are registered in `METRIC_NAMES`** (`uc_node/src/obs/metrics.rs:60-95`; a test enforces the registry). **Every alert rule needs** an entry in `scripts/m10_alert_fire.sh`'s `RULES` table AND a `build_<Name>` in `RULE_BUILDERS` AND a scenario in `uc_node/examples/m10_alerts.rs`, or the script's completeness cross-check fails.
- **`cargo fmt --all -- --check`** is CI's first step; `cargo clippy --workspace --all-targets -- -D warnings` must be clean; `fuzz/` is outside the workspace (needs nightly; `cargo +nightly fuzz build` from `fuzz/` to check it compiles).
- **No `RELEASES.md` entry in this plan** — plan D writes the `2.13.0` release notes before tagging; this plan records what it shipped in the spec's errata section and the cluster-FSM explainer.
- **Scratch under `$HOME/scratch/`, never `/tmp`.** Test instance dirs via the suites' `tempdir()` (`CARGO_TARGET_TMPDIR`).

### Errata against the spec text (decided while planning; record in the spec's "as built" section in Task 10)

1. **Refusal numbers 52–59, not 51–54** (51 was taken; +3 staged-file codes; +1 `report_stale`).
2. **The pin rides a staged file** (`upgrade.pending`), not the admin line: the line carries 10 payload bytes and the record is 20.
3. **cnc words on the status line (+16/+24)**, not "the row's cnc slot line 7": line 7 has exactly one free word (`_pad: [u64; 1]` at +504) and the pin needs two.
4. **`pin_no_set` accepts only this node's NEWEST complete set** (`uc2_snapshot_set_position`), not any retained set: retention is delete-only and an older set can vanish between the check and the commit; the newest cannot, because this plan also makes retention keep pinned origins.
5. **`uc2ctl upgrade pin` takes an optional `--from`**; absent, it reads the row's attached version word off the cnc page and refuses locally if that word is 0.
6. **`SnapshotReport` has one refusal of its own, 59 `report_stale`**, so "accepted" always means "state changed".
7. **`ClusterFsm::VERSION` stays 1.** The image version (2) is what gates compatibility on disk; the row's `VERSION` is compared by nothing that would change behaviour here.

---

## File structure

| file | responsibility |
|---|---|
| `uc_protocol/src/v2/upgrade.rs` (**new**) | the two payload codecs, the list codecs the image embeds, `verdict` |
| `uc_protocol/src/v2/mod.rs` | `pub mod upgrade;` |
| `uc_protocol/src/v2/frame.rs` | `ClusterKind::{UpgradePin = 4, SnapshotReport = 5}` |
| `uc_protocol/src/version.rs` | `CURRENT = 0.9.0` |
| `uc_protocol/src/v2/cnc.rs` | `CNC_V2_VERSION` 3.3, `CNC_SVC_OFF_UPGRADE_ORIGIN/PINNED_VERSION`, `ADMIN_OP_UPGRADE_PIN`, frozen tests |
| `uc_protocol/src/v2/cluster_image.rs` | image v2 (`pins`, `reports` blobs), v1 accepted |
| `uc_log/src/cnc.rs` | `ServiceStatusLine` gains the two words + accessors |
| `uc_node/src/cluster_fsm.rs` | state, commands, refusals, validate, apply, query, freeze/install, `ClusterView` |
| `uc_node/src/cluster_agent.rs` | pin words on publish, `snapshot_hash_diverged`, offline readers |
| `uc_node/src/node.rs` | reasons 52–59, `apply_upgrade_pin`, dispatch, retention keep-set |
| `uc_node/src/audit.rs` | `op_name(10) = "upgrade_pin"` |
| `uc_node/src/obs/metrics.rs` | three gauges |
| `uc_node/examples/m10_alerts.rs` | `snapshot_hash_diverged` scenario |
| `uc_ctl/src/upgrade.rs` (**new**), `uc_ctl/src/main.rs` | `upgrade pin` / `upgrade show`, reason strings, `status` words |
| `packaging/prometheus/uc2-alerts.yml`, `scripts/m10_alert_fire.sh` | `Uc2SnapshotHashDiverged` |
| `fuzz/fuzz_targets/uc_protocol_cluster_frame.rs`, `fuzz/README.md` | the two new arms |
| docs | `wire-protocol.md`, `cnc-page.md`, `uc2ctl.md`, `limits.md`, `semver-policy.md`, `upgrade-a-cluster.md`, `monitor-a-cluster.md`, `uc2-runbook.md`, `VERIFICATION.md`, `uc2-cluster-fsm-explained.md`, the spec's errata |

---

### Task 1: `uc_protocol::v2::upgrade` — the two payload codecs and the verdict

**Files:**
- Create: `uc_protocol/src/v2/upgrade.rs`
- Modify: `uc_protocol/src/v2/mod.rs` (add `pub mod upgrade;` beside `pub mod settings;`)
- Test: inline `#[cfg(test)] mod tests` in the new file

**Interfaces:**
- Consumes: `crate::v2::cnc::CNC_MAX_SERVICES` (= 8), `crate::v2::config::MAX_MEMBERS` (= 8, `uc_protocol/src/v2/config.rs:16`).
- Produces (later tasks rely on these exact names):
  - `pub const UPGRADE_PIN_LEN: usize = 20;`
  - `pub struct UpgradePin { pub row: u8, pub from: u32, pub to: u32, pub origin: u64 }` (derives `Debug, Clone, Copy, PartialEq, Eq`)
  - `pub fn encode_upgrade_pin(p: &UpgradePin, out: &mut Vec<u8>)`
  - `pub fn decode_upgrade_pin(buf: &[u8]) -> Option<UpgradePin>`
  - `pub const SNAPSHOT_REPORT_HEADER_LEN: usize = 16; pub const SNAPSHOT_REPORT_ENTRY_LEN: usize = 12; pub const MAX_SNAPSHOT_REPORT_NODES: usize = MAX_MEMBERS; pub const SNAPSHOT_REPORT_MAX_LEN: usize = 112;`
  - `pub struct SnapshotReport { pub row: u8, pub position: u64, pub hashes: Vec<(u32, u64)> }` (derives `Debug, Clone, PartialEq, Eq`)
  - `pub fn encode_snapshot_report(r: &SnapshotReport, out: &mut Vec<u8>) -> Option<()>` (`None` if `hashes` is empty, longer than 8, or not strictly increasing by node id)
  - `pub fn decode_snapshot_report(buf: &[u8]) -> Option<SnapshotReport>`
  - `pub struct Verdict { pub agreed: bool, pub majority_hash: Option<u64>, pub minority: Vec<u32> }` + `pub fn verdict(r: &SnapshotReport) -> Verdict`
  - list codecs the image embeds: `pub fn encode_pin_list(pins: &[UpgradePin], out: &mut Vec<u8>)`, `pub fn decode_pin_list(buf: &[u8]) -> Option<Vec<UpgradePin>>` (exact multiple of 20), `pub fn encode_report_list(reports: &[SnapshotReport], out: &mut Vec<u8>) -> Option<()>`, `pub fn decode_report_list(buf: &[u8]) -> Option<Vec<SnapshotReport>>` (each entry `u32 len ‖ report`, exact framing, no trailing bytes).

- [ ] **Step 1: Write the failing tests**

Create `uc_protocol/src/v2/upgrade.rs` with ONLY the test module first (the items it names do not exist yet):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> UpgradePin {
        UpgradePin {
            row: 3,
            from: 0x0100_0000,
            to: 0x0101_0000,
            origin: 8192,
        }
    }

    #[test]
    fn pin_layout_is_frozen() {
        let mut b = Vec::new();
        encode_upgrade_pin(&pin(), &mut b);
        assert_eq!(b.len(), UPGRADE_PIN_LEN);
        assert_eq!(UPGRADE_PIN_LEN, 20);
        assert_eq!(b[0], 3, "row @0");
        assert_eq!(&b[1..4], &[0, 0, 0], "reserved @1 written as zero");
        assert_eq!(&b[4..8], &0x0100_0000u32.to_le_bytes(), "from @4");
        assert_eq!(&b[8..12], &0x0101_0000u32.to_le_bytes(), "to @8");
        assert_eq!(&b[12..20], &8192u64.to_le_bytes(), "origin @12");
        assert_eq!(decode_upgrade_pin(&b), Some(pin()));
    }

    #[test]
    fn pin_decode_is_exact_and_refuses_reserved_row_and_origin() {
        let mut b = Vec::new();
        encode_upgrade_pin(&pin(), &mut b);
        assert_eq!(decode_upgrade_pin(&b[..19]), None, "short");
        let mut long = b.clone();
        long.push(0);
        assert_eq!(decode_upgrade_pin(&long), None, "trailing byte");
        let mut r = b.clone();
        r[2] = 1;
        assert_eq!(decode_upgrade_pin(&r), None, "non-zero reserved");
        let mut row = b.clone();
        row[0] = 8;
        assert_eq!(decode_upgrade_pin(&row), None, "row >= CNC_MAX_SERVICES");
        let mut zero = b.clone();
        zero[12..20].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(decode_upgrade_pin(&zero), None, "origin 0 is 'no pin', never a record");
    }

    fn report(hashes: &[(u32, u64)]) -> SnapshotReport {
        SnapshotReport {
            row: 0,
            position: 4096,
            hashes: hashes.to_vec(),
        }
    }

    #[test]
    fn report_layout_is_frozen() {
        let r = report(&[(0, 0xAA), (1, 0xAA), (2, 0xBB)]);
        let mut b = Vec::new();
        assert_eq!(encode_snapshot_report(&r, &mut b), Some(()));
        assert_eq!(b.len(), SNAPSHOT_REPORT_HEADER_LEN + 3 * SNAPSHOT_REPORT_ENTRY_LEN);
        assert_eq!((SNAPSHOT_REPORT_HEADER_LEN, SNAPSHOT_REPORT_ENTRY_LEN), (16, 12));
        assert_eq!(SNAPSHOT_REPORT_MAX_LEN, 112);
        assert_eq!(MAX_SNAPSHOT_REPORT_NODES, 8);
        assert_eq!(b[0], 0, "row @0");
        assert_eq!(b[1], 3, "count @1");
        assert_eq!(&b[2..8], &[0; 6], "reserved @2");
        assert_eq!(&b[8..16], &4096u64.to_le_bytes(), "position @8");
        assert_eq!(&b[16..20], &0u32.to_le_bytes(), "node_id of entry 0");
        assert_eq!(&b[20..28], &0xAAu64.to_le_bytes(), "hash of entry 0");
        assert_eq!(decode_snapshot_report(&b), Some(r));
    }

    #[test]
    fn report_encoding_is_canonical() {
        let mut b = Vec::new();
        assert_eq!(
            encode_snapshot_report(&report(&[(1, 1), (0, 1)]), &mut b),
            None,
            "node ids must be strictly increasing"
        );
        assert_eq!(encode_snapshot_report(&report(&[(1, 1), (1, 2)]), &mut b), None, "duplicate id");
        assert_eq!(encode_snapshot_report(&report(&[]), &mut b), None, "empty");
        let nine: Vec<(u32, u64)> = (0..9).map(|i| (i, 7)).collect();
        assert_eq!(encode_snapshot_report(&report(&nine), &mut b), None, "count > MAX_MEMBERS");
        // The decoder enforces the same rules on the wire.
        let mut ok = Vec::new();
        encode_snapshot_report(&report(&[(0, 1), (2, 1)]), &mut ok).unwrap();
        let mut swapped = ok.clone();
        swapped[16..20].copy_from_slice(&5u32.to_le_bytes()); // ids now 5, 2
        assert_eq!(decode_snapshot_report(&swapped), None);
        let mut zero_pos = ok.clone();
        zero_pos[8..16].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(decode_snapshot_report(&zero_pos), None);
        let mut short = ok.clone();
        short.pop();
        assert_eq!(decode_snapshot_report(&short), None);
        let mut count = ok.clone();
        count[1] = 1;
        assert_eq!(decode_snapshot_report(&count), None, "count disagrees with length");
    }

    #[test]
    fn verdict_all_equal_majority_and_tie() {
        let v = verdict(&report(&[(0, 9), (1, 9), (2, 9)]));
        assert_eq!((v.agreed, v.majority_hash, v.minority), (true, Some(9), vec![]));
        let v = verdict(&report(&[(0, 9), (1, 9), (2, 4)]));
        assert_eq!((v.agreed, v.majority_hash, v.minority), (false, Some(9), vec![2]));
        let v = verdict(&report(&[(0, 9), (1, 4)]));
        assert_eq!((v.agreed, v.majority_hash, v.minority), (false, None, vec![]));
        let v = verdict(&report(&[(0, 1), (1, 2), (2, 3), (3, 3)]));
        assert_eq!((v.agreed, v.majority_hash, v.minority), (false, None, vec![]), "2 of 4 is not a majority");
        let v = verdict(&report(&[(0, 5)]));
        assert_eq!((v.agreed, v.majority_hash, v.minority), (true, Some(5), vec![]));
    }

    #[test]
    fn lists_roundtrip_with_exact_framing() {
        let pins = vec![pin(), UpgradePin { row: 0, from: 1, to: 2, origin: 100 }];
        let mut b = Vec::new();
        encode_pin_list(&pins, &mut b);
        assert_eq!(b.len(), 2 * UPGRADE_PIN_LEN);
        assert_eq!(decode_pin_list(&b), Some(pins));
        assert_eq!(decode_pin_list(&b[..39]), None, "not a multiple of 20");
        assert_eq!(decode_pin_list(&[]), Some(vec![]));

        let reports = vec![report(&[(0, 1), (1, 1)]), SnapshotReport { row: 1, position: 8192, hashes: vec![(0, 2)] }];
        let mut b = Vec::new();
        assert_eq!(encode_report_list(&reports, &mut b), Some(()));
        assert_eq!(decode_report_list(&b), Some(reports));
        b.push(0);
        assert_eq!(decode_report_list(&b), None, "trailing byte");
        assert_eq!(decode_report_list(&[]), Some(vec![]));
        assert_eq!(decode_report_list(&[9, 0, 0, 0, 1]), None, "length prefix past the end");
    }
}
```

Add `pub mod upgrade;` to `uc_protocol/src/v2/mod.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_protocol upgrade::tests 2>&1 | tail -20`
Expected: compile errors — `UpgradePin`, `encode_upgrade_pin`, … not found.

- [ ] **Step 3: Write the implementation** (above the test module)

```rust
//! FSM upgrade lifecycle (spec §2.5, §6.5.2): the two `CLUSTER` payloads
//! the cluster FSM applies at commit, the list codecs the cluster IMAGE
//! embeds them in, and the pure `verdict` over a `SnapshotReport`.
//!
//! `core`-friendly like its siblings (`settings`, `schedule`): no I/O, no
//! allocation beyond the `Vec`s the callers hand in.

use crate::v2::cnc::CNC_MAX_SERVICES;
use crate::v2::config::MAX_MEMBERS;

/// `row u8 @0 ‖ reserved [u8; 3] @1 ‖ from u32 @4 ‖ to u32 @8 ‖ origin u64
/// @12` — exactly 20 bytes, `CLUSTER kind = 4`.
pub const UPGRADE_PIN_LEN: usize = 20;

/// "At position `origin`, row `row` went from `from` to `to`" — an EVENT,
/// not a tunable (spec §2.5): the sequence is what matters, which is why
/// it is its own kind and not a Settings field. `from`/`to` are packed
/// versions (`crate::identity::pack_version`); `origin` is the frame-END
/// position of the coordinated instant whose complete set the row will
/// install at its next attach — never 0, which is the cnc words' "no pin".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradePin {
    pub row: u8,
    pub from: u32,
    pub to: u32,
    pub origin: u64,
}

pub fn encode_upgrade_pin(p: &UpgradePin, out: &mut Vec<u8>) {
    out.push(p.row);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&p.from.to_le_bytes());
    out.extend_from_slice(&p.to.to_le_bytes());
    out.extend_from_slice(&p.origin.to_le_bytes());
}

/// Exact-length, reserved-zero, `row < CNC_MAX_SERVICES`, `origin > 0`.
pub fn decode_upgrade_pin(buf: &[u8]) -> Option<UpgradePin> {
    if buf.len() != UPGRADE_PIN_LEN || buf[1..4] != [0, 0, 0] {
        return None;
    }
    let row = buf[0];
    if row as usize >= CNC_MAX_SERVICES {
        return None;
    }
    let from = u32::from_le_bytes(buf[4..8].try_into().ok()?);
    let to = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    let origin = u64::from_le_bytes(buf[12..20].try_into().ok()?);
    if origin == 0 {
        return None;
    }
    Some(UpgradePin {
        row,
        from,
        to,
        origin,
    })
}

/// `row u8 @0 ‖ count u8 @1 ‖ reserved [u8; 6] @2 ‖ position u64 @8`.
pub const SNAPSHOT_REPORT_HEADER_LEN: usize = 16;
/// `node_id u32 ‖ hash u64`.
pub const SNAPSHOT_REPORT_ENTRY_LEN: usize = 12;
/// One entry per member at most — the leader collects one hash per node.
pub const MAX_SNAPSHOT_REPORT_NODES: usize = MAX_MEMBERS;
/// 16 + 8 × 12: inside the 1312 B crypto-on ceiling at the baseline rung.
pub const SNAPSHOT_REPORT_MAX_LEN: usize =
    SNAPSHOT_REPORT_HEADER_LEN + MAX_SNAPSHOT_REPORT_NODES * SNAPSHOT_REPORT_ENTRY_LEN;

/// The node-side artifact hashes the leader collected for `(row, position)`
/// — `CLUSTER kind = 5` (spec §6.5.2 item 3). The verdict is NOT a field:
/// it is [`verdict`], a pure function every reader recomputes, so the
/// state holds only what was observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotReport {
    pub row: u8,
    pub position: u64,
    /// `(node_id, hash)`, strictly increasing by node id — the canonical
    /// order, so identical observations always encode identically.
    pub hashes: Vec<(u32, u64)>,
}

fn ids_strictly_increasing(hashes: &[(u32, u64)]) -> bool {
    hashes.windows(2).all(|w| w[0].0 < w[1].0)
}

/// `None` when the report is not encodable: empty, more than
/// [`MAX_SNAPSHOT_REPORT_NODES`] entries, ids not strictly increasing,
/// `row` out of range, or `position == 0`.
pub fn encode_snapshot_report(r: &SnapshotReport, out: &mut Vec<u8>) -> Option<()> {
    let n = r.hashes.len();
    if n == 0
        || n > MAX_SNAPSHOT_REPORT_NODES
        || !ids_strictly_increasing(&r.hashes)
        || r.row as usize >= CNC_MAX_SERVICES
        || r.position == 0
    {
        return None;
    }
    out.push(r.row);
    out.push(n as u8);
    out.extend_from_slice(&[0; 6]);
    out.extend_from_slice(&r.position.to_le_bytes());
    for (id, h) in &r.hashes {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&h.to_le_bytes());
    }
    Some(())
}

/// Exact framing: `count` must match the length, reserved must be zero,
/// and every rule `encode_snapshot_report` enforces holds on read too.
pub fn decode_snapshot_report(buf: &[u8]) -> Option<SnapshotReport> {
    if buf.len() < SNAPSHOT_REPORT_HEADER_LEN || buf[2..8] != [0; 6] {
        return None;
    }
    let row = buf[0];
    let n = buf[1] as usize;
    if row as usize >= CNC_MAX_SERVICES
        || n == 0
        || n > MAX_SNAPSHOT_REPORT_NODES
        || buf.len() != SNAPSHOT_REPORT_HEADER_LEN + n * SNAPSHOT_REPORT_ENTRY_LEN
    {
        return None;
    }
    let position = u64::from_le_bytes(buf[8..16].try_into().ok()?);
    if position == 0 {
        return None;
    }
    let mut hashes = Vec::with_capacity(n);
    let mut o = SNAPSHOT_REPORT_HEADER_LEN;
    for _ in 0..n {
        let id = u32::from_le_bytes(buf[o..o + 4].try_into().ok()?);
        let h = u64::from_le_bytes(buf[o + 4..o + 12].try_into().ok()?);
        hashes.push((id, h));
        o += SNAPSHOT_REPORT_ENTRY_LEN;
    }
    if !ids_strictly_increasing(&hashes) {
        return None;
    }
    Some(SnapshotReport {
        row,
        position,
        hashes,
    })
}

/// The deterministic reading of one report (spec §6.5.2 item 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// Every reporter's hash is the same.
    pub agreed: bool,
    /// The hash strictly more than half the reporters hold, if any.
    pub majority_hash: Option<u64>,
    /// Node ids whose hash differs from the majority's; empty when there
    /// is no majority to differ from (two nodes disagreeing names nobody).
    pub minority: Vec<u32>,
}

pub fn verdict(r: &SnapshotReport) -> Verdict {
    let n = r.hashes.len();
    let agreed = r.hashes.windows(2).all(|w| w[0].1 == w[1].1);
    let majority_hash = r
        .hashes
        .iter()
        .map(|(_, h)| *h)
        .find(|h| r.hashes.iter().filter(|(_, x)| x == h).count() * 2 > n);
    let minority = match majority_hash {
        Some(m) => r
            .hashes
            .iter()
            .filter(|(_, h)| *h != m)
            .map(|(id, _)| *id)
            .collect(),
        None => Vec::new(),
    };
    Verdict {
        agreed,
        majority_hash,
        minority,
    }
}

/// The image's pin blob: `count × UPGRADE_PIN_LEN`, in apply order.
pub fn encode_pin_list(pins: &[UpgradePin], out: &mut Vec<u8>) {
    for p in pins {
        encode_upgrade_pin(p, out);
    }
}

pub fn decode_pin_list(buf: &[u8]) -> Option<Vec<UpgradePin>> {
    if buf.len() % UPGRADE_PIN_LEN != 0 {
        return None;
    }
    buf.chunks_exact(UPGRADE_PIN_LEN)
        .map(decode_upgrade_pin)
        .collect()
}

/// The image's report blob: each entry `len u32 ‖ report`, exact framing.
pub fn encode_report_list(reports: &[SnapshotReport], out: &mut Vec<u8>) -> Option<()> {
    for r in reports {
        let mut b = Vec::with_capacity(SNAPSHOT_REPORT_MAX_LEN);
        encode_snapshot_report(r, &mut b)?;
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(&b);
    }
    Some(())
}

pub fn decode_report_list(buf: &[u8]) -> Option<Vec<SnapshotReport>> {
    let mut out = Vec::new();
    let mut o = 0;
    while o < buf.len() {
        let len = u32::from_le_bytes(buf.get(o..o + 4)?.try_into().ok()?) as usize;
        o += 4;
        let end = o.checked_add(len)?;
        out.push(decode_snapshot_report(buf.get(o..end)?)?);
        o = end;
    }
    Some(out)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_protocol upgrade::tests 2>&1 | tail -12`
Expected: `test result: ok. 6 passed`

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p uc_protocol --all-targets -- -D warnings`
Expected: clean.

```bash
git add uc_protocol/src/v2/upgrade.rs uc_protocol/src/v2/mod.rs
git commit -m "uc_protocol: UpgradePin and SnapshotReport payload codecs, list codecs and the verdict (plan B1 T1)"
```

---

### Task 2: `ClusterKind` 4/5, wire `0.9.0`, admin op 10, the fuzz arms

**Files:**
- Modify: `uc_protocol/src/v2/frame.rs:72-101` (`ClusterKind`, `from_u8`, the doc block above it), `uc_protocol/src/version.rs:81,119`, `uc_protocol/src/v2/cnc.rs:160` (after `ADMIN_OP_SNAPSHOT_FETCH`) and the frozen-numbers test near line 830
- Modify: `fuzz/fuzz_targets/uc_protocol_cluster_frame.rs`, `fuzz/README.md:168`, `docs/VERIFICATION.md:746`
- Test: `uc_protocol/src/v2/frame.rs` tests, `uc_protocol/src/version.rs` tests

**Interfaces:**
- Produces: `ClusterKind::UpgradePin` (`= 4`), `ClusterKind::SnapshotReport` (`= 5`); `uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN: u32 = 10`; `uc_protocol::version::CURRENT == 0.9.0`.

- [ ] **Step 1: Write the failing tests**

In `uc_protocol/src/v2/frame.rs`'s test module, beside `cluster_prefix_roundtrips_and_reserved_bytes_are_zero`:

```rust
    /// FSM upgrade lifecycle (spec §2.5, §6.5.2): two more kinds, FROZEN.
    #[test]
    fn upgrade_cluster_kinds_are_frozen() {
        assert_eq!(ClusterKind::UpgradePin as u8, 4);
        assert_eq!(ClusterKind::SnapshotReport as u8, 5);
        assert_eq!(ClusterKind::from_u8(4), Some(ClusterKind::UpgradePin));
        assert_eq!(ClusterKind::from_u8(5), Some(ClusterKind::SnapshotReport));
        assert_eq!(ClusterKind::from_u8(6), None);
        let mut b = vec![0u8; CLUSTER_BODY_PREFIX_LEN + 3];
        write_cluster_prefix(&mut b, ClusterKind::SnapshotReport);
        assert_eq!(read_cluster_prefix(&b).map(|(k, p)| (k, p.len())), Some((ClusterKind::SnapshotReport, 3)));
    }
```

In `uc_protocol/src/version.rs`, change the existing assertion at line 119 to `assert_eq!(CURRENT, ProtocolVersion::new(0, 9, 0));`.

In `uc_protocol/src/v2/cnc.rs`'s test `learner_flag_and_capability_bit_and_snapshot_ops_are_frozen` (near line 830), add:

```rust
        // FSM upgrade lifecycle (plan B1): `uc2ctl upgrade pin`.
        assert_eq!(ADMIN_OP_UPGRADE_PIN, 10);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_protocol -- frozen 2>&1 | tail -15`
Expected: compile error on `ClusterKind::UpgradePin` / `ADMIN_OP_UPGRADE_PIN`; the version assertion fails with `0.8.0`.

- [ ] **Step 3: Implement**

`uc_protocol/src/v2/frame.rs` — extend the enum and `from_u8` (keep every existing doc line; append to the doc block a sentence: "`4` = UpgradePin and `5` = SnapshotReport (FSM upgrade lifecycle, spec §2.5/§6.5.2, wire 0.9.0); their payloads are `crate::v2::upgrade`'s."):

```rust
pub enum ClusterKind {
    Membership = 1,
    ScheduleTable = 2,
    Settings = 3,
    UpgradePin = 4,
    SnapshotReport = 5,
}
```

```rust
    pub const fn from_u8(b: u8) -> Option<ClusterKind> {
        match b {
            1 => Some(ClusterKind::Membership),
            2 => Some(ClusterKind::ScheduleTable),
            3 => Some(ClusterKind::Settings),
            4 => Some(ClusterKind::UpgradePin),
            5 => Some(ClusterKind::SnapshotReport),
            _ => None,
        }
    }
```

`uc_protocol/src/version.rs:81`: `pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 9, 0);` and update the doc comment above it in the same style as the `0.8.0` entry (one line: "`0.9.0` — `CLUSTER` kinds 4 (UpgradePin) and 5 (SnapshotReport); cnc 3.3. A 0.8.0 peer applies either as undecodable and silently diverges, hence the bump.").

`uc_protocol/src/v2/cnc.rs`, after `ADMIN_OP_SNAPSHOT_FETCH` (line 160):

```rust
/// FSM upgrade lifecycle (spec §2.5, plan B1): `uc2ctl upgrade pin`. The
/// 20-byte record is staged at `<instance_dir>/upgrade.pending` and the
/// first ten bytes of its SHA-256 ride `id ‖ ip ‖ port`, exactly as ops 6
/// and 7 do. Leader-only, node-local, single-in-flight.
pub const ADMIN_OP_UPGRADE_PIN: u32 = 10;
```

`fuzz/fuzz_targets/uc_protocol_cluster_frame.rs` — add the import `use uc_protocol::v2::upgrade::{decode_snapshot_report, decode_upgrade_pin};` and two arms:

```rust
            ClusterKind::UpgradePin => {
                let _ = decode_upgrade_pin(payload);
            }
            ClusterKind::SnapshotReport => {
                let _ = decode_snapshot_report(payload);
            }
```

`fuzz/README.md:168` and `docs/VERIFICATION.md:746`: extend the `uc_protocol_cluster_frame` row's decoder list with `upgrade::decode_upgrade_pin`, `upgrade::decode_snapshot_report` (keep the row's shape; add the two names after `settings::decode_settings`).

- [ ] **Step 4: Run the tests, build the fuzz crate**

Run: `cargo test -p uc_protocol 2>&1 | tail -5` — Expected: all pass (the workspace compiles again once every `match kind` in `uc_node` is extended — **it is not yet**: `cargo build --workspace` will fail on `uc_node/src/cluster_fsm.rs`'s non-exhaustive matches until Task 4; that is expected and is why Tasks 2–4 are committed together's neighbours. Run only `-p uc_protocol` here.)

Run: `(cd fuzz && cargo +nightly fuzz build uc_protocol_cluster_frame 2>&1 | tail -3)` — Expected: `Finished`.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/frame.rs uc_protocol/src/version.rs uc_protocol/src/v2/cnc.rs fuzz/fuzz_targets/uc_protocol_cluster_frame.rs fuzz/README.md docs/VERIFICATION.md
git commit -m "uc_protocol: CLUSTER kinds 4/5, admin op 10, wire 0.9.0; fuzz arms (plan B1 T2)"
```

---

### Task 3: Cluster image v2 — pins and reports ride the artifact, v1 still decodes

**Files:**
- Modify: `uc_protocol/src/v2/cluster_image.rs` (module doc, `CLUSTER_IMAGE_VERSION`, `ClusterImageParts`, `encode_cluster_image`, `decode_cluster_image`, tests)
- Test: inline

**Interfaces:**
- Consumes: `crate::v2::settings::{SETTINGS_LEN, SETTINGS_LEN_V1}` (already imported).
- Produces: `ClusterImageParts` gains `pub pins: &'a [u8]` and `pub reports: &'a [u8]` (the blobs from Task 1's list codecs; empty for a v1 image). `CLUSTER_IMAGE_VERSION = 2`. Layout v2: `magic ‖ version=2 u32 ‖ applied u64 ‖ table_position u64 ‖ settings_position u64 ‖ membership (u32 len ‖ bytes) ‖ table (u32 len ‖ bytes) ‖ settings record (self-versioned: 29 B if its version word is 1, 33 B if 2) ‖ pins (u32 len ‖ bytes) ‖ reports (u32 len ‖ bytes) ‖ crc32`. Layout v1 is unchanged and accepted.

- [ ] **Step 1: Write the failing tests**

Keep the existing golden v1 test (it must keep passing — that is the v1-accepted requirement; it asserts `decode_cluster_image(V1_GOLDEN)` returns the parts: extend its assertions with `assert!(parts.pins.is_empty() && parts.reports.is_empty())`). Add:

```rust
    #[test]
    fn v2_roundtrips_pins_and_reports_and_is_exact() {
        let pins = [1u8; 40]; // two 20-byte pin records' worth of bytes: the leaf does not decode them
        let reports = [2u8; 7];
        let p = ClusterImageParts {
            applied: 500,
            table_position: 0,
            settings_position: 400,
            membership: &[9, 9],
            table: &[],
            settings: &V2_SETTINGS,
            pins: &pins,
            reports: &reports,
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        assert_eq!(&img[8..12], &2u32.to_le_bytes(), "version 2");
        let d = decode_cluster_image(&img).unwrap();
        assert_eq!((d.applied, d.settings_position, d.membership, d.pins, d.reports), (500, 400, &[9u8, 9][..], &pins[..], &reports[..]));
        // Exact framing: a pins length that runs into the reports, or past
        // the CRC, is refused; so is a byte after the reports blob.
        let mut bad = img.clone();
        let pins_len_off = 8 + 4 + 24 + 4 + 2 + 4 + 0 + SETTINGS_LEN;
        bad[pins_len_off..pins_len_off + 4].copy_from_slice(&41u32.to_le_bytes());
        fix_crc(&mut bad);
        assert!(decode_cluster_image(&bad).is_none());
        let mut trailing = img.clone();
        let l = trailing.len();
        trailing.insert(l - 4, 0);
        fix_crc(&mut trailing);
        assert!(decode_cluster_image(&trailing).is_none());
    }

    #[test]
    fn v2_carries_a_v1_settings_record_by_its_own_version_word() {
        // A 2.11.0 settings record (29 B) inside a v2 image: the decoder
        // sizes the record from its version word, never from "the rest".
        let p = ClusterImageParts {
            applied: 1,
            table_position: 0,
            settings_position: 0,
            membership: &[],
            table: &[],
            settings: &V1_SETTINGS,
            pins: &[],
            reports: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        let d = decode_cluster_image(&img).unwrap();
        assert_eq!(d.settings, &V1_SETTINGS[..]);
    }

    /// Recompute the trailing CRC after a deliberate mutation.
    fn fix_crc(img: &mut Vec<u8>) {
        let l = img.len();
        let crc = crc32fast::hash(&img[..l - 4]);
        img[l - 4..].copy_from_slice(&crc.to_le_bytes());
    }
```

Define `V2_SETTINGS: [u8; SETTINGS_LEN]` as `encode_settings(&Settings::genesis_default())`'s bytes (call the encoder in a `fn v2_settings() -> Vec<u8>` helper if a const is awkward — the existing golden test already has a v1 settings byte string to reuse for `V1_SETTINGS`; read the file's test module and reuse its constants rather than inventing new ones).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_protocol cluster_image 2>&1 | tail -10`
Expected: compile error — `ClusterImageParts` has no field `pins`.

- [ ] **Step 3: Implement**

`CLUSTER_IMAGE_VERSION = 2` (update its doc: "bumped to 2 by plan B1 for the pin and report blobs; a version-1 image is still ACCEPTED on read, with both blobs empty — the settings v1/v2 precedent, since a restarting 2.12.0 node reads its own artifact").

`ClusterImageParts` gains `pub pins: &'a [u8], pub reports: &'a [u8]`.

`encode_cluster_image` — after `out.extend_from_slice(p.settings);` and before the CRC:

```rust
    let pins_len = payload_len_prefix(p.pins.len())?;
    let reports_len = payload_len_prefix(p.reports.len())?;
    out.extend_from_slice(&pins_len.to_le_bytes());
    out.extend_from_slice(p.pins);
    out.extend_from_slice(&reports_len.to_le_bytes());
    out.extend_from_slice(p.reports);
```

`decode_cluster_image` — replace the version check and the settings tail:

```rust
    let version = u32_at(o)?;
    if version != 1 && version != CLUSTER_IMAGE_VERSION {
        return None;
    }
    o += 4;
    ... (applied, table_position, settings_position, membership, table unchanged) ...
    let (settings, pins, reports) = if version == 1 {
        // 2.11.0/2.12.0 layout: the remainder is exactly one settings record.
        let rest = body.len().checked_sub(o)?;
        if rest != SETTINGS_LEN && rest != SETTINGS_LEN_V1 {
            return None;
        }
        (&body[o..], &body[body.len()..], &body[body.len()..])
    } else {
        // v2: the settings record is sized by ITS OWN version word (the
        // record is exact-length per version), then two length-prefixed
        // blobs, then nothing.
        let sl = match u32_at(o)? {
            1 => SETTINGS_LEN_V1,
            2 => SETTINGS_LEN,
            _ => return None,
        };
        let settings = o.checked_add(sl).and_then(|end| body.get(o..end))?;
        o += sl;
        let pl = u32_at(o)? as usize;
        o += 4;
        let pins = o.checked_add(pl).and_then(|end| body.get(o..end))?;
        o += pl;
        let rl = u32_at(o)? as usize;
        o += 4;
        let reports = o.checked_add(rl).and_then(|end| body.get(o..end))?;
        o += rl;
        if o != body.len() {
            return None;
        }
        (settings, pins, reports)
    };
    Some(ClusterImageParts { applied, table_position, settings_position, membership, table, settings, pins, reports })
```

Update the module doc's layout line and the doc on `MIN_IMAGE_LEN` (v1's minimum is unchanged; a v2 image is at least 8 bytes longer — state it, do not change the constant, since the constant guards the v1 header read and both versions share it).

Update every construction of `ClusterImageParts` in this file's tests with `pins: &[], reports: &[]`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_protocol cluster_image 2>&1 | tail -8`
Expected: all pass, including the untouched v1 golden.

Run: `(cd fuzz && cargo +nightly fuzz build uc_protocol_cluster_image 2>&1 | tail -2)` — Expected: `Finished` (the target re-encodes what it decoded; it needs no source change, but confirm it builds).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cluster_image.rs
git commit -m "uc_protocol: cluster image v2 carries the pin and report blobs; v1 accepted on read (plan B1 T3)"
```

---

### Task 4: The cluster FSM — state, commands, refusals, apply, image

**Files:**
- Modify: `uc_node/src/cluster_fsm.rs` (`ClusterState` 72-140, `ClusterCommand` 142, `ClusterRefusal` 149-165, `validate_replicated` 253, `decode_command`/`encode_command` ~310-340, `apply` 349, `query`, `freeze` 418, `install_snapshot` 449, `ClusterView`/`ClusterViewInner` 489-600, tests)
- Test: inline (`mod tests` at the bottom, fixtures `genesis()`, `fsm()`, `body()`)

**Interfaces:**
- Consumes: Task 1's `uc_protocol::v2::upgrade::*`, Task 2's `ClusterKind::{UpgradePin, SnapshotReport}`, Task 3's `ClusterImageParts { pins, reports }`.
- Produces:
  - `pub const MAX_PINS_PER_ROW: usize = 4;`
  - `ClusterState { …, pub pins: Vec<UpgradePin>, pub reports: Vec<SnapshotReport> }`; `ClusterState::pin_for(&self, row: u8) -> Option<&UpgradePin>` (newest for the row); `ClusterState::report_for(&self, row: u8) -> Option<&SnapshotReport>`.
  - `ClusterCommand::UpgradePin(UpgradePin)`, `ClusterCommand::SnapshotReport(SnapshotReport)`.
  - `ClusterRefusal::{PinFromMismatch, PinNotMonotone, ReportStale}` with `reason_code()` 53 / 55 / 59.
  - `query`: `Some(4)` → `encode_pin_list(state.pins)`; `Some(5)` → `encode_report_list(state.reports)`.
  - `ClusterViewInner { …, pub pins: Vec<UpgradePin>, pub reports: Vec<SnapshotReport> }`, copied by `publish`, read back by `to_state`.

- [ ] **Step 1: Write the failing tests** (append to the existing `mod tests`)

```rust
    fn pin(row: u8, from: u32, to: u32, origin: u64) -> ClusterCommand {
        ClusterCommand::UpgradePin(UpgradePin { row, from, to, origin })
    }
    fn report(row: u8, position: u64, hashes: &[(u32, u64)]) -> ClusterCommand {
        ClusterCommand::SnapshotReport(SnapshotReport { row, position, hashes: hashes.to_vec() })
    }
    fn apply_at(f: &mut ClusterFsm, pos: u64, cmd: &ClusterCommand) -> u8 {
        let mut out = Vec::new();
        let mut ctx = ApplyCtx::new(pos, ClusterFsm::IDENTITY);
        f.apply(&mut ctx, &body(cmd), &mut out);
        out[0]
    }

    #[test]
    fn a_pin_is_applied_and_the_newest_per_row_is_found() {
        let mut f = fsm();
        assert_eq!(apply_at(&mut f, 100, &pin(0, 1, 2, 50)), 0);
        assert_eq!(apply_at(&mut f, 200, &pin(1, 7, 8, 150)), 0);
        assert_eq!(apply_at(&mut f, 300, &pin(0, 2, 3, 250)), 0);
        assert_eq!(f.state().pin_for(0).map(|p| (p.from, p.to, p.origin)), Some((2, 3, 250)));
        assert_eq!(f.state().pin_for(1).map(|p| p.origin), Some(150));
        assert_eq!(f.state().pin_for(2), None);
        assert_eq!(f.state().pins.len(), 3, "history is kept in apply order");
        assert_eq!(f.state().applied, 300);
    }

    #[test]
    fn pin_refusals_are_replicated_state_only() {
        let mut f = fsm();
        assert_eq!(apply_at(&mut f, 100, &pin(0, 1, 2, 50)), 0);
        // 55: origin not above the row's current pin (equal, then below).
        assert_eq!(apply_at(&mut f, 200, &pin(0, 2, 3, 50)), 55);
        assert_eq!(apply_at(&mut f, 300, &pin(0, 2, 3, 40)), 55);
        // 53: `from` is not what the row's pin says it is at.
        assert_eq!(apply_at(&mut f, 400, &pin(0, 1, 3, 90)), 53);
        // A row with NO pin accepts any `from` here — that half of 53 is
        // the leader's door check against the attached version word.
        assert_eq!(apply_at(&mut f, 500, &pin(1, 42, 43, 90)), 0);
        assert_eq!(f.state().applied, 500, "a refusal still advances applied");
        assert_eq!(f.state().pin_for(0).map(|p| p.to), Some(2), "nothing changed on refusal");
    }

    #[test]
    fn pin_history_is_bounded_per_row() {
        let mut f = fsm();
        for i in 1..=6u64 {
            assert_eq!(apply_at(&mut f, i * 100, &pin(0, i as u32, i as u32 + 1, i * 10)), 0);
        }
        assert_eq!(apply_at(&mut f, 700, &pin(1, 0, 1, 5)), 0);
        let row0: Vec<u64> = f.state().pins.iter().filter(|p| p.row == 0).map(|p| p.origin).collect();
        assert_eq!(row0, vec![30, 40, 50, 60], "MAX_PINS_PER_ROW = 4, oldest dropped");
        assert_eq!(MAX_PINS_PER_ROW, 4);
        assert_eq!(f.state().pins.len(), 5, "row 1's entry is untouched");
    }

    #[test]
    fn a_report_is_held_newest_per_row_and_a_stale_one_is_refused() {
        let mut f = fsm();
        assert_eq!(apply_at(&mut f, 100, &report(0, 50, &[(0, 1), (1, 1), (2, 2)])), 0);
        assert_eq!(f.state().report_for(0).map(|r| r.position), Some(50));
        assert_eq!(apply_at(&mut f, 200, &report(0, 40, &[(0, 1)])), 59, "below the held position");
        assert_eq!(apply_at(&mut f, 300, &report(0, 50, &[(0, 1), (1, 1)])), 0, "equal replaces (a fuller vector for the same instant)");
        assert_eq!(f.state().report_for(0).map(|r| r.hashes.len()), Some(2));
        assert_eq!(apply_at(&mut f, 400, &report(3, 10, &[(0, 9)])), 0);
        assert_eq!(f.state().reports.len(), 2, "one entry per row");
        assert_eq!(
            verdict(f.state().report_for(0).unwrap()),
            Verdict { agreed: true, majority_hash: Some(1), minority: vec![] }
        );
    }

    #[test]
    fn pins_and_reports_ride_the_image_and_an_old_image_installs_empty() {
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(0, 1, 2, 50));
        apply_at(&mut f, 200, &report(0, 50, &[(0, 1), (1, 2), (2, 2)]));
        let (img, pos) = f.freeze().unwrap();
        assert_eq!(pos, 200);
        let mut g = ClusterFsm::new(genesis(), vec![]);
        assert_eq!(g.install_snapshot(200, &mut &img[..]).unwrap(), 200);
        assert_eq!(g.state(), f.state());
        // A version-1 image (no blobs) installs with empty histories.
        let v1 = {
            let mut m = Vec::new();
            encode_config(&cluster_to_wire(&genesis().membership, 0), &mut m);
            let mut s = Vec::new();
            encode_settings(&Settings::genesis_default(), &mut s);
            let mut b = Vec::new();
            b.extend_from_slice(b"UCCLUST1");
            b.extend_from_slice(&1u32.to_le_bytes());
            b.extend_from_slice(&7u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&(m.len() as u32).to_le_bytes());
            b.extend_from_slice(&m);
            b.extend_from_slice(&8u32.to_le_bytes());
            b.extend_from_slice(&1u32.to_le_bytes()); // table version
            b.extend_from_slice(&0u32.to_le_bytes()); // table count
            b.extend_from_slice(&s);
            let crc = crc32fast::hash(&b);
            b.extend_from_slice(&crc.to_le_bytes());
            b
        };
        let mut h = fsm();
        assert_eq!(h.install_snapshot(7, &mut &v1[..]).unwrap(), 7);
        assert!(h.state().pins.is_empty() && h.state().reports.is_empty());
    }

    #[test]
    fn queries_4_and_5_return_the_lists() {
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(2, 1, 2, 50));
        let mut out = Vec::new();
        f.query(&[4], &mut out);
        assert_eq!(decode_pin_list(&out).unwrap(), f.state().pins);
        f.query(&[5], &mut out);
        assert_eq!(decode_report_list(&out).unwrap(), vec![]);
    }

    #[test]
    fn the_view_publishes_pins_and_reports() {
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(0, 1, 2, 50));
        apply_at(&mut f, 200, &report(0, 50, &[(0, 1)]));
        let v = ClusterView::new(&genesis());
        v.publish(f.state());
        let st = v.to_state();
        assert_eq!(st.pins, f.state().pins);
        assert_eq!(st.reports, f.state().reports);
        assert_eq!(v.position.load(Ordering::Acquire), 200);
    }
```

Add to the test module's imports: `use uc_protocol::v2::upgrade::{decode_pin_list, decode_report_list, verdict, SnapshotReport, UpgradePin, Verdict};` (and `crc32fast` is already a `uc_node` dependency — check `uc_node/Cargo.toml`; if it is not, use `uc_protocol::v2::cluster_image::encode_cluster_image` with `pins: &[], reports: &[]` for the v1 image instead — **no**: that encodes version 2. Confirm `crc32fast` is a dependency (`grep crc32fast uc_node/Cargo.toml`); it is used by the image leaf, which is in `uc_protocol`, so if `uc_node` lacks it, add `crc32fast = { workspace = true }` under `[dev-dependencies]` following the root manifest's existing workspace entry).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_node --lib cluster_fsm 2>&1 | tail -10`
Expected: compile errors (non-exhaustive matches on `ClusterKind` from Task 2; missing variants/fields).

- [ ] **Step 3: Implement**

Imports at the top of `cluster_fsm.rs`: `use uc_protocol::v2::upgrade::{decode_pin_list, decode_report_list, decode_snapshot_report, decode_upgrade_pin, encode_pin_list, encode_report_list, encode_snapshot_report, encode_upgrade_pin, SnapshotReport, UpgradePin};`

```rust
/// Spec §2.5: "a small bounded per-row history … a handful of entries".
pub const MAX_PINS_PER_ROW: usize = 4;
```

`ClusterState` — two fields after `applied`, with docs:

```rust
    /// FSM upgrade lifecycle (spec §2.5): every accepted `UpgradePin`, in
    /// apply order, at most [`MAX_PINS_PER_ROW`] per row (the oldest for
    /// that row is dropped). Rides the image, so a below-floor joiner holds
    /// the pin BEFORE its service attaches (§3 S4).
    pub pins: Vec<UpgradePin>,
    /// Spec §6.5.2: the newest `SnapshotReport` per row — the observed
    /// `(node, hash)` vector, never the verdict, which is
    /// `uc_protocol::v2::upgrade::verdict`'s to recompute.
    pub reports: Vec<SnapshotReport>,
```

`genesis()` sets both to `Vec::new()`. Methods on `ClusterState`:

```rust
    /// The row's newest pin, if any.
    pub fn pin_for(&self, row: u8) -> Option<&UpgradePin> {
        self.pins.iter().rev().find(|p| p.row == row)
    }
    pub fn report_for(&self, row: u8) -> Option<&SnapshotReport> {
        self.reports.iter().find(|r| r.row == row)
    }
    fn push_pin(&mut self, p: UpgradePin) {
        self.pins.push(p);
        if self.pins.iter().filter(|q| q.row == p.row).count() > MAX_PINS_PER_ROW {
            let oldest = self.pins.iter().position(|q| q.row == p.row).expect("just pushed one");
            self.pins.remove(oldest);
        }
    }
    fn put_report(&mut self, r: SnapshotReport) {
        match self.reports.iter().position(|q| q.row == r.row) {
            Some(i) => self.reports[i] = r,
            None => self.reports.push(r),
        }
    }
```

`ClusterCommand` gains `UpgradePin(UpgradePin)`, `SnapshotReport(SnapshotReport)`. `ClusterRefusal` gains `PinFromMismatch`, `PinNotMonotone`, `ReportStale` with `reason_code` `53`, `55`, `59` (comment: "the 52–59 band, plan B1; 52/54/56–58 are door-only and live in `uc_node::node`").

`validate_replicated` — two arms:

```rust
            ClusterCommand::UpgradePin(p) => {
                // Spec §2.5, replicated half only: the row's history is FSM
                // state. `pin_row_undeclared` (52), the no-pin half of
                // `pin_from_mismatch` (53, against the attached version
                // WORD) and `pin_no_set` (54, this leader's filesystem) are
                // node-local and stay at the door (`Consensus::apply_upgrade_pin`).
                if let Some(cur) = self.state.pin_for(p.row) {
                    if p.origin <= cur.origin {
                        return Err(ClusterRefusal::PinNotMonotone);
                    }
                    if p.from != cur.to {
                        return Err(ClusterRefusal::PinFromMismatch);
                    }
                }
                Ok(())
            }
            ClusterCommand::SnapshotReport(r) => {
                if self.state.report_for(r.row).is_some_and(|held| r.position < held.position) {
                    return Err(ClusterRefusal::ReportStale);
                }
                Ok(())
            }
```

`decode_command`: `ClusterKind::UpgradePin => ClusterCommand::UpgradePin(decode_upgrade_pin(payload)?)`, `ClusterKind::SnapshotReport => ClusterCommand::SnapshotReport(decode_snapshot_report(payload)?)`.

`encode_command`: `ClusterCommand::UpgradePin(p) => { encode_upgrade_pin(p, out); ClusterKind::UpgradePin }`, `ClusterCommand::SnapshotReport(r) => { encode_snapshot_report(r, out).expect("a SnapshotReport in FSM state or built by the leader is encodable: non-empty, ≤ MAX_MEMBERS, ids increasing"); ClusterKind::SnapshotReport }`.

`apply`'s match: `ClusterCommand::UpgradePin(p) => self.state.push_pin(p)`, `ClusterCommand::SnapshotReport(r) => self.state.put_report(r)`.

`query`: `Some(4) => encode_pin_list(&self.state.pins, out)`, `Some(5) => { encode_report_list(&self.state.reports, out).expect("held reports are encodable"); }`.

`freeze`: build `let mut pins = Vec::new(); encode_pin_list(&self.state.pins, &mut pins); let mut reports = Vec::new(); encode_report_list(&self.state.reports, &mut reports).ok_or_else(|| SnapshotError::Codec("cluster image: unencodable report".into()))?;` and pass `pins: &pins, reports: &reports`. Update the doc comment on `ClusterImage` with the v2 layout.

`install_snapshot`: after decoding settings, `let pins = decode_pin_list(parts.pins).ok_or_else(|| bad("cluster image pins"))?; let reports = decode_report_list(parts.reports).ok_or_else(|| bad("cluster image reports"))?;` and set both fields on the installed state.

`ClusterViewInner` gains `pub pins: Vec<UpgradePin>, pub reports: Vec<SnapshotReport>`; `ClusterView::new` initialises from `genesis`; `publish` copies both under the same lock as `membership`/`table`; `to_state` fills them from `inner`.

Every other place that constructs a `ClusterState` literal in `uc_node` must gain the two fields — `grep -rn "settings_position: 0," uc_node/src uc_node/tests uc_node/examples` finds them (`cluster_fsm.rs` tests' `genesis()`, `cluster_agent.rs` tests' `genesis_state()`, `node.rs`, `obs/metrics.rs`'s `test_cluster_view`, `examples/m10_alerts.rs`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_node --lib cluster_fsm 2>&1 | tail -8`
Expected: all pass (the 7 new + the existing).

Run: `cargo build --workspace 2>&1 | tail -3` — Expected: clean (the workspace compiles again). Then `cargo test -p uc_node --lib cluster_agent 2>&1 | tail -5` — Expected: the existing agent tests still pass (the image round-trips with empty blobs).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/cluster_fsm.rs uc_node/src/cluster_agent.rs uc_node/src/node.rs uc_node/src/obs/metrics.rs uc_node/examples/m10_alerts.rs uc_node/Cargo.toml
git commit -m "uc_node: UpgradePin and SnapshotReport in the cluster FSM — state, refusals 53/55/59, image v2, view (plan B1 T4)"
```

---

### Task 5: cnc 3.3 — the two pin words on the status line

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (`CNC_V2_VERSION` line 64, the per-slot layout comment ~300-322, constants after `CNC_SVC_OFF_VERSION`, the offset test ~814-830), `uc_log/src/cnc.rs:175-195` (`ServiceStatusLine`), `docs/reference/cnc-page.md` (the slot table ~108-160 and the version sentence at the top)
- Test: existing offset tests in both crates

**Interfaces:**
- Produces: `uc_protocol::v2::cnc::{CNC_SVC_OFF_UPGRADE_ORIGIN = 16, CNC_SVC_OFF_PINNED_VERSION = 24}`; `CNC_V2_VERSION = (3 << 24) | (3 << 16)`; on `uc_log::cnc::ServiceStatusLine`: `pub fn upgrade_origin(&self) -> u64`, `pub fn pinned_version(&self) -> u32`, `pub fn store_pin(&self, origin: u64, version: u32)` (version first, origin last, both `Release`).

- [ ] **Step 1: Write the failing tests**

`uc_protocol/src/v2/cnc.rs`, in the slot-offset test (the block that asserts `CNC_SVC_OFF_VERSION`), replace the 3.2 assertion and add:

```rust
        // cnc 3.3 (plan B1): the row's pin words on the STATUS line, node-written.
        assert_eq!(CNC_V2_VERSION, (3 << 24) | (3 << 16));
        assert_eq!(CNC_SVC_OFF_UPGRADE_ORIGIN, 16);
        assert_eq!(CNC_SVC_OFF_PINNED_VERSION, 24);
        assert_eq!(CNC_SVC_OFF_UPGRADE_ORIGIN, CNC_SVC_OFF_VERSION + 8);
        const { assert!(CNC_SVC_OFF_PINNED_VERSION + 8 <= 64, "inside the status line") };
```

`uc_log/src/cnc.rs`, beside `service_slots_init_zero_and_are_independent` (line ~1842):

```rust
    #[test]
    fn pin_words_are_zero_at_init_and_store_version_before_origin() {
        let page = CncPage::heap(&test_meta());
        let s = &page.service_slot(2).status;
        assert_eq!((s.upgrade_origin(), s.pinned_version()), (0, 0));
        s.store_pin(8192, 0x0102_0003);
        assert_eq!((s.upgrade_origin(), s.pinned_version()), (8192, 0x0102_0003));
        assert_eq!(page.service_slot(1).status.upgrade_origin(), 0, "slots are independent");
    }
```

(Use whatever the neighbouring test uses to build a heap page — read `service_slots_init_zero_and_are_independent` and copy its construction verbatim.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_protocol cnc 2>&1 | tail -5 && cargo test -p uc_log cnc::tests::pin_words 2>&1 | tail -5`
Expected: compile errors on the missing constants / methods.

- [ ] **Step 3: Implement**

`uc_protocol/src/v2/cnc.rs`:
- `pub const CNC_V2_VERSION: u32 = (3 << 24) | (3 << 16);` and extend its doc comment: "3.3 (plan B1): `upgrade_origin`/`pinned_version` at slot +16/+24."
- After `CNC_SVC_OFF_VERSION`:

```rust
/// FSM upgrade lifecycle (spec §2.5, §3 S4; cnc 3.3): the row's newest
/// `UpgradePin`, republished from cluster-FSM state by the `uc2-cluster`
/// agent on every view publish — node-written, a second writer on the
/// status line (the service writes `status`/`version` at attach; each word
/// still has exactly one writer). `0` = no pin. A service reads them at
/// attach (plan B2): a non-zero origin whose version equals its own
/// `VERSION` means "install `snap-<origin>` unconditionally"; a version
/// that differs is an attach refusal.
pub const CNC_SVC_OFF_UPGRADE_ORIGIN: usize = 16;
/// Low 32 bits = the packed version the pin names (`identity::pack_version`).
pub const CNC_SVC_OFF_PINNED_VERSION: usize = 24;
```

- In the per-slot layout comment add two rows after `+8 version`: `+16 upgrade_origin u64 position (0 = no pin)  writer: node (cluster agent)` and `+24 pinned_version u64 (low 32 = packed version)  writer: node (cluster agent)`.

`uc_log/src/cnc.rs`:

```rust
#[repr(C)]
pub struct ServiceStatusLine {
    status: AtomicU64,
    version: AtomicU64,
    upgrade_origin: AtomicU64,
    pinned_version: AtomicU64,
    _pad: [u64; 4],
}
```

with

```rust
    /// The row's pinned origin (cnc 3.3, spec §3 S4); `0` = no pin.
    pub fn upgrade_origin(&self) -> u64 {
        self.upgrade_origin.load(Ordering::Acquire)
    }
    pub fn pinned_version(&self) -> u32 {
        self.pinned_version.load(Ordering::Acquire) as u32
    }
    /// Version FIRST, origin LAST with `Release`: a reader that `Acquire`s
    /// a non-zero origin sees the version that goes with it.
    pub fn store_pin(&self, origin: u64, version: u32) {
        self.pinned_version.store(version as u64, Ordering::Release);
        self.upgrade_origin.store(origin, Ordering::Release);
    }
```

and two `const _` offset assertions mirroring the existing `version` one (`offset_of!(ServiceStatusLine, upgrade_origin) == cnc::CNC_SVC_OFF_UPGRADE_ORIGIN`, same for `pinned_version`). Update the struct's doc comment to say the line now has two writers, one per word.

`docs/reference/cnc-page.md`: add two rows to the slot table after `version`:

```
| 16 | `upgrade_origin` — u64, the row's pinned origin position; `0` = no pin | **node** (`uc2-cluster` agent), republished on every view publish — cnc 3.3, FSM upgrade lifecycle |
| 24 | `pinned_version` — u64, low 32 = packed version the pin names | **node** (`uc2-cluster` agent), stored BEFORE `upgrade_origin` — cnc 3.3 |
```

and change the page's version statement ("cnc 3.2") to 3.3 with one sentence naming the two words; the status line's "one writer" sentence gets the same per-word qualification line 7 has.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_protocol cnc 2>&1 | tail -4 && cargo test -p uc_log 2>&1 | tail -4`
Expected: pass.

Run: `grep -rn "3 << 24) | (2 << 16)\|cnc 3\.2\b" --include=*.rs . | grep -v target` — Expected: no code hits left (docs are Task 10's sweep; `CLAUDE.md` is plan D's).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs docs/reference/cnc-page.md
git commit -m "cnc 3.3: upgrade_origin and pinned_version on the service status line (plan B1 T5)"
```

---

### Task 6: The cluster agent — pin words on publish, the divergence event, offline readers

**Files:**
- Modify: `uc_node/src/cluster_agent.rs` (`ClusterAgent::new` ~233-265, `do_work`'s apply site ~395-425, `install_from` 485, `replay_from_journal`'s apply site ~631, every `self.view.publish(` call, the readers after `read_committed_settings` ~110)
- Test: inline `mod tests` (fixtures `world()`, `settings_cmd()`, `applies_only_committed_cluster_frames_and_publishes_the_view` at ~872 as the model)

**Interfaces:**
- Consumes: Task 4's `ClusterState::{pin_for, report_for}`, Task 5's `ServiceStatusLine::store_pin`, Task 1's `verdict`.
- Produces: `pub fn read_committed_upgrade(instance_dir: &Path) -> io::Result<Option<(u64, Vec<UpgradePin>, Vec<SnapshotReport>)>>` (`None` = no artifact yet, like `read_committed_settings`); a private `fn publish_view(&mut self)` that replaces every `self.view.publish(self.fsm.state())` and also writes the pin words; obs events `upgrade_pin_applied` (info: `row`, `from`, `to`, `origin`, `position`) and `snapshot_hash_diverged` (warn: `row`, `position`, `node`, `majority_hash`) — one `snapshot_hash_diverged` per minority node.

- [ ] **Step 1: Write the failing tests**

```rust
    /// PAYLOAD only — `Appender::append_cluster(term, kind, payload)` writes
    /// the prefix itself, exactly as `settings_cmd` is used.
    fn pin_payload(row: u8, from: u32, to: u32, origin: u64) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_upgrade_pin(&UpgradePin { row, from, to, origin }, &mut payload);
        payload
    }
    fn report_payload(row: u8, position: u64, hashes: &[(u32, u64)]) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_snapshot_report(&SnapshotReport { row, position, hashes: hashes.to_vec() }, &mut payload).unwrap();
        payload
    }
    /// The eleven-argument construction `applies_only_committed_cluster_frames_and_publishes_the_view`
    /// uses, over an existing `(buffer, cnc, dir)` world — returns the view too.
    fn agent_over(buffer: &Arc<LogBuffer>, cnc: &Arc<CncPage>, dir: &Path) -> (ClusterAgent, Arc<ClusterView>) {
        let (fsm, start) = recover(&dir.join("snapshots/cluster"), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let agent = ClusterAgent::new(
            Arc::clone(buffer),
            Arc::clone(cnc),
            fsm,
            Arc::clone(&view),
            dir.join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir),
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        (agent, view)
    }

    /// Spec §2.5 / §3 S4 step 3: an applied pin lands in the row's cnc
    /// words at commit, and a recovered agent republishes them at boot —
    /// before any service attaches.
    #[test]
    fn an_applied_pin_is_written_to_the_rows_status_line_and_survives_recovery() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let end = app
            .append_cluster(1, ClusterKind::UpgradePin, &pin_payload(2, 0x0100_0000, 0x0101_0000, 4096))
            .unwrap();
        cnc.counters().durable.store_release(end);
        cnc.counters().commit.store_release(end);
        let (mut agent, _view) = agent_over(&buffer, &cnc, dir.path());
        assert!(agent.do_work());
        let s = &cnc.service_slot(2).status;
        assert_eq!((s.upgrade_origin(), s.pinned_version()), (4096, 0x0101_0000));
        assert_eq!(cnc.service_slot(0).status.upgrade_origin(), 0);
        assert_eq!(agent.applied(), end);

        agent.take_snapshot().unwrap();
        // A fresh page (a restarted node) and a recovered agent: the words
        // are republished from the artifact at construction.
        let (_, fresh_cnc, _) = world();
        let (_agent2, _) = agent_over(&buffer, &fresh_cnc, dir.path());
        assert_eq!(fresh_cnc.service_slot(2).status.upgrade_origin(), 4096, "recover republishes the pin words");
    }

    /// One accepted report through `do_work`; the verdict names node 2.
    /// (The `snapshot_hash_diverged` event is emitted on the same branch;
    /// the state is the contract asserted here.)
    #[test]
    fn a_report_with_a_minority_names_it() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let end = app
            .append_cluster(1, ClusterKind::SnapshotReport, &report_payload(0, 4096, &[(0, 1), (1, 1), (2, 2)]))
            .unwrap();
        cnc.counters().durable.store_release(end);
        cnc.counters().commit.store_release(end);
        let (mut agent, view) = agent_over(&buffer, &cnc, dir.path());
        assert!(agent.do_work());
        let st = view.to_state();
        assert_eq!(verdict(st.report_for(0).unwrap()).minority, vec![2]);
    }

    #[test]
    fn read_committed_upgrade_reads_the_artifact_or_says_none() {
        let (buffer, cnc, dir) = world();
        assert!(read_committed_upgrade(dir.path()).unwrap().is_none());
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let end = app
            .append_cluster(1, ClusterKind::UpgradePin, &pin_payload(1, 5, 6, 4096))
            .unwrap();
        cnc.counters().durable.store_release(end);
        cnc.counters().commit.store_release(end);
        let (mut agent, _view) = agent_over(&buffer, &cnc, dir.path());
        assert!(agent.do_work());
        let p = agent.take_snapshot().unwrap();
        let (pos, pins, reports) = read_committed_upgrade(dir.path()).unwrap().unwrap();
        assert_eq!((pos, pins.len(), reports.len()), (p, 1, 0));
        assert_eq!(pins[0].origin, 4096);
    }
```

`world()` returns `(Arc<LogBuffer>, Arc<CncPage>, tempfile::TempDir)` and `read_committed_upgrade` takes the INSTANCE dir, whose cluster artifacts live under `snapshots/cluster` — check `snapshot_dir_of` (line 72) agrees with the `dir.join("snapshots/cluster")` the agent is given, as the existing recovery test does. Imports for the test module: `use uc_protocol::v2::upgrade::{encode_snapshot_report, encode_upgrade_pin, verdict, SnapshotReport, UpgradePin};`, `use uc_protocol::v2::frame::ClusterKind;`, `use std::path::Path;`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_node --lib cluster_agent::tests 2>&1 | tail -10`
Expected: the three new tests fail (no `read_committed_upgrade`; pin words read 0).

- [ ] **Step 3: Implement**

```rust
    /// Publish the view AND the per-row pin words (spec §3 S4 step 3): the
    /// words are a projection of FSM state, republished whole on every
    /// publish — idempotent, so a recovered or freshly-installed state
    /// writes them before any service attaches, with no edge to miss.
    fn publish_view(&mut self) {
        let st = self.fsm.state();
        self.view.publish(st);
        for row in 0..uc_protocol::v2::cnc::CNC_MAX_SERVICES as u8 {
            if let Some(p) = st.pin_for(row) {
                self.cnc.service_slot(row as usize).status.store_pin(p.origin, p.to);
            }
        }
    }
```

Replace every `self.view.publish(self.fsm.state())` in the agent (the constructor's initial publish, the `do_work` publish, `install_from`'s, `replay_from_journal`'s) with `self.publish_view()`.

At both apply sites (`do_work` ~414 and `replay_from_journal` ~631), after the existing `cluster_command_applied` event, call one shared hook. Both kinds put `row` at payload offset 0, and the apply loop's `payload` variable is the whole CLUSTER body (prefix + payload), so the row byte is `payload.get(CLUSTER_BODY_PREFIX_LEN)`:

```rust
                        self.note_applied(
                            payload.first().copied().unwrap_or(0),
                            payload.get(CLUSTER_BODY_PREFIX_LEN).copied().unwrap_or(0),
                            accepted,
                            end,
                        );
```

(`end` is the frame-END position the existing event already reports; in `replay_from_journal` use that site's position variable.)

```rust
    /// Plan B1: name what an ACCEPTED pin or report did, at the time it
    /// did it. `row` is the payload's first byte, the same for both kinds.
    /// A report's verdict is recomputed here from FSM state, not stored.
    fn note_applied(&self, kind: u8, row: u8, accepted: bool, position: u64) {
        if !accepted {
            return;
        }
        let st = self.fsm.state();
        match ClusterKind::from_u8(kind) {
            Some(ClusterKind::UpgradePin) => {
                if let Some(p) = st.pin_for(row) {
                    crate::obs_event!(
                        Info,
                        "upgrade_pin_applied",
                        position = position,
                        row = p.row as u64,
                        from = p.from as u64,
                        to = p.to as u64,
                        origin = p.origin
                    );
                }
            }
            Some(ClusterKind::SnapshotReport) => {
                if let Some(r) = st.report_for(row) {
                    let v = verdict(r);
                    for node in v.minority {
                        crate::obs_event!(
                            Warn,
                            "snapshot_hash_diverged",
                            row = r.row as u64,
                            position = r.position,
                            node = node as u64,
                            majority_hash = v.majority_hash.unwrap_or(0)
                        );
                    }
                }
            }
            _ => {}
        }
    }
```

Imports: `use uc_protocol::v2::frame::{ClusterKind, CLUSTER_BODY_PREFIX_LEN};`, `use uc_protocol::v2::upgrade::{verdict, SnapshotReport, UpgradePin};`.

The reader, after `read_committed_settings`:

```rust
/// `uc2ctl upgrade show`'s reader (plan B1): the pin history and the held
/// snapshot reports in this instance directory's newest cluster artifact,
/// with the artifact's position — `read_committed_settings`'s contract and
/// its staleness caveat, verbatim.
pub fn read_committed_upgrade(
    instance_dir: &Path,
) -> io::Result<Option<(u64, Vec<UpgradePin>, Vec<SnapshotReport>)>> {
    let (fsm, start) = recover(&snapshot_dir_of(instance_dir), ClusterState::genesis_empty(), Vec::new())?;
    if start == 0 {
        return Ok(None);
    }
    let st = fsm.state();
    Ok(Some((st.applied, st.pins.clone(), st.reports.clone())))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_node --lib cluster_agent 2>&1 | tail -6`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/cluster_agent.rs
git commit -m "uc2-cluster agent: republish pin words on every view publish; upgrade_pin_applied and snapshot_hash_diverged events; read_committed_upgrade (plan B1 T6)"
```

---

### Task 7: `uc_node::node` — op 10, refusals 52–58, single-in-flight, retention keeps pinned origins

**Files:**
- Modify: `uc_node/src/node.rs` (reason constants after line 522; `SETTINGS_PENDING_FILE`'s neighbour for `UPGRADE_PENDING_FILE`; `MAX_SETTINGS_BYTES`'s neighbour at 757; the `settings_pending: PathBuf` field at 3239 and its two initialisers at 2203/10995; `apply_settings` at 8007 as the model for `apply_upgrade_pin`; `refuse_settings` at 8300; the dispatch at 7676; `prune_snapshot_dir` at 10406 and its call site at 5878-5890), `uc_node/src/audit.rs:150-162` (`op_name`), `uc_node/src/lib.rs` (re-export `UPGRADE_PENDING_FILE` beside `SETTINGS_PENDING_FILE` — check how that one is exported)
- Test: `uc_node/src/node.rs` `mod tests` (models: `stage_settings_for_test` 11725, `a_settings_command_is_appended_as_a_cluster_frame_and_the_view_follows_at_commit` 11736, `settings_apply_is_single_in_flight_on_the_view_position` 13433, the retention test near 11390)

**Interfaces:**
- Consumes: Task 2's `ADMIN_OP_UPGRADE_PIN`, Task 1's `{decode_upgrade_pin, UpgradePin, UPGRADE_PIN_LEN}`, Task 4's `ClusterCommand::UpgradePin`, `ClusterState::pin_for`, Task 5's `status.version()` (exists) and `store_pin`.
- Produces: `pub const UPGRADE_PENDING_FILE: &str = "upgrade.pending";` `pub const REASON_PIN_ROW_UNDECLARED: u32 = 52; REASON_PIN_FROM_MISMATCH = 53; REASON_PIN_NO_SET = 54; REASON_PIN_NOT_MONOTONE = 55; REASON_PIN_DIGEST = 56; REASON_PIN_MISSING = 57; REASON_PIN_DECODE = 58; REASON_REPORT_STALE = 59;` (all `pub`, exported like `REASON_SETTINGS_*`); `audit::op_name(10) == "upgrade_pin"`; `fn prune_snapshot_dir(dir, suffix, below, keep: &[u64])`.

- [ ] **Step 1: Write the failing tests**

```rust
    fn stage_pin_for_test(h: &Harness, pin: &UpgradePin) {
        let mut bytes = Vec::new();
        encode_upgrade_pin(pin, &mut bytes);
        std::fs::write(&h.cons.upgrade_pending, &bytes).expect("stage the pin");
    }

    /// The door checks are node-local and run BEFORE the FSM's own
    /// `validate` (spec §2.5; Ruling R24's split). Each refusal leaves the
    /// staged file in place and appends nothing.
    /// `(status, reason)` only: the third word is the view position, which
    /// `settings_apply_is_single_in_flight_on_the_view_position` compares
    /// against the live atomic rather than a literal — do the same.
    fn sr(t: (u32, u32, u64)) -> (u32, u32) {
        (t.0, t.1)
    }

    #[test]
    fn upgrade_pin_door_refusals_by_name() {
        let mut h = harness_with_rows(&["a"]); // row 0 declared, rows 1..8 not
        drive_to_serving_leader(&mut h);
        let before = h.cons.last_cluster_append;
        // 52: row 5 is not declared.
        stage_pin_for_test(&h, &UpgradePin { row: 5, from: 1, to: 2, origin: 4096 });
        assert_eq!(sr(h.cons.apply_upgrade_pin_staged()), (1, REASON_PIN_ROW_UNDECLARED));
        // 53 (no-pin half): the attached version word is 1.0.0, `from` says 2.0.0.
        h.cons.cnc.service_slot(0).status.store_version(pack_version(1, 0, 0));
        stage_pin_for_test(&h, &UpgradePin { row: 0, from: pack_version(2, 0, 0), to: pack_version(2, 1, 0), origin: 4096 });
        assert_eq!(sr(h.cons.apply_upgrade_pin_staged()), (1, REASON_PIN_FROM_MISMATCH));
        // 54: no complete set at 4096 (the set word reads 0).
        stage_pin_for_test(&h, &UpgradePin { row: 0, from: pack_version(1, 0, 0), to: pack_version(1, 1, 0), origin: 4096 });
        assert_eq!(sr(h.cons.apply_upgrade_pin_staged()), (1, REASON_PIN_NO_SET));
        // 56: the staged bytes differ from the signed digest.
        h.cons.snapshot_set_position.store(4096, Ordering::Release);
        let (status, reason, _) = h.cons.apply_upgrade_pin(1, 2, 3);
        assert_eq!((status, reason), (1, REASON_PIN_DIGEST));
        // 57 / 58: absent, then undecodable.
        std::fs::remove_file(&h.cons.upgrade_pending).unwrap();
        assert_eq!(h.cons.apply_upgrade_pin(0, 0, 0).1, REASON_PIN_MISSING);
        std::fs::write(&h.cons.upgrade_pending, b"not twenty bytes").unwrap();
        assert_eq!(h.cons.apply_upgrade_pin_staged().1, REASON_PIN_DECODE);
        assert_eq!(h.cons.last_cluster_append, before, "nothing was appended");
    }

    /// Spec §3 S4 steps 2–3: pin → CLUSTER frame → applied at commit →
    /// the row's cnc words hold origin and version.
    #[test]
    fn an_upgrade_pin_is_appended_as_a_cluster_frame_and_the_words_follow_at_commit() {
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        h.cons.cnc.service_slot(0).status.store_version(pack_version(1, 0, 0));
        h.cons.snapshot_set_position.store(4096, Ordering::Release);
        stage_pin_for_test(&h, &UpgradePin { row: 0, from: pack_version(1, 0, 0), to: pack_version(1, 1, 0), origin: 4096 });
        let (status, reason, end) = h.cons.apply_upgrade_pin_staged();
        assert_eq!((status, reason), (0, 0));
        assert!(!h.cons.upgrade_pending.exists(), "consumed on append");
        assert_eq!(h.cons.cnc.service_slot(0).status.upgrade_origin(), 0, "nothing until commit");
        // `commit_through` drives the harness's uc2-cluster agent (that is
        // how `a_settings_command_is_appended_as_a_cluster_frame_and_the_view_follows_at_commit`
        // sees the view move) — nothing else to call.
        h.commit_through(end);
        let s = &h.cons.cnc.service_slot(0).status;
        assert_eq!((s.upgrade_origin(), s.pinned_version()), (4096, pack_version(1, 1, 0)));
        assert_eq!(h.cons.cluster_view.to_state().pin_for(0).map(|p| p.origin), Some(4096));
        // 55 now comes from the FSM (replicated): the same origin again.
        stage_pin_for_test(&h, &UpgradePin { row: 0, from: pack_version(1, 1, 0), to: pack_version(1, 2, 0), origin: 4096 });
        assert_eq!(h.cons.apply_upgrade_pin_staged().1, REASON_PIN_NOT_MONOTONE);
    }

    #[test]
    fn upgrade_pin_is_single_in_flight_on_the_view_position() {
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        h.cons.snapshot_set_position.store(4096, Ordering::Release);
        stage_pin_for_test(&h, &UpgradePin { row: 0, from: 0, to: 1, origin: 4096 });
        assert_eq!(h.cons.apply_upgrade_pin_staged().0, 0);
        stage_settings_for_test(&h, &Settings { snapshot_interval_bytes: 5, ..Settings::genesis_default() });
        assert_eq!(h.cons.apply_settings_staged().0, 2, "retry: the pin is above the view");
        assert!(h.cons.settings_pending.exists());
    }

    /// Retention (spec §2.5 / plan B1 erratum 4): a pinned origin's
    /// artifacts survive a floor above them, in the row dir AND the
    /// cluster dir, while an unpinned older set goes. Built on
    /// `retention_waits_for_the_floor_to_publish_and_never_outruns_the_ship_gate`'s
    /// file layout; calls the pruner directly rather than through the
    /// floor path, since the keep-set is the only thing under test.
    #[test]
    fn retention_keeps_every_pinned_origin() {
        let h = harness_with_rows(&["a"]);
        let (p0, p1, p2) = (2048u64, 4096u64, 6016u64);
        let row_dir = h.cons.snap_root.join("0");
        std::fs::create_dir_all(&row_dir).unwrap();
        std::fs::create_dir_all(&h.cons.cluster_snapshot_dir).unwrap();
        for p in [p0, p1, p2] {
            std::fs::write(row_dir.join(format!("snap-{p}.ultsnap")), b"row").unwrap();
            std::fs::write(h.cons.cluster_snapshot_dir.join(format!("snap-{p}.ultcluster")), b"cluster").unwrap();
        }
        let names = |d: &std::path::Path| -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(d)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            v.sort();
            v
        };
        // Row 0 is pinned at p1 in the committed view.
        let mut st = h.cons.cluster_view.to_state();
        st.pins.push(UpgradePin { row: 0, from: 1, to: 2, origin: p1 });
        st.applied = p1;
        h.cons.cluster_view.publish(&st);

        h.cons.prune_snapshots_below(p2);
        assert_eq!(
            names(&row_dir),
            vec![format!("snap-{p1}.ultsnap"), format!("snap-{p2}.ultsnap")],
            "p0 pruned, the pinned p1 kept, p2 at the floor kept"
        );
        assert_eq!(
            names(&h.cons.cluster_snapshot_dir),
            vec![format!("snap-{p1}.ultcluster"), format!("snap-{p2}.ultcluster")]
        );
    }
```

(No separate follower test: a follower reaching op 10 takes the same `(false, _)` arm ops 6 and 7 take in the dispatch — answer `retry` with the view position, read nothing. If the existing dispatch tests for ops 6/7 parameterise the op, add `ADMIN_OP_UPGRADE_PIN` to them; otherwise the arm is covered by inspection and by the door tests above.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_node --lib upgrade_pin 2>&1 | tail -10`
Expected: compile errors (no `apply_upgrade_pin`, no `upgrade_pending`, no `REASON_PIN_*`).

- [ ] **Step 3: Implement**

Constants (after `REASON_SCHEDULE_TOO_LARGE`, with a band comment like the 44–47 one):

```rust
// FSM upgrade lifecycle (spec §2.5, plan B1): `ADMIN_OP_UPGRADE_PIN` (wire op
// 10). 52/54 and the no-pin half of 53 are DOOR-ONLY (node-local inputs:
// `[services] names`, the attached version word, this leader's newest
// complete set); 55, the pinned half of 53, and 59 are the FSM's own
// (`ClusterRefusal`). 56–58 are the staged-file outcomes 44–46 have.
pub const REASON_PIN_ROW_UNDECLARED: u32 = 52;
pub const REASON_PIN_FROM_MISMATCH: u32 = 53;
pub const REASON_PIN_NO_SET: u32 = 54;
pub const REASON_PIN_NOT_MONOTONE: u32 = 55;
pub const REASON_PIN_DIGEST: u32 = 56;
pub const REASON_PIN_MISSING: u32 = 57;
pub const REASON_PIN_DECODE: u32 = 58;
pub const REASON_REPORT_STALE: u32 = 59;
```

`pub const UPGRADE_PENDING_FILE: &str = "upgrade.pending";` beside `SETTINGS_PENDING_FILE` (and the same `pub use` in `lib.rs` if that one is re-exported there); `const MAX_UPGRADE_PIN_BYTES: u64 = UPGRADE_PIN_LEN as u64;` beside `MAX_SETTINGS_BYTES`; field `upgrade_pending: PathBuf` beside `settings_pending`, initialised at both sites with `.join(UPGRADE_PENDING_FILE)`.

```rust
    /// Plan B1: the leader half of `ADMIN_OP_UPGRADE_PIN` —
    /// [`Self::apply_settings`]'s twin over `<instance_dir>/upgrade.pending`
    /// with the 52–58 band. Same reply triple, same leader-only / node-local
    /// / single-in-flight rules. The three DOOR checks run here because
    /// their inputs are this node's, never the FSM's (spec §2.5, Ruling R24).
    fn apply_upgrade_pin(&mut self, id: u32, ip: u32, port: u16) -> (u32, u32, u64) {
        let view_position = self.cluster_view.position.load(Ordering::Acquire);
        if self.last_cluster_append > view_position {
            return (2, 0, view_position);
        }
        let bytes = match read_staged(&self.upgrade_pending, MAX_UPGRADE_PIN_BYTES) {
            StagedRead::Bytes(b) => b,
            StagedRead::Missing => return self.refuse_upgrade_pin(REASON_PIN_MISSING),
            StagedRead::Unusable => return self.refuse_upgrade_pin(REASON_PIN_DECODE),
        };
        if staged_digest(&bytes) != (id, ip, port) {
            return self.refuse_upgrade_pin(REASON_PIN_DIGEST);
        }
        let Some(pin) = decode_upgrade_pin(&bytes) else {
            return self.refuse_upgrade_pin(REASON_PIN_DECODE);
        };
        // 52: the row must be one THIS node declares — `declared_hashes`'s source.
        if !self.timers.get(pin.row as usize).is_some_and(|t| t.is_some()) {
            return self.refuse_upgrade_pin(REASON_PIN_ROW_UNDECLARED);
        }
        let state = self.cluster_view.to_state();
        // 53, no-pin half: `from` must be the version the row is ATTACHED at.
        // With a pin in the history the FSM checks `from` against it instead.
        if state.pin_for(pin.row).is_none() {
            let attached = self.cnc.service_slot(pin.row as usize).status.version();
            if pin.from != attached {
                return self.refuse_upgrade_pin(REASON_PIN_FROM_MISMATCH);
            }
        }
        // 54: the complete set at `origin` — this node's NEWEST one, the only
        // one retention cannot remove between here and commit.
        if pin.origin != self.snapshot_set_position.load(Ordering::Acquire) {
            return self.refuse_upgrade_pin(REASON_PIN_NO_SET);
        }
        let cmd = ClusterCommand::UpgradePin(pin);
        if let Err(reason) = self.validate_cluster_command(&cmd) {
            return self.refuse_upgrade_pin(reason);
        }
        match self.append_cluster_frame(&cmd) {
            Ok(position) => {
                self.consume_staged(&self.upgrade_pending, position);
                (0, 0, position)
            }
            Err(AppendError::WouldOverrun) => (2, 0, view_position),
            Err(AppendError::PayloadTooLarge) => self.refuse_upgrade_pin(REASON_PIN_DECODE),
        }
    }

    #[cfg(test)]
    fn apply_upgrade_pin_staged(&mut self) -> (u32, u32, u64) {
        let bytes = std::fs::read(&self.upgrade_pending).expect("pin staged for this call");
        let (id, ip, port) = staged_digest(&bytes);
        self.apply_upgrade_pin(id, ip, port)
    }

    fn refuse_upgrade_pin(&self, reason: u32) -> (u32, u32, u64) {
        self.schedule_refused.fetch_add(1, Ordering::Relaxed);
        crate::obs_event!(Warn, "upgrade_pin_refused", node = self.id as u64, reason = reason as u64);
        (1, reason, self.cluster_view.position.load(Ordering::Acquire))
    }
```

Dispatch: widen the condition at 7676 to `req.op == ADMIN_OP_SCHEDULE_APPLY || req.op == ADMIN_OP_SETTINGS_APPLY || req.op == ADMIN_OP_UPGRADE_PIN` and turn the `(leader, settings)` match into a match on `(leader, req.op)`:

```rust
            let (status, reason, version) = match (leader, req.op) {
                (true, ADMIN_OP_SCHEDULE_APPLY) => self.apply_schedule_table(req.id, req.ip, req.port),
                (true, ADMIN_OP_SETTINGS_APPLY) => self.apply_settings(req.id, req.ip, req.port),
                (true, _) => self.apply_upgrade_pin(req.id, req.ip, req.port),
                (false, ADMIN_OP_SCHEDULE_APPLY) => (2, 0, self.schedule_position),
                (false, _) => (2, 0, self.cluster_view.position.load(Ordering::Acquire)),
            };
```

Update the comment above it to name three ops. `audit.rs`: `10 => "upgrade_pin",` plus a sentence in the doc above `op_name` ("for `upgrade_pin` the `id`/`ip`/`port` carry the staged file's digest, as for ops 6 and 7").

Retention: `fn prune_snapshot_dir(dir: &Path, suffix: &str, below: u64, keep: &[u64]) -> (u64, u64)` with `if pos >= below || keep.contains(&pos) { continue; }`; at the call site compute once:

```rust
        // Plan B1: a pinned origin's set must outlive the floor — B2's
        // attach-time install needs it. Read once per retention pass (the
        // pass is throttled), from the committed view.
        let keep: Vec<u64> = {
            let inner = self.cluster_view.snapshot_inner();
            (0..CNC_MAX_SERVICES as u8)
                .filter_map(|row| inner.pins.iter().rev().find(|p| p.row == row).map(|p| p.origin))
                .collect()
        };
```

and pass `&keep` to every `prune_snapshot_dir` call (update the test at ~11390 that calls it directly, passing `&[]`). Extend the `snapshot_set_retained` event with `kept = keep.len() as u64`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_node --lib upgrade_pin 2>&1 | tail -8 && cargo test -p uc_node --lib retention 2>&1 | tail -4 && cargo test -p uc_node --lib settings_apply 2>&1 | tail -4`
Expected: all pass, including the untouched settings tests.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p uc_node --all-targets -- -D warnings 2>&1 | tail -3`

```bash
git add uc_node/src/node.rs uc_node/src/audit.rs uc_node/src/lib.rs
git commit -m "uc_node: admin op 10 upgrade_pin — staged file, door refusals 52/53/54, 56-58, single-in-flight; retention keeps pinned origins (plan B1 T7)"
```

---

### Task 8: `uc2ctl upgrade pin` / `upgrade show`, the reason strings, `status`

**Files:**
- Create: `uc_ctl/src/upgrade.rs`
- Modify: `uc_ctl/src/main.rs` (`mod upgrade;` at ~80; the `Cmd` enum near 493; the dispatch near 521; `reason_str` near 595-625; the `status` per-row `println!` at ~945), `docs/reference/uc2ctl.md` (after `settings show`, ~296; the refusal-reasons table)
- Test: `uc_ctl/src/upgrade.rs` inline tests (`parse_semver`); `uc_ctl/tests/status_services.rs` gains the two words

**Interfaces:**
- Consumes: Task 2's `ADMIN_OP_UPGRADE_PIN`, Task 1's `{encode_upgrade_pin, UpgradePin, verdict}`, Task 6's `uc_node::cluster_agent::read_committed_upgrade`, Task 7's `uc_node::UPGRADE_PENDING_FILE`, `uc_protocol::identity::{pack_version, unpack_version, VersionDisplay}`, `crate::{signed_admin_request, reason_str, open, CommonArgs}`.
- Produces: `pub fn parse_semver(s: &str) -> Result<u32, String>` (`"1.2.3"` → `pack_version(1, 2, 3)`; refuses more/fewer than three parts, non-digits, `major`/`minor` > 255, `patch` > 65535); `pub fn pin(common: &CommonArgs, row: u8, from: Option<&str>, to: &str, origin: u64) -> anyhow::Result<()>`; `pub fn show(common: &CommonArgs) -> anyhow::Result<()>`.

CLI shape:

```
uc2ctl upgrade pin --row <R> --to <MAJOR.MINOR.PATCH> --origin <P> [--from <MAJOR.MINOR.PATCH>] --instance-dir <DIR> --app-id <ID> [--admin-key <PATH>]
uc2ctl upgrade show --instance-dir <DIR> --app-id <ID>
```

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_semver_packs_and_refuses() {
        assert_eq!(parse_semver("1.2.3"), Ok(uc_protocol::identity::pack_version(1, 2, 3)));
        assert_eq!(parse_semver("0.0.0"), Ok(0));
        assert!(parse_semver("1.2").is_err());
        assert!(parse_semver("1.2.3.4").is_err());
        assert!(parse_semver("256.0.0").is_err());
        assert!(parse_semver("1.0.65536").is_err());
        assert!(parse_semver("a.b.c").is_err());
    }
}
```

`uc_ctl/tests/status_services.rs` does not pin the per-row line's full text (no `timers_pending=` assertion in it, checked 2026-09-20). Read it: if it asserts a substring of the row line that the two new words would split, extend the assertion to ` upgrade_origin=0 pinned=0.0.0`; otherwise leave it and rely on the run in Step 4.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_ctl 2>&1 | tail -8`
Expected: `upgrade` module missing / status assertion fails.

- [ ] **Step 3: Implement**

`uc_ctl/src/upgrade.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//! `uc2ctl upgrade pin` / `upgrade show` (FSM upgrade lifecycle spec §2.5,
//! §3 S4 step 2): stage the 20-byte `UpgradePin` record at
//! `<instance_dir>/upgrade.pending`, sign its digest into the admin line,
//! and submit `ADMIN_OP_UPGRADE_PIN` — `settings apply`'s pipeline
//! verbatim. `show` reads the newest cluster artifact.

use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;

use uc_protocol::identity::{pack_version, VersionDisplay};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::upgrade::{encode_upgrade_pin, verdict, UpgradePin};

use crate::CommonArgs;

/// `MAJOR.MINOR.PATCH` → the packed version `S::VERSION` carries.
pub fn parse_semver(s: &str) -> Result<u32, String> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return Err(format!("version {s:?}: expected MAJOR.MINOR.PATCH"));
    }
    let major: u8 = parts[0].parse().map_err(|_| format!("version {s:?}: major must be 0..=255"))?;
    let minor: u8 = parts[1].parse().map_err(|_| format!("version {s:?}: minor must be 0..=255"))?;
    let patch: u16 = parts[2].parse().map_err(|_| format!("version {s:?}: patch must be 0..=65535"))?;
    Ok(pack_version(major, minor, patch))
}

fn stage(instance_dir: &Path, bytes: &[u8]) -> anyhow::Result<std::path::PathBuf> {
    let pending = instance_dir.join(uc_node::UPGRADE_PENDING_FILE);
    let tmp = instance_dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp.display()))?;
        f.write_all(bytes).map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp.display()))?;
        f.sync_all().map_err(|e| anyhow::anyhow!("fsync {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, &pending).map_err(|e| anyhow::anyhow!("staging {}: {e}", pending.display()))?;
    Ok(pending)
}

pub fn pin(common: &CommonArgs, row: u8, from: Option<&str>, to: &str, origin: u64) -> anyhow::Result<()> {
    if row as usize >= uc_protocol::v2::cnc::CNC_MAX_SERVICES {
        anyhow::bail!("--row must be 0..=7");
    }
    if origin == 0 {
        anyhow::bail!("--origin must be a coordinated instant's position (> 0); run `uc2ctl snapshot` first");
    }
    let to = parse_semver(to).map_err(|e| anyhow::anyhow!("--to: {e}"))?;
    let from = match from {
        Some(s) => parse_semver(s).map_err(|e| anyhow::anyhow!("--from: {e}"))?,
        None => {
            // The row's ATTACHED version word — what the node's own door
            // check (53) compares against when the row has no pin yet.
            let cnc = crate::open(common)?;
            let v = cnc.service_slot(row as usize).status.version();
            if v == 0 {
                anyhow::bail!("row {row} has no attached version word on this node; pass --from");
            }
            v
        }
    };
    let mut bytes = Vec::new();
    encode_upgrade_pin(&UpgradePin { row, from, to, origin }, &mut bytes);
    let pending = stage(&common.instance_dir, &bytes)?;
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let resp = crate::signed_admin_request(common, ADMIN_OP_UPGRADE_PIN, id, ip, port, "cluster position")
        .map_err(|e| anyhow::anyhow!("{e} (staged file kept at {})", pending.display()))?;
    match resp.status {
        0 => {
            println!(
                "pinned: row={row} from={} to={} origin={origin} position={}",
                VersionDisplay(from),
                VersionDisplay(to),
                resp.version
            );
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (cluster position {}) — staged file kept at {}",
                crate::reason_str(resp.reason),
                resp.version,
                pending.display()
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: leader unknown or a previous cluster command is still uncommitted \
                 (cluster position {}) — staged file kept at {}, try again",
                resp.version,
                pending.display()
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let Some((position, pins, reports)) = uc_node::cluster_agent::read_committed_upgrade(&common.instance_dir)? else {
        println!("no cluster artifact yet");
        return Ok(());
    };
    println!("position={position}");
    for row in 0..uc_protocol::v2::cnc::CNC_MAX_SERVICES as u8 {
        let history: Vec<&UpgradePin> = pins.iter().filter(|p| p.row == row).collect();
        if let Some(newest) = history.last() {
            print!(
                "  row={row} pinned={} from={} origin={}",
                VersionDisplay(newest.to),
                VersionDisplay(newest.from),
                newest.origin
            );
            if history.len() > 1 {
                let older: Vec<String> = history[..history.len() - 1]
                    .iter()
                    .map(|p| format!("{}->{}@{}", VersionDisplay(p.from), VersionDisplay(p.to), p.origin))
                    .collect();
                print!(" history=[{}]", older.join(","));
            }
            println!();
        }
        if let Some(r) = reports.iter().find(|r| r.row == row) {
            let v = verdict(r);
            match (v.agreed, v.majority_hash) {
                (true, Some(h)) => println!("  row={row} hash_verdict=agreed position={} nodes={} hash=0x{h:016x}", r.position, r.hashes.len()),
                (false, Some(h)) => println!("  row={row} hash_verdict=DIVERGED position={} nodes={} majority=0x{h:016x} minority={:?}", r.position, r.hashes.len(), v.minority),
                (false, None) => println!("  row={row} hash_verdict=NO_MAJORITY position={} nodes={}", r.position, r.hashes.len()),
                (true, None) => unreachable!("agreed implies a majority"),
            }
        }
    }
    Ok(())
}
```

`main.rs`: `mod upgrade;`; a `Cmd::Upgrade(UpgradeArgs)` with `UpgradeCmd::{Pin(UpgradePinArgs), Show(UpgradeShowArgs)}` shaped exactly like `SettingsArgs`/`SettingsCmd` (clap derive; `UpgradePinArgs { #[command(flatten)] common: CommonArgs, #[arg(long)] row: u8, #[arg(long)] from: Option<String>, #[arg(long)] to: String, #[arg(long)] origin: u64 }`); dispatch `UpgradeCmd::Pin(a) => upgrade::pin(&a.common, a.row, a.from.as_deref(), &a.to, a.origin)`, `UpgradeCmd::Show(a) => upgrade::show(&a.common)`. `reason_str` gains:

```rust
        // FSM upgrade lifecycle (spec §2.5, plan B1): `ADMIN_OP_UPGRADE_PIN`
        // (wire op 10) — `uc_node::REASON_PIN_*`.
        52 => "pin_row_undeclared (this node does not declare that row in [services] names)",
        53 => "pin_from_mismatch (--from is not the row's current version: its newest pin's `to`, or, with no pin yet, the version the service is attached at)",
        54 => "pin_no_set (no complete snapshot set at --origin on this node — run `uc2ctl snapshot`, wait for uc2_snapshot_set_position to reach it, and pin THAT position)",
        55 => "pin_not_monotone (--origin is not above the row's current pin)",
        56 => "pin_digest (the staged file changed between staging and applying — re-run `upgrade pin`)",
        57 => "pin_missing (no staged file on this node — was `upgrade pin` run against this same instance dir, or already consumed?)",
        58 => "pin_decode (the staged file is not a 20-byte pin record)",
        59 => "report_stale (a SnapshotReport below the row's held report position — never from uc2ctl)",
```

`status`'s per-row line: append ` upgrade_origin={} pinned={}` with `s.status.upgrade_origin()` and `VersionDisplay(s.status.pinned_version())`.

`docs/reference/uc2ctl.md`: an `### upgrade pin` section (the `settings apply` section's shape: wire op 10, the staged file, the digest, leader-only/single-in-flight/kept-on-refusal, `--from` optional and read from the attached version word, the S4 sequence `snapshot → pin → stop/swap/start`), an `### upgrade show` section with the output shape above and the artifact-lag caveat, the 52–59 rows in the refusal table, and `upgrade_origin=`/`pinned=` in the `status` output description.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_ctl 2>&1 | tail -6 && cargo run -p uc_ctl -- upgrade pin --help | head -12`
Expected: pass; the help lists `--row --from --to --origin`.

- [ ] **Step 5: Commit**

```bash
git add uc_ctl/src/upgrade.rs uc_ctl/src/main.rs uc_ctl/tests/status_services.rs docs/reference/uc2ctl.md
git commit -m "uc2ctl: upgrade pin / upgrade show, refusal strings 52-59, status pin words (plan B1 T8)"
```

---

### Task 9: Gauges, the `Uc2SnapshotHashDiverged` alert, the fire-script builder and scenario

**Files:**
- Modify: `uc_node/src/obs/metrics.rs` (`METRIC_NAMES` ~60-95, `ServiceRow` 310-340 and its builder 372-410, the gauge pushes after `uc2_timers_pending` ~656), `packaging/prometheus/uc2-alerts.yml` (after `Uc2SnapshotSetDiverged`, line 300-313), `scripts/m10_alert_fire.sh` (`RULES` table ~278, a `build_Uc2SnapshotHashDiverged` after `build_Uc2SnapshotSetDiverged` ~672, `RULE_BUILDERS` ~722), `uc_node/examples/m10_alerts.rs` (scenario list ~70, dispatch ~206, a `scenario_snapshot_hash_diverged` after `scenario_schedule_diverged` ~1422), `docs/how-to/monitor-a-cluster.md` (the alert table ~535, the event table ~310), `docs/ops/uc2-runbook.md` (~106-120 metric paragraph)
- Test: `uc_node/src/obs/metrics.rs` render tests (`test_cluster_view` 1519 fixture)

**Interfaces:**
- Consumes: Task 5's `status.upgrade_origin()/pinned_version()`, Task 4's `ClusterViewInner::reports`, Task 1's `verdict`.
- Produces gauges (all `service="<name>",row="<r>"`): `uc2_upgrade_pin_origin`, `uc2_upgrade_pin_version`, `uc2_snapshot_hash_mismatch`; alert `Uc2SnapshotHashDiverged` (critical, `for: 60s`, `expr: max by (service, row) (uc2_snapshot_hash_mismatch) > 0`).

- [ ] **Step 1: Write the failing test**

In `metrics.rs`'s tests, beside the render test that uses `test_cluster_view()`:

```rust
    #[test]
    fn pin_words_and_hash_mismatch_render_per_row() {
        let (sources, _keep) = /* the fixture the neighbouring render test builds, with row 0 declared as "kv" */;
        sources.cnc.service_slot(0).status.store_pin(8192, 0x0101_0000);
        let mut st = sources.cluster_view.to_state();
        st.reports.push(uc_protocol::v2::upgrade::SnapshotReport { row: 0, position: 8192, hashes: vec![(0, 1), (1, 1), (2, 2)] });
        sources.cluster_view.publish(&st);
        let text = render(&sources);
        assert!(text.contains("uc2_upgrade_pin_origin{service=\"kv\",row=\"0\"} 8192"), "{text}");
        assert!(text.contains(&format!("uc2_upgrade_pin_version{{service=\"kv\",row=\"0\"}} {}", 0x0101_0000u64)), "{text}");
        assert!(text.contains("uc2_snapshot_hash_mismatch{service=\"kv\",row=\"0\"} 1"), "{text}");
    }
```

(Read the neighbouring test for the fixture's real name and the render entry point; substitute them. The `METRIC_NAMES` registry test will also fail until the three names are added.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p uc_node --lib obs::metrics 2>&1 | tail -8`
Expected: the new test fails (no such series); the registry test fails once the names appear in output before the registry — either order, both go green in Step 4.

- [ ] **Step 3: Implement**

`ServiceRow` gains `upgrade_origin: u64, pinned_version: u64, hash_mismatch: u64`; the builder reads `slot.status.upgrade_origin()`, `slot.status.pinned_version() as u64`, and — computed ONCE before the loop as `let inner = s.cluster_view.snapshot_inner();` — `inner.reports.iter().find(|r| r.row == id).map(|r| verdict(r).minority.len() as u64).unwrap_or(0)`. Three `push_service_labeled` calls after `uc2_timers_pending` with HELP strings:

- `uc2_upgrade_pin_origin`: "FSM upgrade lifecycle (spec §2.5): the row's pinned origin position from the newest committed UpgradePin, as republished in the cnc status line by the uc2-cluster agent; 0 = no pin. Identical on every node once caught up."
- `uc2_upgrade_pin_version`: "The packed version the row's newest UpgradePin names (`to`); 0 = no pin. A service whose VERSION differs is refused at attach (plan B2)."
- `uc2_snapshot_hash_mismatch`: "Nodes whose artifact hash for the row's newest reported instant differs from the majority's (spec §6.5.2) — recomputed from the committed SnapshotReport at scrape; 0 = agreed or no majority to differ from. Alert: Uc2SnapshotHashDiverged."

Add the three names to `METRIC_NAMES` under a `// FSM upgrade lifecycle (plan B1)` comment.

`uc2-alerts.yml`, after `Uc2SnapshotSetDiverged`:

```yaml
  - alert: Uc2SnapshotHashDiverged
    # FSM upgrade lifecycle spec §6.5.2: every instance of a row applied the
    # same log from the same origin, so their artifacts at one instant must
    # hash identically. The cluster FSM holds the leader's collected vector
    # and every node recomputes the verdict at scrape; a non-zero count is
    # a NAMED minority node whose state diverged — nondeterminism in apply,
    # or a non-canonical image — not a transient. Critical: it is the live
    # form of diff replay's `determinism` mode failing.
    expr: max by (service, row) (uc2_snapshot_hash_mismatch) > 0
    for: 60s
    labels: { severity: critical }
    annotations: { summary: "a node's snapshot artifact for row {{ $labels.row }} hashes differently from the majority's at the newest reported instant — run `uc2ctl upgrade show` for the minority node id, then `uc2-diffreplay determinism` on that row's corpus" }
```

`m10_alerts.rs`: add `"snapshot_hash_diverged"` to the scenario list and dispatch, and:

```rust
fn scenario_snapshot_hash_diverged() -> (SeriesFile, Disclosure) {
    let src = synthetic_sources(0);
    // One committed SnapshotReport for row 0 at 8192: nodes 0 and 1 agree,
    // node 2 does not — the verdict names one minority node.
    let mut st = src.cluster_view.to_state();
    st.reports.push(uc_protocol::v2::upgrade::SnapshotReport {
        row: 0,
        position: 8192,
        hashes: vec![(0, 0xAA), (1, 0xAA), (2, 0xBB)],
    });
    st.applied = 8192;
    src.cluster_view.publish(&st);
    let srv = ObsServer::serve(src.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();
    let mut sf = SeriesFile::new();
    for _ in 0..3 {
        sf.record_round("n0", &scrape(addr), &["uc2_snapshot_hash_mismatch"]);
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();
    (
        sf,
        Disclosure {
            scenario: "snapshot_hash_diverged",
            rules: &["Uc2SnapshotHashDiverged"],
            state: "synthetic",
            method: "one synthetic ObsSource whose cluster view holds a committed SnapshotReport for row 0 \
                     with hashes {0: AA, 1: AA, 2: BB}; the exporter recomputes the verdict at scrape and \
                     renders uc2_snapshot_hash_mismatch{row=\"0\"} = 1 through the real encoder",
        },
    )
}
```

(`synthetic_sources(0)` declares a row — confirm which name it uses at `synthetic_sources_named`, line 381, and keep row 0.)

`m10_alert_fire.sh`: `"Uc2SnapshotHashDiverged": {"severity": "critical", "real": False, "scenario": "snapshot_hash_diverged"},` in `RULES`; the builder:

```python
def build_Uc2SnapshotHashDiverged():
    # FSM upgrade lifecycle spec §6.5.2: a per-row gauge held > 0 — the
    # single-instance hold shape of build_Uc2AgentDead, with the rule's
    # `max by (service, row)` keeping those two labels.
    rows = load_scenario("snapshot_hash_diverged")
    row = select(rows, "uc2_snapshot_hash_mismatch", {"row": "0"})
    # `max by (service, row)` keeps exactly those two labels on the result
    # vector — the `labels_from={"labels": {...}}` idiom
    # build_Uc2ServiceVersionDrift uses for its `by (row)`.
    r = new_rule(
        "critical",
        labels_from={"labels": {"service": row["labels"]["service"], "row": row["labels"]["row"]}},
    )
    add_hold_last(r, row, "uc2_snapshot_hash_mismatch", 60)
    r["eval_time"] = total_for(60)[0]
    return r
```

Add `"Uc2SnapshotHashDiverged": build_Uc2SnapshotHashDiverged,` to `RULE_BUILDERS`.

Docs: a row in `monitor-a-cluster.md`'s alert table (`| Uc2SnapshotHashDiverged (FSM upgrade lifecycle, 2.13.0) | a node's artifact hash for a row's newest reported instant differs from the majority's, for 60s | critical |`), rows for the three gauges where `uc2_timers_pending` is described, and rows for the events `upgrade_pin_applied` (info: `position`, `row`, `from`, `to`, `origin`), `upgrade_pin_refused` (warn: `node`, `reason` 52–58) and `snapshot_hash_diverged` (warn: `row`, `position`, `node`, `majority_hash`) in the event table beside `settings_apply_refused`; `cluster_command_applied`'s `kind` legend gains `4` UpgradePin / `5` SnapshotReport. `uc2-runbook.md`: one sentence beside the `uc2_timers_pending` paragraph naming the three gauges and `uc2ctl upgrade show`.

- [ ] **Step 4: Run the tests, then the fire script's rule check**

Run: `cargo test -p uc_node --lib obs::metrics 2>&1 | tail -5`
Expected: pass, registry included.

Run: `promtool check rules packaging/prometheus/uc2-alerts.yml` (if `promtool` is on PATH or at `~/.local/bin/promtool`; otherwise say so in the report) — Expected: `SUCCESS: 24 rules found`.

Run: `cargo build -p uc_node --release --example m10_alerts 2>&1 | tail -2` — Expected: `Finished`. (The full `scripts/m10_alert_fire.sh` run builds and breaks real clusters; run it if the box has promtool and 10 minutes, and report the `Uc2SnapshotHashDiverged` line; otherwise report it as not run.)

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/obs/metrics.rs packaging/prometheus/uc2-alerts.yml scripts/m10_alert_fire.sh uc_node/examples/m10_alerts.rs docs/how-to/monitor-a-cluster.md docs/ops/uc2-runbook.md
git commit -m "observability: uc2_upgrade_pin_origin/version, uc2_snapshot_hash_mismatch, Uc2SnapshotHashDiverged with fire-script scenario (plan B1 T9)"
```

---

### Task 10: The reference sweep, the explainer, the spec's as-built errata, the whole-workspace proof

**Files:**
- Modify: `docs/reference/wire-protocol.md` (`## Version` ~9-45; the `CLUSTER` kind table ~312-348), `docs/reference/limits.md`, `docs/reference/semver-policy.md`, `docs/how-to/upgrade-a-cluster.md` (every `0.8.0` / `3.2` statement that means "the shipped current"), `docs/reference/instance-directory.md` (`upgrade.pending` beside `settings.pending`, if that file lists the staged files — check), `docs/notes/uc2-cluster-fsm-explained.md` (a new section "Pins and reports"), `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` (a new "Errata (plan B1, as built)" block directly under the §2.5 heading, listing the seven errata from this plan's header verbatim), `docs/VERIFICATION.md` (the fuzz row is Task 2's; add a line to the cluster-FSM proof paragraph naming the new unit tests)
- Test: the full local proof stack

- [ ] **Step 1: The sweep**

Run: `grep -rn "0\.8\.0\|cnc 3\.2\|3\.2\`\|three kinds\|kinds \`1\`" docs/reference docs/how-to README.md QUICKSTART.md | grep -v "^docs/reference/wire-protocol.md.*was\|through \`0.8.0\`"` and rewrite each hit that states the CURRENT wire/cnc version or "three kinds" to `0.9.0` / `3.3` / "five kinds". Statements about history ("`0.8.0` added kinds 24/25") stay.

`wire-protocol.md`: the `## Version` section gets a `0.9.0` entry in the shape of the `0.8.0` one ("two `CLUSTER` kinds, 4 and 5, no layout change; a 0.8.0 peer applies either as undecodable and its cluster FSM silently diverges — stop every node before starting any"); the kind table's `kind` row reads `1` = Membership, `2` = ScheduleTable, `3` = Settings, `4` = UpgradePin, `5` = SnapshotReport; two payload rows:

```
| `4` UpgradePin | **20 B** exactly: `row u8 @0 ‖ reserved [u8; 3] @1 ‖ from u32 @4 ‖ to u32 @8 ‖ origin u64 @12`. `row < 8`, `origin > 0`, reserved zero. An EVENT ("at `origin`, `row` went `from` → `to`"), kept as a per-row history of at most 4 in the cluster FSM and republished into the row's cnc status line (`+16`/`+24`) | `uc_protocol::v2::upgrade` |
| `5` SnapshotReport | `row u8 @0 ‖ count u8 @1 ‖ reserved [u8; 6] @2 ‖ position u64 @8 ‖ count × (node_id u32 ‖ hash u64)`, `1 ≤ count ≤ 8` → **16–112 B**, node ids strictly increasing. The leader's collected per-node artifact hashes for `(row, position)`; the verdict (all equal / majority names minority / no majority) is recomputed by every reader, never carried | `uc_protocol::v2::upgrade` |
```

and the "largest of the three is the table" sentence becomes "the largest of the five is still the table".

`uc2-cluster-fsm-explained.md`: a section "Pins and reports (2.13.0)" — what a pin is, why it is an event and not a setting, the door/replicated split of its refusals with the numbers, the cnc words and their ordering, retention keeping the origin, what a SnapshotReport holds and why the verdict is recomputed, the N = 2 case, and that until plan B3 nothing produces a report.

- [ ] **Step 2: The proof stack**

Run, in order, and paste each tail into the report:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc_crashtest --features hard-crash-tests --all-targets -- -D warnings
cargo clippy -p uc_lincheck --features replay-bin --all-targets -- -D warnings
cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p kv --bin kv-service
cargo test --workspace 2>&1 | grep -E "^test result|FAILED|panicked" | sort | uniq -c
cargo test -p uc_node --test lin_v2
(cd fuzz && cargo +nightly fuzz build)
```

Expected: fmt and both clippy runs clean; every `test result: ok`; `lin_v2` passes; the fuzz crate builds. If `cargo test --workspace` hits the known `uc_node` log-sink capture flake (an assertion on empty captured text — `docs/…/pre-existing flakes` in memory), re-run that one test alone and report both outcomes.

- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs: wire 0.9.0 / cnc 3.3 sweep, CLUSTER kinds 4/5, pins-and-reports explainer, spec errata as built (plan B1 T10)"
```

---

## Self-review (run after writing; findings fixed inline)

**Spec coverage.** §2.5 UpgradePin: payload (T1), kind (T2), history bound + image (T3/T4), cnc words (T5/T6), `uc2ctl upgrade pin/show` (T8), audited (T7), gauges (T9), refusals by name (T7/T8), leader-only single-in-flight (T7), "a below-floor joiner holds the pin before its service attaches" (the artifact carries it — T4; the agent republishes on `install_from` — T6). §2.5/§6.5.2 SnapshotReport: payload + verdict (T1), kind (T2), applied at commit with the verdict in FSM state — **as built: the state holds the report, the verdict is a pure function** (T4), gauge + obs event + alert (T6/T9). §3 S4 steps 2–3: T7/T6. §6.5.2 items 1–3 (hash at stream time, the datagram, the leader's append): **not this plan — B3**; item 5's "metrics-only complement" (a per-node `uc2_snapshot_hash` gauge): **B3**, since no node computes a hash yet. §9.1(2) `ULTSNAP2`: **B2**. Retention of pinned origins: not in the spec text, required by S4 step 4's install — added (T7).

**Placeholder scan.** A first draft of T6 carried `/* copy the fixture */` markers and a `position_of_report_just_applied` placeholder in `note_applied`; both are gone — T6 now has an `agent_over` helper written out from the model test's eleven-argument construction and passes the payload's row byte instead. T7's follower-retry stub was dropped (no model test exists; the arm is shared with ops 6/7) and its retention test is written out against `prune_snapshots_below`. T9's builder uses `new_rule`'s real `labels_from={"labels": {...}}` idiom. Two soft spots remain and are labelled as such: T5's `test_meta()` and T9's `render(&sources)` name the neighbouring test's fixture, which the implementer reads first.

**Type consistency.** `UpgradePin { row: u8, from: u32, to: u32, origin: u64 }` everywhere; `SnapshotReport { row, position, hashes: Vec<(u32, u64)> }` everywhere; `store_pin(origin: u64, version: u32)` (T5) matches T6's call `store_pin(p.origin, p.to)`; `read_committed_upgrade` returns `Option<(u64, Vec<UpgradePin>, Vec<SnapshotReport>)>` in T6 and T8; `apply_upgrade_pin(id: u32, ip: u32, port: u16) -> (u32, u32, u64)` in T7 and its dispatch; `prune_snapshot_dir(dir, suffix, below, keep: &[u64])` in T7 only; reason constants 52–59 in T7, strings 52–59 in T8, `ClusterRefusal` codes 53/55/59 in T4.
