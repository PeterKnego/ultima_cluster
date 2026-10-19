# Committed Cluster Wire Level Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the cluster one committed, monotone wire level so that a `2.16.0` node can run beside a `2.15.0` node, and a cluster upgrades by restarting one node at a time.

**Architecture:** The level is two bytes in the replicated Settings record (v7), kept `max` by the cluster FSM like `datagram_mtu`. Every datagram's unused header byte at offset 13 carries the sender's `WIRE_MAX`; the receiver refuses out-of-range bytes by name, and the leader's consensus agent keeps a per-member table from the durable-report event and raises the level (auto) or admits `uc2ctl wire raise` (manual) only when every member in the committed membership reports at or above the next level. The cluster agent fail-stops a node whose binary range excludes the committed level. Every encoder writes at the committed level.

**Tech Stack:** Rust 1.96 pinned / MSRV 1.89, the existing `uc_protocol` / `uc_net` / `uc_node` / `uc_ctl` crates, libFuzzer via `cargo-fuzz`, the multi-process `uc_crashtest` harness, Python 3 for the fleet driver.

**Spec:** `docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md` — read it first. Task 0 adds its errata block; **read the errata before the body**.

**Worktree:** `~/ultima/uc-wire-level`, branch `design/wire-level`, off `origin/main` at `b361eb29`. Private target dir: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level` for every cargo command in this plan.

## Global Constraints

- Release `2.15.0`. Wire `0.11.0` → `0.12.0` (`CURRENT = ProtocolVersion::new(0, 12, 0)`). cnc `3.4` → `3.5` (`CNC_V2_VERSION = (3 << 24) | (5 << 16)`).
- `WIRE_LEVEL_FIRST = 12`. `WIRE_MAX = CURRENT.minor()` = 12. `WIRE_MIN = WIRE_MAX - 1`, except `WIRE_MIN = 12` while `WIRE_MAX == WIRE_LEVEL_FIRST`. `MIN_COMPATIBLE = ProtocolVersion::new(0, WIRE_MIN, 0)`.
- Settings v7: `SETTINGS_VERSION = 7`, `SETTINGS_LEN = 49`, `SETTINGS_LEN_V6 = 47`, `wire_level u8 @47`, `wire_raise u8 @48`. `WIRE_RAISE_KEEP = 0`, `WIRE_RAISE_AUTO = 1`, `WIRE_RAISE_MANUAL = 2`. A v1–v6 record decodes as `wire_level = 12`, `wire_raise = 1`. Genesis seeds `wire_level = WIRE_MAX`, `wire_raise = WIRE_RAISE_AUTO`.
- Datagram header byte at offset 13 is renamed `wire` and carries the sender's `WIRE_MAX`. A receiver accepts `wire` in `[WIRE_MIN, WIRE_MAX + 1]`; refuses `0` as `prelevel` and anything else as `out_of_range`, counted.
- cnc words: `CNC_OFF_WIRE_LEVEL = 3992` (cluster agent, at apply), `CNC_OFF_WIRE_BINARY = 4000` (node, boot-once, value `(min << 8) | max`), peer slot `CNC_PEER_OFF_WIRE_MAX = 8` (consensus agent, on change).
- Admin op `ADMIN_OP_WIRE_RAISE = 12`, audited `wire_raise`. Refusal `REASON_WIRE_HOLDOUT = 63`, name `wire_holdout`. A follower answers status 2 (retry), never forwards.
- Settings-file key: `wire_raise = "auto" | "manual"` (flat, like every other key). `wire_level` never appears in a file.
- Fail-stop names, exact strings: `WireLevelAboveBinary`, `WireLevelBelowBinary`, `ClusterRecordUndecodable`.
- `NEW_TERM` body: `NEW_TERM_BODY_LEN = 8`, `wire_level u32 LE @0 ‖ reserved u32 @4`, written as zeros, never read.
- Metrics: `uc2_wire_level`, `uc2_wire_binary_min`, `uc2_wire_binary_max`, `uc2_wire_member_max{peer}`, `uc2_wire_datagrams_refused_total{reason}`. `CONTRACT_SERIES.len()` goes 149 → 154. Alert `Uc2WireLevelBelowMembers`, `for: 30m`.
- Feature `wire-level-override` on `uc_node` (off by default): `StartOpts.wire: Option<WireRange>` always exists; the env override `UC2_WIRE_RANGE=min,max` is honoured only when the feature is on.
- **Never** add a `Co-Authored-By` or any attribution trailer to a commit. Before every push: `cargo fmt --all -- --check`, `python3 scripts/check_doc_links.py`, every clippy line in `.github/workflows/ci.yml`, and `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`.
- Perf numbers are fleet-only. No rate bar is added by this plan.
- `CLAUDE.md` is NOT edited by this plan (it needs the maintainer's OK); Task 15 lists what it will need.

## Review Focus

1. **A `0.11.0` peer's datagram (byte 0) under crypto ON.** Expected: refused and counted as `prelevel` after the AEAD open, never before a handshake is dropped silently. Pinned by Task 4's `a_prelevel_byte_is_refused_under_crypto_too`.
2. **A settings file written by `2.14.0` applied after the roll.** Expected: `wire_level` and `wire_raise` unchanged (both decode/parse as keep). Pinned by Task 3's `a_keep_record_changes_neither_wire_field` and Task 8's `a_file_without_wire_raise_keeps_the_mode`.
3. **A learner that has not reported in the leader's term.** Expected: the raise is blocked and `wire show` names it as `no report`; a removed learner stops blocking. Pinned by Task 7's `wire_raise_target_blocks_on_a_silent_learner` and Task 12's `a_removed_holdout_unblocks_the_raise`.
4. **An artifact installed over the snapshot session whose Settings carry a level above this binary.** Expected: the cluster agent fail-stops `WireLevelAboveBinary` at install, not after serving. Pinned by Task 6's `install_from_enforces_the_level`.
5. **`NEW_TERM` positions in every harness that hard-codes 32.** Expected: a term-opening frame is now 64 bytes and every position derived from it still lines up. Pinned by Task 9's updated `uc_log::reader` test and the `uc_node/tests/failover.rs` constant.

---

### Task 0: Spec errata and worktree facts

**Files:**
- Modify: `docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md` (insert after the header block, before `## 1.`)

**Interfaces:**
- Produces: the errata every later task argues from.

- [ ] **Step 1: Insert the errata block**

```markdown
#### Errata (plan, as discovered 2026-10-19) — read before the body

Exploration of `b361eb29` for the implementation plan corrected seven
details of the chat design. The plan implements these, not the body.

1. **§3.4, the unknown-kind drop is not in the receiver.** Replication stores
   frames as bytes; the silent `42 … applied advances` lives in
   `ClusterFsm::apply` (`uc_node/src/cluster_fsm.rs`). The fail-stop
   `ClusterRecordUndecodable` therefore sits in the **cluster agent**, which
   checks a `CLUSTER` record decodes at the committed level before handing
   it to the pure FSM. The FSM's own `42` path is untouched.
2. **§3.2 / §5.1, the receiver writes no peer slot.** The receiver knows a
   sender only by socket address when crypto is off, and `NetEvent` carries
   no header byte. The byte rides on `NetEvent::Report` (the
   `APPEND_POSITION` durable report every follower and learner already
   sends the leader), and the **consensus agent** keeps the table and writes
   the slot word, exactly as it does `reported_durable`.
3. **§5.1, the peer slot has no free word.** It is four 64-byte padded cells.
   `wire_max` goes at `+8` by widening the first cell's struct in
   `uc_log::cnc::PeerSlot`; same 256-byte stride, same writer as
   `id_and_role`.
4. **§4.1, nothing resets the band on a leadership change today.** "Reported
   in this term" is implemented as one map clear in `Action::BecomeLeader`.
5. **§4.3, `tier forget` is not forwarded.** A follower answers status 2 and
   `uc2ctl` re-sends to the leader hint. `wire raise` does the same.
6. **§2.2 / §5.2, the settings file is flat** (`uc_ctl/src/settings.rs`,
   `deny_unknown_fields`). The key is `wire_raise = "auto" | "manual"`, not
   a `[wire]` table.
7. **§5.1, the cnc compatibility sentence is backwards.** `version_compatible`
   accepts an OLDER page, so a 3.5 attacher reads a 3.4 page and a 3.4
   attacher refuses a 3.5 page. That is the standing "restart a host's
   processes together" rule for a cnc minor bump; it is per host, so it does
   not break a roll, and the how-to says so.

Also: `PathBelowCommittedMtu` is a fail-stop panic string in the consensus
agent, not an enum; the two level refusals use the same idiom in the cluster
agent (daemon exit 1, name on stderr), and the test-only range override
lives on `StartOpts` (which has `Default`), not `NodeConfig` (which has ~150
literal call sites).
```

- [ ] **Step 2: Run the link checker and commit**

Run: `cd ~/ultima/uc-wire-level && python3 scripts/check_doc_links.py | tail -1`
Expected: `0 error(s)`

```bash
git add docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md
git commit -m "docs: wire-level spec errata from the plan's exploration"
```

---

### Task 1: `uc_protocol::version` — the binary's range

**Files:**
- Modify: `uc_protocol/src/version.rs` (constants at :101-102, changelog :25-100, tests :104-140)
- Modify: `uc_protocol/src/lib.rs:26` (re-export)

**Interfaces:**
- Produces: `WIRE_LEVEL_FIRST: u8`, `WIRE_MAX: u8`, `WIRE_MIN: u8`, `WireRange { min: u8, max: u8 }` with `WireRange::BINARY`, `contains(level) -> bool`, `accepts_peer(byte) -> bool`, `pack() -> u64`, `unpack(u64) -> WireRange`. Re-exported from `uc_protocol` root beside `MIN_COMPATIBLE`.

- [ ] **Step 1: Write the failing tests** (append inside `mod tests` in `version.rs`)

```rust
    #[test]
    fn current_is_the_wire_level_flag_day() {
        assert_eq!(CURRENT, ProtocolVersion::new(0, 12, 0));
        assert_eq!(WIRE_LEVEL_FIRST, 12);
        assert_eq!(WIRE_MAX, 12);
        // 0.12.0 is the only release whose min equals its max: nothing
        // below it is level-aware.
        assert_eq!(WIRE_MIN, 12);
        assert_eq!(MIN_COMPATIBLE, ProtocolVersion::new(0, WIRE_MIN, 0));
    }

    #[test]
    fn wire_range_contains_and_accepts_one_above() {
        let r = WireRange { min: 12, max: 13 };
        assert!(r.contains(12) && r.contains(13));
        assert!(!r.contains(11) && !r.contains(14));
        // a peer may be ONE level above me (it is rolling ahead) …
        assert!(r.accepts_peer(12) && r.accepts_peer(13) && r.accepts_peer(14));
        // … never a pre-level peer (0), below my min, or two above.
        assert!(!r.accepts_peer(0) && !r.accepts_peer(11) && !r.accepts_peer(15));
        assert_eq!(WireRange::BINARY, WireRange { min: WIRE_MIN, max: WIRE_MAX });
    }

    #[test]
    fn wire_range_packs_min_high_max_low() {
        let r = WireRange { min: 12, max: 13 };
        assert_eq!(r.pack(), (12u64 << 8) | 13);
        assert_eq!(WireRange::unpack(r.pack()), r);
        assert_eq!(WireRange::unpack(0), WireRange { min: 0, max: 0 });
    }
```

Delete the old `current_is_the_snapshot_catalog_wire` test (it pins `0.11.0`).

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol version:: 2>&1 | tail -5`
Expected: compile error, `WIRE_LEVEL_FIRST` not found.

- [ ] **Step 3: Implement** (replace lines 101-102 and add the type)

```rust
// 0.12.0 (wire level, spec docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md):
// the datagram header byte @13 carries the sender's WIRE_MAX (0 = a pre-level
// peer, refused by name); Settings v7 adds `wire_level`/`wire_raise`; NEW_TERM
// gains an 8-byte reserved body; cnc 3.5. A 0.11.0 peer's datagrams are
// REFUSED and counted (`prelevel`) — the first flag day a mixed cluster
// notices. From here a wire MINOR bump is a level (rolling), a MAJOR bump a
// flag day.
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 12, 0);

/// The level at which levels began. A pre-level Settings record (v1–v6)
/// decodes as this level, and `0.12.0` is the one release whose `WIRE_MIN`
/// equals its `WIRE_MAX`.
pub const WIRE_LEVEL_FIRST: u8 = 12;
/// The highest wire level this binary speaks: `CURRENT`'s minor.
pub const WIRE_MAX: u8 = CURRENT.minor();
/// The lowest committed level this binary may run at — one level back
/// (spec §1.3 item 4), by const arithmetic, never hand-edited.
pub const WIRE_MIN: u8 = if WIRE_MAX == WIRE_LEVEL_FIRST { WIRE_LEVEL_FIRST } else { WIRE_MAX - 1 };
/// Enforced since 0.12.0: the receiver refuses a datagram whose wire byte
/// is below `WIRE_MIN` (spec §3.2).
pub const MIN_COMPATIBLE: ProtocolVersion = ProtocolVersion::new(0, WIRE_MIN, 0);

/// A binary's wire-level range (spec §2.1). `BINARY` is this build's; a
/// test-only override (`uc_node`'s `wire-level-override` feature) may
/// substitute another so one tree can play both sides of a roll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireRange {
    pub min: u8,
    pub max: u8,
}

impl WireRange {
    pub const BINARY: WireRange = WireRange { min: WIRE_MIN, max: WIRE_MAX };

    /// Spec §2.3: a node runs only while the committed level is inside.
    pub const fn contains(self, level: u8) -> bool {
        self.min <= level && level <= self.max
    }

    /// Spec §3.2: a peer's header byte is accepted in `[min, max + 1]` — one
    /// above is a peer rolling ahead; `0` is a pre-level peer.
    pub const fn accepts_peer(self, byte: u8) -> bool {
        byte != 0 && self.min <= byte && byte <= self.max.saturating_add(1)
    }

    /// The cnc `wire_binary` word: `(min << 8) | max`.
    pub const fn pack(self) -> u64 {
        ((self.min as u64) << 8) | self.max as u64
    }

    pub const fn unpack(word: u64) -> WireRange {
        WireRange { min: ((word >> 8) & 0xFF) as u8, max: (word & 0xFF) as u8 }
    }
}
```

In `lib.rs:26`: `pub use version::{MIN_COMPATIBLE, ProtocolVersion, WIRE_LEVEL_FIRST, WIRE_MAX, WIRE_MIN, WireRange};`

- [ ] **Step 4: Run to verify they pass**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol version:: 2>&1 | tail -5`
Expected: all `version::` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/version.rs uc_protocol/src/lib.rs
git commit -m "uc_protocol: wire 0.12.0, WIRE_MIN/WIRE_MAX and WireRange"
```

---

### Task 2: Settings v7

**Files:**
- Modify: `uc_protocol/src/v2/settings.rs` (constants :9-40, struct :86-127, `genesis_default` :129-146, `encode_settings` :149-161, `decode_settings` :163-231, tests :238-500)
- Modify: every `Settings { .. }` literal in the workspace (grep `purge_requires_tiers:`)

**Interfaces:**
- Consumes: `WIRE_LEVEL_FIRST`, `WIRE_MAX` from Task 1.
- Produces: `Settings::{wire_level: u8, wire_raise: u8}`, `WIRE_RAISE_KEEP/AUTO/MANUAL: u8`, `SETTINGS_LEN_V6 = 47`, `pub const fn settings_version_for(level: u8) -> u32`, `pub fn decode_settings_at(level: u8, buf: &[u8]) -> Option<Settings>`.

- [ ] **Step 1: Write the failing tests** (append to `mod tests`)

```rust
    #[test]
    fn settings_v7_round_trips_the_wire_fields_and_v1_to_v6_read_first_level_auto() {
        assert_eq!((SETTINGS_VERSION, SETTINGS_LEN, SETTINGS_LEN_V6), (7, 49, 47));
        let mut s = Settings::genesis_default();
        s.wire_level = 13;
        s.wire_raise = WIRE_RAISE_MANUAL;
        let mut b = Vec::new();
        encode_settings(&s, &mut b);
        assert_eq!(b.len(), SETTINGS_LEN);
        assert_eq!((b[47], b[48]), (13, 2));
        assert_eq!(decode_settings(&b), Some(s));
        // every earlier version decodes as level 12, auto — from the record
        // alone (ruling R24), so an old image and a genesis walk agree.
        for (ver, len) in [(1, SETTINGS_LEN_V1), (2, SETTINGS_LEN_V2), (3, SETTINGS_LEN_V3),
                           (4, SETTINGS_LEN_V4), (5, SETTINGS_LEN_V5), (6, SETTINGS_LEN_V6)] {
            let mut old = b[..len].to_vec();
            old[0..4].copy_from_slice(&(ver as u32).to_le_bytes());
            let d = decode_settings(&old).unwrap_or_else(|| panic!("v{ver} must decode"));
            assert_eq!((d.wire_level, d.wire_raise), (WIRE_LEVEL_FIRST, WIRE_RAISE_AUTO), "v{ver}");
        }
    }

    #[test]
    fn genesis_seeds_this_binarys_max_and_auto() {
        let g = Settings::genesis_default();
        assert_eq!((g.wire_level, g.wire_raise), (WIRE_MAX, WIRE_RAISE_AUTO));
    }

    #[test]
    fn decode_at_level_refuses_a_record_above_the_level() {
        assert_eq!(settings_version_for(12), 7);
        let mut b = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut b);
        assert!(decode_settings_at(12, &b).is_some());
        // a v8 record (one byte longer, version 8) is above what level 12
        // knows: refused by the level-aware decoder, like any unknown version.
        let mut v8 = b.clone();
        v8.push(0);
        v8[0..4].copy_from_slice(&8u32.to_le_bytes());
        assert!(decode_settings_at(12, &v8).is_none());
        assert!(decode_settings(&v8).is_none());
    }
```

Update the existing pins: every `assert_eq!(SETTINGS_VERSION, 6)` / `== 47` / `(6, 47, …)` becomes 7 / 49; in `decode_is_total_and_refuses_unknown_version_and_target` the unknown version is now `8` (`v8[0] = 8`), and the "trailing byte" case uses a 50-byte buffer. `settings_v6_round_trips_purge_requires_tiers_and_v1_to_v5_read_zero` keeps its body but asserts `b[46] == 3` on a 49-byte record and uses `SETTINGS_LEN_V6` for the v6 downgrade.

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol settings 2>&1 | tail -5`
Expected: compile error, no field `wire_level`.

- [ ] **Step 3: Implement**

Constants (replace the version/length block):

```rust
/// Encoding version, first word of the payload. `7` since the wire-level
/// spec (`wire_level`/`wire_raise`, 2.15.0). `6` was backup-tier 3b, `5`
/// backup-tier 3a, `4` the snapshot lifecycle (`auto_fetch`), `3` the
/// catalog (`retain_sets`), `2` jumbo (`datagram_mtu`), `1` the 2.11.0
/// shape. Every earlier version is still ACCEPTED on decode, because cluster
/// artifacts and committed frames written by earlier releases persist across
/// the upgrade; the fields it lacks decode from the record ALONE (ruling
/// R24): `datagram_mtu = 0`, `retain_sets = 1`, `auto_fetch = true`, backup
/// retention `0`, `purge_requires_tiers = 0`, and since v7
/// `wire_level = WIRE_LEVEL_FIRST`, `wire_raise = WIRE_RAISE_AUTO`.
pub const SETTINGS_VERSION: u32 = 7;
/// The exact encoded length of a version-7 record — no trailing bytes.
/// Version 7 appends `wire_level u8 @47` and `wire_raise u8 @48`.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2 + 1 + 2 + 8 + 1 + 1 + 1; // 49
/// The exact length of a version-6 record, accepted on decode only.
pub const SETTINGS_LEN_V6: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2 + 1 + 2 + 8 + 1; // 47
```

(keep `_V5` … `_V1` as they are.) Add after `FSM_LAG_LOCKSTEP`:

```rust
/// `wire_raise` values (spec §2.2). `0` keeps the committed mode — the
/// "0 = keep" idiom, so a settings file written before the roll cannot
/// flip it; genesis seeds `WIRE_RAISE_AUTO`.
pub const WIRE_RAISE_KEEP: u8 = 0;
pub const WIRE_RAISE_AUTO: u8 = 1;
pub const WIRE_RAISE_MANUAL: u8 = 2;

/// Spec §3.3: the Settings encoding version a leader writes at a committed
/// wire level — the encode-at-the-committed-level table. One row today; a
/// release that adds a Settings field adds a row and bumps the wire minor.
pub const fn settings_version_for(level: u8) -> u32 {
    let _ = level; // every level since WIRE_LEVEL_FIRST writes v7
    7
}
```

Struct: append two fields with docs:

```rust
    /// Wire-level spec §2.2: the committed cluster wire level (the wire
    /// minor every member must speak). `0` = keep; the FSM keeps
    /// `max(committed, incoming)` like `datagram_mtu`. Leader-owned: never
    /// in an operator file. A v1–v6 record decodes as `WIRE_LEVEL_FIRST`.
    pub wire_level: u8,
    /// Wire-level spec §2.2: who raises — `WIRE_RAISE_AUTO` (the leader, as
    /// soon as every member reports the next level) or
    /// `WIRE_RAISE_MANUAL` (`uc2ctl wire raise`). `WIRE_RAISE_KEEP` (0)
    /// keeps the committed mode. A v1–v6 record decodes as auto.
    pub wire_raise: u8,
```

`genesis_default()`: `wire_level: crate::version::WIRE_MAX, wire_raise: WIRE_RAISE_AUTO,`.

`encode_settings`: after `out.push(s.purge_requires_tiers);` add `out.push(s.wire_level); out.push(s.wire_raise);`.

`decode_settings`: extend the tuple with a sixth slot `wire`:
- arms 1, 2, 3: `(…, (WIRE_LEVEL_FIRST, WIRE_RAISE_AUTO))`
- the combined arm becomes `(4, SETTINGS_LEN_V4) | (5, SETTINGS_LEN_V5) | (6, SETTINGS_LEN_V6) | (7, SETTINGS_LEN) => (…, if version == 6 || version == 7 { buf[46] } else { 0 }, if version == 7 { (buf[47], buf[48]) } else { (WIRE_LEVEL_FIRST, WIRE_RAISE_AUTO) })`
- the `Settings { .. }` literal gains `wire_level: wire.0, wire_raise: wire.1`.

Add:

```rust
/// Spec §3.3/§3.4: decode a record as a node at committed `level` must —
/// refusing any version above `settings_version_for(level)`. A correct peer
/// never sends one; an arrival is corruption, and the cluster agent
/// fail-stops on it (`ClusterRecordUndecodable`).
pub fn decode_settings_at(level: u8, buf: &[u8]) -> Option<Settings> {
    if buf.len() < 4 {
        return None;
    }
    let version = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if version > settings_version_for(level) {
        return None;
    }
    decode_settings(buf)
}
```

Then fix every `Settings { … purge_requires_tiers: … }` literal in the workspace (`grep -rn 'purge_requires_tiers:' --include=*.rs`), adding `wire_level: WIRE_LEVEL_FIRST, wire_raise: WIRE_RAISE_AUTO,` (test fixtures) — `uc_ctl/src/settings.rs`'s `parse_settings` is fixed properly in Task 8, so give it `wire_level: 0, wire_raise: 0` for now.

- [ ] **Step 4: Run to verify they pass, and that the workspace still builds**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol settings 2>&1 | tail -5 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo check --workspace --all-targets 2>&1 | tail -3`
Expected: settings tests PASS; `cargo check` clean (the literal sweep is complete when it is).

- [ ] **Step 5: Commit**

```bash
git add -A uc_protocol uc_node uc_ctl uc_diffreplay testing examples
git commit -m "uc_protocol: Settings v7 — wire_level and wire_raise, v1–v6 read 12/auto"
```

---

### Task 3: The cluster FSM keeps the level; the view publishes it

**Files:**
- Modify: `uc_node/src/cluster_fsm.rs` — `validate_replicated` Settings arm (:725-780), the `Settings` apply arm (:923-947), `ClusterView` (:1226, `publish` :1447-1486, `to_state` :1616)
- Test: `uc_node/src/cluster_fsm.rs` `mod tests`

**Interfaces:**
- Consumes: Task 2's fields and constants.
- Produces: `ClusterView::{wire_level: AtomicU8, wire_raise: AtomicU8}`, `ClusterFsm::settings(&self) -> &Settings`, `ClusterRefusal::SettingsBounds("wire_raise")` (47).

- [ ] **Step 1: Write the failing tests** (in `cluster_fsm.rs` `mod tests`, next to `a_replayed_pre_catalog_settings_record_sets_retention_to_one` at ~:3338, reusing its fixture style: a genesis `ClusterFsm`, an `apply` of an encoded `ClusterCommand::Settings`, then read `fsm.settings()`)

```rust
    #[test]
    fn the_level_is_monotone_and_zero_keeps_both_wire_fields() {
        let mut fsm = genesis_fsm(); // the existing helper used by the settings tests
        let mut s = Settings::genesis_default();
        s.wire_level = 13;
        s.wire_raise = WIRE_RAISE_MANUAL;
        apply_settings(&mut fsm, &s, 1024); // existing helper: encode + apply at position
        assert_eq!((fsm.settings().wire_level, fsm.settings().wire_raise), (13, WIRE_RAISE_MANUAL));
        // an operator record (absent keys = 0) cannot lower the level nor flip the mode
        let mut keep = Settings::genesis_default();
        keep.wire_level = 0;
        keep.wire_raise = WIRE_RAISE_KEEP;
        apply_settings(&mut fsm, &keep, 2048);
        assert_eq!((fsm.settings().wire_level, fsm.settings().wire_raise), (13, WIRE_RAISE_MANUAL));
        // a lower explicit level is also kept at 13 (max), like datagram_mtu
        let mut lower = keep;
        lower.wire_level = 12;
        lower.wire_raise = WIRE_RAISE_AUTO;
        apply_settings(&mut fsm, &lower, 3072);
        assert_eq!((fsm.settings().wire_level, fsm.settings().wire_raise), (13, WIRE_RAISE_AUTO));
    }

    #[test]
    fn a_keep_record_changes_neither_wire_field() {
        // Review Focus 2: a 2.14.0-shaped file (v6, no wire bytes) replayed
        // after the roll decodes as (12, auto) — it must NOT lower a raised
        // level, and the FSM's max rule guarantees it.
        let mut fsm = genesis_fsm();
        let mut s = Settings::genesis_default();
        s.wire_level = 13;
        apply_settings(&mut fsm, &s, 1024);
        let mut b = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut b);
        let v6 = { let mut v = b[..SETTINGS_LEN_V6].to_vec(); v[0..4].copy_from_slice(&6u32.to_le_bytes()); v };
        apply_raw_settings(&mut fsm, &v6, 2048); // existing raw-body helper (see v2_settings_body's user)
        assert_eq!(fsm.settings().wire_level, 13);
    }

    #[test]
    fn a_bad_wire_raise_is_refused_47() {
        let fsm = genesis_fsm();
        let mut s = Settings::genesis_default();
        s.wire_raise = 3;
        assert_eq!(
            fsm.validate(&ClusterCommand::Settings(s)).unwrap_err().reason_code(),
            47
        );
    }

    #[test]
    fn the_view_publishes_the_wire_fields_and_to_state_round_trips_them() {
        let mut st = ClusterState::genesis(Settings::genesis_default(), /* membership */ test_config());
        st.settings.wire_level = 13;
        st.settings.wire_raise = WIRE_RAISE_MANUAL;
        let view = ClusterView::new(&st);
        view.publish(&st);
        assert_eq!(view.wire_level.load(Ordering::Acquire), 13);
        assert_eq!(view.wire_raise.load(Ordering::Acquire), WIRE_RAISE_MANUAL);
        assert_eq!(view.to_state().settings, st.settings);
    }
```

If `genesis_fsm` / `apply_settings` / `apply_raw_settings` are named differently in that test module, use the names the existing settings tests use — do not add a parallel set of helpers.

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib cluster_fsm::tests::the_level_is_monotone 2>&1 | tail -5`
Expected: compile error (`wire_level` on the view / `settings()` missing).

- [ ] **Step 3: Implement**

`validate_replicated`, Settings arm, after the `purge_requires_tiers` check:

```rust
                if s.wire_raise > WIRE_RAISE_MANUAL {
                    return Err(ClusterRefusal::SettingsBounds("wire_raise"));
                }
```

Apply arm, beside `keep`:

```rust
                let keep_level = self.state.settings.wire_level.max(s.wire_level);
                // Wire-level spec §2.2: `0` keeps the committed mode.
                let keep_raise = if s.wire_raise == WIRE_RAISE_KEEP {
                    self.state.settings.wire_raise
                } else {
                    s.wire_raise
                };
                …
                self.state.settings = s;
                self.state.settings.datagram_mtu = keep;
                self.state.settings.wire_level = keep_level;
                self.state.settings.wire_raise = keep_raise;
```

Accessor on `ClusterFsm`: `pub fn settings(&self) -> &Settings { &self.state.settings }`.

`ClusterView`: add `pub wire_level: AtomicU8, pub wire_raise: AtomicU8,` beside `snapshot_target`; in `new` and `publish` store `st.settings.wire_level` / `wire_raise` (before `position`, like the others); in `to_state` read them back into the `Settings { .. }` literal.

- [ ] **Step 4: Run the FSM test module**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib cluster_fsm 2>&1 | tail -5`
Expected: PASS (the old-image install tests still pass: the settings blob inside an image is self-versioned, so `CLUSTER_IMAGE_VERSION` stays 6).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/cluster_fsm.rs
git commit -m "uc_node: cluster FSM keeps wire_level max and wire_raise, view publishes both"
```

---

### Task 4: The datagram byte — stamp, refuse, count, report

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (`DatagramHeader` :957-964, `OFF_DGRAM_FLAGS` :107, encoder/decoder :967-990, tests :1182-1250)
- Modify: `uc_net/src/sender.rs` (`SenderConfig` :390, literals :1117-1125, :1472-1480, :2309-2317, :2367-2375)
- Modify: `uc_net/src/receiver.rs` (`NetEvent::Report` :185, `consensus_event` :327, `FollowerConfig` :461, `FollowerStats` :553, `crypto_admit` :1952-2000, `on_datagram` :2242-2260, the seven outgoing literals, `FakeLeader::send` :3809, tests)
- Modify: `uc_node/src/node.rs` (`stage` :14064, `crypto_send` :5588, `feed_net` Report arm :11127-11170)
- Modify: test fixtures listed in the exploration: `uc_net/tests/replication.rs:121`, `uc_net/tests/snapshot_session.rs:341,378`, `uc_node/tests/crypto_adversarial.rs` (5), `uc_node/tests/reconfig.rs` (2), `uc_crypto/src/{seal,handshake,transport}.rs` (9), `fuzz/src/seeds.rs:49,457`

**Interfaces:**
- Consumes: `WireRange` (Task 1).
- Produces: `DatagramHeader.wire: u8` (was `flags`), `OFF_DGRAM_WIRE = 13`, `SenderConfig.wire: WireRange`, `FollowerConfig.wire: WireRange` (both default `WireRange::BINARY` in their `new`), `FollowerStats::{wire_refused_prelevel, wire_refused_out_of_range}: AtomicU64`, `NetEvent::Report { …, wire: u8 }`, `fn stage(kind, position, term, body, wire: u8)` in `node.rs`.

- [ ] **Step 1: Rename the field** — in `datagram.rs` rename `flags` → `wire`, `OFF_DGRAM_FLAGS` → `OFF_DGRAM_WIRE`, doc: "the sender's `WIRE_MAX` (wire-level spec §3.1); `0` = a pre-level peer". Update the literal-bytes test: keep `0x5a` as the test value so the offset pin is unchanged. Then fix every literal the compiler names (`wire: 0` in fixtures that go through a real `FollowerReceiver` must become `wire: uc_protocol::WIRE_MAX`; fixtures that only exercise `uc_crypto` seal/open may keep `wire: 0`).

- [ ] **Step 2: Write the failing receiver tests** (in `receiver.rs` `mod tests`, modelled on `statuses_refused` at :5244 — a `FakeLeader` sending to a real `FollowerReceiver`, then polling a stat)

```rust
    #[test]
    fn a_prelevel_byte_is_refused_and_counted() {
        let (rx, leader) = follower_with_fake_leader(); // the harness the statuses_refused test builds
        leader.send_with_wire(DGRAM_KIND_HEARTBEAT, 0, 1, &[], 0); // byte 0 = a 0.11.0 peer
        wait_for(|| rx.stats().wire_refused_prelevel.load(Ordering::Relaxed) == 1);
        assert_eq!(rx.stats().datagrams_accepted_or_equivalent(), 0, "nothing past the byte check");
    }

    #[test]
    fn one_above_is_accepted_two_above_is_out_of_range() {
        let (rx, leader) = follower_with_fake_leader();
        leader.send_with_wire(DGRAM_KIND_HEARTBEAT, 0, 1, &[], WIRE_MAX + 1);
        leader.send_with_wire(DGRAM_KIND_HEARTBEAT, 0, 1, &[], WIRE_MAX + 2);
        wait_for(|| rx.stats().wire_refused_out_of_range.load(Ordering::Relaxed) == 1);
        assert_eq!(rx.stats().wire_refused_prelevel.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_prelevel_byte_is_refused_under_crypto_too() {
        // Review Focus 1: the byte is AAD, so the check runs on the
        // authenticated header after open; a sealed datagram whose byte is 0
        // must still be refused as prelevel, not opened and used.
        let (rx, peer) = crypto_follower_with_peer(); // the CryptoPeer harness at :5787
        peer.send_sealed_with_wire(DGRAM_KIND_HEARTBEAT, 0);
        wait_for(|| rx.stats().wire_refused_prelevel.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn the_report_event_carries_the_senders_byte() {
        let h = DatagramHeader { position: 64, leadership_term_id: 3, kind: DGRAM_KIND_APPEND_POSITION, wire: 13, key_epoch: 0 };
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + APPEND_POSITION_BODY_LEN];
        write_datagram_header(&mut d, &h);
        match consensus_event(&h, &d, "127.0.0.1:1".parse().unwrap()) {
            Some(NetEvent::Report { wire, durable, .. }) => assert_eq!((wire, durable), (13, 64)),
            other => panic!("{other:?}"),
        }
    }
```

Add a `send_with_wire(kind, position, term, body, wire)` to `FakeLeader` (the existing `send` calls it with `WIRE_MAX`), and the same on `CryptoPeer`. Replace `datagrams_accepted_or_equivalent()` with whatever counter the harness already asserts on for "a datagram got through" (`stats().datagrams` is the obvious one — the refusal must happen before it is incremented; if `datagrams` is counted at `recv_from`, assert on `dropped_malformed`/a DATA-side counter instead and say which in the test).

- [ ] **Step 3: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_net --lib wire_ 2>&1 | tail -5`
Expected: compile error (`wire_refused_prelevel`, `send_with_wire`).

- [ ] **Step 4: Implement**

`FollowerConfig` and `SenderConfig`: add `pub wire: WireRange`, set to `WireRange::BINARY` in `new`. `FollowerStats`: add the two counters with docs ("Wire-level spec §3.2: inbound datagrams whose header byte @13 is `0` (a pre-level peer) / outside `[WIRE_MIN, WIRE_MAX + 1]`").

Receiver helper, beside `note_status_refused`:

```rust
    /// Wire-level spec §3.2: the first thing decided about a datagram. True =
    /// carry on. False = refused and counted; the throttled line names the
    /// peer and the byte, once per `STATUS_REFUSED_LOG_INTERVAL_NS` per peer.
    #[inline]
    fn wire_admit(&mut self, h: &DatagramHeader, from: SocketAddr) -> bool {
        if self.cfg.wire.accepts_peer(h.wire) {
            return true;
        }
        self.note_wire_refused(from, h.wire);
        false
    }

    #[cold]
    fn note_wire_refused(&mut self, from: SocketAddr, byte: u8) {
        if byte == 0 {
            self.stats.wire_refused_prelevel.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.wire_refused_out_of_range.fetch_add(1, Ordering::Relaxed);
        }
        let now = self.now_ns();
        let due = self
            .wire_refused_log
            .get(&from)
            .is_none_or(|&last| now.saturating_sub(last) >= STATUS_REFUSED_LOG_INTERVAL_NS);
        if due {
            if self.wire_refused_log.len() >= STATUS_REFUSED_LOG_MAX_PEERS {
                self.wire_refused_log.clear();
            }
            self.wire_refused_log.insert(from, now);
            let r = self.cfg.wire;
            eprintln!(
                "uc_net: wire_refused: datagram from {from} carries wire level {byte} (0 = a pre-level peer); \
                 this node accepts {}..={} — every node must be within one wire level of the cluster",
                r.min, r.max + 1
            );
        }
    }
```

(`wire_refused_log: HashMap<SocketAddr, u64>` initialised beside `status_refused_log`.) Call sites:
- `on_datagram`, immediately after the `read_datagram_header` at :2250: `if !self.wire_admit(&h, from) { return; }`
- `crypto_admit`, immediately after the decode at :1967: `if !self.wire_admit(&h, from) { return None; }` (this is what catches a pre-level handshake, which never reaches `on_datagram`).

`consensus_event`: `NetEvent::Report { from, term: …, durable: …, durable_term: …, wire: h.wire }`.

Outgoing literals: `wire: self.cfg.wire.max` at the four sender sites and the seven receiver sites. In `node.rs`: `fn stage(kind: u8, position: u64, term: u32, body: &[u8], wire: u8) -> Vec<u8>` with `wire` written into the header; both callers pass `self.wire.max` (the `wire: WireRange` field on `Consensus` is added in Task 6 — until then pass `uc_protocol::WIRE_MAX` and leave a `// Task 6: self.wire.max` comment; Task 6 removes it). `crypto_send` the same. `feed_net`'s Report arm destructures `wire` and, for now, ignores it with `let _ = wire;` (Task 7 uses it).

- [ ] **Step 5: Run the net and node suites**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_net 2>&1 | tail -5 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --test crypto_adversarial --test reconfig --test smoke 2>&1 | tail -5`
Expected: PASS. A fixture that sends through a real receiver with `wire: 0` fails with `wire_refused_prelevel == 1` — fix the fixture, not the check.

- [ ] **Step 6: Commit**

```bash
git add -A uc_protocol uc_net uc_node uc_crypto fuzz/src
git commit -m "wire: datagram byte 13 carries WIRE_MAX; receiver refuses prelevel/out-of-range by name; Report carries the byte"
```

---

### Task 5: cnc 3.5 — three words

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (`CNC_V2_VERSION` :80 and its doc :48-79, constants near :315, peer-slot block :129-152, offset tests ~:849-907, the `(3 << 24) | (4 << 16)` literals at :648, :968, :1033, :1038)
- Modify: `uc_log/src/cnc.rs` (`PeerSlot` :136-152, accessors near :1145-1157, offset tests :1525-1605, roundtrip tests near :2143)

**Interfaces:**
- Produces: `CNC_OFF_WIRE_LEVEL = 3992`, `CNC_OFF_WIRE_BINARY = 4000`, `CNC_PEER_OFF_WIRE_MAX = 8`; `CncPage::{wire_level() -> u64, store_wire_level(u64), wire_binary() -> u64, store_wire_binary(u64)}`; `PeerSlot.head: PeerHeadLine { id_and_role, wire_max }` with the same `store_release`/`load_acquire` API as `PaddedAtomicU64`.

- [ ] **Step 1: Write the failing tests**

In `uc_protocol/src/v2/cnc.rs`'s offsets test (the function holding the :905 assert), add:

```rust
        assert_eq!(CNC_V2_VERSION, (3 << 24) | (5 << 16));
        assert_eq!(CNC_OFF_WIRE_LEVEL, 3992);
        assert_eq!(CNC_OFF_WIRE_LEVEL, CNC_OFF_PAYLOAD_CEILING + 8);
        assert_eq!(CNC_OFF_WIRE_BINARY, 4000);
        const { assert!(CNC_OFF_WIRE_BINARY + 8 <= CNC_OFF_INGRESS_HOLES_SKIPPED + 64) };
        assert_eq!(CNC_PEER_OFF_WIRE_MAX, 8);
```

and change every `(3 << 24) | (4 << 16)` literal to `(5 << 16)`. In `uc_log/src/cnc.rs`, add to `cnc_offsets_match_protocol_constants` the three equalities, and beside `payload_ceiling_roundtrip_and_offset_pin`:

```rust
    #[test]
    fn wire_words_roundtrip_and_offset_pin() {
        let (cnc, raw) = fresh_page(); // whatever payload_ceiling_roundtrip_and_offset_pin uses
        cnc.store_wire_level(13);
        cnc.store_wire_binary((12u64 << 8) | 13);
        assert_eq!(u64::from_le_bytes(raw[3992..4000].try_into().unwrap()), 13);
        assert_eq!(u64::from_le_bytes(raw[4000..4008].try_into().unwrap()), (12 << 8) | 13);
        assert_eq!((cnc.wire_level(), cnc.wire_binary()), (13, (12 << 8) | 13));
        cnc.peer_slot(2).head.wire_max.store_release(13);
        let base = 1408 + 2 * 256;
        assert_eq!(u64::from_le_bytes(raw[base + 8..base + 16].try_into().unwrap()), 13);
        assert_eq!(std::mem::size_of::<PeerSlot>(), 256);
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol cnc 2>&1 | tail -3`
Expected: compile error.

- [ ] **Step 3: Implement**

`uc_protocol/src/v2/cnc.rs`:

```rust
pub const CNC_V2_VERSION: u32 = (3 << 24) | (5 << 16);
```

with a `3.5` paragraph in the doc block: "3.5 (wire level): `wire_level` at 3992 (cluster-agent-written at apply), `wire_binary` at 4000 (node-written at boot, `(min << 8) | max`), and `wire_max` at peer slot `+8` (consensus-written on change). A 3.4 attacher refuses a 3.5 page — restart a host's processes together, as for every cnc minor."

```rust
/// Wire-level spec §5.1: the committed cluster wire level. Fourth `u64` of
/// the 3968 line. Writer: the `uc2-cluster` agent, when it applies a
/// Settings record (and at install); readers: the sender (encode at the
/// committed level), `uc2ctl status`/`wire show`, `/metrics`.
pub const CNC_OFF_WIRE_LEVEL: usize = 3992;
const _: () = assert!(CNC_OFF_WIRE_LEVEL == CNC_OFF_PAYLOAD_CEILING + 8);
/// Wire-level spec §5.1: this node's binary range, `(WIRE_MIN << 8) |
/// WIRE_MAX`. Writer: the node, once at boot. Lets `uc2ctl` print the
/// NODE's range rather than its own.
pub const CNC_OFF_WIRE_BINARY: usize = 4000;
const _: () = assert!(CNC_OFF_WIRE_BINARY + 8 <= CNC_OFF_INGRESS_HOLES_SKIPPED + 64);
```

In the peer-slot block: `pub const CNC_PEER_OFF_WIRE_MAX: usize = 8;` with the layout comment line `+8  wire_max  u64 = the peer's reported WIRE_MAX (0 = no report this term)  writer: consensus, on change`.

`uc_log/src/cnc.rs`: read `PaddedAtomicU64` (:120-135) and define beside it

```rust
/// The first line of a peer slot: `id_and_role` (boot-once) and, since cnc
/// 3.5, `wire_max` (on change). Both consensus-written, both rarely, so
/// sharing the line costs nothing.
#[repr(C, align(64))]
pub struct PeerHeadLine {
    pub id_and_role: AtomicU64,
    pub wire_max: AtomicU64,
}
```

and change `PeerSlot`'s first field to `pub head: PeerHeadLine`. If `PaddedAtomicU64` exposes `store_release`/`load_acquire` as inherent methods, give `AtomicU64` callers the std calls (`store(v, Ordering::Release)`); the two existing `id_and_role.store_release(..)` sites in `node.rs:5956/5962` become `head.id_and_role.store(.., Ordering::Release)`, and `uc_ctl`'s status reader of `id_and_role` the matching `load(Ordering::Acquire)`. Add `wire_level`/`store_wire_level`/`wire_binary`/`store_wire_binary` beside `payload_ceiling` with the same `ptr_at(..) as *const AtomicU64` shape and offset comments. Update the layout tests at :1559-1580 for the head line.

- [ ] **Step 4: Run both crates' tests, then the attachers that pin a page version**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol -p uc_log 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_client -p uc_ctl 2>&1 | tail -3`
Expected: PASS (`uc_client/tests/engine_synthetic.rs:1238` builds a 3.3 page on purpose and still passes: older is accepted).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs uc_node/src/node.rs uc_ctl/src
git commit -m "cnc 3.5: wire_level @3992, wire_binary @4000, peer-slot wire_max @+8"
```

---

### Task 6: The node knows its range; the cluster agent enforces the level

**Files:**
- Modify: `uc_node/src/node.rs` (`StartOpts` :346, `Node::start_with` :1423 and the boot block near :1675, `Consensus` fields near :4018-4061, the Task 4 `// Task 6` sites)
- Modify: `uc_node/src/cluster_agent.rs` (`ClusterAgent::new` :381, the apply loop :752-770, the second apply site :1028, `install_from` ~:830-880)
- Modify: `uc_node/Cargo.toml` `[features]`
- Create: `uc_node/src/wire.rs`
- Modify: `uc_node/src/lib.rs` (module + re-exports), `uc_node/src/bin/uc2-node.rs` (pass the env override), `testing/uc_crashtest/src/bin/uc_crashtest-node.rs` (same)
- Test: `uc_node/src/wire.rs` unit tests, `uc_node/src/cluster_agent.rs` tests

**Interfaces:**
- Consumes: Task 1 `WireRange`, Task 3 `ClusterFsm::settings()`, Task 5 cnc accessors, Task 2 `decode_settings_at`.
- Produces: `StartOpts.wire: Option<WireRange>`; `uc_node::wire::{range_from_env() -> Option<WireRange>, check_level(level: u8, range: WireRange) -> Result<(), WireFault>, WireFault::{AboveBinary, BelowBinary}}`; `ClusterAgent::new(.., wire: WireRange)` (last parameter); `Consensus.wire: WireRange`; `Node::wire_range(&self) -> WireRange`.

- [ ] **Step 1: Write the failing tests**

`uc_node/src/wire.rs` (new file, with its tests):

```rust
    #[test]
    fn check_level_names_the_side() {
        let r = WireRange { min: 12, max: 13 };
        assert_eq!(check_level(12, r), Ok(()));
        assert_eq!(check_level(13, r), Ok(()));
        assert_eq!(check_level(14, r), Err(WireFault::AboveBinary { level: 14, range: r }));
        assert_eq!(check_level(11, r), Err(WireFault::BelowBinary { level: 11, range: r }));
        assert!(WireFault::AboveBinary { level: 14, range: r }.to_string().contains("WireLevelAboveBinary"));
        assert!(WireFault::BelowBinary { level: 11, range: r }.to_string().contains("install the release at wire level 12"));
    }

    #[test]
    fn env_override_parses_min_comma_max_or_is_none() {
        assert_eq!(parse_range("12,13"), Some(WireRange { min: 12, max: 13 }));
        assert_eq!(parse_range("13,12"), None); // min above max
        assert_eq!(parse_range("x"), None);
    }
```

`cluster_agent.rs` tests (next to the existing agent tests; the agent is constructed in-process with a tempdir, a `LogBuffer`, a cnc page and a `ClusterFsm` — copy the fixture the nearest existing `ClusterAgent::new` test uses):

```rust
    #[test]
    #[should_panic(expected = "WireLevelAboveBinary")]
    fn a_committed_level_above_the_binary_fail_stops_at_apply() {
        let mut agent = agent_fixture(WireRange { min: 12, max: 12 });
        let mut s = Settings::genesis_default();
        s.wire_level = 13;
        append_committed_settings(&agent, &s); // encode + append a CLUSTER kind=3 frame and advance commit
        agent.do_work();
    }

    #[test]
    #[should_panic(expected = "WireLevelBelowBinary")]
    fn a_committed_level_below_the_binary_fail_stops_at_apply() {
        let mut agent = agent_fixture(WireRange { min: 13, max: 14 });
        append_committed_settings(&agent, &Settings::genesis_default()); // level 12
        agent.do_work();
    }

    #[test]
    #[should_panic(expected = "WireLevelAboveBinary")]
    fn install_from_enforces_the_level() {
        // Review Focus 4: an artifact whose Settings carry level 13 installed
        // on a [12,12] binary must stop the node at install.
        let mut agent = agent_fixture(WireRange { min: 12, max: 12 });
        let path = write_cluster_artifact_with_level(&agent, 13); // freeze a ClusterFsm whose settings.wire_level = 13
        agent.install_from(2048, &path);
    }

    #[test]
    #[should_panic(expected = "ClusterRecordUndecodable")]
    fn an_undecodable_cluster_record_fail_stops() {
        let mut agent = agent_fixture(WireRange::BINARY);
        append_committed_cluster_body(&agent, &[8, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3]); // kind 8 does not exist
        agent.do_work();
    }

    #[test]
    fn an_accepted_settings_record_publishes_the_cnc_level_word() {
        let mut agent = agent_fixture(WireRange { min: 12, max: 13 });
        let mut s = Settings::genesis_default();
        s.wire_level = 13;
        append_committed_settings(&agent, &s);
        agent.do_work();
        assert_eq!(agent.cnc().wire_level(), 13);
    }
```

Before writing these, grep `uc_node/tests` and `uc_node/src` for tests that append a deliberately undecodable `CLUSTER` frame and expect `applied` to advance (search `42` near `cluster`); the fail-stop changes their outcome, and each must be re-pointed at the FSM-level test (`cluster_fsm.rs` keeps its 42 tests) or deleted with a one-line commit note.

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib wire:: cluster_agent::tests::a_committed_level 2>&1 | tail -3`
Expected: compile error.

- [ ] **Step 3: Implement**

`uc_node/Cargo.toml`:

```toml
# Wire-level spec §6.2: lets a test or the two-binary crashtest give one
# tree a different wire range (`StartOpts.wire`, env `UC2_WIRE_RANGE=min,max`
# in the daemon and the crashtest node bin). Off in every normal build; the
# env var is ignored unless compiled in.
wire-level-override = []
```

`uc_node/src/wire.rs`:

```rust
//! The binary's wire-level range and the two refusals a committed level can
//! trigger (wire-level spec §2.3). The check is one function so the cluster
//! agent calls it at boot (the recovered artifact), at every accepted
//! Settings apply, and at every artifact install.

use std::fmt;
use uc_protocol::WireRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFault {
    AboveBinary { level: u8, range: WireRange },
    BelowBinary { level: u8, range: WireRange },
}

impl fmt::Display for WireFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            WireFault::AboveBinary { level, range } => write!(
                f,
                "WireLevelAboveBinary level={level} binary={}..{} — this cluster committed wire level \
                 {level} and this binary speaks at most {}: upgrade this node to a release whose wire \
                 minor is at least {level}",
                range.min, range.max, range.max
            ),
            WireFault::BelowBinary { level, range } => write!(
                f,
                "WireLevelBelowBinary level={level} binary={}..{} — this binary rolls from wire level {} \
                 and the cluster is at {level}: install the release at wire level {} first (one level at a \
                 time, stop-all included)",
                range.min, range.max, range.min, level + 1
            ),
        }
    }
}

pub fn check_level(level: u8, range: WireRange) -> Result<(), WireFault> {
    if level > range.max {
        Err(WireFault::AboveBinary { level, range })
    } else if level < range.min {
        Err(WireFault::BelowBinary { level, range })
    } else {
        Ok(())
    }
}

pub(crate) fn parse_range(s: &str) -> Option<WireRange> {
    let (a, b) = s.trim().split_once(',')?;
    let (min, max) = (a.trim().parse::<u8>().ok()?, b.trim().parse::<u8>().ok()?);
    (min <= max && min != 0).then_some(WireRange { min, max })
}

/// `UC2_WIRE_RANGE=min,max`, honoured only under `wire-level-override`.
pub fn range_from_env() -> Option<WireRange> {
    #[cfg(feature = "wire-level-override")]
    {
        return std::env::var("UC2_WIRE_RANGE").ok().and_then(|s| parse_range(&s));
    }
    #[cfg(not(feature = "wire-level-override"))]
    {
        None
    }
}
```

(`parse_range` must stay compiled in both configurations so its test runs without the feature; mark it `#[cfg_attr(not(feature = "wire-level-override"), allow(dead_code))]`.)

`lib.rs`: `pub mod wire;` and `pub use wire::{WireFault, check_level};`. `StartOpts`: add `pub wire: Option<WireRange>` with doc "Wire-level spec §6.2: this node's range; `None` = `WireRange::BINARY`. The daemon fills it from `uc_node::wire::range_from_env()`." In `start_with`: `let wire = opts.wire.unwrap_or(WireRange::BINARY);` then `cnc.store_wire_binary(wire.pack());` beside the `store_payload_ceiling` at :1679; set `SenderConfig.wire = wire`, `FollowerConfig.wire = wire`, `Consensus { wire, .. }`, `ClusterAgent::new(.., wire)`. Replace the Task 4 `// Task 6` sites with `self.wire.max`. `Node::wire_range()` returns it. `uc2-node.rs` and `uc_crashtest-node.rs`: `opts.wire = uc_node::wire::range_from_env();` (the crashtest bin calls `Node::start_with(cfg, StartOpts { wire: …, ..Default::default() })` instead of `Node::start`).

`cluster_agent.rs`:

```rust
    /// Wire-level spec §2.3: the one place the committed level meets the
    /// binary. Publishes the cnc word first so an operator reading the page
    /// after the fail-stop sees the level that stopped the node.
    fn enforce_wire_level(&self, settings: &Settings, position: u64) {
        self.cnc.store_wire_level(settings.wire_level as u64);
        if let Err(fault) = crate::wire::check_level(settings.wire_level, self.wire) {
            crate::obs_event!(Error, "wire_level_fault", position, level = settings.wire_level as u64,
                              binary_min = self.wire.min as u64, binary_max = self.wire.max as u64);
            panic!("cluster fatal (fail-stop): {fault} position={position}");
        }
    }
```

Call it: at the end of `new` with `fsm.settings()` when `start > 0`; in both apply loops right after an ACCEPTED `ClusterKind::Settings` apply (`if accepted && kind == ClusterKind::Settings as u8 { self.enforce_wire_level(self.fsm.settings(), position) }`); in `install_from` after the FSM installed the image. Before each `fsm.apply` add the decode gate:

```rust
            // Wire-level spec §3.4: a CLUSTER record a correct peer could not
            // have sent at the committed level is corruption, not a refusal.
            let level = self.fsm.settings().wire_level;
            if !ClusterFsm::decodable_at(level, payload) {
                crate::obs_event!(Error, "cluster_record_undecodable", position, level = level as u64);
                panic!("cluster fatal (fail-stop): ClusterRecordUndecodable position={position} level={level} — \
                        a CLUSTER record in the committed log does not decode at the committed wire level; \
                        the journal is corrupt or a peer violated the level; restore from backup");
            }
```

with, in `cluster_fsm.rs`:

```rust
    /// True iff `body` is a CLUSTER record this level knows how to decode:
    /// a known kind, a clean prefix, and a payload `decode_command` accepts —
    /// Settings through `decode_settings_at(level, ..)`.
    pub fn decodable_at(level: u8, body: &[u8]) -> bool {
        match read_cluster_prefix(body) {
            None => false,
            Some((ClusterKind::Settings, p)) => decode_settings_at(level, p).is_some(),
            Some((k, p)) => ClusterFsm::decode_command(k, p).is_some(),
        }
    }
```

- [ ] **Step 4: Run the unit tests and the node integration suites that exercise the cluster agent**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --test smoke --test lifecycle --test catalog --test backup_tier 2>&1 | tail -3`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_node testing/uc_crashtest/src/bin/uc_crashtest-node.rs
git commit -m "uc_node: binary wire range on StartOpts; cluster agent enforces the committed level and record decodability"
```

---

### Task 7: The leader's table and the auto raise

**Files:**
- Modify: `uc_node/src/node.rs` — `Consensus` fields (~:4018-4061), `feed_net` Report arm (:11164), `publish_peer_band` (:5952-5987), `rebuild_peer_maps` (:5995-6030), `Action::BecomeLeader` (:13353-13950), the leader-only block (~:5040-5100), beside `maybe_commit_datagram_mtu` (:9015) and `audit_datagram_mtu` (:9458)
- Create: `uc_node/src/wire_raise.rs` (the pure predicate)
- Test: `uc_node/src/wire_raise.rs` tests

**Interfaces:**
- Consumes: Task 4 `NetEvent::Report.wire`, Task 5 `head.wire_max`, Task 3 view atomics, Task 6 `Consensus.wire`.
- Produces: `pub fn wire_raise_target(own_max: u8, committed: u8, members: &[NodeId], self_id: NodeId, reported: &HashMap<NodeId, u8>) -> Result<u8, WireHoldout>`, `pub enum WireHoldout { AtLevel, NoReport(NodeId), Below { id: NodeId, reported: u8 } }` (both `pub` from `uc_node` for `uc2ctl wire show`); `Consensus::{peer_wire: HashMap<NodeId, u8>, maybe_commit_wire_level() -> bool, apply_wire_raise() -> (u32, u32, u64)}`.

- [ ] **Step 1: Write the failing tests** (`uc_node/src/wire_raise.rs`)

```rust
    fn reported(pairs: &[(NodeId, u8)]) -> HashMap<NodeId, u8> { pairs.iter().copied().collect() }

    #[test]
    fn raisable_when_every_member_reports_the_next_level() {
        let r = reported(&[(2, 13), (3, 13)]);
        assert_eq!(wire_raise_target(13, 12, &[1, 2, 3], 1, &r), Ok(13));
    }

    #[test]
    fn at_level_when_nothing_is_above_the_committed_level() {
        let r = reported(&[(2, 12), (3, 12)]);
        assert_eq!(wire_raise_target(12, 12, &[1, 2, 3], 1, &r), Err(WireHoldout::AtLevel));
    }

    #[test]
    fn wire_raise_target_blocks_on_a_silent_learner() {
        // Review Focus 3: a member (learner 4) with no report this term blocks.
        let r = reported(&[(2, 13), (3, 13)]);
        assert_eq!(wire_raise_target(13, 12, &[1, 2, 3, 4], 1, &r), Err(WireHoldout::NoReport(4)));
    }

    #[test]
    fn the_first_holdout_below_target_is_named_with_its_byte() {
        let r = reported(&[(2, 12), (3, 13)]);
        assert_eq!(wire_raise_target(13, 12, &[1, 2, 3], 1, &r), Err(WireHoldout::Below { id: 2, reported: 12 }));
    }

    #[test]
    fn the_target_is_the_minimum_and_at_most_one_above() {
        // self at 14 (impossible under one-level-back, but the function must
        // still answer min): members at 13 → target 13.
        let r = reported(&[(2, 13), (3, 14)]);
        assert_eq!(wire_raise_target(14, 12, &[1, 2, 3], 1, &r), Ok(13));
    }

    #[test]
    fn the_leaders_own_max_counts() {
        let r = reported(&[(2, 13), (3, 13)]);
        assert_eq!(wire_raise_target(12, 12, &[1, 2, 3], 1, &r), Err(WireHoldout::Below { id: 1, reported: 12 }));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib wire_raise 2>&1 | tail -3`
Expected: compile error.

- [ ] **Step 3: Implement**

`uc_node/src/wire_raise.rs`:

```rust
//! Wire-level spec §4.1: the raise predicate, pure so the leader (auto),
//! the admin door (`uc2ctl wire raise`) and `uc2ctl wire show` all answer
//! the same question from the same inputs.

use std::collections::HashMap;
use uc_consensus::election::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireHoldout {
    /// Every member reported and none is above the committed level.
    AtLevel,
    /// A member in the committed membership has not reported this term.
    NoReport(NodeId),
    /// The first member (membership order, self first) below the target.
    Below { id: NodeId, reported: u8 },
}

/// `Ok(target)` iff every member reported and `target = min(own_max,
/// reported…) > committed`. Members are checked in `members` order with
/// `self_id` read from `own_max`, so the holdout named is deterministic.
pub fn wire_raise_target(
    own_max: u8,
    committed: u8,
    members: &[NodeId],
    self_id: NodeId,
    reported: &HashMap<NodeId, u8>,
) -> Result<u8, WireHoldout> {
    let mut target = own_max;
    for &id in members {
        let max = if id == self_id { own_max } else { *reported.get(&id).ok_or(WireHoldout::NoReport(id))? };
        if max == 0 {
            return Err(WireHoldout::NoReport(id));
        }
        target = target.min(max);
    }
    if target <= committed {
        return Err(WireHoldout::AtLevel);
    }
    // Name the first member that is holding the level, so a `Below` holdout
    // is reported even when the min alone would say AtLevel.
    Ok(target)
}
```

Then make `Below` reachable: before the `AtLevel` return, if `target <= committed` find the first member whose max `<= committed` while some other member (or self) reports above `committed`; return `Below { id, reported }` for it, else `AtLevel`. (Write it as: compute `highest = max over members`; if `highest > committed` and `target <= committed`, the holdout is the first member with `max <= committed`.) Adjust the test expectations above to that rule — `the_leaders_own_max_counts` expects `Below { id: 1, reported: 12 }`, which this rule produces.

`Consensus`: `peer_wire: HashMap<NodeId, u8>` beside `peer_reported`; `next_wire_check_ns: u64`. `feed_net` Report arm: `self.peer_wire.insert(id, wire);` (latest wins, no max). `rebuild_peer_maps`: `self.peer_wire.retain(|id, _| live.contains(id));`. `BecomeLeader`: `self.peer_wire.clear();` (erratum 4). `publish_peer_band`: beside `reported_durable`, `self.cnc.peer_slot(i).head.wire_max.store(self.peer_wire.get(id).copied().unwrap_or(0) as u64, Ordering::Release)` only when the value differs from the slot's current one (one `load(Acquire)` compare).

```rust
    /// Wire-level spec §4.2: the auto raise. Same throttle, single-in-flight
    /// and append path as `maybe_commit_datagram_mtu`.
    fn maybe_commit_wire_level(&mut self) -> bool {
        if self.pass_mono_ns < self.next_wire_check_ns {
            return false;
        }
        self.next_wire_check_ns = self.pass_mono_ns + 100_000_000;
        if self.cluster_view.wire_raise.load(Ordering::Acquire) != WIRE_RAISE_AUTO {
            return false;
        }
        let view_position = self.cluster_view.position.load(Ordering::Acquire);
        if self.last_cluster_append > view_position {
            return false;
        }
        let committed = self.cluster_view.wire_level.load(Ordering::Acquire);
        let membership = self.cluster_view.membership();
        let members: Vec<NodeId> = membership.voters.iter().chain(membership.learners.iter()).map(|(id, _)| *id).collect();
        let Ok(target) = crate::wire_raise::wire_raise_target(self.wire.max, committed, &members, self.id, &self.peer_wire) else {
            return false;
        };
        let mut settings = self.cluster_view.to_state().settings;
        settings.wire_level = target;
        let cmd = ClusterCommand::Settings(settings);
        if self.validate_cluster_command(&cmd).is_err() {
            return false;
        }
        match self.append_cluster_frame(&cmd) {
            Ok(position) => {
                crate::obs_event!(Info, "wire_level_proposed", node = self.id as u64, level = target as u64, position, origin = "auto");
                self.audit_wire_level(target, position, "auto");
                true
            }
            Err(_) => false,
        }
    }
```

`audit_wire_level(level, position, origin)` mirrors `audit_datagram_mtu` with op name `wire_raise` and detail `level=13 origin=auto`. Call `maybe_commit_wire_level` in the leader-only block right after `maybe_commit_datagram_mtu` (:5066), same gating. `apply_wire_raise()` (used by Task 8) is the manual twin: no mode check, no throttle, returns `(2, 0, view_position)` when in flight, `(1, REASON_WIRE_HOLDOUT, view_position)` on `Err(_)` (AtLevel included: there is nothing to raise), `(0, 0, position)` on append, with `origin = "manual"` in the event and audit; `lib.rs` gets `pub mod wire_raise; pub use wire_raise::{WireHoldout, wire_raise_target};`.

- [ ] **Step 4: Run**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib wire_raise 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --test failover --test learner 2>&1 | tail -3`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/node.rs uc_node/src/wire_raise.rs uc_node/src/lib.rs
git commit -m "uc_node: leader keeps the per-member wire table and raises the level in auto mode"
```

---

### Task 8: `uc2ctl wire show` / `wire raise`, admin op 12, refusal 63, the settings key

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (after `ADMIN_OP_TIER_FORGET` :197; op test ~:1025)
- Modify: `uc_node/src/node.rs` (`REASON_WIRE_HOLDOUT` beside :712; admin handler after the op-11 block at :11494), `uc_node/src/lib.rs` (re-export), `uc_node/src/audit.rs` (`op_name` ~:172, doc :125-160, test :741)
- Modify: `uc_ctl/src/main.rs` (`Cmd` enum ~:705, dispatch ~:779, `reason_str` :827 and its tests, `run_status` :1372)
- Create: `uc_ctl/src/wire.rs`
- Modify: `uc_ctl/src/settings.rs` (`SettingsFile` :46-58, `parse_settings`, `render_settings_line`, tests)
- Create: `uc_ctl/tests/wire_bin.rs` (modelled on `uc_ctl/tests/tier_forget_bin.rs`)
- Modify: `docs/reference/uc2ctl.md` (new `### \`wire show\`` / `### \`wire raise\`` before `### \`cluster\`` :638; `status` field table :719; refusal row 63 after :1218; the 47 row's field count "Seven" → "Eight")

**Interfaces:**
- Consumes: Task 7 `apply_wire_raise`, `wire_raise_target`, `WireHoldout`; Task 5 cnc words; Task 3 view.
- Produces: `ADMIN_OP_WIRE_RAISE = 12`, `REASON_WIRE_HOLDOUT = 63`, `uc_ctl::wire::{show, raise}`, `SettingsFile.wire_raise: Option<String>`.

- [ ] **Step 1: Write the failing tests**

`uc_ctl/src/settings.rs` tests:

```rust
    #[test]
    fn a_file_without_wire_raise_keeps_the_mode() {
        // Review Focus 2: a 2.14.0 file has no wire_raise → KEEP (0), and the
        // level is never a file key → always 0 (keep).
        let s = parse_settings("retain_sets = 2\n").unwrap();
        assert_eq!((s.wire_level, s.wire_raise), (0, WIRE_RAISE_KEEP));
        assert_eq!(parse_settings("wire_raise = \"manual\"\n").unwrap().wire_raise, WIRE_RAISE_MANUAL);
        assert_eq!(parse_settings("wire_raise = \"auto\"\n").unwrap().wire_raise, WIRE_RAISE_AUTO);
        assert!(parse_settings("wire_raise = \"later\"\n").unwrap_err().contains("wire_raise"));
        assert!(parse_settings("wire_level = 13\n").is_err(), "leader-owned, refused as an unknown key");
    }

    #[test]
    fn settings_show_prints_the_wire_fields_after_purge_requires_tiers() {
        let mut s = Settings::genesis_default();
        s.wire_level = 13;
        s.wire_raise = WIRE_RAISE_MANUAL;
        let line = render_settings_line(4096, &s);
        assert!(line.contains("purge_requires_tiers=0 wire_level=13 wire_raise=manual datagram_mtu="), "{line}");
    }
```

`uc_ctl/src/main.rs`: `reason_str_names_63` asserting `reason_str(63).starts_with("wire_holdout")`. `uc_node/src/audit.rs`: `op_name(12) == "wire_raise"`, `op_name(13) == "unknown"`. `uc_ctl/tests/wire_bin.rs` (one real node, as `tier_forget_bin.rs` does it):

```rust
#[test]
fn wire_show_prints_the_level_and_raise_says_at_level_on_a_solo_cluster() {
    let (dir, _node) = common::start_solo_node(); // the helper tier_forget_bin uses
    let out = uc2ctl(&["wire", "show", "--instance-dir", dir, "--app-id", common::APP_ID]);
    assert!(out.stdout.contains("wire: level=12 binary=12..12 raise=auto"), "{}", out.stdout);
    // a solo cluster at its binary's max has nothing to raise: 63.
    let out = uc2ctl(&["wire", "raise", "--instance-dir", dir, "--app-id", common::APP_ID]);
    assert!(out.stdout.contains("refused: wire_holdout"), "{}", out.stdout);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_ctl settings:: 2>&1 | tail -3`
Expected: FAIL (unknown key `wire_raise`).

- [ ] **Step 3: Implement**

`cnc.rs`:

```rust
/// Wire-level spec §4.3: `uc2ctl wire raise`. Appends the Settings record
/// that raises the committed wire level to the next one, iff every member
/// in the committed membership reports at or above it. `id`/`ip`/`port`
/// unused (0). Leader-only (a follower answers `retry`), single-in-flight;
/// refused `63 wire_holdout` otherwise. Works, idempotently, in auto mode.
pub const ADMIN_OP_WIRE_RAISE: u32 = 12;
```

`node.rs`: `pub const REASON_WIRE_HOLDOUT: u32 = 63;` (doc: "door-only; the holdout is named by `uc2ctl wire show` on the leader and in the audit detail"). Handler, after the op-11 block, same shape:

```rust
        if req.op == ADMIN_OP_WIRE_RAISE {
            let leader = matches!(self.sm.role(), Role::Leader) && self.appender.is_some();
            let (status, reason, version) = if leader {
                self.apply_wire_raise()
            } else {
                (2, 0, self.cluster_view.position.load(Ordering::Acquire))
            };
            let detail = format!("level={}", self.cluster_view.wire_level.load(Ordering::Acquire));
            let (status, reason) = self.audit_admin_detailed(actor.as_deref(), AuditOrigin::Local, audited, status, reason, version, Some(detail.as_str()));
            self.write_admin_reply(req.seq, status, reason, version);
            return;
        }
```

(`apply_wire_raise` puts the holdout in its own audit detail via `audit_wire_level`'s `origin` string, e.g. `holdout=id:2 reported=12`.) `audit.rs`: `12 => "wire_raise"`, doc paragraph "12 `wire_raise`: `id`/`ip`/`port` zero; detail carries `level=` and, on refusal, the holdout."

`uc_ctl/src/settings.rs`: `wire_raise: Option<String>` in `SettingsFile`; in `parse_settings` map `None → WIRE_RAISE_KEEP`, `"auto" → WIRE_RAISE_AUTO`, `"manual" → WIRE_RAISE_MANUAL`, other → `Err(format!("wire_raise: unknown value {other:?} (want \"auto\" or \"manual\")"))`; `wire_level: 0` hard-coded with the same comment `datagram_mtu` has. `render_settings_line`: insert ` wire_level={} wire_raise={}` after `purge_requires_tiers`, mode rendered `auto`/`manual`/`keep`.

`uc_ctl/src/wire.rs`:

```rust
use uc_protocol::v2::cnc::ADMIN_OP_WIRE_RAISE;
use uc_protocol::WireRange;
use crate::CommonArgs;

pub fn raise(common: &CommonArgs) -> anyhow::Result<()> {
    let resp = crate::signed_admin_request(common, ADMIN_OP_WIRE_RAISE, 0, 0, 0, "cluster position")?;
    match resp.status {
        0 => { println!("raised: wire level record appended at position={}", resp.version); Ok(()) }
        1 => {
            println!("refused: {} (cluster position {}) — `uc2ctl wire show` on the leader names the holdout",
                     crate::reason_str(resp.reason), resp.version);
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason))
        }
        2 => { println!("retry: not the leader, or a previous cluster command is still uncommitted (cluster position {}) — re-run against the leader", resp.version); anyhow::bail!("retry: try again") }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let cnc = crate::open_cnc(common)?; // the opener run_status uses
    let level = cnc.wire_level() as u8;
    let binary = WireRange::unpack(cnc.wire_binary());
    let mode = crate::view::show_doc(common).ok().map(|d| d.cluster.settings.wire_raise);
    let mode = match mode { Some(1) => "auto", Some(2) => "manual", Some(_) => "keep", None => "unknown (no cluster view yet)" };
    println!("wire: level={level} binary={}..{} raise={mode}", binary.min, binary.max);
    let leader = cnc.node_flags() & NODE_FLAG_LEADER != 0;
    if !leader {
        println!("members: (this node is a follower; the table is leader-authoritative — run on the leader)");
        return Ok(());
    }
    let mut reported = std::collections::HashMap::new();
    let mut members = vec![cnc.node_id() as NodeId];
    for i in 0..CNC_MAX_PEER_SLOTS {
        let slot = cnc.peer_slot(i);
        let id_and_role = slot.head.id_and_role.load(Ordering::Acquire);
        if id_and_role == 0 { continue; }
        let (id, role) = ((id_and_role >> 8) as NodeId, (id_and_role & 0xFF) as u8);
        let max = slot.head.wire_max.load(Ordering::Acquire) as u8;
        println!("  member id={id} role={} wire_max={}", if role == CNC_PEER_ROLE_LEARNER { "learner" } else { "voter" }, if max == 0 { "no report".to_string() } else { max.to_string() });
        members.push(id);
        if max != 0 { reported.insert(id, max); }
    }
    match uc_node::wire_raise_target(binary.max, level, &members, members[0], &reported) {
        Ok(t) => println!("verdict: raisable to {t}"),
        Err(uc_node::WireHoldout::AtLevel) => println!("verdict: at level (nothing above {level})"),
        Err(uc_node::WireHoldout::NoReport(id)) => println!("verdict: holdout id={id} (no report this term)"),
        Err(uc_node::WireHoldout::Below { id, reported }) => println!("verdict: holdout id={id} reported {reported}"),
    }
    Ok(())
}
```

(If `run_status` opens the page inline rather than through a helper, lift that opening into `crate::open_cnc` and use it from both; `cnc.node_id()` is whatever accessor `status` uses for the node's own id.) `main.rs`: `Wire(WireArgs)` with `WireCmd::{Show(CommonArgs), Raise(CommonArgs)}`, dispatch, `reason_str` arm:

```rust
        // Wire-level spec §4.3: a member is holding the level, or nothing is above it.
        63 => "wire_holdout (a member in the committed membership reports below the next level, has not reported this term, or the cluster is already at every member's level — `uc2ctl wire show` on the leader names it)",
```

`run_status`: after the `ceiling:` line, `println!("wire: level={} binary={}..{}", cnc.wire_level(), b.min, b.max)` with `b = WireRange::unpack(cnc.wire_binary())`.

Docs: `uc2ctl.md` sections for the two verbs (usage line, prose, refusal), the `status` table row `wire`, refusal row 63, the `settings show` sample line, and the 47 row's field list gaining `wire_raise`.

- [ ] **Step 4: Run**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_ctl -p uc_node --lib audit 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_ctl --test wire_bin --test settings_apply_bin --test status_services 2>&1 | tail -3`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cnc.rs uc_node uc_ctl docs/reference/uc2ctl.md
git commit -m "uc2ctl wire show/raise: admin op 12, refusal 63 wire_holdout, wire_raise settings key"
```

---

### Task 9: `NEW_TERM` gets its reserved body

**Files:**
- Modify: `uc_protocol/src/v2/frame.rs` (beside `FRAME_TYPE_NEW_TERM` :36)
- Modify: `uc_log/src/buffer.rs` (`append_new_term` :722-780, its test :1135)
- Modify: `uc_log/src/reader.rs` test :237
- Modify: `uc_node/tests/failover.rs:51` (`const NEW_TERM: u64 = 32;` → 64)
- Modify: `fuzz/src/seeds.rs:906` (`03-new-term` seed)

**Interfaces:**
- Produces: `NEW_TERM_BODY_LEN: usize = 8`, `NEW_TERM_FRAME_LEN: usize = 64` (aligned), `write_new_term_body(buf: &mut [u8])` (zeros).

- [ ] **Step 1: Write the failing tests**

`frame.rs` tests: `assert_eq!((NEW_TERM_BODY_LEN, NEW_TERM_FRAME_LEN), (8, 64));` in `frame_type_codes_are_stable`. `buffer.rs` `append_new_term` test: the header's `length` is `HEADER_LEN + NEW_TERM_BODY_LEN` (40) and the next frame starts at 64; the 8 body bytes are zero. `reader.rs:237`: `vec![(0, FRAME_TYPE_NEW_TERM, 8), (64, FRAME_TYPE_MESSAGE, 4)]`.

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol -p uc_log new_term 2>&1 | tail -3`
Expected: FAIL.

- [ ] **Step 3: Implement**

```rust
/// Wire-level spec §3.5 (0.12.0): `NEW_TERM` carries an 8-byte reserved
/// body, `wire_level u32 LE @0 ‖ reserved u32 @4`, written as ZEROS and read
/// by nobody. It is the carrier a future per-term frame relayout (the
/// assessment's Level 3) stamps, reserved now so that relayout can be a
/// level instead of a flag day. The frame is 64 B aligned instead of 32 B.
pub const NEW_TERM_BODY_LEN: usize = 8;
pub const NEW_TERM_FRAME_LEN: usize = align_frame_len((HEADER_LEN + NEW_TERM_BODY_LEN) as u32) as usize;

pub fn write_new_term_body(buf: &mut [u8]) {
    buf[..NEW_TERM_BODY_LEN].fill(0);
}
```

`append_new_term`: `let total = HEADER_LEN + NEW_TERM_BODY_LEN;`, write the body after the header, commit `total as u32`. `failover.rs`: `const NEW_TERM: u64 = 64;` — then run that suite; every position derived from the constant must still line up (Review Focus 5). Fuzz seed: `header(FRAME_TYPE_NEW_TERM, (HEADER_LEN + 8) as u32)` plus 8 zero bytes.

- [ ] **Step 4: Run**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol -p uc_log 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --test failover --test cold_restart --test lifecycle 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_service 2>&1 | tail -3`
Expected: PASS (apply/replay loops skip `NEW_TERM` by type, body or not).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/frame.rs uc_log/src uc_node/tests/failover.rs fuzz/src/seeds.rs
git commit -m "frame: NEW_TERM carries an 8-byte reserved body (Level 3 carrier)"
```

---

### Task 10: Metrics, the alert, and its fire scenario

**Files:**
- Modify: `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` :39-170, renderers near :1226/:1365/:1796, tests :2393, :2741)
- Modify: `uc_node/src/obs/mod.rs` (`ObsSources` ~:50-125: add `wire: WireRange`)
- Modify: `uc_node/tests/obs_http.rs` (a scrape test modelled on `a_scrape_carries_every_backup_tier_family` ~:549)
- Modify: `packaging/prometheus/uc2-alerts.yml` (append after :507)
- Modify: `scripts/m10_alert_fire.sh` (`RULE_META` :252-291, a `build_Uc2WireLevelBelowMembers`, `RULE_BUILDERS` :872-908)
- Modify: `uc_node/examples/m10_alerts.rs` (`ALL_SCENARIOS` :88, dispatch :233, a `scenario_wire_level_below_members`)
- Modify: `docs/how-to/monitor-a-cluster.md` (family count; a paragraph per series beside `uc2_settings_position` :429)

**Interfaces:**
- Consumes: Task 4 counters, Task 5 cnc words and peer slots, Task 6 `Node::wire_range`.
- Produces: the five series, `ObsSources.wire: WireRange`.

- [ ] **Step 1: Write the failing tests**

`metrics.rs`: bump the count pin to `154`; add

```rust
    #[test]
    fn the_wire_level_family_renders_from_the_page_and_the_binary() {
        let s = synthetic_sources();
        s.cnc.store_wire_level(12);
        s.cnc.store_wire_binary(WireRange { min: 12, max: 13 }.pack());
        s.cnc.peer_slot(0).head.id_and_role.store((2 << 8) | 1, Ordering::Release);
        s.cnc.peer_slot(0).head.wire_max.store(13, Ordering::Release);
        s.receiver.wire_refused_prelevel.fetch_add(2, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(text.contains("\nuc2_wire_level 12\n"), "{text}");
        assert!(text.contains("\nuc2_wire_binary_min 12\n") && text.contains("\nuc2_wire_binary_max 13\n"));
        assert!(text.contains("\nuc2_wire_member_max{peer=\"2\"} 13\n"), "{text}");
        assert!(text.contains("\nuc2_wire_datagrams_refused_total{reason=\"prelevel\"} 2\n"));
        assert!(text.contains("\nuc2_wire_datagrams_refused_total{reason=\"out_of_range\"} 0\n"));
    }
```

`obs_http.rs`: `a_scrape_carries_every_wire_level_family` looping the five names over a scrape of `synthetic_server()`.

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib obs::metrics 2>&1 | tail -3`
Expected: FAIL on the count pin and the new test.

- [ ] **Step 3: Implement**

`CONTRACT_SERIES`: a `// Wire level (spec §5.3)` group of five. Renderers:

```rust
    push_gauge(out, "uc2_wire_level", "The committed cluster wire level (wire-level spec §5.3) — a fleet-wide constant once settled; the wire minor every member must speak.", s.cnc.wire_level());
    let b = WireRange::unpack(s.cnc.wire_binary());
    push_gauge(out, "uc2_wire_binary_min", "The lowest committed wire level this binary runs at (WIRE_MIN).", b.min as u64);
    push_gauge(out, "uc2_wire_binary_max", "The highest wire level this binary speaks (WIRE_MAX); `min by ()` over the fleet is the raise target.", b.max as u64);
    let members: Vec<(String, u64)> = (0..CNC_MAX_PEER_SLOTS).filter_map(|i| {
        let h = &s.cnc.peer_slot(i).head;
        let idr = h.id_and_role.load(Ordering::Acquire);
        (idr != 0).then(|| (format!("peer=\"{}\"", idr >> 8), h.wire_max.load(Ordering::Acquire)))
    }).collect();
    push_labeled(out, "uc2_wire_member_max", "Each member's reported WIRE_MAX as the LEADER sees it (0 = no report this term); zeros on a follower.", "gauge", &members);
    push_labeled(out, "uc2_wire_datagrams_refused_total", "Inbound datagrams refused on their wire byte: `prelevel` (byte 0, a 0.11.0 peer) or `out_of_range` (outside [WIRE_MIN, WIRE_MAX+1]).", "counter", &[
        ("reason=\"prelevel\"".to_string(), s.receiver.wire_refused_prelevel.load(Ordering::Relaxed)),
        ("reason=\"out_of_range\"".to_string(), s.receiver.wire_refused_out_of_range.load(Ordering::Relaxed)),
    ]);
```

(Match `push_labeled`'s actual label-sample shape at `metrics.rs:347`; if the band shows no slots, the family still renders with its `# TYPE` line — follow what `uc2_agent_alive` does for an empty set.)

Alert (append to `uc2-alerts.yml`):

```yaml
  - alert: Uc2WireLevelBelowMembers
    # Wire-level spec §5.4: every binary in the fleet supports a higher
    # wire level than the cluster has committed, and it has stayed that way
    # for 30 minutes. In MANUAL mode this is the reminder that the roll is
    # complete and `uc2ctl wire raise` is due (the rollback window is still
    # open). In AUTO mode it can only mean a member is not reporting or the
    # leader is not raising — `uc2ctl wire show` on the leader names which.
    expr: uc2_wire_level < scalar(min(uc2_wire_binary_max))
    for: 30m
    labels:
      severity: warning
    annotations:
      summary: "{{ $labels.instance }} runs at wire level {{ $value }} while every binary supports a higher one (30m)"
      description: "Run `uc2ctl wire show` on the leader: in manual mode raise with `uc2ctl wire raise`; in auto mode the verdict names the holdout."
```

`m10_alert_fire.sh`: `"Uc2WireLevelBelowMembers": {"severity": "warning", "real": False, "scenario": "wire_level_below_members"},` and

```python
def build_Uc2WireLevelBelowMembers():
    rows = load_scenario("wire_level_below_members")
    level = select(rows, "uc2_wire_level", {})
    r = new_rule("warning", labels_from=level)
    add_hold_last(r, level, "uc2_wire_level", 1800)
    add_hold_last(r, select(rows, "uc2_wire_binary_max", {}), "uc2_wire_binary_max", 1800)
    r["eval_time"] = total_for(1800)[0]
    return r
```

plus its `RULE_BUILDERS` entry. `m10_alerts.rs`: `scenario_wire_level_below_members()` builds `synthetic_sources(0)`, stores `wire_level = 12` and `wire_binary = (12 << 8) | 13` on the page, serves, records `["uc2_wire_level", "uc2_wire_binary_max"]`, discloses `rules: &["Uc2WireLevelBelowMembers"], state: "synthetic"`. Run `scripts/m10_alert_fire.sh` locally (needs `promtool`): the completeness cross-check must pass and the new rule must fire.

`monitor-a-cluster.md`: family count +5, one paragraph per series.

- [ ] **Step 4: Run**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --lib obs 2>&1 | tail -3 && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --test obs_http 2>&1 | tail -3 && scripts/m10_alert_fire.sh 2>&1 | tail -3`
Expected: PASS; the fire script reports every rule fires (24 before, 25 now).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/obs uc_node/tests/obs_http.rs uc_node/examples/m10_alerts.rs packaging/prometheus/uc2-alerts.yml scripts/m10_alert_fire.sh docs/how-to/monitor-a-cluster.md
git commit -m "obs: five uc2_wire_* series and the Uc2WireLevelBelowMembers alert"
```

---

### Task 11: The golden corpus and the `uc_protocol_compat` fuzz target

**Files:**
- Create: `uc_protocol/tests/golden_level12.rs`
- Create: `fuzz/fuzz_targets/uc_protocol_compat.rs`; modify `fuzz/Cargo.toml` (a `[[bin]]` after `uc_node_tier_object`), `fuzz/src/seeds.rs` (two seeds), `fuzz/README.md` (target list)
- Modify: `docs/VERIFICATION.md` (:32 "twenty-six" → "twenty-seven", :1030 heading and table row)

**Interfaces:**
- Consumes: Tasks 1, 2, 4, 9.

- [ ] **Step 1: Write the golden test** (it fails until the hex literals are filled; fill them from the FIRST run's output, then it pins)

```rust
//! Wire-level spec §6.1: the bytes a level-12 binary emits, pinned. From
//! 0.13.0 on this file is what "encode at the committed level" means in
//! practice: a release that changes a level-12 encoding — even by one
//! byte — fails here, which is the regression class the 2.11.0 relayout
//! cost 27 %. Refresh the hex ONLY in the task that bumps the wire minor,
//! and only for the NEW level's file (`golden_level13.rs`), never this one.
use uc_protocol::v2::datagram::{DGRAM_KIND_DATA, DatagramHeader, write_datagram_header, DATAGRAM_HEADER_LEN};
use uc_protocol::v2::frame::{ClusterKind, CLUSTER_BODY_PREFIX_LEN, NEW_TERM_BODY_LEN, write_new_term_body};
use uc_protocol::v2::settings::{Settings, encode_settings, WIRE_RAISE_AUTO};

fn hex(b: &[u8]) -> String { b.iter().map(|x| format!("{x:02x}")).collect() }

#[test]
fn settings_v7_genesis_at_level_12() {
    let mut s = Settings::genesis_default();
    s.wire_level = 12;
    s.wire_raise = WIRE_RAISE_AUTO;
    let mut b = Vec::new();
    encode_settings(&s, &mut b);
    assert_eq!(hex(&b), "<fill from first run: 49 bytes, 98 hex chars>");
}

#[test]
fn datagram_header_data_at_level_12() {
    let h = DatagramHeader { position: 0x40, leadership_term_id: 1, kind: DGRAM_KIND_DATA, wire: 12, key_epoch: 0 };
    let mut b = [0u8; DATAGRAM_HEADER_LEN];
    write_datagram_header(&mut b, &h);
    assert_eq!(hex(&b), "4000000000000000010000000" "1" "0c" "0000"); // position, term, kind 1, wire 12, epoch 0
}

#[test]
fn new_term_body_is_eight_zero_bytes() {
    let mut b = [0xffu8; NEW_TERM_BODY_LEN];
    write_new_term_body(&mut b);
    assert_eq!(hex(&b), "0000000000000000");
}

#[test]
fn cluster_kinds_known_at_level_12() {
    // 1..=7 decode; 8 does not. A kind added by a later level starts at 8.
    for k in 1..=7u8 { assert!(ClusterKind::from_u8(k).is_some(), "{k}"); }
    assert!(ClusterKind::from_u8(8).is_none());
    assert_eq!(CLUSTER_BODY_PREFIX_LEN, 8);
}
```

Write the datagram hex as one literal once computed (the split above is explanatory). The placeholder string in `settings_v7_genesis_at_level_12` is replaced by the real hex in this same task — the committed file has no placeholder.

- [ ] **Step 2: Run, fill, run again**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_protocol --test golden_level12 2>&1 | grep -E 'left|right|test result'`
Expected first run: FAIL printing the actual hex; paste it; second run PASS.

- [ ] **Step 3: The fuzz target**

`fuzz/fuzz_targets/uc_protocol_compat.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::datagram::read_datagram_header;
use uc_protocol::v2::settings::{decode_settings_at, settings_version_for};
use uc_protocol::WireRange;

// Byte 0 is the committed level; the rest is an untrusted record. Two
// properties: no decoder panics, and nothing above the level is accepted.
fuzz_target!(|data: &[u8]| {
    let Some((&level, rest)) = data.split_first() else { return };
    if let Some(s) = decode_settings_at(level, rest) {
        let version = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        assert!(version <= settings_version_for(level), "accepted a v{version} record at level {level}");
        let _ = s;
    }
    if let Some(h) = read_datagram_header(rest) {
        let range = WireRange { min: level.saturating_sub(1).max(1), max: level };
        if range.accepts_peer(h.wire) {
            assert!(h.wire != 0 && h.wire <= level.saturating_add(1));
        }
    }
});
```

`fuzz/Cargo.toml` entry after `uc_node_tier_object` (same five keys). Seeds: `12 ‖ <a v7 genesis record>` and `12 ‖ <a DATA header with wire 12>`. Run `scripts/fuzz_smoke.sh 30 uc_protocol_compat` (needs nightly + cargo-fuzz): must complete with no crash.

- [ ] **Step 4: Docs and commit**

`docs/VERIFICATION.md`: twenty-six → twenty-seven, table row for `uc_protocol_compat` ("level-aware Settings decode + datagram byte: nothing above the level is accepted"). `fuzz/README.md` target list.

```bash
git add uc_protocol/tests/golden_level12.rs fuzz docs/VERIFICATION.md
git commit -m "proof: golden level-12 corpus and the uc_protocol_compat fuzz target"
```

---

### Task 12: In-process cluster tests

**Files:**
- Create: `uc_node/tests/wire_level.rs`

**Interfaces:**
- Consumes: `StartOpts.wire` (Task 6), `Node::cluster_view()` (`wire_level`/`wire_raise` atomics), the cnc admin band for `wire raise` (as `hard_crash.rs`'s `command_instant_via_admin` drives op 8), `ADMIN_OP_WIRE_RAISE`.

- [ ] **Step 1: Build the harness** — read the three-node in-process harness at the top of `uc_node/tests/jumbo.rs` (socket pre-binding, `members`, `Node::start_with`) and write a local wrapper:

```rust
fn start(i: usize, socks: &mut Vec<Option<UdpSocket>>, members: &[(u32, SocketAddr)], dir: &Path, wire: WireRange) -> Node {
    let cfg = node_config(i as u32, members, dir); // copied from jumbo.rs's config builder
    Node::start_with(cfg, StartOpts { socket: socks[i].take(), wire: Some(wire), ..Default::default() }).unwrap()
}

fn committed_level(n: &Node) -> u8 { n.cluster_view().wire_level.load(Ordering::Acquire) }

fn wait_level(nodes: &[&Node], want: u8, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while nodes.iter().any(|n| committed_level(n) != want) {
        assert!(Instant::now() < deadline, "level {want} not reached: {:?}", nodes.iter().map(|n| committed_level(n)).collect::<Vec<_>>());
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn admin_wire_raise(dir: &Path) -> (u32, u32) { /* write AdminReq { op: ADMIN_OP_WIRE_RAISE, .. } like command_instant_via_admin, poll the reply, return (status, reason) */ }
```

- [ ] **Step 2: Write the scenarios**

```rust
#[test]
fn a_holdout_blocks_the_raise_and_its_restart_lands_it() {
    let (members, mut socks, dirs) = three_node_fixture();
    let a = WireRange { min: 12, max: 13 };
    let old = WireRange { min: 12, max: 12 };
    let n0 = start(0, &mut socks, &members, &dirs[0], a);
    let n1 = start(1, &mut socks, &members, &dirs[1], a);
    let mut n2 = start(2, &mut socks, &members, &dirs[2], old);
    await_leader(&[&n0, &n1, &n2], 30);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(committed_level(&n0), 12, "a [12,12] member holds the level");
    drop(n2);
    socks[2] = Some(rebind(members[2].1));
    n2 = start(2, &mut socks, &members, &dirs[2], a);
    wait_level(&[&n0, &n1, &n2], 13, 30);
}

#[test]
fn manual_mode_holds_until_wire_raise_and_the_verb_is_idempotent_in_auto() {
    let (members, mut socks, dirs) = three_node_fixture();
    let a = WireRange { min: 12, max: 13 };
    let nodes = start_all(&members, &mut socks, &dirs, a);
    let leader = await_leader(&nodes, 30);
    apply_settings_via_admin(&dirs[leader], |s| s.wire_raise = WIRE_RAISE_MANUAL); // op 7 through the staged file, as settings_apply_bin does
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(committed_level(&nodes[leader]), 12, "manual holds");
    assert_eq!(admin_wire_raise(&dirs[(leader + 1) % 3]).0, 2, "a follower answers retry");
    assert_eq!(admin_wire_raise(&dirs[leader]).0, 0);
    wait_level(&nodes.iter().collect::<Vec<_>>(), 13, 30);
    assert_eq!(admin_wire_raise(&dirs[leader]), (1, 63), "at level: nothing to raise");
}

#[test]
fn a_removed_holdout_unblocks_the_raise() {
    // Review Focus 3: a learner at [12,12] holds; `remove` it (admin op 4,
    // as reconfig.rs drives it); the raise lands without it.
    …start 2 voters at [12,13] and 1 learner at [12,12]; assert level 12 after 2 s;
    …remove the learner via the admin band; wait_level 13.
}

#[test]
fn a_leader_change_mid_roll_refills_the_table_and_raises() {
    …start all three at [12,13] with the leader's node dropped right after it is elected (before its first 100 ms check lands the raise is unlikely but tolerated: assert the level is 13 on the survivors within 30 s of the new election either way).
}

#[test]
fn a_two_above_binary_refuses_to_start() {
    // Spec §2.3 WireLevelBelowBinary: a [13,14] binary joining a level-12 cluster.
    // The cluster agent panics inside its thread; observe it through the
    // node's agent-finished flag (the same `obs.agents` pair the daemon's
    // fail-stop loop reads) and the obs log line `wire_level_fault`.
    …start n0, n1 at [12,13]; start n2 at [13,14]; wait up to 10 s for n2's cluster agent finished flag.
}
```

Use whatever `Node` exposes for the agent-finished flags (`Node::agents()` / the `ObsSources.agents` pairs); if nothing public exposes them, add `pub fn cluster_agent_finished(&self) -> bool` to `Node` in this task.

- [ ] **Step 3: Run**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_node --test wire_level 2>&1 | tail -8`
Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/wire_level.rs uc_node/src/node.rs
git commit -m "uc_node: in-process wire-level scenarios — holdout, manual, removal, leader change, refusal"
```

---

### Task 13: The two-binary rolling-upgrade crashtest

**Files:**
- Modify: `testing/uc_crashtest/Cargo.toml` (`[features] wire-level-override = ["uc_node/wire-level-override"]`)
- Modify: `testing/uc_crashtest/tests/common/mod.rs` (a `spawn_node_multi_bin`)
- Create: `testing/uc_crashtest/tests/rolling_upgrade.rs`
- Modify: `.github/workflows/nightly.yml` (the hard-crash job runs this test with both features)
- Modify: `docs/VERIFICATION.md` §5 scenario table and the nightly line (:1262)

**Interfaces:**
- Consumes: Task 6's env override in `uc_crashtest-node`, Task 5's `wire_level()` cnc accessor.
- Produces: env `UC2_BASE_NODE_BIN` (optional: a real previous-release node binary; absent → same tree with `UC2_WIRE_RANGE=12,12`).

- [ ] **Step 1: The spawn helper** (in `common/mod.rs`, beside `spawn_node_with`)

```rust
/// Wire-level spec §6.3: spawn a node from `bin` with extra env — the
/// two-binary roll. `bin` is `NODE_BIN` or `UC2_BASE_NODE_BIN`.
pub fn spawn_node_multi_bin(bin: &str, instance_dir: &Path, id: u32, bind: SocketAddr, members: &str, env: &[(&str, &str)]) -> Reap {
    let mut c = Command::new(bin);
    c.args(["--instance-dir", instance_dir.to_str().unwrap(), "--app-id", APP_ID, "--bind", &bind.to_string(), "--id", &id.to_string(), "--members", members, "--services", "register"]);
    for (k, v) in env { c.env(k, v); }
    c.stdout(Stdio::null()).stderr(Stdio::piped());
    Reap(c.spawn().expect("spawn node"))
}
```

(Match the exact flag set `spawn_node_multi` in `hard_crash.rs:1230` passes; move that function's body here if it is private to `hard_crash.rs` so both tests share it.)

- [ ] **Step 2: The test**

```rust
#![cfg(all(feature = "hard-crash-tests", feature = "wire-level-override"))]
mod common;
use common::*;

const OLD: &str = "12,12";
const NEW: &str = "12,13";

fn old_side() -> (String, Vec<(&'static str, &'static str)>) {
    match std::env::var("UC2_BASE_NODE_BIN") {
        Ok(bin) => (bin, vec![]),                 // a real previous release: no override
        Err(_) => (NODE_BIN.to_string(), vec![("UC2_WIRE_RANGE", OLD)]),
    }
}

fn cnc_level(dir: &Path) -> u64 { open_cnc(dir).map(|c| c.wire_level()).unwrap_or(0) }

#[test]
fn rolling_upgrade_one_node_at_a_time_under_load_is_linearizable_and_raises() {
    shorten_client_timeout();
    let tmp = tempdir();
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = addrs.iter().enumerate().map(|(i, a)| (i as u32, *a)).collect();
    let members_str = members_arg(&members);
    let dirs: Vec<PathBuf> = (0..3).map(|i| tmp.path().join(format!("n{i}"))).collect();
    let (old_bin, old_env) = old_side();
    let mut nodes: Vec<Option<Reap>> = dirs.iter().enumerate()
        .map(|(i, d)| Some(spawn_node_multi_bin(&old_bin, d, i as u32, addrs[i], &members_str, &old_env))).collect();
    for d in &dirs { wait_for_ready(d, Duration::from_secs(10)); }
    let mut svcs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();
    await_single_leader_multi(&dirs, 30);
    assert_eq!(cnc_level(&dirs[0]), 12);
    let (history, stop, last_seen) = (Arc::new(History::new()), Arc::new(AtomicBool::new(false)), Arc::new(AtomicU64::new(0)));
    warmup_write_multi(&dirs);
    let handles = spawn_workers_multi(&dirs, &history, &stop, &last_seen, 7, Duration::from_millis(15), 3);
    // Roll: restart nodes 0 and 1 on the new side; SIGKILL node 2 mid-roll
    // and bring it back new — the spec's §6.3 shape.
    for i in [0usize, 1] {
        let old_id = open_cnc(&dirs[i]).map(|c| c.instance_id());
        svcs[i] = None; nodes[i] = None;
        nodes[i] = Some(spawn_node_multi_bin(NODE_BIN, &dirs[i], i as u32, addrs[i], &members_str, &[("UC2_WIRE_RANGE", NEW)]));
        wait_for_fresh_cnc_instance(&dirs[i], old_id, Duration::from_secs(10));
        svcs[i] = Some(spawn_service(&dirs[i]));
        await_single_leader_multi(&dirs, 30);
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(cnc_level(&dirs[i]), 12, "a [12,12] member still holds the level");
    }
    { let old_id = open_cnc(&dirs[2]).map(|c| c.instance_id());
      nodes[2].as_mut().unwrap().0.kill().unwrap(); // SIGKILL, not a drop
      svcs[2] = None; nodes[2] = None;
      nodes[2] = Some(spawn_node_multi_bin(NODE_BIN, &dirs[2], 2, addrs[2], &members_str, &[("UC2_WIRE_RANGE", NEW)]));
      wait_for_fresh_cnc_instance(&dirs[2], old_id, Duration::from_secs(10));
      svcs[2] = Some(spawn_service(&dirs[2])); }
    let deadline = Instant::now() + Duration::from_secs(30);
    while dirs.iter().any(|d| cnc_level(d) != 13) {
        assert!(Instant::now() < deadline, "raise did not land: {:?}", dirs.iter().map(cnc_level).collect::<Vec<_>>());
        std::thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    let entries = Arc::try_unwrap(history).ok().expect("sole owner").into_entries();
    assert_linearizable(&entries, "rolling_upgrade", "run0");
    // And the old binary is now refused by name: start it on node 2's dir.
    svcs[2] = None; nodes[2] = None;
    let mut old = spawn_node_multi_bin(&old_bin, &dirs[2], 2, addrs[2], &members_str, &old_env);
    let status = poll_exit(&mut old.0, Duration::from_secs(20)).expect("the old binary must exit");
    assert_eq!(status.code(), Some(1));
    let mut err = String::new();
    old.0.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    assert!(err.contains("WireLevelAboveBinary"), "{err}");
}
```

(`assert_linearizable`, `spawn_workers_multi`, `warmup_write_multi`, `await_single_leader_multi`, `members_arg`, `free_addr`, `open_cnc`, `wait_for_fresh_cnc_instance` live in `hard_crash.rs` today; move the ones this test needs into `common/mod.rs` in this task rather than duplicating them. `instance_id()` is whatever accessor `wait_for_fresh_cnc_instance` reads.)

- [ ] **Step 3: Run**

Run: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level cargo test -p uc_crashtest --features hard-crash-tests,wire-level-override --test rolling_upgrade 2>&1 | tail -5`
Expected: 1 passed (budget ~2 minutes). Then `cargo test -p uc_crashtest --features hard-crash-tests --test hard_crash` still passes (the helper move).

- [ ] **Step 4: Nightly and docs, commit**

`nightly.yml`: the hard-crash job's command gains `--features hard-crash-tests,wire-level-override` and `--test rolling_upgrade` beside `--test hard_crash`. `docs/VERIFICATION.md` §5: a scenario row and the sentence "From `2.16.0` on, `UC2_BASE_NODE_BIN` points the old side at the previous release's `uc_crashtest-node`."

```bash
git add testing/uc_crashtest .github/workflows/nightly.yml docs/VERIFICATION.md
git commit -m "crashtest: two-binary rolling upgrade under load, SIGKILL mid-roll, old binary refused by name"
```

---

### Task 14: The fleet row driver

**Files:**
- Create: `bench-infra/scripts/wire_roll_gate.py`
- Create: `docs/benchmarks/uc2-wire-level-gate-2026-10-19.md` (the pre-committed row, NOT RUN)

**Interfaces:**
- Consumes: `jumbo_gate.py`'s `render_node_toml`, `build_uc_node`, `start_unit`/`kill_unit`/`truncate_log`, `scrape_prom`, `read_gauge`; `m14_fleet_gate.py`'s `sync_tree_to`, `m6.build_fleet_hosts`.
- Produces: `wire_roll_gate.py --selftest` (pure verdict arithmetic, no ssh) and `--run` (fleet).

- [ ] **Step 1: The pure verdict and its selftest**

```python
ROW_COMMIT_GAP_BAR_SECS = 5.0   # one node's restart: commit may stall while a quorum re-forms, never longer

def verdict_row_roll(level_by_node_final, gaps_secs, linearizable):
    """Functional clauses only (spec §6.4): every node ends at the target
    level, no restart stalled commit past the bar, and the load's history is
    linearizable. No rate bar."""
    clauses = {
        "level": all(v == 13 for v in level_by_node_final.values()),
        "gaps": max(gaps_secs, default=0.0) <= ROW_COMMIT_GAP_BAR_SECS,
        "linearizable": bool(linearizable),
    }
    return ("PASS" if all(clauses.values()) else "FAIL"), clauses

def selftest():
    def check(name, got, want):
        assert got == want, f"{name}: {got!r} != {want!r}"
    check("pass", verdict_row_roll({0: 13, 1: 13, 2: 13}, [1.2, 0.8, 2.9], True)[0], "PASS")
    check("holdout", verdict_row_roll({0: 13, 1: 12, 2: 13}, [1.0], True)[0], "FAIL")
    check("gap", verdict_row_roll({0: 13, 1: 13, 2: 13}, [6.0], True)[0], "FAIL")
    check("lin", verdict_row_roll({0: 13, 1: 13, 2: 13}, [], False)[0], "FAIL")
    print("selftest OK")
```

- [ ] **Step 2: The fleet arm** — one function, `run_roll(hosts, args)`:
  1. `sync_tree_to(hosts, args.tree, m6.SshHost.UC_SRC)`; build `uc2-node` with `--features wire-level-override` (copy `build_uc_node` and add the flag; name it `build_uc_node_override`).
  2. Start three real daemons with `start_jumbo_cluster`'s shape but `env UC2_WIRE_RANGE=12,12` on the unit (`start_unit`'s env parameter, or an `Environment=` drop-in — read `start_unit` in `bench-infra/scripts/m6_fleet_gate.py` to see which it supports), metrics on.
  3. From the fourth host run the load the M14 gate uses for row a (`m12_gate` in its steady window) for the whole roll.
  4. For each node in turn: `kill_unit`, restart with `UC2_WIRE_RANGE=12,13`, wait for `uc2_agent_alive{agent="cluster"} == 1` on it, sample `uc2_commit_position` on the leader at 100 ms and record the longest interval without movement as that restart's gap.
  5. Wait until `read_gauge(scrape_prom(h), "uc2_wire_level") == 13` on all three (60 s budget).
  6. Stop the load, read the linearizability verdict the load harness prints, `verdict_row_roll`, print the table, exit non-zero on FAIL.

- [ ] **Step 3: The gate doc** — pre-committed bars, `NOT RUN`, the command line, the clause table, and the honest-failure protocol sentence copied from `docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md`'s header.

- [ ] **Step 4: Run the selftest and commit**

Run: `python3 bench-infra/scripts/wire_roll_gate.py --selftest`
Expected: `selftest OK`

```bash
git add bench-infra/scripts/wire_roll_gate.py docs/benchmarks/uc2-wire-level-gate-2026-10-19.md
git commit -m "bench-infra: wire_roll_gate.py — the rolling-upgrade fleet row (functional clauses, not run)"
```

---

### Task 15: Documentation

**Files:**
- Create: `docs/notes/uc2-wire-level-explained.md`
- Modify: `docs/how-to/upgrade-a-cluster.md` (a new `## Rolling upgrades since 2.15.0` section after `## Why this is a flag day, not a rolling restart` :8, which is rewritten to say "was, until 2.15.0"; a `## Wire + cnc change in 2.15.0: the wire level (\`0.12.0\`, cnc \`3.5\`)` flag-day section after the 0.11.0 one at :1191)
- Modify: `docs/reference/semver-policy.md` (lead-in :215-243 and a `### The wire level (0.12.0, cnc 3.5) — the last additive flag day` section after :406)
- Modify: `docs/reference/wire-protocol.md` (`## Version` :9, `## Datagram header` :46-54 gains the per-field table with `wire` @13, the `CLUSTER` Settings row :432 gains v7, a `NEW_TERM` body line under `## Log frames`)
- Modify: `docs/notes/uc2-rolling-upgrade-compatibility-assessment.md` (a "Taken up" note at the top pointing at the spec and listing the four corrections from spec §1.2)
- Modify: `docs/ops/uc2-runbook.md` (cnc decode: the three new words; the two fail-stop names beside `PathBelowCommittedMtu`)
- Modify: `docs/how-to/cut-a-release.md` (one checklist line: "a release that bumps the wire minor: one gate, delete the previous gate, refresh the golden corpus, `UC2_BASE_NODE_BIN` for the crashtest")
- Modify: `scripts/uc2_flag_day.sh` header comment ("stop-all; for a rolling upgrade since 2.15.0 see upgrade-a-cluster.md § Rolling")

**Interfaces:** none; prose.

- [ ] **Step 1: The explainer** — sections: What a level is (one paragraph, the three promises and how the level keeps them); The one rule for senders; Reports and refusals (the byte, the three cases, the two fail-stops with their exact names and what to do); The raise (predicate, auto vs manual, the rollback window, the race to set manual early); One level back, stop-all included; Operator surface (`wire show`, `wire raise`, `wire_raise` key, the five metrics, the alert); What is still a flag day; Precedents (etcd, CockroachDB, Kafka, Aeron).

- [ ] **Step 2: The rolling procedure** (in `upgrade-a-cluster.md`):

```markdown
## Rolling upgrades since 2.15.0

1. Check the pair is permitted: the new release's wire minor is the cluster's
   level or one above it (`uc2ctl wire show` → `level=`; the release notes
   name the minor). Two above: install the middle release first — this is
   true for a stop-all upgrade too.
2. Decide whether you want a rollback window. If yes, before the first
   restart: `uc2ctl settings apply` a file containing `wire_raise = "manual"`.
3. One host at a time: stop the node and every process attached to its
   instance directory (services, clients, the gateway — a cnc minor bump means
   they restart together, the standing per-host rule); install; start the
   node; wait for `can_serve` (`uc2ctl status`, or `/readyz`); start the
   services; move on. Under load, commit stalls only while a quorum re-forms.
4. `uc2ctl wire show` on the leader after the last host: `verdict: raisable`.
   Auto mode raises within a second; manual: `uc2ctl wire raise` — the
   point of no return. Until then you may roll any host back.
5. `uc2_wire_level` is the new level on every node. Done.
```

plus the 2.15.0 flag-day section (stop-all, what changes, "the first flag day a mixed cluster is refused rather than misparsed", the `prelevel` counter).

- [ ] **Step 3: semver-policy** — the lead-in's "Mixed-version operation is not supported" becomes the level rule: wire minor = level, rolling, one level back; wire major = flag day; the stop-all consequence; cnc minor = per-host restart together. The new section records the 0.12.0 surface.

- [ ] **Step 4: Run the link checker and commit**

Run: `python3 scripts/check_doc_links.py | tail -1`
Expected: `0 error(s)`

```bash
git add docs scripts/uc2_flag_day.sh
git commit -m "docs: the wire level — explainer, rolling-upgrade how-to, semver rule, wire reference, runbook"
```

`CLAUDE.md` will need, with the maintainer's OK at the cut: the shipped-wire bullet (`0.12.0`, cnc `3.5`), the "flag days, never mixed-version" sentence amended to the level rule, the fuzz count 27, and the `wire-level-override` feature beside `mutation-testing`. `RELEASES.md` / `docs/releases.md` entries are written at the cut per the release rule, with the #166 bullet still owed.

---

### Task 16: The whole-tree proof stack before the PR

**Files:** none new.

- [ ] **Step 1: Format, lints, MSRV**

```bash
cd ~/ultima/uc-wire-level
cargo fmt --all -- --check
export CARGO_TARGET_DIR=$HOME/.cache/cargo-target-wire-level
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc_node --features wire-level-override --all-targets -- -D warnings
cargo clippy -p uc_crashtest --features hard-crash-tests,wire-level-override --all-targets -- -D warnings
# every other feature-gated clippy line in .github/workflows/ci.yml, verbatim
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings
python3 scripts/check_doc_links.py
```

Expected: all clean.

- [ ] **Step 2: The correctness stack**

```bash
cargo test --workspace 2>&1 | tail -3
cargo test -p uc_node --test lin_v2 2>&1 | tail -3
cargo test -p uc_node --test lin_partition_v2 2>&1 | tail -3
cargo test -p uc_crashtest --features hard-crash-tests,wire-level-override 2>&1 | tail -3
cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1 2>&1 | tail -3
scripts/fuzz_smoke.sh 30 uc_protocol_compat uc_protocol_settings uc_protocol_datagram 2>&1 | tail -3
```

Expected: every line `test result: ok`; the fuzz smoke finds nothing.

- [ ] **Step 3: Push and open the PR** — the maintainer merges; the PR body lists the seven errata, the three fixed numbers (op 12, reason 63, words 3992/4000), and the NOT-RUN fleet row.

```bash
git push -u origin design/wire-level
gh pr create --title "The committed cluster wire level (#31, 2.15.0)" --body-file <(…)
```

---

## Self-review

**Spec coverage.** §2.1 numbering → T1. §2.2 record → T2, T3. §2.3 invariant and refusals → T6. §2.4 stop-all consequence → T15 (docs; the mechanism is T6's `BelowBinary`). §3.1 byte → T4. §3.2 receiver cases → T4. §3.3 encode at committed level → T2 (`settings_version_for`, `decode_settings_at`), T6 (the agent decodes at level), T11 (golden corpus pins it). §3.4 backstop → T6 (`ClusterRecordUndecodable`). §3.5 NEW_TERM → T9. §4.1–4.4 raise → T7, T8, T12. §5.1 cnc → T5. §5.2 verbs/key → T8. §5.3 metrics → T10. §5.4 alert → T10. §5.5 docs → T15, T8 (reference), T10 (monitor), T11/T13 (VERIFICATION). §6.1 → T2, T3, T4, T7, T11. §6.2 → T6 (feature), T12. §6.3 → T13. §6.4 → T14. §7.2 release discipline → T15 (cut-a-release line). Gap found and closed: the spec's "the leader's own `WIRE_MAX`" in the predicate had no test — T7's `the_leaders_own_max_counts`.

**Placeholders.** The golden test's `<fill from first run>` string is replaced inside T11 before commit (Step 2 says so); no other TBDs. T12 and T14 point at existing harness helpers by file rather than inventing names, and say what to do if the name differs.

**Type consistency.** `WireRange { min, max }` (T1) is what T4 configs, T5 `pack/unpack`, T6 `check_level`, T7 `own_max = self.wire.max`, T8 `unpack`, T10 renderers and T12/T13 use. `NetEvent::Report.wire: u8` (T4) feeds `peer_wire` (T7). `PeerSlot.head.{id_and_role, wire_max}` (T5) is what T7 writes and T8/T10 read. `wire_raise_target(own_max, committed, &members, self_id, &reported) -> Result<u8, WireHoldout>` is identical in T7 and T8. `ADMIN_OP_WIRE_RAISE = 12` and `REASON_WIRE_HOLDOUT = 63` are the same in T8's three crates.

**Review Focus.** 1 → T4 `a_prelevel_byte_is_refused_under_crypto_too`. 2 → T3 `a_keep_record_changes_neither_wire_field` + T8 `a_file_without_wire_raise_keeps_the_mode`. 3 → T7 `wire_raise_target_blocks_on_a_silent_learner` + T12 `a_removed_holdout_unblocks_the_raise`. 4 → T6 `install_from_enforces_the_level`. 5 → T9's reader test and the `failover.rs` constant.
