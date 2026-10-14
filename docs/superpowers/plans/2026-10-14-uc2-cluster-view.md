# Cluster View (storage-service project 4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every node answers `GET /cluster` with one versioned JSON document — the committed cluster state, its own holdings and services, and (on the leader) every member's holdings — over an always-on unix socket and the `[metrics]` TCP listener; `uc2ctl cluster`/`catalog` and the three `show` commands read it, with an offline artifact fallback.

**Architecture:** A new `uc_node::view` module owns serde model types (the schema), pure builders from `ClusterState` + live inputs, attention codes, and converters back to domain types. The obs HTTP router gains `/cluster`; `ObsServer` serves a TCP and/or unix listener from one accept loop; the `uc2-node` daemon always binds `<instance_dir>/obs.sock`. `uc2ctl` fetches over the socket, or builds the same document offline from the newest cluster artifact through the same builders.

**Tech Stack:** Rust 2024 (MSRV 1.89), `serde` + `serde_json` (new workspace dependency), `std::os::unix::net`, existing `uc_node`/`uc_ctl`/`uc_obs` crates.

**Spec:** `docs/superpowers/specs/2026-10-14-uc2-cluster-view-design.md` — read it end to end before Task 1. The "Planning rulings" section below amends it where the code disagreed with the spec's assumptions; those rulings are binding and go into the spec's errata block in Task 10.

## Global Constraints

- Schema string is exactly `uc2.cluster-view/1` (`uc_node::view::SCHEMA`).
- Hashes serialize as `"0x" + 16 lowercase hex digits`; tier ids as 16 lowercase hex digits with no prefix (`format!("{:016x}")`, as `uc2ctl tier show` prints them).
- Times serialize as RFC 3339 UTC with nanoseconds, `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ` (9 fraction digits, always).
- No JSON number in any rendered document may exceed 2^53 (9007199254740992). Any `u64` that can exceed it (sentinels, user timer ids, wall-clock ns) is a decimal STRING.
- Absent values are `null`, never a sentinel `0`. Attention context fields that do not apply are OMITTED (`skip_serializing_if = "Option::is_none"`), not null.
- Enum values are lowercase snake_case words.
- No node↔node wire change, no `cnc2.dat` layout change, no on-disk format change. No new admin op.
- The view is rendered on the obs thread; no hot-path agent (consensus/sender/receiver/archive/cluster) does work for it.
- The socket file is `<instance_dir>/obs.sock`, mode `0600`; constant `uc_node::obs::socket::OBS_SOCKET_FILE = "obs.sock"`.
- `tier_behind` threshold: `uc_node::view::TIER_BEHIND_BYTES = 256 * 1024 * 1024`, pinned to `packaging/prometheus/uc2-alerts.yml`'s `uc2_backup_lag_bytes > 268435456`.
- Never add a `Co-Authored-By` or any attribution trailer to commits.
- Scratch files go under `$HOME/scratch/`, never `/tmp`. Test instance dirs go under `env!("CARGO_TARGET_TMPDIR")` (existing `scratch()`/`tempdir()` helpers).
- Do not edit `CLAUDE.md`.
- Before the branch is pushed: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, the MSRV gate `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`, every feature-gated clippy line in `.github/workflows/ci.yml`, and `python3 scripts/check_doc_links.py`. Use a private `CARGO_TARGET_DIR` for this worktree (e.g. `$HOME/.cache/cargo-target-uc-cv`).

## Planning rulings (amend the spec; Task 10 records them as errata)

- **PR1 — soft table reaches the obs thread through two new `ObsSources` fields**, `soft_wire: Arc<Mutex<uc_net::SoftTableWire>>` and `soft_stale_ns: u64`. `soft_table_from_wire` and `addr_of` in `node.rs` become `pub(crate)`. Every struct-literal `ObsSources { .. }` site is updated (list in Task 5).
- **PR2 — `stalled` on a set means "commanded and superseded"**: `state == Commanded` and a NEWER entry exists in the catalog. This is the condition `Uc2SnapshotStalled` keys on ("the instant advanced while the set did not"); `CatalogQuery::stalled(timeout)` has no production caller and no timeout constant exists, so the view does not invent one. The newest commanded set is "in progress", not stalled.
- **PR3 — `tier_behind` uses a new Rust constant** (`TIER_BEHIND_BYTES`), pinned to the alert file by a test. Tier `lag_bytes` is `commit - through` against THIS node's commit, on every node (the metric stays leader-only; the view is per-node and says whose commit it used via the header's `commit`). Offline: `null`.
- **PR4 — schedule entries name their row by matching `identity_hash` against the row names** (live: `cnc.service_names()`; offline: a readable stale `cnc2.dat`), `null` when no name matches.
- **PR5 — `view_position` is `ClusterView::consumed`**, read separately from `to_state()` (whose `applied` is the view TAG `position`). Offline: the artifact's position.
- **PR6 — `purge_gate.position`**: `null` when K = 0; otherwise `tier::gate_position(k, tiers)` verbatim, which is `0` ("holds every purge") when fewer than K tiers report. A `tiers_reporting` count is added so `0` is self-explaining.
- **PR7 — `joined`** mirrors `uc_service::attach::node_booting`: `joined = !(services_declared == 0 && any row name is Some)`.
- **PR8 — `uc2ctl cluster`/`catalog` take the existing `CommonArgs`** (so `--app-id` is required, as for every other subcommand); it names the `cnc2.dat` used for liveness and offline row names.
- **PR9 — show commands render through the view**: `settings show`, `schedule show`, `upgrade show` fetch the document (live, else offline), convert its sub-objects back to the domain types with `uc_node::view::convert`, and print through their EXISTING renderers — so the text is identical by construction. The model is therefore LOSSLESS for those three sub-objects (packed versions, exact ns as strings).
- **PR10 — when the live read is unavailable but the instance lock is held** (node booting, or an embedded `Node` run without the daemon): `cluster`/`catalog` print `node running but its obs socket is not bound (starting, or not run by uc2-node)` and exit non-zero; the three `show` commands fall back to the offline document silently (their pre-existing behavior).
- **PR11 — end-to-end content tests run in-process `Node`s** (the `uc_node/tests/catalog.rs` harness) and call `view::render_live(&node.observability())`; the socket is tested against one real daemon (`lifecycle.rs`) and in `uc_ctl` tests that bind an `ObsServer` themselves. No 3-daemon harness is built.
- **PR12 — RFC 3339 rendering is a new public `uc_obs::time::rfc3339_ns`** (uc_obs is the dependency-free leaf both daemons share). `uc_ctl`'s private second-precision helper stays as is.
- **PR13 — offline row names come from a stale `cnc2.dat` when it opens**; otherwise names are `null`. Offline membership comes from the artifact.

## Review Focus

1. A replicated value at a `u64` sentinel (`fsm_lag` lockstep = `u64::MAX`, a user `timer_id` near `u64::MAX`, an anchor in ns) must never come out as a JSON number above 2^53 — pinned by Task 3's `no_number_in_a_rendered_document_exceeds_2_pow_53`.
2. A node whose `Holdings.catalog_position` was computed against an OLDER catalog must render `sets_held: null` and must not appear in any `held_by`, rather than mislabelling sets — pinned by Task 5's `holdings_against_an_older_catalog_render_no_sets_held`.
3. A client that requests `/cluster` and never reads the body must not wedge the single-threaded obs server — pinned by Task 6's `a_client_that_never_reads_a_cluster_body_does_not_wedge_metrics`.
4. `uc2ctl` handed a document with a different `schema` (a future `/2`) must refuse by name, not misparse — pinned by Task 8's `a_foreign_schema_is_refused_by_name`.
5. A declared row with no attached service and no readable name (offline, cnc gone) must render `name: null` and text `row<N>`, never panic — pinned by Task 3's `a_row_without_a_name_renders_null` and Task 8's offline text test.

---

## File structure

| File | Responsibility |
|---|---|
| `uc_obs/src/time.rs` (new) | `rfc3339_ns(ns: u64) -> String` |
| `uc_node/src/view/mod.rs` (new) | module root, `SCHEMA`, `TIER_BEHIND_BYTES`, re-exports |
| `uc_node/src/view/model.rs` (new) | every serde type of the document (the schema) |
| `uc_node/src/view/build.rs` (new) | `ClusterState` → `ClusterPart` (catalog derived fields, tiers, gate) |
| `uc_node/src/view/attention.rs` (new) | the `attention` list |
| `uc_node/src/view/live.rs` (new) | `LiveInputs`, `gather(&ObsSources)`, `build_live_doc`, `render_live` |
| `uc_node/src/view/offline.rs` (new) | `render_offline`/`build_offline_doc` from the newest artifact |
| `uc_node/src/view/convert.rs` (new) | view sub-objects → `Settings` / `ScheduleTable` / upgrade tuple |
| `uc_node/src/view/example.json` (new) | the golden example response |
| `uc_node/src/obs/socket.rs` (new) | bind/connect `obs.sock` (incl. the `/proc/self/fd` long-path route) |
| `uc_node/src/obs/http.rs` | `/cluster` route; TCP+unix accept loop; generic `Conn` |
| `uc_node/src/obs/mod.rs` | two new `ObsSources` fields |
| `uc_node/src/node.rs` | fill the new fields; `pub(crate)` soft-table helpers |
| `uc_node/src/cluster_agent.rs` | `read_committed_state` |
| `uc_node/src/bin/uc2-node.rs` | always start the obs server with the socket |
| `uc_ctl/src/view.rs` (new) | fetch (live/offline), schema check, `cluster`/`catalog` text |
| `uc_ctl/src/main.rs`, `settings.rs`, `schedule.rs`, `upgrade.rs` | new subcommands; show via the view |
| docs (Task 10) | reference page, uc2ctl, runbook, security, BACKLOG, spec errata |

---

### Task 1: `serde_json` workspace dependency and `uc_obs::time::rfc3339_ns`

**Files:**
- Modify: `Cargo.toml` (`[workspace.dependencies]`), `uc_node/Cargo.toml`, `uc_ctl/Cargo.toml`, `uc_diffreplay/Cargo.toml:44`
- Create: `uc_obs/src/time.rs`
- Modify: `uc_obs/src/lib.rs` (add `pub mod time;` beside `pub mod log;`)

**Interfaces:**
- Produces: `uc_obs::time::rfc3339_ns(ns: u64) -> String`; `serde_json` available to `uc_node` (dep + dev-dep) and `uc_ctl` (dep).

- [ ] **Step 1: Write the failing test** in `uc_obs/src/time.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! RFC 3339 rendering for machine-read output (the cluster view). UTC only,
//! always nine fraction digits, so every rendered time has one fixed width
//! and sorts lexically. Dependency-free on purpose (this crate is the leaf
//! every daemon shares).

const NS_PER_SEC: u64 = 1_000_000_000;

/// `ns` nanoseconds since the Unix epoch as `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`.
pub fn rfc3339_ns(ns: u64) -> String {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::rfc3339_ns;

    #[test]
    fn renders_the_epoch_a_leap_day_and_the_largest_u64() {
        assert_eq!(rfc3339_ns(0), "1970-01-01T00:00:00.000000000Z");
        // 2024-02-29T12:34:56.000000789Z
        assert_eq!(
            rfc3339_ns(1_709_210_096_000_000_789),
            "2024-02-29T12:34:56.000000789Z"
        );
        // u64::MAX ns is 2554-07-21T23:34:33.709551615Z.
        assert_eq!(rfc3339_ns(u64::MAX), "2554-07-21T23:34:33.709551615Z");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p uc_obs time::`
Expected: FAIL — panics at `todo!()` (after adding `pub mod time;` to `uc_obs/src/lib.rs` so it compiles).

- [ ] **Step 3: Implement** (Howard Hinnant's `civil_from_days`, the same algorithm `uc_ctl/src/schedule.rs` uses):

```rust
pub fn rfc3339_ns(ns: u64) -> String {
    let secs = ns / NS_PER_SEC;
    let frac = ns % NS_PER_SEC;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:09}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's `civil_from_days`: proleptic Gregorian, day 0 = 1970-01-01.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cargo test -p uc_obs time::`
Expected: PASS.

- [ ] **Step 5: Add `serde_json`.** In the root `Cargo.toml` `[workspace.dependencies]`, after `serde = …`, add `serde_json = "1"`. In `uc_node/Cargo.toml` add `serde_json = { workspace = true }` to BOTH `[dependencies]` and `[dev-dependencies]`. In `uc_ctl/Cargo.toml` add `serde_json = { workspace = true }` to `[dependencies]` and `serde = { workspace = true }` if absent. In `uc_diffreplay/Cargo.toml:44` replace `serde_json = "1"` with `serde_json = { workspace = true }`.

Run: `cargo build --workspace --locked`
Expected: builds; `Cargo.lock` unchanged except possibly dependency lists (serde_json 1.0.149 is already locked).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock uc_node/Cargo.toml uc_ctl/Cargo.toml uc_diffreplay/Cargo.toml uc_obs/src/time.rs uc_obs/src/lib.rs
git commit -m "obs: rfc3339_ns for machine-read output; serde_json becomes a workspace dependency"
```

---

### Task 2: The document model (`uc_node::view::model`)

**Files:**
- Create: `uc_node/src/view/mod.rs`, `uc_node/src/view/model.rs`
- Modify: `uc_node/src/lib.rs` (add `pub mod view;` in the `pub mod` list, alphabetical after `services`/`tier`)

**Interfaces:**
- Produces (all `pub`, all `#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]`): `ClusterViewDoc`, `Source`, `Role`, `LeaderRef`, `ClusterPart`, `MembershipView`, `MemberView`, `MemberRole`, `SettingsView`, `FsmLagView`, `TargetView`, `ScheduleView`, `ScheduleEntryView`, `RuleView`, `UpgradeRowView`, `PinView`, `RunningView`, `ReportView`, `ReportNodeView`, `HashVerdictView`, `CatalogView`, `SetView`, `SetKindView`, `SetStateView`, `RowEntryView`, `VerdictView`, `TierView`, `PurgeGateView`, `SelfPart`, `HoldingsView`, `RowAppliedView`, `ServiceView`, `NodeEntry`, `Attention`, `Hex64`, `VersionView`, `DecU64`; constants `view::SCHEMA: &str`, `view::TIER_BEHIND_BYTES: u64`.

- [ ] **Step 1: Create `uc_node/src/view/mod.rs`:**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster view (storage-service project 4, spec
//! `docs/superpowers/specs/2026-10-14-uc2-cluster-view-design.md`): one
//! versioned JSON document an operator or an AI reads — the committed
//! cluster state, this node's holdings and services, and (on the leader)
//! every member's holdings. Served as `GET /cluster`; `uc2ctl` builds the
//! same document offline from the newest cluster artifact.
//!
//! The serde types in [`model`] ARE the schema: adding a field keeps
//! [`SCHEMA`]; renaming, removing or changing a field's meaning bumps it.

pub mod model;

pub use model::ClusterViewDoc;

/// The document's schema tag.
pub const SCHEMA: &str = "uc2.cluster-view/1";

/// `tier_behind`'s threshold: the `Uc2BackupBehind` alert's level
/// (`uc2_backup_lag_bytes > 268435456`), without the alert's `for: 15m`.
pub const TIER_BEHIND_BYTES: u64 = 256 * 1024 * 1024;
```

- [ ] **Step 2: Write `uc_node/src/view/model.rs` with its tests first** (the tests at the bottom fail to compile until the types exist; write types and tests in one go, then run):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster view's serde types — the schema `uc2.cluster-view/1`.
//! Conventions (spec §5): hashes are `Hex64` strings, times RFC 3339
//! strings, any u64 that can exceed 2^53 a `DecU64` string, absent `null`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A u64 hash as `"0x" + 16 lowercase hex digits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hex64(pub u64);

impl Serialize for Hex64 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("0x{:016x}", self.0))
    }
}

impl<'de> Deserialize<'de> for Hex64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let digits = s
            .strip_prefix("0x")
            .ok_or_else(|| serde::de::Error::custom("hash must start with 0x"))?;
        if digits.len() != 16 {
            return Err(serde::de::Error::custom("hash must have 16 hex digits"));
        }
        u64::from_str_radix(digits, 16)
            .map(Hex64)
            .map_err(serde::de::Error::custom)
    }
}

/// A u64 that can exceed 2^53, as a decimal string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecU64(pub u64);

impl Serialize for DecU64 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for DecU64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map(DecU64).map_err(serde::de::Error::custom)
    }
}

/// A packed FSM version: the text an operator reads and the packed word a
/// tool compares (`uc_protocol::identity::pack_version`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionView {
    pub text: String,
    pub packed: u32,
}

impl VersionView {
    pub fn of(packed: u32) -> Self {
        VersionView {
            text: uc_protocol::identity::VersionDisplay(packed).to_string(),
            packed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Live,
    Artifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Leader,
    Follower,
    Learner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRef {
    pub id: u32,
    /// The leader's replication address from the committed membership;
    /// `null` when it is not a member this node knows.
    pub addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterViewDoc {
    pub schema: String,
    pub source: Source,
    pub node_id: Option<u32>,
    pub role: Option<Role>,
    pub joined: Option<bool>,
    pub leader: Option<LeaderRef>,
    pub view_position: u64,
    pub commit: Option<u64>,
    pub read_at: String,
    pub cluster: ClusterPart,
    #[serde(rename = "self")]
    pub self_part: Option<SelfPart>,
    pub nodes: Option<Vec<NodeEntry>>,
    /// `"leader"` on a live non-leader (ask the leader for `nodes`), else null.
    pub nodes_from: Option<String>,
    pub attention: Vec<Attention>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterPart {
    pub membership: MembershipView,
    pub settings: SettingsView,
    pub schedule: ScheduleView,
    pub upgrades: Vec<UpgradeRowView>,
    pub catalog: CatalogView,
    pub tiers: Vec<TierView>,
    pub purge_gate: PurgeGateView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    pub config_version: u64,
    pub members: Vec<MemberView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberView {
    pub id: u32,
    pub addr: String,
    pub role: MemberRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberRole {
    Voter,
    Learner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingsView {
    pub position: u64,
    pub fsm_lag: FsmLagView,
    pub admission_bytes: u64,
    pub snapshot_interval_bytes: u64,
    pub snapshot_target: TargetView,
    /// Raw replicated word; `0` is the BASELINE rung (`datagram_mtu_rung`).
    pub datagram_mtu: u32,
    pub datagram_mtu_rung: String,
    pub retain_sets: u16,
    pub auto_fetch: bool,
    pub backup_retain_sets: u16,
    pub backup_retain_age_secs: u64,
    pub purge_requires_tiers: u8,
}

/// `Settings::fsm_lag_bytes`: `0` derived, `u64::MAX` lockstep, else bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FsmLagView {
    Derived,
    Lockstep,
    Bytes { bytes: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetView {
    All,
    Learners,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleView {
    pub position: u64,
    pub entries: Vec<ScheduleEntryView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleEntryView {
    /// The row whose identity hash matches, `null` when no name matches.
    pub row_name: Option<String>,
    pub identity_hash: Hex64,
    pub timer_id: DecU64,
    pub rule: RuleView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuleView {
    Every {
        period_ns: DecU64,
        anchor: String,
        anchor_ns: DecU64,
    },
    DailyAt {
        secs_of_day: u32,
    },
    Once {
        at: String,
        at_ns: DecU64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeRowView {
    pub row: u8,
    pub name: Option<String>,
    /// Every pin on this row, oldest first.
    pub pins: Vec<PinView>,
    pub running: Option<RunningView>,
    pub report: Option<ReportView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinView {
    pub from: VersionView,
    pub to: VersionView,
    pub origin: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningView {
    pub version: VersionView,
    pub record_pos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportView {
    pub position: u64,
    pub verdict: HashVerdictView,
    pub majority_hash: Option<Hex64>,
    pub minority: Vec<u32>,
    pub nodes: Vec<ReportNodeView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportNodeView {
    pub node: u32,
    pub hash: Hex64,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HashVerdictView {
    Agreed,
    Diverged,
    NoMajority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogView {
    pub retain_sets: u16,
    pub empty: bool,
    pub floor: Option<u64>,
    /// Oldest first.
    pub sets: Vec<SetView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetView {
    pub position: u64,
    pub kind: SetKindView,
    pub commanded_at: String,
    pub state: SetStateView,
    pub agreed: bool,
    pub stalled: bool,
    /// Names of the diverged rows (`"cluster"` for the cluster artifact,
    /// `"row<N>"` for a row with no known name).
    pub diverged_rows: Vec<String>,
    pub size: Option<u64>,
    /// Declared rows only (`ClusterState::declared_mask`).
    pub rows: Vec<RowEntryView>,
    pub cluster: RowEntryView,
    /// Leader only: node ids holding this set complete on disk.
    pub held_by: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetKindView {
    Full,
    Standby,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetStateView {
    Commanded,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowEntryView {
    /// The row id; `255` for the cluster artifact.
    pub row: u8,
    pub name: Option<String>,
    /// `null` while unreported.
    pub version: Option<VersionView>,
    pub hash: Option<Hex64>,
    pub verdict: VerdictView,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictView {
    Unreported,
    Agreed,
    Diverged,
    NoMajority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierView {
    /// 16 lowercase hex digits, as `uc2ctl tier show` prints it.
    pub id: String,
    pub oldest_set: u64,
    pub newest_set: u64,
    pub through: u64,
    pub reporter: u64,
    pub updated_position: u64,
    /// `commit - through` against this node's commit; `null` offline.
    pub lag_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeGateView {
    pub requires_tiers: u8,
    /// `null` when `requires_tiers == 0`; `0` holds every purge (fewer
    /// than K tiers report — see `tiers_reporting`).
    pub position: Option<u64>,
    pub tiers_reporting: usize,
    /// This node's low-space release level; `null` offline.
    pub released: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelfPart {
    pub holdings: HoldingsView,
    pub services: Vec<ServiceView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoldingsView {
    pub journal_first: u64,
    pub durable: u64,
    pub commit: u64,
    pub applied: Vec<RowAppliedView>,
    pub free_bytes: u64,
    pub journal_bytes: u64,
    pub snapshots_bytes: u64,
    /// Set positions held complete on disk; `null` when the holdings were
    /// computed against a different catalog than the one rendered.
    pub sets_held: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowAppliedView {
    pub row: u8,
    pub name: Option<String>,
    pub applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceView {
    pub row: u8,
    pub name: Option<String>,
    pub attached: bool,
    pub version: Option<VersionView>,
    pub applied: u64,
    pub lag_bytes: u64,
    pub timers_pending: u64,
    pub pinned: bool,
    pub pinned_version: Option<VersionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: u32,
    pub addr: String,
    pub role: MemberRole,
    pub holdings: HoldingsView,
    pub age_ms: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attention {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<u32>,
}

impl Attention {
    pub fn code(code: &str) -> Self {
        Attention {
            code: code.to_string(),
            set: None,
            row: None,
            tier: None,
            lag_bytes: None,
            node: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_prefixed_hex_strings_and_round_trip() {
        let j = serde_json::to_string(&Hex64(0x3f9a)).unwrap();
        assert_eq!(j, "\"0x0000000000003f9a\"");
        assert_eq!(serde_json::from_str::<Hex64>(&j).unwrap(), Hex64(0x3f9a));
        assert!(serde_json::from_str::<Hex64>("\"3f9a\"").is_err());
        assert!(serde_json::from_str::<Hex64>("\"0x3f9a\"").is_err());
    }

    #[test]
    fn large_u64s_are_decimal_strings_and_round_trip() {
        let j = serde_json::to_string(&DecU64(u64::MAX)).unwrap();
        assert_eq!(j, "\"18446744073709551615\"");
        assert_eq!(serde_json::from_str::<DecU64>(&j).unwrap(), DecU64(u64::MAX));
    }

    #[test]
    fn enums_are_lowercase_words_and_fsm_lag_is_tagged() {
        assert_eq!(serde_json::to_string(&VerdictView::NoMajority).unwrap(), "\"no_majority\"");
        assert_eq!(serde_json::to_string(&Role::Learner).unwrap(), "\"learner\"");
        assert_eq!(
            serde_json::to_string(&FsmLagView::Bytes { bytes: 7 }).unwrap(),
            r#"{"mode":"bytes","bytes":7}"#
        );
        assert_eq!(
            serde_json::to_string(&FsmLagView::Lockstep).unwrap(),
            r#"{"mode":"lockstep"}"#
        );
    }

    #[test]
    fn attention_omits_context_that_does_not_apply() {
        let mut a = Attention::code("set_diverged");
        a.set = Some(1024);
        a.row = Some("kv".into());
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            r#"{"code":"set_diverged","set":1024,"row":"kv"}"#
        );
    }

    #[test]
    fn a_version_carries_its_text_and_its_packed_word() {
        let v = serde_json::to_value(VersionView::of(0)).unwrap();
        assert_eq!(v["text"], "unversioned");
        assert_eq!(v["packed"], 0);
        // `self` as the key of `self_part` is pinned on whole documents by
        // Task 8's offline test and Task 5's golden example.
    }
}
```

Add `pub mod model;` is already in `mod.rs`; add `pub mod view;` to `uc_node/src/lib.rs`.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p uc_node --lib view::model`
Expected: PASS (5 tests).

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/view uc_node/src/lib.rs
git commit -m "view: the uc2.cluster-view/1 document model"
```

---

### Task 3: Building the cluster part from `ClusterState`; `read_committed_state`; converters back

**Files:**
- Create: `uc_node/src/view/build.rs`, `uc_node/src/view/convert.rs`
- Modify: `uc_node/src/view/mod.rs` (add `pub mod build; pub mod convert;`), `uc_node/src/cluster_agent.rs` (add `read_committed_state`)

**Interfaces:**
- Consumes: Task 2's model types; `ClusterState` (`uc_node::cluster_fsm`), `CatalogQuery`/`SoftTable` (`uc_node::catalog`), `tier::gate_position`, `uc_protocol::v2::upgrade::verdict`.
- Produces:
  - `pub type RowNames = [Option<String>; CNC_MAX_SERVICES];`
  - `pub fn build::row_name(names: &RowNames, row: u8) -> Option<String>` — `"cluster"` for `CLUSTER_ROW`, else `names[row]`.
  - `pub fn build::name_for_hash(names: &RowNames, hash: u64) -> Option<String>` — the name whose `FsmName::new(name).hash()` equals `hash` (Step 3 shows how).
  - `pub fn build::cluster_part(st: &ClusterState, names: &RowNames, commit: Option<u64>, gate_released: Option<bool>) -> ClusterPart` (`held_by` left `None`; Task 5 fills it).
  - `pub fn build::stalled(sets: &[SetEntry], i: usize) -> bool` (PR2).
  - `pub fn cluster_agent::read_committed_state(instance_dir: &Path) -> io::Result<Option<ClusterState>>`.
  - `pub fn convert::settings(v: &SettingsView) -> Settings`, `pub fn convert::table(v: &ScheduleView) -> ScheduleTable`, `pub fn convert::upgrade(v: &[UpgradeRowView]) -> (Vec<UpgradePin>, Vec<SnapshotReport>, [Option<RowRunning>; CNC_MAX_SERVICES])`.

- [ ] **Step 1: Write the failing tests** at the bottom of `uc_node/src/view/build.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};

    fn names() -> RowNames {
        let mut n: RowNames = Default::default();
        n[0] = Some("kv".into());
        n
    }

    fn state_with_one_agreed_and_one_commanded() -> ClusterState {
        let mut st = ClusterState::genesis_empty();
        st.running[0] = Some(uc_protocol::v2::upgrade::RowRunning {
            row: 0,
            version: 0,
            record_pos: 64,
        });
        let mut a = SetEntry::commanded(1024, SetKind::Full, 1_700_000_000_000_000_000);
        a.state = SetState::Complete;
        a.rows[0] = RowEntry { version: 0, hash: 0xaa, verdict: RowVerdict::Agreed, size: 10 };
        a.cluster = RowEntry { version: 0, hash: 0xbb, verdict: RowVerdict::Agreed, size: 5 };
        let b = SetEntry::commanded(2048, SetKind::Standby, 1_700_000_001_000_000_000);
        st.catalog = vec![a, b];
        st
    }

    #[test]
    fn the_catalog_reports_floor_agreement_size_and_declared_rows_only() {
        let st = state_with_one_agreed_and_one_commanded();
        let p = cluster_part(&st, &names(), Some(4096), Some(false));
        assert_eq!(p.catalog.floor, Some(1024));
        assert!(!p.catalog.empty);
        assert_eq!(p.catalog.sets.len(), 2);
        let s0 = &p.catalog.sets[0];
        assert!(s0.agreed);
        assert_eq!(s0.size, Some(15));
        assert_eq!(s0.rows.len(), 1, "only declared rows are listed");
        assert_eq!(s0.rows[0].name.as_deref(), Some("kv"));
        assert_eq!(s0.rows[0].hash, Some(Hex64(0xaa)));
        assert_eq!(s0.cluster.row, uc_protocol::v2::catalog::CLUSTER_ROW);
        assert_eq!(s0.cluster.name.as_deref(), Some("cluster"));
        assert_eq!(s0.commanded_at, "2023-11-14T22:13:20.000000000Z");
        let s1 = &p.catalog.sets[1];
        assert_eq!(s1.state, SetStateView::Commanded);
        assert_eq!(s1.rows[0].verdict, VerdictView::Unreported);
        assert_eq!(s1.rows[0].version, None, "unreported rows print no version");
        assert_eq!(s1.rows[0].hash, None);
        assert_eq!(s1.size, None);
        assert!(!s1.stalled, "the newest commanded set is in progress, not stalled");
    }

    #[test]
    fn a_commanded_set_superseded_by_a_newer_one_is_stalled() {
        let mut st = state_with_one_agreed_and_one_commanded();
        st.catalog.push(SetEntry::commanded(3072, SetKind::Full, 1_700_000_002_000_000_000));
        let p = cluster_part(&st, &names(), None, None);
        assert!(p.catalog.sets[1].stalled);
        assert!(!p.catalog.sets[2].stalled);
    }

    #[test]
    fn a_diverged_row_is_named_and_the_cluster_artifact_is_called_cluster() {
        let mut st = state_with_one_agreed_and_one_commanded();
        st.catalog[0].rows[0].verdict = RowVerdict::Diverged;
        st.catalog[0].cluster.verdict = RowVerdict::Diverged;
        let p = cluster_part(&st, &names(), None, None);
        assert_eq!(p.catalog.sets[0].diverged_rows, vec!["kv".to_string(), "cluster".to_string()]);
        assert!(!p.catalog.sets[0].agreed);
    }

    #[test]
    fn a_row_without_a_name_renders_null() {
        let st = state_with_one_agreed_and_one_commanded();
        let p = cluster_part(&st, &Default::default(), None, None);
        assert_eq!(p.catalog.sets[0].rows[0].name, None);
        let mut st2 = st.clone();
        st2.catalog[0].rows[0].verdict = RowVerdict::Diverged;
        let p2 = cluster_part(&st2, &Default::default(), None, None);
        assert_eq!(p2.catalog.sets[0].diverged_rows, vec!["row0".to_string()]);
    }

    #[test]
    fn tiers_lag_against_the_given_commit_and_the_gate_follows_gate_position() {
        use uc_protocol::v2::backup::BackupTierEntry;
        let mut st = ClusterState::genesis_empty();
        st.settings.purge_requires_tiers = 2;
        st.backups = vec![BackupTierEntry {
            tier_id: 0xfeed, oldest_set: 1024, newest_set: 2048, through: 3000, reporter: 1, updated_pos: 2500,
        }];
        let p = cluster_part(&st, &Default::default(), Some(5000), Some(true));
        assert_eq!(p.tiers[0].id, "000000000000feed");
        assert_eq!(p.tiers[0].lag_bytes, Some(2000));
        assert_eq!(p.purge_gate.requires_tiers, 2);
        assert_eq!(p.purge_gate.position, Some(0), "fewer than K tiers: holds every purge");
        assert_eq!(p.purge_gate.tiers_reporting, 1);
        assert_eq!(p.purge_gate.released, Some(true));
        let off = cluster_part(&st, &Default::default(), None, None);
        assert_eq!(off.tiers[0].lag_bytes, None);
        st.settings.purge_requires_tiers = 0;
        assert_eq!(cluster_part(&st, &Default::default(), None, None).purge_gate.position, None);
    }

    #[test]
    fn settings_schedule_and_upgrades_round_trip_through_the_converters() {
        use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
        use uc_protocol::v2::upgrade::{SnapshotReport, UpgradePin};
        let mut st = ClusterState::genesis_empty();
        st.settings.fsm_lag_bytes = u64::MAX;
        st.settings.datagram_mtu = 8960;
        st.settings_position = 77;
        st.table_position = 99;
        st.table.entries = vec![
            ScheduleEntry { identity_hash: 0xabc, timer_id: u64::MAX, rule: ScheduleRule::Every { period_ns: u64::MAX, anchor_ns: 1_700_000_000_123_456_789 } },
            ScheduleEntry { identity_hash: 0xabc, timer_id: 2, rule: ScheduleRule::DailyAt { secs_of_day: 50_400 } },
            ScheduleEntry { identity_hash: 0xdef, timer_id: 3, rule: ScheduleRule::Once { at_ns: 5 } },
        ];
        st.pins = vec![
            UpgradePin { row: 0, from: 1, to: 2, origin: 100 },
            UpgradePin { row: 0, from: 2, to: 3, origin: 200 },
        ];
        st.reports = vec![SnapshotReport { row: 0, position: 300, hashes: vec![(1, 0xaa, 9), (2, 0xaa, 9), (3, 0xbb, 9)] }];
        st.running[0] = Some(uc_protocol::v2::upgrade::RowRunning { row: 0, version: 3, record_pos: 200 });
        let p = cluster_part(&st, &names(), None, None);
        assert_eq!(super::super::convert::settings(&p.settings), st.settings);
        assert_eq!(super::super::convert::table(&p.schedule), st.table);
        let (pins, reports, running) = super::super::convert::upgrade(&p.upgrades);
        assert_eq!(pins, st.pins);
        assert_eq!(reports, st.reports);
        assert_eq!(running, st.running);
        assert_eq!(p.settings.position, 77);
        assert_eq!(p.schedule.position, 99);
        assert_eq!(p.upgrades[0].report.as_ref().unwrap().verdict, HashVerdictView::Diverged);
    }

    /// Review focus 1: no JSON number above 2^53 anywhere, even with every
    /// u64 field at its extreme.
    #[test]
    fn no_number_in_a_rendered_document_exceeds_2_pow_53() {
        use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
        let mut st = state_with_one_agreed_and_one_commanded();
        st.settings.fsm_lag_bytes = u64::MAX;
        st.table.entries = vec![ScheduleEntry {
            identity_hash: u64::MAX,
            timer_id: u64::MAX,
            rule: ScheduleRule::Once { at_ns: u64::MAX },
        }];
        st.catalog[0].time_ns = u64::MAX;
        let part = cluster_part(&st, &names(), Some(1 << 40), Some(false));
        let v = serde_json::to_value(&part).unwrap();
        fn walk(v: &serde_json::Value, path: &str) {
            match v {
                serde_json::Value::Number(n) => {
                    if let Some(u) = n.as_u64() {
                        assert!(u <= 1u64 << 53, "{path} = {u} exceeds 2^53");
                    }
                }
                serde_json::Value::Array(a) => a.iter().enumerate().for_each(|(i, x)| walk(x, &format!("{path}[{i}]"))),
                serde_json::Value::Object(o) => o.iter().for_each(|(k, x)| walk(x, &format!("{path}.{k}"))),
                _ => {}
            }
        }
        walk(&v, "cluster");
    }
}
```

The fixture uses `SetEntry::commanded`, which is a private `fn` today (`uc_protocol/src/v2/catalog.rs`); if it is not `pub`, make it `pub` (a constructor, no behavior change). The positions in `no_number…` stay under 2^53 because the state holds no position above it; `time_ns = u64::MAX` must surface only as a string.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p uc_node --lib view::build`
Expected: FAIL to compile — `cluster_part`, `RowNames`, `convert` not found.

- [ ] **Step 3: Implement `build.rs`** (above the tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ClusterState` → the document's `cluster` part. Pure: the same function
//! serves the live view and uc2ctl's offline artifact view, so they cannot
//! drift. Every derived field uses the query the node itself acts on.

use uc_protocol::v2::backup::BackupTierEntry;
use uc_protocol::v2::catalog::{CLUSTER_ROW, RowEntry, RowVerdict, SetEntry, SetKind, SetState};
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::schedule::ScheduleRule;
use uc_protocol::v2::settings::Target;
use uc_protocol::v2::upgrade::verdict;

use super::model::*;
use crate::catalog::{CatalogQuery, SoftTable};
use crate::cluster_fsm::ClusterState;

pub type RowNames = [Option<String>; CNC_MAX_SERVICES];

pub fn row_name(names: &RowNames, row: u8) -> Option<String> {
    if row == CLUSTER_ROW {
        return Some("cluster".to_string());
    }
    names.get(row as usize).cloned().flatten()
}

/// The text a diverged row is listed by: its name, else `row<N>`.
fn row_label(names: &RowNames, row: u8) -> String {
    row_name(names, row).unwrap_or_else(|| format!("row{row}"))
}

pub fn name_for_hash(names: &RowNames, hash: u64) -> Option<String> {
    names.iter().flatten().find(|n| {
        uc_protocol::identity::FsmName::new(n).map(|f| f.hash()) == Ok(hash)
    }).cloned()
}

/// PR2: commanded and superseded by a newer catalog entry.
pub fn stalled(sets: &[SetEntry], i: usize) -> bool {
    sets[i].state == SetState::Commanded && i + 1 < sets.len()
}

fn some_nonzero(v: u64) -> Option<u64> {
    (v != 0).then_some(v)
}

fn row_entry(names: &RowNames, row: u8, e: &RowEntry) -> RowEntryView {
    let reported = e.verdict != RowVerdict::Unreported;
    RowEntryView {
        row,
        name: row_name(names, row),
        version: reported.then(|| VersionView::of(e.version)),
        hash: reported.then_some(Hex64(e.hash)),
        verdict: match e.verdict {
            RowVerdict::Unreported => VerdictView::Unreported,
            RowVerdict::Agreed => VerdictView::Agreed,
            RowVerdict::Diverged => VerdictView::Diverged,
            RowVerdict::NoMajority => VerdictView::NoMajority,
        },
        size: if reported { some_nonzero(e.size) } else { None },
    }
}

pub fn cluster_part(
    st: &ClusterState,
    names: &RowNames,
    commit: Option<u64>,
    gate_released: Option<bool>,
) -> ClusterPart {
    let declared = st.declared_mask();
    let empty_soft = SoftTable::default();
    let q = CatalogQuery {
        sets: &st.catalog,
        catalog_position: 0,
        soft: &empty_soft,
        now_ns: 0,
        stale_ns: 0,
    };
    let diverged = q.diverged();
    let sets = st
        .catalog
        .iter()
        .enumerate()
        .map(|(i, e)| SetView {
            position: e.position,
            kind: match e.kind {
                SetKind::Full => SetKindView::Full,
                SetKind::Standby => SetKindView::Standby,
            },
            commanded_at: uc_obs::time::rfc3339_ns(e.time_ns),
            state: match e.state {
                SetState::Commanded => SetStateView::Commanded,
                SetState::Complete => SetStateView::Complete,
            },
            agreed: e.is_agreed(),
            stalled: stalled(&st.catalog, i),
            diverged_rows: diverged
                .iter()
                .filter(|(p, _)| *p == e.position)
                .map(|(_, r)| row_label(names, *r))
                .collect(),
            size: some_nonzero(e.total_size()),
            rows: (0..CNC_MAX_SERVICES as u8)
                .filter(|r| declared & (1 << r) != 0)
                .map(|r| row_entry(names, r, &e.rows[r as usize]))
                .collect(),
            cluster: row_entry(names, CLUSTER_ROW, &e.cluster),
            held_by: None,
        })
        .collect();

    let k = st.settings.purge_requires_tiers;
    ClusterPart {
        membership: membership(st),
        settings: settings(st),
        schedule: schedule(st, names),
        upgrades: upgrades(st, names),
        catalog: CatalogView {
            retain_sets: st.retain_sets(),
            empty: st.catalog_empty(),
            floor: q.newest_agreed(u64::MAX),
            sets,
        },
        tiers: st.backups.iter().map(|t| tier(t, commit)).collect(),
        purge_gate: PurgeGateView {
            requires_tiers: k,
            position: crate::tier::gate_position(k, &st.backups),
            tiers_reporting: st.backups.len(),
            released: gate_released,
        },
    }
}

fn tier(t: &BackupTierEntry, commit: Option<u64>) -> TierView {
    TierView {
        id: format!("{:016x}", t.tier_id),
        oldest_set: t.oldest_set,
        newest_set: t.newest_set,
        through: t.through,
        reporter: t.reporter,
        updated_position: t.updated_pos,
        lag_bytes: commit.map(|c| c.saturating_sub(t.through)),
    }
}

fn membership(st: &ClusterState) -> MembershipView {
    let m = &st.membership;
    let mut members: Vec<MemberView> = m
        .voters
        .iter()
        .map(|(id, a)| MemberView { id: *id, addr: crate::node::addr_of(*a).to_string(), role: MemberRole::Voter })
        .chain(m.learners.iter().map(|(id, a)| MemberView {
            id: *id,
            addr: crate::node::addr_of(*a).to_string(),
            role: MemberRole::Learner,
        }))
        .collect();
    members.sort_by_key(|x| x.id);
    MembershipView { config_version: m.version, members }
}

fn settings(st: &ClusterState) -> SettingsView {
    let s = &st.settings;
    SettingsView {
        position: st.settings_position,
        fsm_lag: match s.fsm_lag_bytes {
            0 => FsmLagView::Derived,
            u64::MAX => FsmLagView::Lockstep,
            b => FsmLagView::Bytes { bytes: b },
        },
        admission_bytes: s.admission_bytes,
        snapshot_interval_bytes: s.snapshot_interval_bytes,
        snapshot_target: match s.snapshot_target {
            Target::All => TargetView::All,
            Target::Learners => TargetView::Learners,
        },
        datagram_mtu: s.datagram_mtu,
        datagram_mtu_rung: if s.datagram_mtu == 0 { "baseline" } else { "discovered" }.to_string(),
        retain_sets: s.retain_sets,
        auto_fetch: s.auto_fetch,
        backup_retain_sets: s.backup_retain_sets,
        backup_retain_age_secs: s.backup_retain_age_secs,
        purge_requires_tiers: s.purge_requires_tiers,
    }
}

fn schedule(st: &ClusterState, names: &RowNames) -> ScheduleView {
    ScheduleView {
        position: st.table_position,
        entries: st
            .table
            .entries
            .iter()
            .map(|e| ScheduleEntryView {
                row_name: name_for_hash(names, e.identity_hash),
                identity_hash: Hex64(e.identity_hash),
                timer_id: DecU64(e.timer_id),
                rule: match e.rule {
                    ScheduleRule::Every { period_ns, anchor_ns } => RuleView::Every {
                        period_ns: DecU64(period_ns),
                        anchor: uc_obs::time::rfc3339_ns(anchor_ns),
                        anchor_ns: DecU64(anchor_ns),
                    },
                    ScheduleRule::DailyAt { secs_of_day } => RuleView::DailyAt { secs_of_day },
                    ScheduleRule::Once { at_ns } => RuleView::Once {
                        at: uc_obs::time::rfc3339_ns(at_ns),
                        at_ns: DecU64(at_ns),
                    },
                },
            })
            .collect(),
    }
}

fn upgrades(st: &ClusterState, names: &RowNames) -> Vec<UpgradeRowView> {
    (0..CNC_MAX_SERVICES as u8)
        .filter_map(|row| {
            let pins: Vec<PinView> = st
                .pins
                .iter()
                .filter(|p| p.row == row)
                .map(|p| PinView { from: VersionView::of(p.from), to: VersionView::of(p.to), origin: p.origin })
                .collect();
            let running = st.running[row as usize].map(|r| RunningView {
                version: VersionView::of(r.version),
                record_pos: r.record_pos,
            });
            let report = st.reports.iter().find(|r| r.row == row).map(|r| {
                let v = verdict(r);
                ReportView {
                    position: r.position,
                    verdict: match (v.agreed, v.majority_hash) {
                        (true, _) => HashVerdictView::Agreed,
                        (false, Some(_)) => HashVerdictView::Diverged,
                        (false, None) => HashVerdictView::NoMajority,
                    },
                    majority_hash: v.majority_hash.map(Hex64),
                    minority: v.minority,
                    nodes: r.hashes.iter().map(|(n, h, s)| ReportNodeView { node: *n, hash: Hex64(*h), size: *s }).collect(),
                }
            });
            (!pins.is_empty() || running.is_some() || report.is_some()).then(|| UpgradeRowView {
                row,
                name: row_name(names, row),
                pins,
                running,
                report,
            })
        })
        .collect()
}
```

Adjust to the real APIs where they differ, without changing behavior: `FsmName::new` may be named differently (check `uc_protocol::identity` for the constructor `cnc.service_names()` produces, and compare with `.hash()` as `uc_ctl/src/schedule.rs`'s `name_of` does); `addr_of` in `node.rs` (line ~14638) becomes `pub(crate) fn addr_of`; `verdict`'s `minority` type is `Vec<u32>`. `SnapshotReport` `hashes` is `Vec<(u32, u64, u64)>`.

- [ ] **Step 4: Implement `convert.rs`:**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! View sub-objects back to the domain types, so `uc2ctl`'s three `show`
//! commands print through their existing renderers (planning ruling PR9).
//! Lossless by construction: `build`'s round-trip test pins it.

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule, ScheduleTable};
use uc_protocol::v2::settings::{Settings, Target};
use uc_protocol::v2::upgrade::{RowRunning, SnapshotReport, UpgradePin};

use super::model::*;

pub fn settings(v: &SettingsView) -> Settings {
    Settings {
        fsm_lag_bytes: match v.fsm_lag {
            FsmLagView::Derived => 0,
            FsmLagView::Lockstep => u64::MAX,
            FsmLagView::Bytes { bytes } => bytes,
        },
        admission_bytes: v.admission_bytes,
        snapshot_interval_bytes: v.snapshot_interval_bytes,
        snapshot_target: match v.snapshot_target {
            TargetView::All => Target::All,
            TargetView::Learners => Target::Learners,
        },
        datagram_mtu: v.datagram_mtu,
        retain_sets: v.retain_sets,
        auto_fetch: v.auto_fetch,
        backup_retain_sets: v.backup_retain_sets,
        backup_retain_age_secs: v.backup_retain_age_secs,
        purge_requires_tiers: v.purge_requires_tiers,
    }
}

pub fn table(v: &ScheduleView) -> ScheduleTable {
    ScheduleTable {
        entries: v
            .entries
            .iter()
            .map(|e| ScheduleEntry {
                identity_hash: e.identity_hash.0,
                timer_id: e.timer_id.0,
                rule: match &e.rule {
                    RuleView::Every { period_ns, anchor_ns, .. } => ScheduleRule::Every {
                        period_ns: period_ns.0,
                        anchor_ns: anchor_ns.0,
                    },
                    RuleView::DailyAt { secs_of_day } => ScheduleRule::DailyAt { secs_of_day: *secs_of_day },
                    RuleView::Once { at_ns, .. } => ScheduleRule::Once { at_ns: at_ns.0 },
                },
            })
            .collect(),
    }
}

pub type UpgradeParts = (
    Vec<UpgradePin>,
    Vec<SnapshotReport>,
    [Option<RowRunning>; CNC_MAX_SERVICES],
);

pub fn upgrade(rows: &[UpgradeRowView]) -> UpgradeParts {
    let mut pins = Vec::new();
    let mut reports = Vec::new();
    let mut running: [Option<RowRunning>; CNC_MAX_SERVICES] = [None; CNC_MAX_SERVICES];
    for r in rows {
        pins.extend(r.pins.iter().map(|p| UpgradePin {
            row: r.row,
            from: p.from.packed,
            to: p.to.packed,
            origin: p.origin,
        }));
        if let Some(rep) = &r.report {
            reports.push(SnapshotReport {
                row: r.row,
                position: rep.position,
                hashes: rep.nodes.iter().map(|n| (n.node, n.hash.0, n.size)).collect(),
            });
        }
        if let Some(run) = &r.running {
            running[r.row as usize] = Some(RowRunning {
                row: r.row,
                version: run.version.packed,
                record_pos: run.record_pos,
            });
        }
    }
    (pins, reports, running)
}
```

Note: `pins` round-trip only when the original `st.pins` is ordered by row (each row's pins contiguous). `ClusterState.pins` is appended in commit order across rows; the round-trip test uses one row. `upgrade show` filters per row, so per-row order is all that matters — document that on `upgrade` and do not sort.

- [ ] **Step 5: Add `read_committed_state`** to `uc_node/src/cluster_agent.rs`, beside the other `read_committed_*` readers:

```rust
/// The whole committed `ClusterState` out of the newest cluster artifact,
/// `None` when there is none yet. The cluster view's offline source.
pub fn read_committed_state(instance_dir: &Path) -> io::Result<Option<ClusterState>> {
    let (fsm, start) = recover(&snapshot_dir_of(instance_dir), ClusterState::genesis_empty(), Vec::new())?;
    if start == 0 {
        return Ok(None);
    }
    Ok(Some(fsm.state().clone()))
}
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p uc_node --lib view::`
Expected: PASS (Task 2's 5 + Task 3's 7).

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/view uc_node/src/cluster_agent.rs uc_node/src/node.rs uc_protocol/src/v2/catalog.rs
git commit -m "view: build the cluster part from ClusterState; lossless converters back; read_committed_state"
```

---

### Task 4: Attention codes

**Files:**
- Create: `uc_node/src/view/attention.rs`
- Modify: `uc_node/src/view/mod.rs` (`pub mod attention;`)

**Interfaces:**
- Consumes: Task 3's `ClusterPart`.
- Produces: `pub fn attention::from_cluster(part: &ClusterPart) -> Vec<Attention>` (`catalog_empty`, `set_stalled`, `set_diverged`, `tier_behind`, `purge_gate_released`) and `pub fn attention::node_stale(nodes: &[NodeEntry]) -> Vec<Attention>`.

- [ ] **Step 1: Write the failing tests** at the bottom of `attention.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_fsm::ClusterState;
    use crate::view::build::cluster_part;

    #[test]
    fn a_genesis_state_reports_catalog_empty_only() {
        let part = cluster_part(&ClusterState::genesis_empty(), &Default::default(), Some(0), Some(false));
        let codes: Vec<String> = from_cluster(&part).into_iter().map(|a| a.code).collect();
        assert_eq!(codes, vec!["catalog_empty".to_string()]);
    }

    #[test]
    fn diverged_stalled_tier_behind_and_gate_release_are_each_reported_with_context() {
        use uc_protocol::v2::backup::BackupTierEntry;
        use uc_protocol::v2::catalog::{RowVerdict, SetEntry, SetKind, SetState};
        let mut st = ClusterState::genesis_empty();
        let mut a = SetEntry::commanded(1024, SetKind::Full, 0);
        a.state = SetState::Complete;
        a.cluster.verdict = RowVerdict::Diverged;
        st.catalog = vec![
            a,
            SetEntry::commanded(2048, SetKind::Full, 0),
            SetEntry::commanded(3072, SetKind::Full, 0),
        ];
        st.backups = vec![BackupTierEntry { tier_id: 1, oldest_set: 0, newest_set: 0, through: 0, reporter: 1, updated_pos: 0 }];
        let commit = crate::view::TIER_BEHIND_BYTES + 1;
        let part = cluster_part(&st, &Default::default(), Some(commit), Some(true));
        let att = from_cluster(&part);
        let json = serde_json::to_string(&att).unwrap();
        assert!(json.contains(r#"{"code":"set_diverged","set":1024,"row":"cluster"}"#), "{json}");
        assert!(json.contains(r#"{"code":"set_stalled","set":2048}"#), "{json}");
        assert!(!json.contains(r#""set":3072"#), "the newest commanded set is not stalled: {json}");
        assert!(json.contains(&format!(r#"{{"code":"tier_behind","tier":"0000000000000001","lag_bytes":{commit}}}"#)), "{json}");
        assert!(json.contains(r#"{"code":"purge_gate_released"}"#), "{json}");
        assert!(!json.contains("catalog_empty"), "{json}");
    }

    #[test]
    fn a_tier_exactly_at_the_threshold_is_not_behind() {
        use uc_protocol::v2::backup::BackupTierEntry;
        let mut st = ClusterState::genesis_empty();
        st.backups = vec![BackupTierEntry { tier_id: 1, oldest_set: 0, newest_set: 0, through: 0, reporter: 1, updated_pos: 0 }];
        let part = cluster_part(&st, &Default::default(), Some(crate::view::TIER_BEHIND_BYTES), None);
        assert!(from_cluster(&part).iter().all(|a| a.code != "tier_behind"));
    }

    #[test]
    fn the_threshold_matches_the_alert_rule() {
        let rules = include_str!("../../../packaging/prometheus/uc2-alerts.yml");
        let needle = format!("uc2_backup_lag_bytes > {}", crate::view::TIER_BEHIND_BYTES);
        assert!(rules.contains(&needle), "Uc2BackupBehind no longer reads `{needle}`");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_node --lib view::attention`
Expected: FAIL to compile — `from_cluster` not found.

- [ ] **Step 3: Implement** (above the tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The document's `attention` list (spec §5): only conditions the node
//! already detects, each named by a stable code the reference page maps to
//! its runbook section. No diagnosis logic of its own — every code reads a
//! field `build` already computed with the node's own query.

use super::model::{Attention, ClusterPart, NodeEntry, SetStateView};
use super::TIER_BEHIND_BYTES;

pub fn from_cluster(part: &ClusterPart) -> Vec<Attention> {
    let mut out = Vec::new();
    if part.catalog.empty {
        out.push(Attention::code("catalog_empty"));
    }
    for s in &part.catalog.sets {
        if s.stalled && s.state == SetStateView::Commanded {
            let mut a = Attention::code("set_stalled");
            a.set = Some(s.position);
            out.push(a);
        }
        for row in &s.diverged_rows {
            let mut a = Attention::code("set_diverged");
            a.set = Some(s.position);
            a.row = Some(row.clone());
            out.push(a);
        }
    }
    for t in &part.tiers {
        if let Some(lag) = t.lag_bytes.filter(|l| *l > TIER_BEHIND_BYTES) {
            let mut a = Attention::code("tier_behind");
            a.tier = Some(t.id.clone());
            a.lag_bytes = Some(lag);
            out.push(a);
        }
    }
    if part.purge_gate.released == Some(true) {
        out.push(Attention::code("purge_gate_released"));
    }
    out
}

pub fn node_stale(nodes: &[NodeEntry]) -> Vec<Attention> {
    nodes
        .iter()
        .filter(|n| n.stale)
        .map(|n| {
            let mut a = Attention::code("node_stale");
            a.node = Some(n.id);
            a
        })
        .collect()
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_node --lib view::attention`
Expected: PASS (4).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/view
git commit -m "view: attention codes for conditions the node already detects"
```

---

### Task 5: The live document and the `/cluster` route

**Files:**
- Create: `uc_node/src/view/live.rs`, `uc_node/src/view/example.json`
- Modify: `uc_node/src/view/mod.rs` (`pub mod live; pub use live::render_live;`), `uc_node/src/obs/mod.rs` (two fields + `for_tests`), `uc_node/src/node.rs` (`observability()`, `pub(crate)` helpers, `Node::soft_table` uses the shared helper), `uc_node/src/obs/http.rs` (route), `uc_node/src/obs/metrics.rs` (four test literals at ~2072, ~3022, ~3142, ~3255), `uc_node/tests/obs_http.rs:79`, `uc_node/examples/m10_alerts.rs:415`, `fuzz/fuzz_targets/uc_node_http.rs`, `fuzz/corpus/uc_node_http/14-get-cluster` (new)

**Interfaces:**
- Consumes: Tasks 2–4; `ObsSources`; `CatalogQuery::holders`.
- Produces:
  - `ObsSources.soft_wire: Arc<std::sync::Mutex<uc_net::SoftTableWire>>`, `ObsSources.soft_stale_ns: u64`.
  - `pub(crate) fn node::soft_table_with_self(wire: &SoftTableWire, membership: &ClusterConfig, self_id: u32, own: Holdings, now_ns: u64) -> SoftTable`.
  - `pub struct live::LiveInputs { pub node_id: u32, pub flags: u64, pub leader_hint: u64, pub commit: u64, pub consumed: u64, pub declared: u64, pub names: RowNames, pub state: ClusterState, pub catalog_version: u64, pub own: Holdings, pub soft: SoftTable, pub soft_stale_ns: u64, pub gate_released: bool, pub services: Vec<ServiceView> }`
  - `pub fn live::gather(s: &ObsSources, now_ns: u64) -> LiveInputs`
  - `pub fn live::build_live_doc(i: &LiveInputs, now_ns: u64) -> ClusterViewDoc`
  - `pub fn live::render_live(s: &ObsSources) -> String` (pretty-printed JSON + trailing newline)
  - `pub fn live::holdings_view(h: &Holdings, names: &RowNames, declared_rows: u64, sets: &[SetEntry], catalog_version: u64) -> HoldingsView`

- [ ] **Step 1: Add the two `ObsSources` fields** in `uc_node/src/obs/mod.rs` after `snapshot_holdings`:

```rust
    /// Cluster view (project 4): the leader's per-peer `STATUS` holdings —
    /// the SAME map the sender fills — and the staleness bar
    /// (`SOFT_STALE_FACTOR × election_timeout_max_ns`) a view entry is
    /// judged against. Filled only on a node that has led; the view prints
    /// it only while the leader flag is set.
    pub soft_wire: Arc<std::sync::Mutex<uc_net::SoftTableWire>>,
    pub soft_stale_ns: u64,
```

In `for_tests()`: `soft_wire: Arc::new(std::sync::Mutex::new(uc_net::SoftTableWire::new())), soft_stale_ns: 900_000_000,`. In `Node::observability()`: `soft_wire: Arc::clone(&self.soft_wire), soft_stale_ns: self.soft_stale_ns(),`. In every other literal listed under **Files**, add the same two lines `for_tests()` uses (or `..` from a fresh `SoftTableWire`). Build: `cargo build -p uc_node --all-targets` until it compiles.

- [ ] **Step 2: Share the soft-table construction.** In `node.rs`, make `soft_table_from_wire` and `addr_of` `pub(crate)`, and add:

```rust
/// The soft table the catalog queries read: every member's newest `STATUS`
/// holdings plus this node's own cache stamped `now_ns` (a node never sends
/// itself a `STATUS`, but it is as much a holder as any follower). Shared by
/// [`Node::soft_table`] and the cluster view so both read one table.
pub(crate) fn soft_table_with_self(
    wire: &SoftTableWire,
    membership: &ClusterConfig,
    self_id: NodeId,
    own: Holdings,
    now_ns: u64,
) -> crate::catalog::SoftTable {
    let mut t = soft_table_from_wire(wire, membership);
    t.record(self_id, own, now_ns);
    t
}
```

and rewrite `Node::soft_table` to call it (`soft_table_with_self(&wire, &membership, self.node_id, own, unix_now_ns())`). Run `cargo test -p uc_node --test catalog a_killed_node_leaves_holders_after_the_stale_timeout` — PASS (unchanged behavior).

- [ ] **Step 3: Write the failing tests** at the bottom of `live.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_fsm::ClusterState;
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};
    use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER, NODE_FLAG_LEARNER};

    const NOW: u64 = 1_700_000_100_000_000_000;

    fn names() -> RowNames {
        let mut n: RowNames = Default::default();
        n[0] = Some("kv".into());
        n
    }

    /// A 3-voter cluster with one agreed set at 1024, held by nodes 1 and 2.
    fn inputs(flags: u64) -> LiveInputs {
        let mut st = ClusterState::genesis_empty();
        st.membership.version = 3;
        st.membership.voters = vec![(1, (0x7f00_0001, 7001)), (2, (0x7f00_0001, 7002)), (3, (0x7f00_0001, 7003))];
        st.running[0] = Some(uc_protocol::v2::upgrade::RowRunning { row: 0, version: 0, record_pos: 64 });
        let mut a = SetEntry::commanded(1024, SetKind::Full, NOW - 5_000_000_000);
        a.state = SetState::Complete;
        a.rows[0] = RowEntry { version: 0, hash: 0xaa, verdict: RowVerdict::Agreed, size: 10 };
        a.cluster = RowEntry { version: 0, hash: 0xbb, verdict: RowVerdict::Agreed, size: 5 };
        st.catalog = vec![a];
        let held = Holdings { commit: 4096, catalog_position: 0xc0ffee, sets_held: 1, ..Default::default() };
        let mut soft = crate::catalog::SoftTable::default();
        soft.record(1, held, NOW);
        soft.record(2, held, NOW - 100_000_000);
        soft.record(3, Holdings { catalog_position: 0xc0ffee, ..Default::default() }, NOW - 5_000_000_000);
        LiveInputs {
            node_id: 1,
            flags,
            leader_hint: 1,
            commit: 4096,
            consumed: 4000,
            declared: 1,
            names: names(),
            state: st,
            catalog_version: 0xc0ffee,
            own: held,
            soft,
            soft_stale_ns: 900_000_000,
            gate_released: false,
            services: vec![],
        }
    }

    #[test]
    fn the_leader_fills_nodes_held_by_and_marks_a_silent_member_stale() {
        let doc = build_live_doc(&inputs(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE), NOW);
        assert_eq!(doc.role, Some(Role::Leader));
        assert_eq!(doc.joined, Some(true));
        assert_eq!(doc.view_position, 4000, "view_position is the walk cursor, not the view tag");
        assert_eq!(doc.commit, Some(4096));
        let nodes = doc.nodes.as_ref().expect("leader prints nodes");
        assert_eq!(nodes.iter().map(|n| n.id).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(nodes[1].age_ms, 100);
        assert!(!nodes[1].stale);
        assert!(nodes[2].stale);
        assert_eq!(doc.nodes_from, None);
        assert_eq!(doc.cluster.catalog.sets[0].held_by, Some(vec![1, 2]));
        assert!(doc.attention.iter().any(|a| a.code == "node_stale" && a.node == Some(3)));
        assert_eq!(doc.self_part.as_ref().unwrap().holdings.sets_held, Some(vec![1024]));
    }

    #[test]
    fn a_follower_prints_no_nodes_and_says_ask_the_leader() {
        let mut i = inputs(0);
        i.node_id = 2;
        let doc = build_live_doc(&i, NOW);
        assert_eq!(doc.role, Some(Role::Follower));
        assert_eq!(doc.nodes, None);
        assert_eq!(doc.nodes_from.as_deref(), Some("leader"));
        assert_eq!(doc.cluster.catalog.sets[0].held_by, None);
        assert_eq!(doc.leader.as_ref().map(|l| l.addr.clone()), Some(Some("127.0.0.1:7001".to_string())));
        assert!(doc.attention.iter().all(|a| a.code != "node_stale"));
    }

    #[test]
    fn a_demoted_leader_never_prints_its_old_soft_table() {
        // The soft table still holds every member (it was filled while this
        // node led); the leader flag is what gates `nodes`, not the table.
        let doc = build_live_doc(&inputs(0), NOW);
        assert_eq!(doc.nodes, None);
    }

    #[test]
    fn a_learner_is_a_learner_and_an_unjoined_node_says_so() {
        let mut i = inputs(NODE_FLAG_LEARNER);
        i.declared = 0;
        let doc = build_live_doc(&i, NOW);
        assert_eq!(doc.role, Some(Role::Learner));
        assert_eq!(doc.joined, Some(false));
    }

    /// Review focus 2.
    #[test]
    fn holdings_against_an_older_catalog_render_no_sets_held() {
        let mut i = inputs(NODE_FLAG_LEADER);
        let stale = Holdings { catalog_position: 0xdead, sets_held: 1, ..Default::default() };
        i.own = stale;
        i.soft.record(1, stale, NOW);
        i.soft.record(2, stale, NOW);
        let doc = build_live_doc(&i, NOW);
        assert_eq!(doc.self_part.as_ref().unwrap().holdings.sets_held, None);
        assert_eq!(doc.cluster.catalog.sets[0].held_by, Some(vec![]));
    }

    #[test]
    fn the_golden_example_matches() {
        let mut i = inputs(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
        i.services = vec![ServiceView {
            row: 0,
            name: Some("kv".into()),
            attached: true,
            version: Some(VersionView::of(0)),
            applied: 4096,
            lag_bytes: 0,
            timers_pending: 0,
            pinned: false,
            pinned_version: None,
        }];
        let rendered = serde_json::to_string_pretty(&build_live_doc(&i, NOW)).unwrap() + "\n";
        let golden = include_str!("example.json");
        assert_eq!(
            rendered, golden,
            "the cluster view's shape changed; if intended, regenerate \
             uc_node/src/view/example.json and follow the /1 stability rules"
        );
    }

    #[test]
    fn a_rendered_view_carries_no_key_material() {
        let mut s = crate::obs::ObsSources::for_tests();
        s.crypto_enabled = true;
        let body = render_live(&s);
        // The obs sources hold no key material at all; this pins that the
        // view did not grow a path to it. The marker strings are the
        // field names a key would travel under.
        for marker in ["admin_key", "group_key", "psk", "private", "secret"] {
            assert!(!body.contains(marker), "the view mentions {marker}");
        }
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["schema"], crate::view::SCHEMA);
        assert_eq!(v["source"], "live");
    }

    #[test]
    fn the_worst_case_document_stays_small() {
        use uc_protocol::v2::backup::BackupTierEntry;
        let mut i = inputs(NODE_FLAG_LEADER);
        i.declared = 0xff;
        for r in 0..8u8 {
            i.names[r as usize] = Some(format!("row-name-of-thirty-two-bytes-{r:02}"));
            i.state.running[r as usize] = Some(uc_protocol::v2::upgrade::RowRunning { row: r, version: 1, record_pos: 64 });
        }
        i.state.membership.voters = (1..=8).map(|id| (id, (0x7f00_0001, 7000 + id as u16))).collect();
        i.state.catalog = (0..40u64).map(|k| SetEntry::commanded(1024 * (k + 1), SetKind::Full, NOW)).collect();
        i.state.backups = (0..4u64)
            .map(|t| BackupTierEntry { tier_id: t + 1, oldest_set: 0, newest_set: 0, through: 0, reporter: 1, updated_pos: 0 })
            .collect();
        let body = serde_json::to_string_pretty(&build_live_doc(&i, NOW)).unwrap();
        assert!(body.len() < 512 * 1024, "worst case is {} bytes", body.len());
    }
}
```

`40` sets is above the catalog's practical ceiling (`MAX_RETAIN_SETS` + commanded); if `MAX_RETAIN_SETS` is larger than 32, raise 40 to `MAX_RETAIN_SETS + 8` and keep the 512 KiB bound.

- [ ] **Step 4: Run to verify failure**

Run: `cargo test -p uc_node --lib view::live`
Expected: FAIL to compile.

- [ ] **Step 5: Implement `live.rs`** (above the tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The live document: everything `build` renders from `ClusterState`, plus
//! the header, this node's holdings and services, and — on the leader only —
//! every member's holdings. `gather` reads the obs sources once; the build
//! is a pure function of what it gathered, so tests drive it directly.

use uc_protocol::v2::catalog::SetEntry;
use uc_protocol::v2::cnc::{CNC_MAX_SERVICES, NODE_FLAG_LEADER, NODE_FLAG_LEARNER};
use uc_protocol::v2::datagram::Holdings;

use super::build::{RowNames, cluster_part, row_name};
use super::model::*;
use crate::catalog::{CatalogQuery, SoftTable};
use crate::cluster_fsm::ClusterState;
use crate::obs::ObsSources;

pub struct LiveInputs {
    pub node_id: u32,
    pub flags: u64,
    pub leader_hint: u64,
    pub commit: u64,
    pub consumed: u64,
    pub declared: u64,
    pub names: RowNames,
    pub state: ClusterState,
    pub catalog_version: u64,
    pub own: Holdings,
    pub soft: SoftTable,
    pub soft_stale_ns: u64,
    pub gate_released: bool,
    pub services: Vec<ServiceView>,
}

pub fn holdings_view(
    h: &Holdings,
    names: &RowNames,
    declared_rows: u64,
    sets: &[SetEntry],
    catalog_version: u64,
) -> HoldingsView {
    HoldingsView {
        journal_first: h.journal_first,
        durable: h.durable,
        commit: h.commit,
        applied: (0..CNC_MAX_SERVICES as u8)
            .filter(|r| declared_rows & (1 << r) != 0)
            .map(|r| RowAppliedView { row: r, name: row_name(names, r), applied: h.applied[r as usize] })
            .collect(),
        free_bytes: h.free_bytes,
        journal_bytes: h.journal_bytes,
        snapshots_bytes: h.snapshots_bytes,
        sets_held: (h.catalog_position == catalog_version).then(|| {
            sets.iter()
                .enumerate()
                .filter(|(i, _)| *i < 64 && h.sets_held & (1 << i) != 0)
                .map(|(_, e)| e.position)
                .collect()
        }),
    }
}

pub fn build_live_doc(i: &LiveInputs, now_ns: u64) -> ClusterViewDoc {
    let is_leader = i.flags & NODE_FLAG_LEADER != 0;
    let role = if is_leader {
        Role::Leader
    } else if i.flags & NODE_FLAG_LEARNER != 0 {
        Role::Learner
    } else {
        Role::Follower
    };
    let declared_rows = i.state.declared_mask();
    let mut cluster = cluster_part(&i.state, &i.names, Some(i.commit), Some(i.gate_released));
    let members = cluster.membership.members.clone();
    let mut nodes = None;
    if is_leader {
        let q = CatalogQuery {
            sets: &i.state.catalog,
            catalog_position: i.catalog_version,
            soft: &i.soft,
            now_ns,
            stale_ns: i.soft_stale_ns,
        };
        for s in &mut cluster.catalog.sets {
            s.held_by = Some(q.holders(s.position));
        }
        nodes = Some(
            members
                .iter()
                .filter_map(|m| {
                    let e = i.soft.by_node.get(&m.id)?;
                    let age_ns = now_ns.saturating_sub(e.last_seen_ns);
                    Some(NodeEntry {
                        id: m.id,
                        addr: m.addr.clone(),
                        role: m.role,
                        holdings: holdings_view(&e.holdings, &i.names, declared_rows, &i.state.catalog, i.catalog_version),
                        age_ms: age_ns / 1_000_000,
                        stale: age_ns > i.soft_stale_ns,
                    })
                })
                .collect::<Vec<_>>(),
        );
    }
    let mut attention = super::attention::from_cluster(&cluster);
    if let Some(n) = &nodes {
        attention.extend(super::attention::node_stale(n));
    }
    let leader = (i.leader_hint != u64::MAX).then(|| {
        let id = i.leader_hint as u32;
        LeaderRef { id, addr: members.iter().find(|m| m.id == id).map(|m| m.addr.clone()) }
    });
    ClusterViewDoc {
        schema: super::SCHEMA.to_string(),
        source: Source::Live,
        node_id: Some(i.node_id),
        role: Some(role),
        joined: Some(!(i.declared == 0 && i.names.iter().any(Option::is_some))),
        leader,
        view_position: i.consumed,
        commit: Some(i.commit),
        read_at: uc_obs::time::rfc3339_ns(now_ns),
        cluster,
        self_part: Some(SelfPart {
            holdings: holdings_view(&i.own, &i.names, declared_rows, &i.state.catalog, i.catalog_version),
            services: i.services.clone(),
        }),
        nodes,
        nodes_from: (!is_leader).then(|| "leader".to_string()),
        attention,
    }
}
```

Note: in `the_leader_fills_nodes…` node 3's holdings carry `sets_held = 0`, so `held_by` is `[1, 2]`; `holders()` also drops stale entries — confirm the expectation against `CatalogQuery::holders`'s liveness rule and keep the test's intent (a stale or non-holding node is not a holder).

Then `gather` and `render_live`:

```rust
pub fn gather(s: &ObsSources, now_ns: u64) -> LiveInputs {
    use std::sync::atomic::Ordering;
    let status = s.cnc.status();
    let state = s.cluster_view.to_state();
    let names: RowNames = s.cnc.service_names().map(|n| n.map(|n| n.as_str().to_string()));
    let own = *s.snapshot_holdings.lock().unwrap_or_else(|e| e.into_inner());
    let wire = s.soft_wire.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let soft = crate::node::soft_table_with_self(&wire, &state.membership, s.node_id, own, now_ns);
    let commit = s.cnc.counters().commit.load_acquire();
    LiveInputs {
        node_id: s.node_id,
        flags: status.flags.load_acquire(),
        leader_hint: status.leader_hint.load_acquire(),
        commit,
        consumed: s.cluster_view.consumed.load(Ordering::Acquire),
        declared: s.cnc.services_declared(),
        services: services(s, &names, commit),
        names,
        catalog_version: s.cluster_view.catalog_version.load(Ordering::Acquire),
        own,
        soft,
        soft_stale_ns: s.soft_stale_ns,
        gate_released: s.backup_gate_released_level.load(Ordering::Acquire),
        state,
    }
}

/// Per declared row, read exactly as `/metrics`' `service_rows` reads it.
fn services(s: &ObsSources, names: &RowNames, commit: u64) -> Vec<ServiceView> {
    use uc_log::cnc::{PinRead, unpack_service_status};
    let declared = s.cnc.services_declared();
    (0..CNC_MAX_SERVICES)
        .filter(|id| declared & (1 << id) != 0)
        .map(|id| {
            let slot = s.cnc.service_slot(id);
            let (_, attached, _) = unpack_service_status(slot.status.load_acquire());
            let applied = slot.applied.load_acquire();
            let (pinned, pinned_version) = match slot.status.pin() {
                PinRead::Pinned { to, .. } => (true, Some(VersionView::of(to))),
                _ => (false, None),
            };
            ServiceView {
                row: id as u8,
                name: names[id].clone(),
                attached,
                version: attached.then(|| VersionView::of(slot.status.version())),
                applied,
                lag_bytes: commit.saturating_sub(applied),
                timers_pending: slot.identity.timers_pending(),
                pinned,
                pinned_version,
            }
        })
        .collect()
}

pub fn render_live(s: &ObsSources) -> String {
    let now = crate::obs::metrics::now_unix_ns();
    let doc = build_live_doc(&gather(s, now), now);
    let mut body = serde_json::to_string_pretty(&doc).expect("the view serializes");
    body.push('\n');
    body
}
```

Match accessor names to the real ones the fact sheet lists (`unpack_service_status`, `PinRead`, `slot.identity.timers_pending()` — check their module paths in `uc_log::cnc`; `metrics.rs`'s `service_rows` is the model to copy).

- [ ] **Step 6: Generate the golden file.** Temporarily run the golden test with an empty `example.json` (create it empty), copy the `left` value from the failure into `uc_node/src/view/example.json`, then re-run. Read the file once: it must show `"schema": "uc2.cluster-view/1"`, hashes as `"0x…"`, `"commanded_at": "2023-11-14T…Z"`-style times, and `"self"` as a key.

Run: `cargo test -p uc_node --lib view::live`
Expected: PASS (8).

- [ ] **Step 7: Route `/cluster`.** In `obs/http.rs` `route`, add before `_ => not_found()`:

```rust
        "/cluster" => (200, "application/json", crate::view::render_live(sources)),
```

and update `route`'s doc comment to list `/cluster`. Add `fuzz/corpus/uc_node_http/14-get-cluster` containing `GET /cluster HTTP/1.1\r\nHost: x\r\n\r\n`. The fuzz target's `200 | 404 | 503` assertion is unchanged (`/cluster` answers 200).

In `uc_node/tests/obs_http.rs`, extend `metrics_healthz_readyz_serve_and_404_otherwise` (or add `the_cluster_route_serves_the_view`) with:

```rust
    let (code, body) = get(addr, "/cluster");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).expect("/cluster is JSON");
    assert_eq!(v["schema"], "uc2.cluster-view/1");
```

Run: `cargo test -p uc_node --test obs_http` and `cargo build --manifest-path fuzz/Cargo.toml` (nightly not required for a build check; if it needs nightly, run `cargo +nightly fuzz build uc_node_http`).
Expected: PASS / builds.

- [ ] **Step 8: Commit**

```bash
git add uc_node fuzz/corpus/uc_node_http/14-get-cluster
git commit -m "view: the live document and GET /cluster (leader-only nodes and held_by; golden example)"
```

---

### Task 6: The obs socket and the always-on obs server

**Files:**
- Create: `uc_node/src/obs/socket.rs`
- Modify: `uc_node/src/obs/mod.rs` (`pub mod socket;`), `uc_node/src/obs/http.rs`, `uc_node/src/bin/uc2-node.rs`, `uc_node/tests/obs_http.rs`, `uc_node/tests/lifecycle.rs`, `uc_node/tests/backup.rs`

**Interfaces:**
- Produces:
  - `pub const socket::OBS_SOCKET_FILE: &str = "obs.sock";`
  - `pub fn socket::bind(instance_dir: &Path) -> io::Result<UnixListener>` — unlinks a stale file, binds (via `/proc/self/fd/<fd>/obs.sock` when the direct path exceeds 107 bytes), chmods `0600`.
  - `pub fn socket::connect(instance_dir: &Path) -> io::Result<UnixStream>`
  - `pub fn socket::get(instance_dir: &Path, path: &str) -> io::Result<(u16, String)>` — one HTTP/1.1 GET over the socket (used by `uc2ctl` and tests).
  - `ObsServer::serve_on(sources: ObsSources, tcp: Option<SocketAddr>, unix_dir: Option<&Path>) -> io::Result<ObsServer>`; `ObsServer::serve` keeps its signature (wraps `serve_on(.., Some(bind), None)`); `ObsServer::tcp_addr(&self) -> Option<SocketAddr>`; `local_addr()` keeps returning `SocketAddr` (`self.tcp_addr().expect("served without TCP")`); `stop()` unlinks the socket file it bound.

- [ ] **Step 1: Write the failing tests** in `socket.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> std::path::PathBuf {
        let d = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn echo_once(l: UnixListener) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut s, _) = l.accept().unwrap();
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").unwrap();
        })
    }

    #[test]
    fn binds_0600_replaces_a_stale_file_and_round_trips() {
        use std::os::unix::fs::PermissionsExt;
        let d = dir("obs-sock-basic");
        std::fs::write(d.join(OBS_SOCKET_FILE), b"stale").unwrap();
        let l = bind(&d).unwrap();
        let mode = std::fs::metadata(d.join(OBS_SOCKET_FILE)).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let t = echo_once(l);
        assert_eq!(get(&d, "/cluster").unwrap(), (200, "ok".to_string()));
        t.join().unwrap();
    }

    #[test]
    fn a_path_longer_than_sun_path_binds_and_connects_through_proc_self_fd() {
        let long = "x".repeat(120);
        let d = dir(&format!("obs-sock-{long}"));
        assert!(d.join(OBS_SOCKET_FILE).as_os_str().len() > 107);
        let t = echo_once(bind(&d).unwrap());
        assert_eq!(get(&d, "/").unwrap().0, 200);
        t.join().unwrap();
    }

    #[test]
    fn a_leftover_socket_with_no_listener_refuses() {
        let d = dir("obs-sock-leftover");
        drop(bind(&d).unwrap());
        assert!(d.join(OBS_SOCKET_FILE).exists(), "dropping a listener leaves the file");
        let e = connect(&d).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::ConnectionRefused);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_node --lib obs::socket`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `socket.rs`** (above the tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `<instance_dir>/obs.sock`: the obs router over a unix socket, so a
//! same-host reader (`uc2ctl`) needs no `[metrics]` listener. Trust is host
//! access plus the file's `0600` mode — the model `cnc2.dat` already uses.
//! `sun_path` holds 107 bytes; a longer instance dir is reached through
//! `/proc/self/fd/<dirfd>/obs.sock` (the node is Linux-only).

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const OBS_SOCKET_FILE: &str = "obs.sock";
const SUN_PATH_MAX: usize = 107;

/// Run `f` on a path that names `<dir>/obs.sock` and fits `sun_path`.
fn with_short_path<T>(dir: &Path, f: impl FnOnce(&Path) -> io::Result<T>) -> io::Result<T> {
    let direct = dir.join(OBS_SOCKET_FILE);
    if direct.as_os_str().len() <= SUN_PATH_MAX {
        return f(&direct);
    }
    let handle = File::open(dir)?;
    let via: PathBuf = format!("/proc/self/fd/{}/{OBS_SOCKET_FILE}", handle.as_raw_fd()).into();
    let r = f(&via);
    drop(handle);
    r
}

pub fn bind(instance_dir: &Path) -> io::Result<UnixListener> {
    let path = instance_dir.join(OBS_SOCKET_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let l = with_short_path(instance_dir, |p| UnixListener::bind(p))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    l.set_nonblocking(true)?;
    Ok(l)
}

pub fn connect(instance_dir: &Path) -> io::Result<UnixStream> {
    with_short_path(instance_dir, |p| UnixStream::connect(p))
}

/// One `GET` over the socket: `(status, body)`.
pub fn get(instance_dir: &Path, path: &str) -> io::Result<(u16, String)> {
    let mut s = connect(instance_dir)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.set_write_timeout(Some(Duration::from_secs(5)))?;
    write!(s, "GET {path} HTTP/1.1\r\nHost: obs\r\nConnection: close\r\n\r\n")?;
    let mut raw = String::new();
    s.read_to_string(&mut raw)?;
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no header terminator"))?;
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no status code"))?;
    Ok((code, body.to_string()))
}
```

- [ ] **Step 4: Run the socket tests**

Run: `cargo test -p uc_node --lib obs::socket`
Expected: PASS (3).

- [ ] **Step 5: Generalize `ObsServer`.** In `http.rs`:
  - Add a private trait and impls:

```rust
trait Conn: Read + Write {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()>;
}
impl Conn for TcpStream {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> { TcpStream::set_read_timeout(self, d) }
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> { TcpStream::set_write_timeout(self, d) }
}
impl Conn for std::os::unix::net::UnixStream {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> { std::os::unix::net::UnixStream::set_read_timeout(self, d) }
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> { std::os::unix::net::UnixStream::set_write_timeout(self, d) }
}
```

  - Make `handle_conn<C: Conn>(mut stream: C, sources: &ObsSources)` and `write_response<C: Conn>(stream: &mut C, …)` generic (bodies unchanged).
  - `ObsServer` gains `unix_path: Option<PathBuf>`; `local_addr: Option<SocketAddr>`.
  - `serve_on`:

```rust
pub fn serve_on(sources: ObsSources, tcp: Option<SocketAddr>, unix_dir: Option<&Path>) -> io::Result<ObsServer> {
    let tcp = match tcp {
        Some(bind) => {
            let l = TcpListener::bind(bind)?;
            l.set_nonblocking(true)?;
            Some(l)
        }
        None => None,
    };
    let local_addr = tcp.as_ref().map(|l| l.local_addr()).transpose()?;
    let unix = unix_dir.map(super::socket::bind).transpose()?;
    let unix_path = unix_dir.map(|d| d.join(super::socket::OBS_SOCKET_FILE));
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let thread = thread::Builder::new().name("uc2-obs".to_string()).spawn(move || {
        let sources = sources;
        while !stop_thread.load(Ordering::Acquire) {
            let mut served = false;
            if let Some(l) = &tcp {
                if let Ok((s, _)) = l.accept() {
                    let _ = s.set_nonblocking(false);
                    handle_conn(s, &sources);
                    served = true;
                }
            }
            if let Some(l) = &unix {
                if let Ok((s, _)) = l.accept() {
                    let _ = s.set_nonblocking(false);
                    handle_conn(s, &sources);
                    served = true;
                }
            }
            if !served {
                thread::sleep(ACCEPT_POLL);
            }
        }
    })?;
    Ok(ObsServer { thread: Some(thread), stop, local_addr, unix_path })
}
```

  Any accept error other than a connection is treated as an empty poll (as today). `stop(mut self)`: after joining, `if let Some(p) = &self.unix_path { let _ = std::fs::remove_file(p); }`. Update the module doc comment: two listeners, one loop, `/cluster`.

- [ ] **Step 6: Review-focus 3 test** in `uc_node/tests/obs_http.rs`:

```rust
/// Review focus 3: a client that asks for /cluster and never reads the body
/// must not wedge the single-threaded server for longer than the write
/// deadline.
#[test]
fn a_client_that_never_reads_a_cluster_body_does_not_wedge_metrics() {
    use std::io::Write;
    let (srv, _sources) = synthetic_server();
    let addr = srv.local_addr();
    let mut lazy = std::net::TcpStream::connect(addr).unwrap();
    lazy.write_all(b"GET /cluster HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    // Never read from `lazy`.
    let t0 = std::time::Instant::now();
    let (code, _) = get(addr, "/metrics");
    assert_eq!(code, 200);
    assert!(t0.elapsed() < std::time::Duration::from_secs(5), "took {:?}", t0.elapsed());
    drop(lazy);
    srv.stop();
}
```

And a unix-door test:

```rust
#[test]
fn the_unix_socket_serves_the_same_routes_and_is_removed_on_stop() {
    let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let sources = synthetic_sources();
    let srv = uc_node::obs::http::ObsServer::serve_on(sources, None, Some(root.path())).unwrap();
    let (code, body) = uc_node::obs::socket::get(root.path(), "/cluster").unwrap();
    assert_eq!(code, 200);
    assert!(body.contains("uc2.cluster-view/1"));
    assert_eq!(uc_node::obs::socket::get(root.path(), "/metrics").unwrap().0, 200);
    srv.stop();
    assert!(!root.path().join("obs.sock").exists());
}
```

`ObsSources::for_tests()` is `cfg(any(test, fuzzing))` and invisible to integration tests, so first factor `synthetic_server()`'s struct literal (`uc_node/tests/obs_http.rs:79`) into `fn synthetic_sources() -> ObsSources` and have `synthetic_server()` call it.

Run: `cargo test -p uc_node --test obs_http`
Expected: PASS.

- [ ] **Step 7: The daemon always serves the socket.** In `uc2-node.rs`, replace the `if let Some(addr) = opts.obs.metrics_bind { … }` block with:

```rust
    let obs = node.observability();
    // Cluster view (project 4): the obs server always runs — the unix socket
    // in the instance dir (bound after `Node::start_with` took
    // `instance.lock`, so replacing a stale socket file is safe) and the TCP
    // listener when `[metrics]` is configured.
    let srv = match ObsServer::serve_on(obs.clone(), opts.obs.metrics_bind, Some(&instance_dir)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("uc2-node: failed to start the observability endpoint: {e}");
            node.stop();
            return ExitCode::from(1);
        }
    };
    let sock = instance_dir.join(uc_node::obs::socket::OBS_SOCKET_FILE);
    uc_node::obs_event!(Info, "obs_socket_listening", node = id as u64, path = sock.display().to_string().as_str());
    if let Some(addr) = srv.tcp_addr() {
        let url = format!("http://{addr}/metrics");
        uc_node::obs_event!(Info, "metrics_listening", node = id as u64, url = url.as_str());
    }
```

and at shutdown replace `if let Some(srv) = srv { srv.stop(); }` with `srv.stop();` (keep the comment). Keep `metrics_listening`'s exact text: an existing lifecycle test greps for it.

- [ ] **Step 8: Daemon tests** in `uc_node/tests/lifecycle.rs` (reuse `daemon_config`/`scratch`; pick an unused port such as 19731):

```rust
#[test]
fn the_daemon_serves_the_cluster_view_on_its_obs_socket_and_removes_it_on_sigterm() {
    use std::os::unix::fs::PermissionsExt;
    let root = scratch();
    let (cfg, inst) = daemon_config(root.path(), 19731, "127.0.0.1:19731", "");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config").arg(&cfg)
        .spawn().unwrap();
    let sock = inst.join("obs.sock");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let body = loop {
        if let Ok((200, b)) = uc_node::obs::socket::get(&inst, "/cluster") {
            if b.contains("\"joined\": true") { break b; }
        }
        assert!(std::time::Instant::now() < deadline, "no joined view on {}", sock.display());
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    assert_eq!(std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777, 0o600);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["role"], "leader");
    assert_eq!(v["cluster"]["catalog"]["empty"], true);
    assert!(v["attention"].as_array().unwrap().iter().any(|a| a["code"] == "catalog_empty"));
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    assert!(child.wait().unwrap().success());
    assert!(!sock.exists(), "a clean shutdown removes obs.sock");
}

#[test]
fn a_sigkilled_daemon_leaves_a_socket_that_refuses() {
    let root = scratch();
    let (cfg, inst) = daemon_config(root.path(), 19732, "127.0.0.1:19732", "");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config").arg(&cfg)
        .spawn().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while uc_node::obs::socket::get(&inst, "/healthz").is_err() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(inst.join("obs.sock").exists());
    assert_eq!(uc_node::obs::socket::connect(&inst).unwrap_err().kind(), std::io::ErrorKind::ConnectionRefused);
    assert!(matches!(uc_node::ipc::probe_instance_lock(&inst).unwrap(), uc_node::ipc::InstanceLock::Free));
}
```

Check `daemon_config`'s exact argument meaning and the daemon's CLI flag for the config path in the existing `daemon_starts_from_a_config_file_and_stops_cleanly_on_sigterm` and copy them. The view's `"joined": true` spelling must match `serde_json::to_string_pretty`'s output (`"joined": true`).

In `uc_node/tests/backup.rs`, add after Test 1:

```rust
#[test]
fn backup_never_copies_the_obs_socket() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let node = start_node(&dir, "backup-obs", PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);
    match node.stop_draining(Duration::from_secs(10)) {
        uc_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
    drop(uc_node::obs::socket::bind(&dir).unwrap()); // leaves obs.sock behind
    let out = root.path().join("backup-obs-out");
    backup_instance(&dir, &out).expect("backup_instance");
    let mut stack = vec![out.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            assert_ne!(p.file_name().unwrap(), "obs.sock", "{} was copied", p.display());
            if p.is_dir() { stack.push(p); }
        }
    }
}
```

Run: `cargo test -p uc_node --test lifecycle the_daemon_serves a_sigkilled && cargo test -p uc_node --test backup backup_never_copies_the_obs_socket`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add uc_node
git commit -m "obs: an always-on unix socket beside the TCP listener; the daemon serves /cluster on obs.sock"
```

---

### Task 7: End-to-end content on a live 3-node cluster

**Files:**
- Modify: `uc_node/tests/catalog.rs` (append a `// ---- cluster view (project 4)` section reusing its `Cluster`/`spawn` harness)

**Interfaces:**
- Consumes: `uc_node::view::render_live`, the catalog harness (`spawn`, `await_single_leader`, `await_capable`, `command_instant`, `instant_until_complete`, `await_agreed`, and the diverged scenario's setup in `a_diverged_row_completes_the_set_but_never_moves_the_floor`).

- [ ] **Step 1: Write the tests.** Append to `uc_node/tests/catalog.rs` (it already has `serialize`, `spawn`, `opts`, `start_sums`, `await_single_leader`, `await_capable`, `submit_frames`, `instant_until_complete`, `await_agreed`, `await_until`, and the `SumSm` row named `"sum"`):

```rust
// ================================================================ cluster view (project 4)

fn view_of(node: &Node) -> serde_json::Value {
    serde_json::from_str(&uc_node::view::render_live(&node.observability())).expect("view is JSON")
}

#[test]
fn the_view_before_the_first_instant_shows_genesis_settings_and_catalog_empty() {
    let _g = serialize();
    let mut c = spawn(3, 0, opts("view-genesis", false), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    await_capable(&c, &[0]);
    let v = view_of(c.node(leader));
    assert_eq!(v["role"], "leader");
    assert_eq!(v["joined"], true);
    assert_eq!(v["cluster"]["catalog"]["empty"], true);
    assert_eq!(v["cluster"]["membership"]["members"].as_array().unwrap().len(), 3);
    assert!(v["attention"].as_array().unwrap().iter().any(|a| a["code"] == "catalog_empty"));
    assert_eq!(v["self"]["services"][0]["name"], "sum");
    assert_eq!(v["self"]["services"][0]["attached"], true);
    let follower = c.voters().into_iter().find(|&i| i != leader).unwrap();
    let f = view_of(c.node(follower));
    assert_eq!(f["role"], "follower");
    assert!(f["nodes"].is_null());
    assert_eq!(f["nodes_from"], "leader");
    assert_eq!(f["leader"]["id"], v["node_id"]);
    assert_eq!(
        f["cluster"]["settings"], v["cluster"]["settings"],
        "the cluster part is the same on every node"
    );
}

#[test]
fn after_an_instant_the_set_is_agreed_and_held_by_every_holder() {
    let _g = serialize();
    let mut c = spawn(3, 0, opts("view-agreed", false), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let voters = c.voters();
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 2000);
    let p = instant_until_complete(&c, leader, &voters);
    await_agreed(&c, &voters, p);
    // Holdings travel on STATUS; wait until the leader lists all three holders.
    await_until(30, "the leader's view lists every holder of p", || {
        let v = view_of(c.node(leader));
        v["cluster"]["catalog"]["sets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["position"] == p && s["held_by"].as_array().is_some_and(|h| h.len() == 3))
    });
    let v = view_of(c.node(leader));
    let set = v["cluster"]["catalog"]["sets"].as_array().unwrap().iter().find(|s| s["position"] == p).unwrap().clone();
    assert_eq!(set["agreed"], true);
    assert_eq!(set["rows"][0]["name"], "sum");
    assert_eq!(set["rows"][0]["verdict"], "agreed");
    assert_eq!(v["cluster"]["catalog"]["floor"], p);
    assert!(v["attention"].as_array().unwrap().iter().all(|a| a["code"] != "catalog_empty"));
    assert_eq!(v["nodes"].as_array().unwrap().len(), 3);
    assert!(v["nodes"].as_array().unwrap().iter().all(|n| n["stale"] == false));
}
```

And at the END of the existing `a_diverged_row_completes_the_set_but_never_moves_the_floor` body (after its last assertion, while `c` and `p1` are still live), add:

```rust
    // Cluster view (project 4): the same verdict, as an attention code.
    let v = view_of(c.node(leader));
    assert!(
        v["attention"].as_array().unwrap().iter().any(|a| a["code"] == "set_diverged" && a["set"] == p1 && a["row"] == "sum"),
        "the view names the diverged row: {}",
        v["attention"]
    );
```

The demoted-leader case is pinned by Task 5's `a_demoted_leader_never_prints_its_old_soft_table` (the build gates `nodes` on the leader flag, not on the table's contents); it is not repeated end to end, because the catalog harness has no leader-deposing helper and the flag is read from the same cnc word in both paths.

- [ ] **Step 2: Run them**

Run: `cargo test -p uc_node --test catalog view`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/catalog.rs
git commit -m "tests: the cluster view on a live 3-node cluster — genesis, agreed set, diverged row, roles"
```

---

### Task 8: `uc2ctl cluster` / `catalog` and the offline document

**Files:**
- Create: `uc_node/src/view/offline.rs`, `uc_ctl/src/view.rs`, `uc_ctl/tests/cluster_view_bin.rs`
- Modify: `uc_node/src/view/mod.rs` (`pub mod offline;`), `uc_ctl/src/main.rs` (`mod view;`, `Cmd::Cluster(ViewArgs)`, `Cmd::Catalog(ViewArgs)`, dispatch)

**Interfaces:**
- Consumes: Tasks 3–6.
- Produces:
  - `pub fn offline::build_offline_doc(st: &ClusterState, names: &RowNames, artifact_position: u64, now_ns: u64) -> ClusterViewDoc`
  - `pub fn offline::render_offline(instance_dir: &Path, names: &RowNames) -> io::Result<Option<ClusterViewDoc>>`
  - `uc_ctl::view::Fetched { Live(ClusterViewDoc), Offline(ClusterViewDoc), NoArtifact }`
  - `pub fn uc_ctl::view::fetch(common: &CommonArgs, require_live_when_held: bool) -> anyhow::Result<Fetched>`
  - `pub fn uc_ctl::view::parse(body: &str) -> anyhow::Result<ClusterViewDoc>` (schema check)
  - `#[derive(clap::Args)] struct ViewArgs { #[command(flatten)] common: CommonArgs, #[arg(long)] json: bool }`

- [ ] **Step 1: Offline builder** in `uc_node/src/view/offline.rs`, with its test:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The document built from the newest cluster artifact, for `uc2ctl` on a
//! stopped node. Same `build` as the live path; no `self`, `nodes` or
//! services, and no live-only attention codes.

use std::io;
use std::path::Path;

use super::build::{RowNames, cluster_part};
use super::model::*;
use crate::cluster_fsm::ClusterState;

pub fn build_offline_doc(st: &ClusterState, names: &RowNames, artifact_position: u64, now_ns: u64) -> ClusterViewDoc {
    let cluster = cluster_part(st, names, None, None);
    let attention = super::attention::from_cluster(&cluster);
    ClusterViewDoc {
        schema: super::SCHEMA.to_string(),
        source: Source::Artifact,
        node_id: None,
        role: None,
        joined: None,
        leader: None,
        view_position: artifact_position,
        commit: None,
        read_at: uc_obs::time::rfc3339_ns(now_ns),
        cluster,
        self_part: None,
        nodes: None,
        nodes_from: None,
        attention,
    }
}

pub fn render_offline(instance_dir: &Path, names: &RowNames) -> io::Result<Option<ClusterViewDoc>> {
    let Some(st) = crate::cluster_agent::read_committed_state(instance_dir)? else {
        return Ok(None);
    };
    let pos = st.applied;
    Ok(Some(build_offline_doc(&st, names, pos, crate::obs::metrics::now_unix_ns())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_offline_document_has_no_live_parts() {
        let d = build_offline_doc(&ClusterState::genesis_empty(), &Default::default(), 4096, 0);
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["source"], "artifact");
        assert_eq!(v["view_position"], 4096);
        for k in ["self", "nodes", "role", "joined", "leader", "commit", "nodes_from"] {
            assert!(v[k].is_null(), "{k} must be null offline");
        }
        assert!(v["cluster"]["tiers"].as_array().unwrap().is_empty());
        assert!(v["cluster"]["purge_gate"]["released"].is_null());
    }
}
```

Run: `cargo test -p uc_node --lib view::offline` — PASS.

- [ ] **Step 2: Write the failing `uc_ctl` tests** in `uc_ctl/src/view.rs` (unit) and `uc_ctl/tests/cluster_view_bin.rs` (binary):

Unit tests (bottom of `uc_ctl/src/view.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Review focus 4.
    #[test]
    fn a_foreign_schema_is_refused_by_name() {
        let doc = uc_node::view::offline::build_offline_doc(
            &uc_node::ClusterState::genesis_empty(), &Default::default(), 0, 0);
        let mut v = serde_json::to_value(&doc).unwrap();
        v["schema"] = "uc2.cluster-view/2".into();
        let e = parse(&v.to_string()).unwrap_err().to_string();
        assert!(e.contains("uc2.cluster-view/2") && e.contains("uc2.cluster-view/1"), "{e}");
    }

    #[test]
    fn catalog_text_names_rows_and_falls_back_to_row_n() {
        use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};
        let mut st = uc_node::ClusterState::genesis_empty();
        st.running[2] = Some(uc_protocol::v2::upgrade::RowRunning { row: 2, version: 0, record_pos: 64 });
        let mut a = SetEntry::commanded(1024, SetKind::Full, 0);
        a.state = SetState::Complete;
        a.rows[2] = RowEntry { version: 0, hash: 0xaa, verdict: RowVerdict::Diverged, size: 1 };
        st.catalog = vec![a];
        let doc = uc_node::view::offline::build_offline_doc(&st, &Default::default(), 1024, 0);
        let text = catalog_text(&doc);
        assert!(text.contains("offline: newest artifact at 1024"), "{text}");
        assert!(text.contains("row2"), "{text}");
        assert!(text.contains("set_diverged"), "{text}");
    }
}
```

Binary tests: create `uc_ctl/tests/cluster_view_bin.rs`. Copy VERBATIM from `uc_ctl/tests/snapshot_bin.rs` its imports, `APP` (rename the value to `"ctlview"`), `bin`, `make_config`, `start_node`, `await_leader`, `TrivialSm` with both trait impls, `start_capable_service`, `await_row0_capable`, `Run`, `run_ctl` and `parse_instant` — those are the in-process node + snapshot-capable service bring-up. Then add:

```rust
fn scratch(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new().prefix(prefix).tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("tempdir")
}

fn serve(node: &Node, dir: &Path) -> uc_node::obs::http::ObsServer {
    uc_node::obs::http::ObsServer::serve_on(node.observability(), None, Some(dir)).expect("obs socket")
}

#[test]
fn cluster_json_reads_the_live_view_over_the_socket() {
    let root = scratch("uc2ctl-view-live-");
    let (node, dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    let srv = serve(&node, &dir);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["cluster", "--json", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).expect("stdout is JSON");
    assert_eq!(v["schema"], "uc2.cluster-view/1");
    assert_eq!(v["source"], "live");
    assert_eq!(v["role"], "leader");
    srv.stop();
    node.stop();
}

#[test]
fn catalog_text_on_a_live_node_prints_the_source_and_attention() {
    let root = scratch("uc2ctl-view-catalog-");
    let (node, dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    let srv = serve(&node, &dir);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["catalog", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("source=live"), "{}", r.stdout);
    assert!(r.stdout.contains("catalog_empty"), "{}", r.stdout);
    srv.stop();
    node.stop();
}

#[test]
fn a_held_lock_without_a_socket_is_reported_by_name() {
    let root = scratch("uc2ctl-view-nosock-");
    let (node, dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["cluster", "--instance-dir", d, "--app-id", APP]);
    assert_ne!(r.status, 0, "{r:?}");
    assert!(r.stderr.contains("obs socket is not bound"), "{}", r.stderr);
    node.stop();
}

#[test]
fn a_stopped_node_reads_offline_and_a_leftover_socket_is_ignored() {
    let root = scratch("uc2ctl-view-offline-");
    let (node, dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    let svc = start_capable_service(&dir, APP);
    await_row0_capable(&dir, APP, 20);
    let d = dir.to_str().unwrap().to_string();
    let r = run_ctl(&["snapshot", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let p = parse_instant(&r.stdout);
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.snapshot_set_position() != p {
        assert!(Instant::now() < deadline, "set {p} never completed");
        std::thread::sleep(Duration::from_millis(20));
    }
    svc.stop();
    node.stop();
    drop(uc_node::obs::socket::bind(&dir).unwrap()); // a dead obs.sock
    let r = run_ctl(&["catalog", "--json", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).unwrap();
    assert_eq!(v["source"], "artifact");
    assert!(v["holdings"].is_null());
    assert_eq!(v["catalog"]["sets"][0]["rows"][0]["name"], "sum", "names come from the stale cnc2.dat");
    let r = run_ctl(&["catalog", "--instance-dir", &d, "--app-id", APP]);
    assert!(r.stdout.contains(&format!("offline: newest artifact at {p}")), "{}", r.stdout);
}

#[test]
fn a_stopped_node_with_no_artifact_says_so() {
    let root = scratch("uc2ctl-view-none-");
    let (node, dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    node.stop();
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["cluster", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("no cluster artifact yet"), "{}", r.stdout);
}
```

Use the real stop methods (`svc.stop()` / `node.stop()` or their `Drop`) exactly as `snapshot_bin.rs` does; if `Node::stop` takes `self`, the test already owns `node`.

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p uc_ctl view::` and `cargo test -p uc_ctl --test cluster_view_bin`
Expected: FAIL to compile (no `view` module, no subcommands).

- [ ] **Step 4: Implement `uc_ctl/src/view.rs`:**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl cluster` / `catalog`: the cluster view (project 4). Live over
//! `<instance_dir>/obs.sock` when the node runs; else built offline from the
//! newest cluster artifact through the node's own builder.

use uc_node::view::ClusterViewDoc;
use uc_node::view::build::RowNames;

use crate::CommonArgs;

pub enum Fetched {
    Live(ClusterViewDoc),
    Offline(ClusterViewDoc),
    NoArtifact,
}

pub fn parse(body: &str) -> anyhow::Result<ClusterViewDoc> {
    let v: serde_json::Value = serde_json::from_str(body)?;
    let schema = v["schema"].as_str().unwrap_or("<missing>");
    if schema != uc_node::view::SCHEMA {
        anyhow::bail!(
            "the node serves cluster view schema {schema}; this uc2ctl reads {} — use the uc2ctl that matches the node",
            uc_node::view::SCHEMA
        );
    }
    Ok(serde_json::from_value(v)?)
}

/// Row names for the offline view: from a readable (possibly stale)
/// `cnc2.dat`, else none (planning ruling PR13).
fn offline_names(common: &CommonArgs) -> RowNames {
    match crate::open(common) {
        Ok(cnc) => cnc.service_names().map(|n| n.map(|n| n.as_str().to_string())),
        Err(_) => Default::default(),
    }
}

/// `require_live_when_held`: `cluster`/`catalog` refuse a held lock with no
/// socket (PR10); the `show` commands pass `false` and fall back offline.
pub fn fetch(common: &CommonArgs, require_live_when_held: bool) -> anyhow::Result<Fetched> {
    use uc_node::ipc::{InstanceLock, probe_instance_lock};
    let dir = &common.instance_dir;
    if matches!(probe_instance_lock(dir)?, InstanceLock::Held) {
        match uc_node::obs::socket::get(dir, "/cluster") {
            Ok((200, body)) => return Ok(Fetched::Live(parse(&body)?)),
            Ok((code, _)) => anyhow::bail!("the node answered /cluster with HTTP {code}"),
            Err(e) if require_live_when_held => anyhow::bail!(
                "node running but its obs socket is not bound (starting, or not run by uc2-node): {e}"
            ),
            Err(_) => {}
        }
    }
    match uc_node::view::offline::render_offline(dir, &offline_names(common))? {
        Some(doc) => Ok(Fetched::Offline(doc)),
        None => Ok(Fetched::NoArtifact),
    }
}

fn header(doc: &ClusterViewDoc) -> String {
    match doc.source {
        uc_node::view::model::Source::Live => format!(
            "source=live node={} role={} joined={} view_position={} commit={}",
            doc.node_id.map_or("?".into(), |n| n.to_string()),
            doc.role.map_or("?".into(), |r| format!("{r:?}").to_lowercase()),
            doc.joined.unwrap_or(false),
            doc.view_position,
            doc.commit.unwrap_or(0),
        ),
        uc_node::view::model::Source::Artifact => {
            format!("source=artifact (offline: newest artifact at {})", doc.view_position)
        }
    }
}

pub fn catalog_text(doc: &ClusterViewDoc) -> String {
    use std::fmt::Write;
    let c = &doc.cluster.catalog;
    let mut s = header(doc) + "\n";
    let _ = writeln!(
        s,
        "catalog retain_sets={} empty={} floor={}",
        c.retain_sets,
        c.empty,
        c.floor.map_or("none".into(), |f| f.to_string())
    );
    for set in &c.sets {
        let _ = writeln!(
            s,
            "  set={} kind={:?} state={:?} agreed={} stalled={} size={} held_by={}",
            set.position,
            set.kind,
            set.state,
            set.agreed,
            set.stalled,
            set.size.map_or("unknown".into(), |b| b.to_string()),
            set.held_by.as_ref().map_or("(leader only)".into(), |h| format!("{h:?}")),
        );
        for r in set.rows.iter().chain(std::iter::once(&set.cluster)) {
            let _ = writeln!(
                s,
                "    {} verdict={:?} hash={}",
                r.name.clone().unwrap_or_else(|| format!("row{}", r.row)),
                r.verdict,
                r.hash.map_or("-".into(), |h| format!("0x{:016x}", h.0)),
            );
        }
    }
    for t in &doc.cluster.tiers {
        let _ = writeln!(
            s,
            "tier={} oldest_set={} newest_set={} through={} lag_bytes={}",
            t.id,
            t.oldest_set,
            t.newest_set,
            t.through,
            t.lag_bytes.map_or("-".into(), |l| l.to_string())
        );
    }
    let g = &doc.cluster.purge_gate;
    let _ = writeln!(
        s,
        "purge_gate requires_tiers={} position={} tiers_reporting={} released={}",
        g.requires_tiers,
        g.position.map_or("off".into(), |p| p.to_string()),
        g.tiers_reporting,
        g.released.map_or("-".into(), |r| r.to_string())
    );
    if let Some(sp) = &doc.self_part {
        let h = &sp.holdings;
        let _ = writeln!(
            s,
            "self journal_first={} durable={} free_bytes={} journal_bytes={} snapshots_bytes={} sets_held={}",
            h.journal_first, h.durable, h.free_bytes, h.journal_bytes, h.snapshots_bytes,
            h.sets_held.as_ref().map_or("unknown".into(), |v| format!("{v:?}"))
        );
    }
    for a in &doc.attention {
        let _ = writeln!(s, "attention {}", serde_json::to_string(a).unwrap_or_default());
    }
    s
}

pub fn cluster_text(doc: &ClusterViewDoc) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let m = &doc.cluster.membership;
    let _ = writeln!(s, "membership config_version={}", m.config_version);
    for x in &m.members {
        let _ = writeln!(s, "  id={} addr={} role={:?}", x.id, x.addr, x.role);
    }
    let _ = writeln!(s, "schedule entries={} position={}", doc.cluster.schedule.entries.len(), doc.cluster.schedule.position);
    for u in &doc.cluster.upgrades {
        let _ = writeln!(
            s,
            "upgrade row={} running={} pins={}",
            u.name.clone().unwrap_or_else(|| format!("row{}", u.row)),
            u.running.as_ref().map_or("-".into(), |r| r.version.text.clone()),
            u.pins.len()
        );
    }
    if let Some(sp) = &doc.self_part {
        for sv in &sp.services {
            let _ = writeln!(
                s,
                "service {} attached={} version={} lag_bytes={} pinned={}",
                sv.name.clone().unwrap_or_else(|| format!("row{}", sv.row)),
                sv.attached,
                sv.version.as_ref().map_or("-".into(), |v| v.text.clone()),
                sv.lag_bytes,
                sv.pinned
            );
        }
    }
    catalog_text(doc) + &s
}

pub fn run(common: &CommonArgs, json: bool, catalog_only: bool) -> anyhow::Result<()> {
    let doc = match fetch(common, true)? {
        Fetched::Live(d) | Fetched::Offline(d) => d,
        Fetched::NoArtifact => {
            println!("no cluster artifact yet (node not running; nothing to read offline)");
            return Ok(());
        }
    };
    if json {
        let v = if catalog_only {
            serde_json::json!({
                "schema": doc.schema,
                "source": doc.source,
                "node_id": doc.node_id,
                "role": doc.role,
                "view_position": doc.view_position,
                "commit": doc.commit,
                "read_at": doc.read_at,
                "catalog": doc.cluster.catalog,
                "tiers": doc.cluster.tiers,
                "purge_gate": doc.cluster.purge_gate,
                "holdings": doc.self_part.as_ref().map(|s| &s.holdings),
                "nodes": doc.nodes,
                "attention": doc.attention.iter().filter(|a| a.code != "node_stale" || doc.nodes.is_some()).collect::<Vec<_>>(),
            })
        } else {
            serde_json::to_value(&doc)?
        };
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else if catalog_only {
        print!("{}", catalog_text(&doc));
    } else {
        print!("{}", cluster_text(&doc));
    }
    Ok(())
}
```

Note on `--json`: `cluster --json` re-serializes the parsed document (which round-trips losslessly through the model), not the raw body; this keeps one code path for live and offline. That is a deviation from spec §6.3's "verbatim" wording — record it in Task 10's errata.

In `main.rs`: add `mod view;`, the `ViewArgs` struct, two `Cmd` variants with doc comments (`/// The live cluster view (project 4): membership, settings, schedule, upgrades, catalog, tiers, holdings. Offline from the newest artifact when the node is stopped.` and `/// The storage part of the cluster view: catalog sets, floor, tiers, purge gate, holdings.`), and dispatch `Cmd::Cluster(a) => view::run(&a.common, a.json, false)`, `Cmd::Catalog(a) => view::run(&a.common, a.json, true)`. Ensure `CommonArgs` is `pub(crate)` if `view.rs` needs it.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p uc_ctl view:: && cargo test -p uc_ctl --test cluster_view_bin`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/view uc_ctl
git commit -m "uc2ctl: cluster and catalog read the live view over obs.sock, offline from the newest artifact"
```

---

### Task 9: `settings show`, `schedule show`, `upgrade show` through the view

**Files:**
- Modify: `uc_ctl/src/settings.rs` (`show`), `uc_ctl/src/schedule.rs` (`show`), `uc_ctl/src/upgrade.rs` (`show`)
- Test: `uc_ctl/tests/cluster_view_bin.rs` (one new test); the existing tests of these commands must pass unchanged.

**Interfaces:**
- Consumes: `uc_ctl::view::fetch(common, false)`, `uc_node::view::convert::{settings, table, upgrade}`.

- [ ] **Step 1: Write the failing test** in `uc_ctl/tests/cluster_view_bin.rs`:

```rust
#[test]
fn settings_show_reads_the_live_genesis_before_any_instant() {
    let root = scratch("uc2ctl-view-settings-");
    let (node, dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    let srv = serve(&node, &dir);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["settings", "show", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(!r.stdout.contains("no cluster artifact yet"), "{}", r.stdout);
    assert!(r.stdout.contains("retain_sets=1"), "{}", r.stdout);
    assert!(r.stdout.contains("snapshot_target=all"), "{}", r.stdout);
    srv.stop();
    node.stop();
}
```

Run: `cargo test -p uc_ctl --test cluster_view_bin settings_show_reads_the_live_genesis_before_any_instant`
Expected: FAIL — prints "no cluster artifact yet".

- [ ] **Step 2: Rewrite the three `show` functions.** Each keeps its existing printing code and swaps only the data source.

`settings.rs`:

```rust
pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let doc = match crate::view::fetch(common, false)? {
        crate::view::Fetched::Live(d) | crate::view::Fetched::Offline(d) => d,
        crate::view::Fetched::NoArtifact => {
            println!("no cluster artifact yet");
            return Ok(());
        }
    };
    let s = uc_node::view::convert::settings(&doc.cluster.settings);
    let position = match doc.source {
        uc_node::view::model::Source::Live => doc.view_position,
        uc_node::view::model::Source::Artifact => doc.view_position,
    };
    println!("{}", render_settings_line(position, &s));
    Ok(())
}
```

(The `match` collapses to `doc.view_position`; write it that way — the live position is the walk cursor, the offline one the artifact's, both carried in `view_position`.)

`schedule.rs`: replace the artifact read with `fetch(common, false)`; on `NoArtifact` print the EXISTING "no cluster artifact yet (the table is committed on the log; …)" line; on a live document whose `schedule.position == 0 && entries.is_empty()` print `no schedule table committed`; otherwise `let table = uc_node::view::convert::table(&doc.cluster.schedule); let position = doc.cluster.schedule.position;` and keep the existing loop, but resolve names from the view (`e.row_name`) by iterating `doc.cluster.schedule.entries` alongside `table.entries`, falling back to `format!("0x{hash:016x}")` exactly as `name_of` does. Delete the now-unused `crate::open(common)` call only if nothing else in `show` uses it.

`upgrade.rs`: replace `read_committed_upgrade` with `fetch(common, false)`; `NoArtifact` prints the existing `no cluster artifact yet`; then `let (pins, reports, running) = uc_node::view::convert::upgrade(&doc.cluster.upgrades); let position = doc.view_position;` and keep the rest of the function verbatim.

Remove the module-doc sentences in `schedule.rs` (around line 556) that say there is "no way to read another process's in-memory view" — that is no longer true; replace them with one sentence: the command reads the node's live cluster view over `obs.sock`, falling back to the newest artifact when the node is stopped.

- [ ] **Step 3: Run everything that exercises these commands**

Run: `cargo test -p uc_ctl`
Expected: PASS, including the new test and every pre-existing `settings`/`schedule`/`upgrade` show test unchanged.

- [ ] **Step 4: Commit**

```bash
git add uc_ctl
git commit -m "uc2ctl: settings, schedule and upgrade show read the live cluster view (artifact when stopped)"
```

---

### Task 10: Docs, security notes, backlog and spec errata

**Files:**
- Create: `docs/reference/cluster-view.md`
- Modify: `docs/reference/uc2ctl.md`, `docs/ops/uc2-runbook.md`, `docs/security/attack-surface.md`, `docs/security/threat-model.md`, `docs/BACKLOG.md`, `docs/superpowers/specs/2026-10-14-uc2-cluster-view-design.md` (errata block), `docs/reference/` index page if one lists reference pages

- [ ] **Step 1: Write `docs/reference/cluster-view.md`** with these sections, in this order:
  1. What it is (one paragraph), the two doors (`<instance_dir>/obs.sock`, `GET /cluster` on `[metrics]`), and `uc2ctl cluster`/`catalog`.
  2. Conventions: hashes, `DecU64` strings, RFC 3339 nanosecond times, `null` vs omitted attention context, the 2^53 bound on positions (9 PiB of log).
  3. Field reference — one table per object, every field of `uc_node/src/view/model.rs` with its meaning (copy names exactly; the doc comments in `model.rs` are the source).
  4. Who fills what: `self` everywhere live; `nodes`/`held_by` leader only (`nodes_from: "leader"` elsewhere); a demoted node prints none; `joined`; `view_position` vs `commit`.
  5. `attention` codes table: `catalog_empty`, `set_stalled`, `set_diverged`, `purge_gate_released`, `tier_behind`, `node_stale` — meaning, source (gauge/alert), and a link to the runbook section to read (anchors from `docs/ops/uc2-runbook.md`).
  6. Size bound (the worst case Task 5 renders) and the example document — include `uc_node/src/view/example.json` verbatim in a ```json block.
  7. Stability: `/1` rules; what a `/2` means; `uc2ctl` refuses a foreign schema by name.
  8. For a GUI or agent: supply your own list of node URLs (as a Prometheus scrape config does), poll each, the leader's document is the only one with `nodes`; no writes over HTTP (link the backlog item).
  9. Offline: `source: "artifact"`, which fields are null, row names from a stale `cnc2.dat`.

- [ ] **Step 2: Update the other docs:**
  - `docs/reference/uc2ctl.md`: new `cluster` and `catalog` sections (synopsis, text sample, `--json`, offline behavior, the "obs socket is not bound" refusal); `settings show`/`schedule show`/`upgrade show` now read the live view (and the old "no cluster artifact yet until the first instant" caveat goes — replace, don't annotate); the `snapshot show` paragraph at ~438 that says the live read "is project 4" now points to `uc2ctl catalog`.
  - `docs/ops/uc2-runbook.md`: the instance-dir layout lists `obs.sock`; the catalog paragraph (~222–245) that says "today there is no live read" is rewritten to "read it with `uc2ctl catalog` / `GET /cluster`" and keeps the five gauges as the alerting surface; add the anchors the reference page's attention table links to.
  - `docs/security/attack-surface.md` and `threat-model.md`: the spec §7 entry (exposed / never exposed / local socket trust / bounded output / residual) — in each doc's existing style.
  - `docs/BACKLOG.md`: replace the "Project 4 (not this spec)" entry with a shipped one-liner pointing at the reference page; add a new entry **"Admin writes over HTTP (for a GUI)"** listing the open questions: authenticating a remote HMAC-signed request, replay protection, TLS, CORS versus a GUI backend that polls the nodes.
- [ ] **Step 3: Spec errata.** Append to the spec a `#### Errata (planning, 2026-10-14)` block listing PR1–PR13 (one bullet each, as in this plan's "Planning rulings"), plus: `cluster --json` re-serializes the parsed document rather than printing the body verbatim (§6.3), and `catalog --json` prints a storage sub-object under the same `schema`. Leave space for an `#### Errata (as built)` block the final review adds.
- [ ] **Step 4: Check links and commit**

Run: `python3 scripts/check_doc_links.py`
Expected: `0 error(s)`.

```bash
git add docs
git commit -m "docs: the cluster view reference, uc2ctl cluster/catalog, runbook, security notes; spec planning errata"
```

- [ ] **Step 5: Full local gate before the branch leaves the box**

Run, with `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-uc-cv`:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings
grep -n "cargo clippy" .github/workflows/ci.yml   # run every feature-gated line it lists
cargo test --workspace
cargo test -p uc_node --test lifecycle --test obs_http --test catalog --test backup
```

Expected: all green. Record any red with its output; do not push.
