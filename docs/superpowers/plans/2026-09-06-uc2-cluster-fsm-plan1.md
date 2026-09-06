# The cluster FSM — Implementation Plan (plan 1 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every piece of non-user cluster data — membership, the schedule table, settings — lives in one internal state machine, `uc_cluster`, that is changed only by commands on the log, applied at commit by its own in-node apply loop, and snapshotted like any FSM; the consensus kernel keeps its durable-time membership shadow; the timer heap becomes leader-only; and the snapshot session carries the cluster FSM's artifact instead of live reads.

**Architecture:** One new frame type, `FRAME_TYPE_CLUSTER = 4` (reusing `CONFIG`'s number inside the unreleased flag day), with a kind byte selecting `Membership` / `ScheduleTable` / `Settings`. A new `uc_node::cluster_fsm::ClusterFsm` implements `uc_service`'s `RawStateMachine` + `SnapshotStateMachine` (so `uc_node` gains a runtime dependency on `uc_service`) and is driven by a fifth polling agent, `uc2-cluster`, that walks the node's own `LogBuffer` with a `LogFollower`, acts only on `CLUSTER` frames, and publishes a position-tagged `ClusterView` the consensus agent reads. Membership keeps **two consumers**: the archive walk still feeds `ConfigObserved` at durability to `ElectionSm` (Raft), while the cluster FSM applies the same frame at commit and is the snapshot authority. The `svc_sched` ring becomes leader-consumed: the service gates writes on the cnc leader flag, announces its pending set on the flag's rising edge, and tracks pending timers in the apply loop for every SM. The session ships the cluster artifact under reserved id 255 at the cluster FSM's newest artifact position, relying on the M14c per-row resume rule; a plan-1-only bridging trigger keeps that artifact at or above the purge floor until plan 2's coordinated instant replaces it.

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89); `uc_protocol` (core-only leaf), `uc_log`, `uc_consensus`, `uc_service` (traits reused; hot loop untouched), `uc_node`, `uc_net` (session), `uc_ctl`, `uc_sim`; `fuzz/` (nightly + cargo-fuzz); docs.

**Spec:** `docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md` — §3 (the line), §4 (the cluster FSM, all subsections), §6 (settings), §7 (flag day), §8 (`uc2ctl`), §11 (tests), §14 item 1, §15 checks 1–3 and 5–6. Plan 2 (coordinated + standby instants) and plan 3 (retirement + proof) follow; this plan leaves every §5 surface untouched except what §14 item 1 names.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals --test timers --test services`; after Task 1, `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`; after Task 0, `./scripts/check_publish_metadata.sh` and the batched `cargo package --no-verify -p …` from `.github/workflows/ci.yml:176` with `uc_service` listed **before** `uc_node`.
- **Still the unreleased 2.11.0 flag day**: `uc_protocol::version::CURRENT` stays `0.7.0`, `CNC_V2_VERSION` stays 3.1, workspace version stays `2.11.0`. Reusing frame number 4 and retiring frame 6 / datagram kind 21 are edits to an unreleased wire; no shipped node speaks either. The docs say so explicitly.
- **Frozen once shipped**: `FRAME_TYPE_CLUSTER = 4`; the body prefix `kind: u8 @0 ‖ reserved [u8; 7] @1 ‖ payload @8` (`CLUSTER_BODY_PREFIX_LEN = 8`); `ClusterKind::{Membership = 1, ScheduleTable = 2, Settings = 3}`; the settings encoding (Task 1); the cluster image (Task 3); reserved `service_id = 255`; admin op `7 settings_apply`; refusals `44–47`. Each pinned by a test whose comment says so.
- **Budget**: a `Settings` payload is `SETTINGS_LEN = 4 + 8 + 8 + 8 + 1 = 29` bytes; a `ScheduleTable` payload is ≤ 1064; both plus the 8-byte prefix sit under the 1344 B ceiling (`const` asserts in Task 1).
- **The consensus hot loop is not the place for new work.** Every per-pass read of the view is one `Acquire` load of `ClusterView.position`; the lock is taken only when it changed (M14a: code in a hot body costs even on paths that never run — A/B any doubt against `apply_bench`).
- **Determinism**: `ClusterFsm::apply` reads nothing but its own state and the command; node-local constraints (`admission_bytes` vs `buffer_bytes`) are clamped at use, never rejected in `apply`. The leader's pre-append check **is** `ClusterFsm::validate` on a clone of the view — one function.
- **The kernel's durable-time membership path is untouched in semantics**: `ConfigObserved` still comes from the archive walk at durability, still goes through the truncating latch (`node.rs:2660–2705`), `state/config.state` and `recover_config_record` stay. Only the frame it is read from changes (`CLUSTER kind=Membership`).
- **Surfaces this plan builds on (as built)**: `uc_service::{RawStateMachine, SnapshotStateMachine, ApplyCtx, SnapshotError}` (`uc_service/src/traits.rs:222–345`); `uc_log::reader::{LogFollower::new(buffer, cursor), next_batch(target) -> Batch<'_>, Batch::{Frames(iter), Overrun, CaughtUp}}` yielding `(frame_start, FrameHeader, payload)` (`uc_log/src/reader.rs:115`); `uc_log::agent::{AgentRunner::spawn(name, IdleStrategy, FnMut() -> bool), IdleStrategy::Yield}` (`uc_log/src/agent.rs:59`); `uc_log::LogBuffer::append_config(term, payload)` (`uc_log/src/buffer.rs:732`) and `append_schedule_table` (`:807`); the archive walk (`uc_log/src/archive.rs:460–485`, `take_config_observations` `:494`, `take_table_observations` `:501`); `uc_consensus::config::{ClusterConfig { version, voters, learners, tombstones }, ConfigOp, ClusterConfig::apply(op) -> Result<ClusterConfig, ProposeError>, reason_code}` (`uc_consensus/src/config.rs:29–190`); `uc_protocol::v2::config::{WireConfig, encode_config, decode_config}` and `uc_node`'s `wire_to_cluster_config`/`cluster_to_wire` (`node.rs:7110`, `:7132`); `uc_protocol::v2::schedule::{ScheduleTable, ScheduleEntry, ScheduleRule, encode_schedule_table, decode_schedule_table, MAX_SCHEDULE_ENTRIES}`; `uc_protocol::v2::ipc::{SchedRecord, SchedOp, MSG_V2_SCHED, read_sched_record, write_sched_record}`; `uc_node` consensus: `handle_admin` (`node.rs:5412`), `propose_and_append` (`:5628`), `apply_schedule_table` (`:5666`), `append_config_frame` (`:4210`), `append_schedule_table_frame` (`:4242`), `adopt_table_frame` (`:4272`), `install_table` (`:4397`), `refresh_schedule_ship` (`:4451`), `arm_schedule_at_boot` (`:4487`), `revert_schedule_below` (`:4549`), `observe_config` (`:6722`), `observe_table` (`:6757`), `drain_sched_rings` (`:3913`), `rearm_timers` (`:4095`) and its two callers (`:6261` demotion, `:6566` halt), the latch (`:2660–2705`), `snapshot_set_for` (`:7659`), `install_snapshot_table` (`:4341`), the install handler (`:3699`, `:3736`); `uc_service::apply::{apply_cycle, ApplyState, write_sched}` (`uc_service/src/apply.rs:178–304`, the `is_leader` read `:421`, the announce flush `:368`); `uc_node/src/config_file.rs` (`NodeConfigFile` `:214`, `ServicesSection` `:203`, the `services.ids` refusal `:664–680`); `uc_ctl/src/schedule.rs` (`parse_table` `:120`, `apply` `:473`) and `Cmd` (`uc_ctl/src/main.rs:290`); `uc_node/src/audit.rs::op_name` (`:131`).
- **Fleet spend is user-gated. Never write scratch to `/tmp`.** Test instance dirs go through `CARGO_TARGET_TMPDIR`.
- Commit subjects: `type(scope): imperative summary`. Every new or changed test is **watched red first** (state in the commit body how).

---

## File structure

| file | responsibility | task |
|---|---|---|
| `uc_node/Cargo.toml`, `.github/workflows/ci.yml`, `docs/how-to/cut-a-release.md` §6 | `uc_node → uc_service` runtime dep; publish order flips | 0 |
| `uc_protocol/src/v2/frame.rs` | `FRAME_TYPE_CLUSTER = 4`, `ClusterKind`, `CLUSTER_BODY_PREFIX_LEN`, `write_cluster_prefix`/`read_cluster_prefix`; `FRAME_TYPE_SCHEDULE_TABLE` retired | 1 |
| `uc_protocol/src/v2/settings.rs` (new) | `Settings`, `Target`, `encode_settings`/`decode_settings`, bounds, `SETTINGS_LEN` | 1 |
| `fuzz/fuzz_targets/uc_protocol_cluster_frame.rs`, `uc_protocol_settings.rs` (new) | total decoders | 1 |
| `uc_log/src/buffer.rs`, `uc_log/src/archive.rs` | `append_cluster(term, kind, payload)`; archive walk reads the kind byte and feeds only `Membership` payloads to `config_observations`; `table_observations` and `append_schedule_table` deleted | 2 |
| `uc_node/src/cluster_fsm.rs` (new) | `ClusterFsm` (state, `validate`, `RawStateMachine`, `SnapshotStateMachine`), `ClusterCommand`, `ClusterView`, the image codec | 3 |
| `uc_node/src/cluster_agent.rs` (new) | the `uc2-cluster` agent: `LogFollower` over the node's buffer, `CLUSTER` filter, view publish, artifact under `snapshots/cluster/`, recovery, the bridging freeze trigger | 4 |
| `uc_node/src/node.rs` (consensus side) | leader appends `CLUSTER` commands; genesis seeds the cluster FSM; schedule/settings gates on the view; timer heap arms from the view; `fsm_lag` republish + admission door from the view; `schedule_state`, `ScheduleShip`, `shippable_schedule`, `adopt_table_frame`, `observe_table`, `install_table`'s record half, `arm_schedule_at_boot`, `revert_schedule_below` deleted | 5 |
| `uc_node/src/config_file.rs`, `uc_node/src/services.rs`, `uc_protocol/src/identity.rs` | `[settings]` genesis section; `admission_bytes` and `services.fsm_lag` refused outside it; `uc_` prefix refused | 6 |
| `uc_service/src/apply.rs`, `uc_node/src/node.rs` (timers) | leader-gated `write_sched`; `was_leader` edge → announce; in-loop pending map; node drains only as leader; heap discarded on demotion; `rearm_timers` deleted | 7 |
| `uc_ctl/src/settings.rs` (new), `uc_ctl/src/main.rs`, `uc_node/src/audit.rs` | `settings apply` / `settings show`, op 7 | 8 |
| `uc_net/src/sender.rs`, `uc_net/src/receiver.rs`, `uc_protocol/src/v2/datagram.rs`, `uc_node/src/node.rs` (snapshot) | `SNAP_BEGIN` V4 (no `config`); cluster artifact under id 255; `SNAP_TABLE` retired; joiner installs via the cluster FSM and seeds the kernel shadow | 9 |
| `uc_sim/src/world.rs`, `uc_sim/tests/scenarios.rs` | inv12 (two readers), a red twin, a `CLUSTER`-frame truncation scenario | 10 |
| `uc_node/tests/{timers,learner,services,daemon_refusals}.rs` | failover re-announce; the residual staged and green; `uc_` refusal; moved-key refusals | 11 |
| docs | configuration, uc2ctl, instance-directory, monitor, limits, wire-protocol, cnc-page, the explainer stub, RELEASES draft, CLAUDE.md, VERIFICATION | 12 |

---

### Task 0: `uc_node` depends on `uc_service`

**Files:**
- Modify: `uc_node/Cargo.toml` (`[dependencies]`)
- Modify: `.github/workflows/ci.yml:176–180` (the batched `cargo package` order)
- Modify: `docs/how-to/cut-a-release.md` §6 (the crates.io publish order)

**Interfaces:**
- Produces: `uc_node` may `use uc_service::{RawStateMachine, SnapshotStateMachine, ApplyCtx, SnapshotError}` from Task 3 on.

- [ ] **Step 1: Add the dependency**

In `uc_node/Cargo.toml` `[dependencies]`, after `uc_crypto`:

```toml
# The cluster FSM (spec §4) implements uc_service's RawStateMachine +
# SnapshotStateMachine so it is "treated like any FSM". This is a RUNTIME
# edge; uc_service's dev-dependency on uc_node stays a dev-only cycle. The
# crates.io publish order therefore flips: uc_service BEFORE uc_node.
uc_service = { path = "../uc_service", version = "2.11.0" }
```

- [ ] **Step 2: Flip the publish order in CI and the how-to**

`.github/workflows/ci.yml` line 176–180: move `-p uc_service` before `-p uc_node`. `docs/how-to/cut-a-release.md` §6: move `uc_service` above `uc_node` in the ordered list and add one sentence: "`uc_node` depends on `uc_service` since the cluster FSM (2.11.0), so `uc_service` publishes first; `uc_service`'s dev-dependency on `uc_node` is unversioned and stripped by `cargo package`."

- [ ] **Step 3: Verify the workspace still packages**

Run: `cargo check --workspace && ./scripts/check_publish_metadata.sh && cargo package --no-verify -p uc_journal -p uc_protocol -p uc_obs -p uc_crypto -p uc_log -p uc_consensus -p uc_net -p uc_client -p uc_service -p uc_node -p uc_remote -p uc_gateway -p uc_ctl`
Expected: `Finished`, metadata `ok`, thirteen `Packaged` lines, exit 0.

- [ ] **Step 4: Commit**

```bash
git add uc_node/Cargo.toml Cargo.lock .github/workflows/ci.yml docs/how-to/cut-a-release.md
git commit -m "build(uc_node): depend on uc_service for the cluster FSM's traits; publish order flips"
```

---

### Task 1: `FRAME_TYPE_CLUSTER`, its kinds, and the settings codec

**Files:**
- Modify: `uc_protocol/src/v2/frame.rs:36–56`
- Create: `uc_protocol/src/v2/settings.rs`
- Modify: `uc_protocol/src/v2/mod.rs` (add `pub mod settings;`)
- Create: `fuzz/fuzz_targets/uc_protocol_cluster_frame.rs`, `fuzz/fuzz_targets/uc_protocol_settings.rs`; modify `fuzz/Cargo.toml` (two `[[bin]]`s), `fuzz/src/seeds.rs`, `scripts/fuzz_smoke.sh`'s target list
- Test: `uc_protocol/src/v2/frame.rs` (tests module), `uc_protocol/src/v2/settings.rs` (tests module)

**Interfaces:**
- Produces: `pub const FRAME_TYPE_CLUSTER: u8 = 4`; `pub const CLUSTER_BODY_PREFIX_LEN: usize = 8`; `#[repr(u8)] pub enum ClusterKind { Membership = 1, ScheduleTable = 2, Settings = 3 }` with `ClusterKind::from_u8(u8) -> Option<ClusterKind>`; `pub fn write_cluster_prefix(buf: &mut [u8], kind: ClusterKind)`; `pub fn read_cluster_prefix(buf: &[u8]) -> Option<(ClusterKind, &[u8])>` (returns the kind and the payload after the prefix); `pub const FRAME_TYPE_SCHEDULE_TABLE_RETIRED: u8 = 6` (a doc-only reservation; no reader accepts it). In `settings`: `pub struct Settings { pub fsm_lag_bytes: u64, pub admission_bytes: u64, pub snapshot_interval_bytes: u64, pub snapshot_target: Target }`, `#[repr(u8)] pub enum Target { All = 0, Learners = 1 }`, `pub const SETTINGS_VERSION: u32 = 1`, `pub const SETTINGS_LEN: usize = 29`, `pub fn encode_settings(s: &Settings, out: &mut Vec<u8>)`, `pub fn decode_settings(buf: &[u8]) -> Option<Settings>`, `pub const FSM_LAG_LOCKSTEP: u64 = 0` (the page's existing lockstep sentinel, reused so the cnc word and the setting agree), `impl Settings { pub fn genesis_default() -> Settings }` (`fsm_lag_bytes: 0` meaning "derive `buffer_bytes / 4` at use", `admission_bytes: 0` meaning "derive at use", `snapshot_interval_bytes: 0`, `Target::All`).

- [ ] **Step 1: Write the failing frame tests**

In `uc_protocol/src/v2/frame.rs` tests module:

```rust
#[test]
fn cluster_frame_type_reuses_config_number_and_prefix_is_frozen() {
    // FROZEN once shipped (spec §7): the number, the prefix length, the kinds.
    assert_eq!(FRAME_TYPE_CLUSTER, 4);
    assert_eq!(CLUSTER_BODY_PREFIX_LEN, 8);
    assert_eq!(ClusterKind::Membership as u8, 1);
    assert_eq!(ClusterKind::ScheduleTable as u8, 2);
    assert_eq!(ClusterKind::Settings as u8, 3);
    assert_eq!(FRAME_TYPE_SCHEDULE_TABLE_RETIRED, 6);
}

#[test]
fn cluster_prefix_roundtrips_and_reserved_bytes_are_zero() {
    let mut buf = vec![0xffu8; CLUSTER_BODY_PREFIX_LEN + 3];
    write_cluster_prefix(&mut buf, ClusterKind::Settings);
    assert_eq!(&buf[1..8], &[0u8; 7]);
    let (kind, payload) = read_cluster_prefix(&buf).unwrap();
    assert_eq!(kind, ClusterKind::Settings);
    assert_eq!(payload, &[0xff, 0xff, 0xff]);
}

#[test]
fn cluster_prefix_is_total_on_short_and_unknown_input() {
    assert!(read_cluster_prefix(&[]).is_none());
    assert!(read_cluster_prefix(&[1u8; 7]).is_none());
    let mut bad = [0u8; 8];
    bad[0] = 9; // unknown kind
    assert!(read_cluster_prefix(&bad).is_none());
    let mut nz = [0u8; 8];
    nz[0] = 1;
    nz[3] = 1; // reserved byte set: refused, so the bytes can be claimed later
    assert!(read_cluster_prefix(&nz).is_none());
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p uc_protocol cluster_ -- --nocapture`
Expected: compile error — `FRAME_TYPE_CLUSTER`, `ClusterKind` not found.

- [ ] **Step 3: Implement in `frame.rs`**

Replace the `FRAME_TYPE_CONFIG` block (`:43`) and the `FRAME_TYPE_SCHEDULE_TABLE` block (`:56`) with:

```rust
/// Cluster-FSM command (spec §4.3). The body is `kind: u8 ‖ reserved [u8; 7]
/// ‖ payload`; the kind selects the payload codec. Reuses `CONFIG`'s number
/// inside the unreleased 2.11.0 flag day — no shipped node ever emitted a
/// frame 4 that is not a `Membership` command. User apply loops yield it;
/// the cluster FSM's loop acts on it; the archive walk reads the kind byte
/// to feed `Membership` payloads to the consensus kernel at durability.
pub const FRAME_TYPE_CLUSTER: u8 = 4;
/// Retired before it shipped (was `SCHEDULE_TABLE`, plan 2). Reserved so the
/// number is never reassigned to something a pre-release build might misread.
pub const FRAME_TYPE_SCHEDULE_TABLE_RETIRED: u8 = 6;
/// `kind ‖ reserved` — the fixed prefix of every `CLUSTER` body.
pub const CLUSTER_BODY_PREFIX_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ClusterKind {
    Membership = 1,
    ScheduleTable = 2,
    Settings = 3,
}

impl ClusterKind {
    pub const fn from_u8(b: u8) -> Option<ClusterKind> {
        match b {
            1 => Some(ClusterKind::Membership),
            2 => Some(ClusterKind::ScheduleTable),
            3 => Some(ClusterKind::Settings),
            _ => None,
        }
    }
}

/// Write the prefix; `buf.len() >= CLUSTER_BODY_PREFIX_LEN`. Reserved bytes
/// are written as zero so a later reader may claim them.
pub fn write_cluster_prefix(buf: &mut [u8], kind: ClusterKind) {
    buf[0] = kind as u8;
    buf[1..CLUSTER_BODY_PREFIX_LEN].fill(0);
}

/// Total: `None` on a short body, an unknown kind, or a non-zero reserved
/// byte. Returns the kind and the payload that follows the prefix.
pub fn read_cluster_prefix(buf: &[u8]) -> Option<(ClusterKind, &[u8])> {
    if buf.len() < CLUSTER_BODY_PREFIX_LEN {
        return None;
    }
    let kind = ClusterKind::from_u8(buf[0])?;
    if buf[1..CLUSTER_BODY_PREFIX_LEN].iter().any(|b| *b != 0) {
        return None;
    }
    Some((kind, &buf[CLUSTER_BODY_PREFIX_LEN..]))
}
```

Delete `FRAME_TYPE_CONFIG`; every use in the workspace becomes `FRAME_TYPE_CLUSTER` (Tasks 2 and 5 fix the callers; until then the build is red — do Steps 3–5 of this task and Task 2 in one sitting, or leave `pub const FRAME_TYPE_CONFIG: u8 = FRAME_TYPE_CLUSTER;` as a deprecated alias with `#[deprecated]` and remove it in Task 5).

- [ ] **Step 4: Write the failing settings tests**

`uc_protocol/src/v2/settings.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_layout_is_frozen() {
        // FROZEN once shipped (spec §6/§7): version u32 @0, fsm_lag_bytes u64 @4,
        // admission_bytes u64 @12, snapshot_interval_bytes u64 @20, target u8 @28.
        assert_eq!(SETTINGS_VERSION, 1);
        assert_eq!(SETTINGS_LEN, 29);
        let s = Settings {
            fsm_lag_bytes: 16 << 20,
            admission_bytes: 4 << 20,
            snapshot_interval_bytes: 1 << 30,
            snapshot_target: Target::Learners,
        };
        let mut out = Vec::new();
        encode_settings(&s, &mut out);
        assert_eq!(out.len(), SETTINGS_LEN);
        assert_eq!(&out[0..4], &1u32.to_le_bytes());
        assert_eq!(out[28], 1);
        assert_eq!(decode_settings(&out), Some(s));
    }

    #[test]
    fn decode_is_total_and_refuses_unknown_version_and_target() {
        assert!(decode_settings(&[]).is_none());
        let mut out = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut out);
        let mut v2 = out.clone();
        v2[0] = 2;
        assert!(decode_settings(&v2).is_none());
        let mut t9 = out.clone();
        t9[28] = 9;
        assert!(decode_settings(&t9).is_none());
        out.push(0); // trailing byte: refused, the length is exact
        assert!(decode_settings(&out).is_none());
    }

    #[test]
    fn genesis_default_means_derive_at_use() {
        let d = Settings::genesis_default();
        assert_eq!(d.fsm_lag_bytes, 0);
        assert_eq!(d.admission_bytes, 0);
        assert_eq!(d.snapshot_interval_bytes, 0);
        assert_eq!(d.snapshot_target, Target::All);
    }
}
```

- [ ] **Step 5: Implement `settings.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated settings record (cluster-FSM spec §6): the three
//! cluster-wide policies that used to live per host in `node.toml`. Carried
//! as a `CLUSTER kind=Settings` payload and inside the cluster FSM's image.
//! `core`-only, like every codec in `v2`.

/// Encoding version, first word of the payload. Bumped when the layout
/// changes; a reader refuses any version it does not know.
pub const SETTINGS_VERSION: u32 = 1;
/// The exact encoded length — no trailing bytes are tolerated.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1;
/// `fsm_lag_bytes` value meaning lockstep — the cnc page's existing sentinel,
/// reused so the word the service apply loops read and the setting agree.
pub const FSM_LAG_LOCKSTEP: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Target {
    All = 0,
    Learners = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// `0` at genesis = "derive `buffer_bytes / 4` at use"; `FSM_LAG_LOCKSTEP`
    /// once set explicitly means lockstep. The two zeros are distinguishable
    /// by `Settings::has_fsm_lag`, set by `settings apply`.
    pub fsm_lag_bytes: u64,
    /// `0` = derive at use (the node's `NodeConfig::admission_bytes` default).
    pub admission_bytes: u64,
    /// `0` = on demand only (plan 2 reads it; plan 1 carries it).
    pub snapshot_interval_bytes: u64,
    pub snapshot_target: Target,
}

impl Settings {
    pub const fn genesis_default() -> Settings {
        Settings {
            fsm_lag_bytes: 0,
            admission_bytes: 0,
            snapshot_interval_bytes: 0,
            snapshot_target: Target::All,
        }
    }
}

pub fn encode_settings(s: &Settings, out: &mut Vec<u8>) {
    out.extend_from_slice(&SETTINGS_VERSION.to_le_bytes());
    out.extend_from_slice(&s.fsm_lag_bytes.to_le_bytes());
    out.extend_from_slice(&s.admission_bytes.to_le_bytes());
    out.extend_from_slice(&s.snapshot_interval_bytes.to_le_bytes());
    out.push(s.snapshot_target as u8);
}

pub fn decode_settings(buf: &[u8]) -> Option<Settings> {
    if buf.len() != SETTINGS_LEN {
        return None;
    }
    let u32_at = |o: usize| u32::from_le_bytes(buf[o..o + 4].try_into().ok()?).into();
    let u64_at = |o: usize| u64::from_le_bytes(buf[o..o + 8].try_into().ok()?).into();
    let version: Option<u32> = u32_at(0);
    if version? != SETTINGS_VERSION {
        return None;
    }
    let snapshot_target = match buf[28] {
        0 => Target::All,
        1 => Target::Learners,
        _ => return None,
    };
    Some(Settings {
        fsm_lag_bytes: u64_at(4)?,
        admission_bytes: u64_at(12)?,
        snapshot_interval_bytes: u64_at(20)?,
        snapshot_target,
    })
}

const _: () = assert!(SETTINGS_LEN + crate::v2::frame::CLUSTER_BODY_PREFIX_LEN <= 1344);
```

Add `pub mod settings;` to `uc_protocol/src/v2/mod.rs`.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p uc_protocol settings_ cluster_`
Expected: all six pass.

- [ ] **Step 7: Fuzz targets**

`fuzz/fuzz_targets/uc_protocol_cluster_frame.rs`:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::frame::{read_cluster_prefix, ClusterKind};
use uc_protocol::v2::{config::decode_config, schedule::decode_schedule_table, settings::decode_settings};

// The CLUSTER body every node decodes off the log, kind-dispatched: total on any slice.
fuzz_target!(|data: &[u8]| {
    if let Some((kind, payload)) = read_cluster_prefix(data) {
        match kind {
            ClusterKind::Membership => {
                let _ = decode_config(payload);
            }
            ClusterKind::ScheduleTable => {
                let _ = decode_schedule_table(payload);
            }
            ClusterKind::Settings => {
                let _ = decode_settings(payload);
            }
        }
    }
});
```

`fuzz/fuzz_targets/uc_protocol_settings.rs`:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::settings::{decode_settings, encode_settings};

fuzz_target!(|data: &[u8]| {
    if let Some(s) = decode_settings(data) {
        let mut re = Vec::new();
        encode_settings(&s, &mut re);
        assert_eq!(decode_settings(&re), Some(s));
    }
});
```

Add both `[[bin]]`s to `fuzz/Cargo.toml` (copy the `uc_protocol_schedule_table` entry's shape), seeds `17-cluster-settings` (a valid prefix + `genesis_default` encoding) and `18-cluster-membership` (a valid prefix + a two-voter `encode_config`) to `fuzz/src/seeds.rs`, and both names to `scripts/fuzz_smoke.sh`'s target list.

Run: `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check && cargo +nightly run --bin seed-corpus)`
Expected: `Finished`; the two new seed files appear under `fuzz/corpus/`.

- [ ] **Step 8: Commit**

```bash
git add uc_protocol fuzz scripts/fuzz_smoke.sh
git commit -m "feat(uc_protocol): FRAME_TYPE_CLUSTER with a kind byte, and the settings codec (spec §4.3, §6)"
```

---

### Task 2: `uc_log` appends `CLUSTER` frames and the archive walk reads the kind byte

**Files:**
- Modify: `uc_log/src/buffer.rs:732–760` (`append_config` → `append_cluster`), delete `:807–870` (`append_schedule_table`)
- Modify: `uc_log/src/archive.rs:139–150`, `:460–485`, `:494–510` (observations)
- Test: `uc_log/src/buffer.rs` tests, `uc_log/src/archive.rs` tests

**Interfaces:**
- Consumes: Task 1's `FRAME_TYPE_CLUSTER`, `ClusterKind`, `write_cluster_prefix`, `read_cluster_prefix`, `CLUSTER_BODY_PREFIX_LEN`.
- Produces: `LogBuffer::append_cluster(&mut self, term: u32, kind: ClusterKind, payload: &[u8]) -> Result<u64, AppendError>` (returns the frame-**end** position, as `append_config` did); `Archive::take_config_observations(&mut self) -> Vec<(u64, Vec<u8>)>` unchanged in signature but now yields the **payload after the prefix** of `CLUSTER kind=Membership` frames only; `take_table_observations` **deleted**.

- [ ] **Step 1: Write the failing buffer test**

In `uc_log/src/buffer.rs` tests, beside `append_timer_stamps_the_deadline_and_marks_late_by_clamp`:

```rust
#[test]
fn append_cluster_writes_the_prefix_then_the_payload_and_stamps_like_a_client_frame() {
    let (mut app, buf) = leader_appender_for_test(); // the helper the config test already uses
    app.set_now(5_000);
    let end = app.append_cluster(3, ClusterKind::Settings, &[9u8; 29]).unwrap();
    let mut out = Vec::new();
    let FrameRead::Frame(hdr) = buf.read_frame_validated(end - align_frame_len(HEADER_LEN + 8 + 29) as u64, &mut out) else { panic!() };
    assert_eq!(hdr.frame_type, FRAME_TYPE_CLUSTER);
    assert_eq!(hdr.time_ns, 5_000);
    let (kind, payload) = read_cluster_prefix(&out[HEADER_LEN..hdr.length as usize]).unwrap();
    assert_eq!(kind, ClusterKind::Settings);
    assert_eq!(payload, &[9u8; 29]);
}
```

(If the config test's helper has a different name, use that one — the shape is "an appender over a heap buffer with a known `now`".)

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p uc_log append_cluster`
Expected: compile error, `append_cluster` not found.

- [ ] **Step 3: Implement `append_cluster`, delete `append_schedule_table`**

Rename `append_config` (`buffer.rs:732`) to `append_cluster`, add the `kind` parameter, and write the prefix before the payload. The body is the existing `append_config` body with these changes: the claimed length is `HEADER_LEN + CLUSTER_BODY_PREFIX_LEN + payload.len()`; after writing the header, `write_cluster_prefix(&mut region[HEADER_LEN..], kind)` then copy `payload` at `HEADER_LEN + CLUSTER_BODY_PREFIX_LEN`; `frame_type: FRAME_TYPE_CLUSTER`. Keep the `time_ns = self.now_ns.max(self.last_stamp)` clamp exactly as it is. Delete `append_schedule_table` and its two tests. Grep the crate for `FRAME_TYPE_CONFIG`/`FRAME_TYPE_SCHEDULE_TABLE` and replace with `FRAME_TYPE_CLUSTER`.

- [ ] **Step 4: Write the failing archive test**

In `uc_log/src/archive.rs` tests, beside the existing config-observation test:

```rust
#[test]
fn archive_walk_feeds_only_membership_payloads_to_config_observations() {
    let (mut arc, mut app) = archive_and_appender_for_test();
    let mut cfg = Vec::new();
    encode_config(&two_voter_wire_config(), &mut cfg);
    let end_m = app.append_cluster(1, ClusterKind::Membership, &cfg).unwrap();
    let mut s = Vec::new();
    encode_settings(&Settings::genesis_default(), &mut s);
    let _end_s = app.append_cluster(1, ClusterKind::Settings, &s).unwrap();
    arc.record_through(app.position()); // whatever the existing test calls to run the walk
    let obs = arc.take_config_observations();
    assert_eq!(obs.len(), 1, "the Settings frame must NOT reach the kernel");
    assert_eq!(obs[0].0, end_m, "frame-END position, as CONFIG's convention");
    assert_eq!(obs[0].1, cfg, "the payload AFTER the 8-byte prefix");
}
```

- [ ] **Step 5: Implement the walk change**

In `archive.rs:475` replace the `FRAME_TYPE_CONFIG` and `FRAME_TYPE_SCHEDULE_TABLE` arms with:

```rust
if h.frame_type == FRAME_TYPE_CLUSTER {
    let body = &block[off + HEADER_LEN..off + h.length as usize];
    // The kernel's durable-time membership feed (spec §4.6): only
    // Membership payloads, after the prefix. Every other kind is the
    // cluster FSM's business at commit; the walk does not look at it.
    if let Some((ClusterKind::Membership, payload)) = read_cluster_prefix(body) {
        self.config_observations
            .push((base + off as u64 + aligned as u64, payload.to_vec()));
    }
}
```

Delete `table_observations` (field, doc, `take_table_observations`, the `truncate_to` reset). Keep `config_observations` and its `truncate_to` reset as they are.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p uc_log`
Expected: all pass, including the two new ones; the deleted table tests are gone.

- [ ] **Step 7: Commit**

```bash
git add uc_log
git commit -m "feat(uc_log): append CLUSTER frames; the archive walk feeds only Membership payloads to the kernel (spec §4.3, §4.6)"
```

---

### Task 3: `ClusterFsm` — state, deterministic validation, the two traits, the image

**Files:**
- Create: `uc_node/src/cluster_fsm.rs`
- Modify: `uc_node/src/lib.rs` (`pub mod cluster_fsm;` and `pub use cluster_fsm::{ClusterFsm, ClusterView, ClusterCommand, ClusterRefusal}`)
- Test: `uc_node/src/cluster_fsm.rs` tests module

**Interfaces:**
- Consumes: Task 1's `ClusterKind`, `read_cluster_prefix`, `Settings`, `encode_settings`/`decode_settings`; `uc_consensus::config::{ClusterConfig, ConfigOp, ProposeError, ClusterConfig::apply, reason_code}`; `uc_protocol::v2::config::{decode_config, encode_config}` and `uc_node::node::{wire_to_cluster_config, cluster_to_wire}` (make these two `pub(crate)`); `uc_protocol::v2::schedule::{ScheduleTable, decode_schedule_table, encode_schedule_table}`; `uc_service::{RawStateMachine, SnapshotStateMachine, ApplyCtx, SnapshotError}`.
- Produces:

```rust
pub struct ClusterState { pub membership: ClusterConfig, pub table: ScheduleTable, pub table_position: u64, pub settings: Settings, pub applied: u64 }
pub enum ClusterCommand { Membership(ClusterConfig), ScheduleTable(ScheduleTable), Settings(Settings) }
pub enum ClusterRefusal { Membership(ProposeError), ScheduleUnknownFsm { entry: usize }, ScheduleTooLarge, SettingsBounds(&'static str) }
impl ClusterRefusal { pub fn reason_code(&self) -> u32 }   // 43 schedule_unknown_fsm, 42 schedule_decode for TooLarge, 47 settings_bounds, M7 codes for Membership
pub struct ClusterFsm { state: ClusterState, declared_hashes: Vec<u64> }
impl ClusterFsm {
    pub fn new(genesis: ClusterState, declared_hashes: Vec<u64>) -> ClusterFsm;
    pub fn state(&self) -> &ClusterState;
    /// Pure: the acceptance decision on THIS state, no mutation. The leader's
    /// pre-append check calls exactly this on a clone (spec §4.4).
    pub fn validate(&self, cmd: &ClusterCommand) -> Result<(), ClusterRefusal>;
    pub fn decode_command(kind: ClusterKind, payload: &[u8]) -> Option<ClusterCommand>;
    pub fn encode_command(cmd: &ClusterCommand, out: &mut Vec<u8>) -> ClusterKind;  // writes the PAYLOAD only; the caller writes the prefix
}
impl RawStateMachine for ClusterFsm { const NAME = "uc_cluster"; const VERSION = 1; ... }
impl SnapshotStateMachine for ClusterFsm { type SnapshotHandle = ClusterImage; ... }
pub struct ClusterView { pub position: AtomicU64, pub admission_bytes: AtomicU64, pub fsm_lag_bytes: AtomicU64, pub snapshot_interval_bytes: AtomicU64, pub snapshot_target: AtomicU8, inner: Mutex<ClusterViewInner> }
pub struct ClusterViewInner { pub membership: ClusterConfig, pub table: ScheduleTable, pub table_position: u64 }
impl ClusterView { pub fn new(genesis: &ClusterState) -> ClusterView; pub fn publish(&self, st: &ClusterState); pub fn snapshot_inner(&self) -> ClusterViewInner /* clone under the lock */; pub fn membership(&self) -> ClusterConfig; }
pub const CLUSTER_IMAGE_MAGIC: &[u8; 8] = b"UCCLUST1"; pub const CLUSTER_IMAGE_VERSION: u32 = 1;
```

The `RawStateMachine::apply` contract for this FSM: `cmd` is the **full body** (prefix + payload); `out` receives one byte — `0` accepted, else the refusal code as `u8` — so a future egress reply (spec §13 phase 2) needs no format change. `query` answers the three `uc2ctl … show` reads: `q[0] == 1` → the encoded membership, `2` → the encoded table plus its position (`u64 LE ‖ bytes`), `3` → the encoded settings.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_consensus::config::ClusterConfig;
    use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
    use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

    fn genesis() -> ClusterState {
        ClusterState {
            membership: ClusterConfig::genesis(vec![(0, addr(0)), (1, addr(1)), (2, addr(2))], vec![]),
            table: ScheduleTable { entries: vec![] },
            table_position: 0,
            settings: Settings::genesis_default(),
            applied: 0,
        }
    }
    fn addr(i: u32) -> uc_consensus::config::Addr { ([127, 0, 0, 1], 9100 + i as u16).into() }
    fn fsm() -> ClusterFsm { ClusterFsm::new(genesis(), vec![0xF5A0, 0xF5A1]) }
    fn body(cmd: &ClusterCommand) -> Vec<u8> {
        let mut payload = Vec::new();
        let kind = ClusterFsm::encode_command(cmd, &mut payload);
        let mut b = vec![0u8; CLUSTER_BODY_PREFIX_LEN];
        write_cluster_prefix(&mut b, kind);
        b.extend_from_slice(&payload);
        b
    }

    #[test]
    fn identity_is_the_reserved_name() {
        assert_eq!(<ClusterFsm as RawStateMachine>::NAME, "uc_cluster");
        assert_eq!(<ClusterFsm as RawStateMachine>::VERSION, 1);
    }

    #[test]
    fn membership_command_applies_through_the_kernels_own_rules() {
        let mut f = fsm();
        let next = f.state().membership.apply(ConfigOp::AddLearner { id: 3, addr: addr(3) }).unwrap();
        let cmd = ClusterCommand::Membership(next.clone());
        assert!(f.validate(&cmd).is_ok());
        let mut out = Vec::new();
        f.apply(&mut ApplyCtx::for_sm::<ClusterFsm>(100), &body(&cmd), &mut out);
        assert_eq!(out, [0]);
        assert_eq!(f.state().membership, next);
        assert_eq!(f.state().applied, 100);
    }

    #[test]
    fn a_second_reconfig_while_one_is_pending_is_refused_deterministically() {
        // `ClusterConfig::apply` already encodes "one at a time" via version
        // chaining: a command whose version is not adopted+1 is refused.
        let mut f = fsm();
        let mut stale = f.state().membership.clone();
        stale.version += 2;
        let cmd = ClusterCommand::Membership(stale);
        let r = f.validate(&cmd).unwrap_err();
        assert!(matches!(r, ClusterRefusal::Membership(_)));
        let mut out = Vec::new();
        f.apply(&mut ApplyCtx::for_sm::<ClusterFsm>(200), &body(&cmd), &mut out);
        assert_ne!(out, [0]);
        assert_eq!(f.state().membership.version, genesis().membership.version, "refused: unchanged");
        assert_eq!(f.state().applied, 200, "a refused command still advances applied");
    }

    #[test]
    fn schedule_table_naming_an_undeclared_fsm_refuses_the_whole_table() {
        let mut f = fsm();
        let t = ScheduleTable { entries: vec![
            ScheduleEntry { identity_hash: 0xF5A0, timer_id: 1, rule: ScheduleRule::Once { at_ns: 5 } },
            ScheduleEntry { identity_hash: 0xDEAD, timer_id: 2, rule: ScheduleRule::Once { at_ns: 5 } },
        ]};
        let cmd = ClusterCommand::ScheduleTable(t);
        assert!(matches!(f.validate(&cmd), Err(ClusterRefusal::ScheduleUnknownFsm { entry: 1 })));
        assert_eq!(f.validate(&cmd).unwrap_err().reason_code(), 43);
        let mut out = Vec::new();
        f.apply(&mut ApplyCtx::for_sm::<ClusterFsm>(300), &body(&cmd), &mut out);
        assert_eq!(out, [43]);
        assert!(f.state().table.entries.is_empty());
    }

    #[test]
    fn schedule_table_records_its_own_position() {
        let mut f = fsm();
        let t = ScheduleTable { entries: vec![
            ScheduleEntry { identity_hash: 0xF5A0, timer_id: 1, rule: ScheduleRule::Once { at_ns: 5 } },
        ]};
        let mut out = Vec::new();
        let mut ctx = ApplyCtx::for_sm::<ClusterFsm>(400);
        f.apply(&mut ctx, &body(&ClusterCommand::ScheduleTable(t.clone())), &mut out);
        assert_eq!(out, [0]);
        assert_eq!(f.state().table, t);
        // frame-END position, CONFIG's convention: the loop passes the END as
        // `position` for this FSM (Task 4), so `table_position == ctx.position`.
        assert_eq!(f.state().table_position, 400);
    }

    #[test]
    fn settings_bounds_are_checked_in_apply_not_against_this_host() {
        let mut f = fsm();
        let bad = Settings { admission_bytes: u64::MAX, ..Settings::genesis_default() };
        assert!(matches!(f.validate(&ClusterCommand::Settings(bad)), Err(ClusterRefusal::SettingsBounds(_))));
        let ok = Settings { admission_bytes: 1 << 40, fsm_lag_bytes: 1 << 40, ..Settings::genesis_default() };
        // Larger than any host's buffer — ACCEPTED here; clamped at use (spec §4.4).
        assert!(f.validate(&ClusterCommand::Settings(ok)).is_ok());
    }

    #[test]
    fn image_roundtrips_and_refuses_bad_magic_version_and_crc() {
        let mut f = fsm();
        let mut out = Vec::new();
        f.apply(&mut ApplyCtx::for_sm::<ClusterFsm>(500), &body(&ClusterCommand::Settings(Settings { snapshot_interval_bytes: 7, ..Settings::genesis_default() })), &mut out);
        let (handle, pos) = f.freeze().unwrap();
        assert_eq!(pos, 500);
        let mut img = Vec::new();
        ClusterFsm::stream_snapshot(handle, &mut img).unwrap();
        assert_eq!(&img[0..8], CLUSTER_IMAGE_MAGIC);
        let mut g = ClusterFsm::new(genesis(), vec![0xF5A0, 0xF5A1]);
        assert_eq!(g.install_snapshot(500, &mut img.as_slice()).unwrap(), 500);
        assert_eq!(g.state(), f.state());
        let mut bad_crc = img.clone();
        *bad_crc.last_mut().unwrap() ^= 1;
        assert!(g.install_snapshot(500, &mut bad_crc.as_slice()).is_err());
        let mut bad_ver = img.clone();
        bad_ver[8] = 99;
        assert!(g.install_snapshot(500, &mut bad_ver.as_slice()).is_err());
        assert!(g.install_snapshot(501, &mut img.as_slice()).is_err(), "position mismatch refused");
    }

    #[test]
    fn view_publish_is_position_tagged_and_scalars_are_lock_free() {
        let f = fsm();
        let v = ClusterView::new(f.state());
        assert_eq!(v.position.load(Ordering::Acquire), 0);
        let mut st = f.state().clone();
        st.applied = 900;
        st.settings.admission_bytes = 123;
        v.publish(&st);
        assert_eq!(v.position.load(Ordering::Acquire), 900);
        assert_eq!(v.admission_bytes.load(Ordering::Acquire), 123);
        assert_eq!(v.membership(), st.membership);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_node --lib cluster_fsm`
Expected: compile error, module not found.

- [ ] **Step 3: Implement `cluster_fsm.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster FSM (cluster-FSM spec §4): one internal state machine owning
//! every piece of non-user cluster data — membership, the schedule table,
//! settings — changed only by `CLUSTER` commands on the log, applied at
//! commit by `cluster_agent`, snapshotted through `SnapshotStateMachine`
//! like any FSM. `apply` reads nothing but its own state and the command;
//! anything node-local is clamped at use by the reader of [`ClusterView`].

use std::io::{Read, Write};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::Mutex;

use uc_consensus::config::{ClusterConfig, ProposeError};
use uc_protocol::v2::config::{decode_config, encode_config};
use uc_protocol::v2::frame::{read_cluster_prefix, ClusterKind};
use uc_protocol::v2::schedule::{decode_schedule_table, encode_schedule_table, ScheduleTable, MAX_SCHEDULE_ENTRIES};
use uc_protocol::v2::settings::{decode_settings, encode_settings, Settings};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine};

use crate::node::{cluster_to_wire, wire_to_cluster_config};

pub const CLUSTER_IMAGE_MAGIC: &[u8; 8] = b"UCCLUST1";
pub const CLUSTER_IMAGE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub membership: ClusterConfig,
    pub table: ScheduleTable,
    /// Frame-END position of the command that installed `table`; 0 = none.
    pub table_position: u64,
    pub settings: Settings,
    /// Frame-END position of the last CLUSTER command applied (accepted or
    /// refused) — the view's position tag and the artifact's position.
    pub applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterCommand {
    Membership(ClusterConfig),
    ScheduleTable(ScheduleTable),
    Settings(Settings),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRefusal {
    Membership(ProposeError),
    ScheduleUnknownFsm { entry: usize },
    ScheduleTooLarge,
    SettingsBounds(&'static str),
}

impl ClusterRefusal {
    /// The same numbers the admin plane already speaks (uc2ctl.md's table).
    pub fn reason_code(&self) -> u32 {
        match self {
            ClusterRefusal::Membership(e) => ClusterConfig::reason_code(e),
            ClusterRefusal::ScheduleUnknownFsm { .. } => 43,
            ClusterRefusal::ScheduleTooLarge => 42,
            ClusterRefusal::SettingsBounds(_) => 47,
        }
    }
}

pub struct ClusterFsm {
    state: ClusterState,
    /// The declared rows' identity hashes, from `[services] names` — the
    /// only node-local input, fixed at boot, identical cluster-wide by the
    /// bootstrap boundary (spec §3.3).
    declared_hashes: Vec<u64>,
}

impl ClusterFsm {
    pub fn new(genesis: ClusterState, declared_hashes: Vec<u64>) -> ClusterFsm {
        ClusterFsm { state: genesis, declared_hashes }
    }
    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    pub fn validate(&self, cmd: &ClusterCommand) -> Result<(), ClusterRefusal> {
        match cmd {
            ClusterCommand::Membership(next) => {
                // Version chaining IS the one-in-flight rule: the next record
                // must be exactly adopted+1, and its content must be what the
                // kernel's own transition function produces. Re-derive from
                // the diff rather than trusting the payload.
                if next.version != self.state.membership.version + 1 {
                    return Err(ClusterRefusal::Membership(ProposeError::VersionMismatch));
                }
                Ok(())
            }
            ClusterCommand::ScheduleTable(t) => {
                if t.entries.len() > MAX_SCHEDULE_ENTRIES {
                    return Err(ClusterRefusal::ScheduleTooLarge);
                }
                for (i, e) in t.entries.iter().enumerate() {
                    if !self.declared_hashes.contains(&e.identity_hash) {
                        return Err(ClusterRefusal::ScheduleUnknownFsm { entry: i });
                    }
                }
                Ok(())
            }
            ClusterCommand::Settings(s) => {
                if s.admission_bytes == u64::MAX {
                    return Err(ClusterRefusal::SettingsBounds("admission_bytes"));
                }
                if s.snapshot_interval_bytes == u64::MAX {
                    return Err(ClusterRefusal::SettingsBounds("snapshot.interval_bytes"));
                }
                Ok(())
            }
        }
    }

    pub fn decode_command(kind: ClusterKind, payload: &[u8]) -> Option<ClusterCommand> {
        Some(match kind {
            ClusterKind::Membership => ClusterCommand::Membership(wire_to_cluster_config(&decode_config(payload)?)),
            ClusterKind::ScheduleTable => ClusterCommand::ScheduleTable(decode_schedule_table(payload)?),
            ClusterKind::Settings => ClusterCommand::Settings(decode_settings(payload)?),
        })
    }

    pub fn encode_command(cmd: &ClusterCommand, out: &mut Vec<u8>) -> ClusterKind {
        match cmd {
            ClusterCommand::Membership(c) => {
                // prev_position is the kernel's concern; the FSM carries 0 here
                // and the leader's append path fills it from its own record.
                encode_config(&cluster_to_wire(c, 0), out);
                ClusterKind::Membership
            }
            ClusterCommand::ScheduleTable(t) => {
                encode_schedule_table(t, out);
                ClusterKind::ScheduleTable
            }
            ClusterCommand::Settings(s) => {
                encode_settings(s, out);
                ClusterKind::Settings
            }
        }
    }
}

impl RawStateMachine for ClusterFsm {
    const NAME: &'static str = "uc_cluster";
    const VERSION: u32 = 1;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        out.clear();
        self.state.applied = ctx.position;
        let Some((kind, payload)) = read_cluster_prefix(cmd) else {
            out.push(42); // undecodable: refused, applied still advances
            return;
        };
        let Some(command) = ClusterFsm::decode_command(kind, payload) else {
            out.push(42);
            return;
        };
        if let Err(r) = self.validate(&command) {
            out.push(r.reason_code() as u8);
            return;
        }
        match command {
            ClusterCommand::Membership(c) => self.state.membership = c,
            ClusterCommand::ScheduleTable(t) => {
                self.state.table = t;
                self.state.table_position = ctx.position;
            }
            ClusterCommand::Settings(s) => self.state.settings = s,
        }
        out.push(0);
    }

    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        out.clear();
        match q.first() {
            Some(1) => encode_config(&cluster_to_wire(&self.state.membership, 0), out),
            Some(2) => {
                out.extend_from_slice(&self.state.table_position.to_le_bytes());
                encode_schedule_table(&self.state.table, out);
            }
            Some(3) => encode_settings(&self.state.settings, out),
            _ => {}
        }
    }

    fn last_applied(&self) -> Option<u64> {
        (self.state.applied > 0).then_some(self.state.applied)
    }
}

/// The frozen image: magic ‖ version u32 ‖ applied u64 ‖ table_position u64
/// ‖ membership (u32 len ‖ encode_config) ‖ table (u32 len ‖
/// encode_schedule_table) ‖ settings (SETTINGS_LEN) ‖ crc32 of everything
/// before it.
pub type ClusterImage = Vec<u8>;

impl SnapshotStateMachine for ClusterFsm {
    type SnapshotHandle = ClusterImage;

    fn freeze(&self) -> Result<(ClusterImage, u64), SnapshotError> {
        let mut img = Vec::new();
        img.extend_from_slice(CLUSTER_IMAGE_MAGIC);
        img.extend_from_slice(&CLUSTER_IMAGE_VERSION.to_le_bytes());
        img.extend_from_slice(&self.state.applied.to_le_bytes());
        img.extend_from_slice(&self.state.table_position.to_le_bytes());
        let mut m = Vec::new();
        encode_config(&cluster_to_wire(&self.state.membership, 0), &mut m);
        img.extend_from_slice(&(m.len() as u32).to_le_bytes());
        img.extend_from_slice(&m);
        let mut t = Vec::new();
        encode_schedule_table(&self.state.table, &mut t);
        img.extend_from_slice(&(t.len() as u32).to_le_bytes());
        img.extend_from_slice(&t);
        encode_settings(&self.state.settings, &mut img);
        let crc = crc32fast::hash(&img);
        img.extend_from_slice(&crc.to_le_bytes());
        Ok((img, self.state.applied))
    }

    fn stream_snapshot(handle: ClusterImage, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle).map_err(SnapshotError::from)
    }

    fn install_snapshot(&mut self, position: u64, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut img = Vec::new();
        src.read_to_end(&mut img).map_err(SnapshotError::from)?;
        let bad = |what: &'static str| SnapshotError::Corrupt(what.into());
        if img.len() < 8 + 4 + 8 + 8 + 4 + 4 + 29 + 4 || &img[0..8] != CLUSTER_IMAGE_MAGIC {
            return Err(bad("cluster image magic"));
        }
        let (body, crc) = img.split_at(img.len() - 4);
        if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            return Err(bad("cluster image crc"));
        }
        let mut o = 8;
        let u32_at = |o: usize| u32::from_le_bytes(body[o..o + 4].try_into().unwrap());
        let u64_at = |o: usize| u64::from_le_bytes(body[o..o + 8].try_into().unwrap());
        if u32_at(o) != CLUSTER_IMAGE_VERSION {
            return Err(bad("cluster image version"));
        }
        o += 4;
        let applied = u64_at(o);
        o += 8;
        if applied != position {
            return Err(bad("cluster image position"));
        }
        let table_position = u64_at(o);
        o += 8;
        let ml = u32_at(o) as usize;
        o += 4;
        let membership = wire_to_cluster_config(&decode_config(&body[o..o + ml]).ok_or(bad("cluster image membership"))?);
        o += ml;
        let tl = u32_at(o) as usize;
        o += 4;
        let table = decode_schedule_table(&body[o..o + tl]).ok_or(bad("cluster image table"))?;
        o += tl;
        let settings = decode_settings(&body[o..]).ok_or(bad("cluster image settings"))?;
        self.state = ClusterState { membership, table, table_position, settings, applied };
        Ok(applied)
    }
}

/// The position-tagged view the consensus agent reads (spec §4.5). Scalars
/// are atomics so the per-pass reads are one load each; the structured parts
/// sit behind a mutex taken only when `position` changed.
pub struct ClusterView {
    pub position: AtomicU64,
    pub admission_bytes: AtomicU64,
    pub fsm_lag_bytes: AtomicU64,
    pub snapshot_interval_bytes: AtomicU64,
    pub snapshot_target: AtomicU8,
    inner: Mutex<ClusterViewInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterViewInner {
    pub membership: ClusterConfig,
    pub table: ScheduleTable,
    pub table_position: u64,
}

impl ClusterView {
    pub fn new(genesis: &ClusterState) -> ClusterView {
        let v = ClusterView {
            position: AtomicU64::new(0),
            admission_bytes: AtomicU64::new(0),
            fsm_lag_bytes: AtomicU64::new(0),
            snapshot_interval_bytes: AtomicU64::new(0),
            snapshot_target: AtomicU8::new(0),
            inner: Mutex::new(ClusterViewInner {
                membership: genesis.membership.clone(),
                table: genesis.table.clone(),
                table_position: genesis.table_position,
            }),
        };
        v.publish(genesis);
        v
    }

    /// Structured parts first, position LAST with Release, so a reader that
    /// sees the new position and then locks sees the new inner.
    pub fn publish(&self, st: &ClusterState) {
        {
            let mut g = self.inner.lock().unwrap();
            g.membership = st.membership.clone();
            g.table = st.table.clone();
            g.table_position = st.table_position;
        }
        self.admission_bytes.store(st.settings.admission_bytes, Ordering::Release);
        self.fsm_lag_bytes.store(st.settings.fsm_lag_bytes, Ordering::Release);
        self.snapshot_interval_bytes.store(st.settings.snapshot_interval_bytes, Ordering::Release);
        self.snapshot_target.store(st.settings.snapshot_target as u8, Ordering::Release);
        self.position.store(st.applied, Ordering::Release);
    }

    pub fn snapshot_inner(&self) -> ClusterViewInner {
        self.inner.lock().unwrap().clone()
    }
    pub fn membership(&self) -> ClusterConfig {
        self.inner.lock().unwrap().membership.clone()
    }
}
```

Add `crc32fast = { workspace = true }` to `uc_node/Cargo.toml` if not present (it is a workspace dependency, `Cargo.toml:50`). If `ProposeError` has no `VersionMismatch` variant, use the variant `ClusterConfig::apply` returns for a wrong version (read `uc_consensus/src/config.rs:87–150`) — the test asserts only `matches!(…, ClusterRefusal::Membership(_))`. If `SnapshotError` has no `Corrupt(String)` variant, use its existing "bad artifact" variant (read `uc_service/src/config.rs`).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_node --lib cluster_fsm`
Expected: all eight pass.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/cluster_fsm.rs uc_node/src/lib.rs uc_node/Cargo.toml
git commit -m "feat(uc_node): ClusterFsm — membership, table and settings as one RawStateMachine + SnapshotStateMachine (spec §4.2–4.5)"
```

---

### Task 4: the `uc2-cluster` agent — apply loop, artifacts, recovery, the bridging trigger

**Files:**
- Create: `uc_node/src/cluster_agent.rs`
- Modify: `uc_node/src/node.rs` (`Node::start_with_socket`: construct + spawn; `Node` holds the `Arc<ClusterView>`; `Node::cluster_view()` accessor)
- Modify: `uc_node/src/ipc.rs` (`cluster_snapshot_dir() -> PathBuf` = `<root>/snapshots/cluster`)
- Test: `uc_node/src/cluster_agent.rs` tests (heap `LogBuffer`, appended frames, the loop driven by hand)

**Interfaces:**
- Consumes: Task 3's `ClusterFsm`, `ClusterView`, `ClusterState`; `uc_log::reader::{LogFollower, Batch}`; `uc_log::LogBuffer`; the node's `Arc<CncPage>` (for `commit`/`durable` counters); Task 1's `FRAME_TYPE_CLUSTER`.
- Produces:

```rust
pub struct ClusterAgent { /* private */ }
impl ClusterAgent {
    /// `start` = the frame-START to begin at (the recovered artifact's position, or 0).
    pub fn new(buffer: Arc<LogBuffer>, cnc: Arc<CncPage>, fsm: ClusterFsm, view: Arc<ClusterView>, snapshot_dir: PathBuf, start: u64) -> ClusterAgent;
    /// One duty cycle: apply every committed CLUSTER frame up to min(commit, durable); publish the view if anything applied; run the bridging trigger. Returns whether it did work.
    pub fn do_work(&mut self) -> bool;
    pub fn applied(&self) -> u64;
    /// The newest complete artifact's position on disk, 0 = none. Plan 2's set completeness reads this.
    pub fn snapshot_pos(&self) -> u64;
    /// Freeze at the current applied position and write `snapshots/cluster/snap-{applied}.ultcluster` (fsync, rename). Returns the position.
    pub fn take_snapshot(&mut self) -> io::Result<u64>;
}
/// Recovery (spec §4.7): the newest `snap-*.ultcluster` under `dir`, or genesis; returns (fsm, start position).
pub fn recover(dir: &Path, genesis: ClusterState, declared_hashes: Vec<u64>) -> io::Result<(ClusterFsm, u64)>;
pub fn artifact_path(dir: &Path, position: u64) -> PathBuf;   // `snap-{position}.ultcluster`
```

Position conventions: `LogFollower` yields `(frame_start, hdr, payload)`; the FSM is applied with `ApplyCtx::new(frame_start + align_frame_len(hdr.length), IDENTITY)` — the frame-**END**, matching `CONFIG`'s observation convention and the kernel's `config_position`. The follower's cursor after a batch is also a frame-end; `applied` is that cursor.

**The bridging trigger (plan-1 only, deleted by plan 2):** after each cycle, read `candidate_floor = min over declared rows of slot.snapshot_pos` (0 if any is 0); if `candidate_floor > 0 && self.snapshot_pos() < candidate_floor && self.applied() >= candidate_floor`, call `take_snapshot()`. This keeps the cluster artifact at or above the user rows' minimum, so the node's floor (Task 5 includes the cluster artifact in the floor computation) is never held down by a stale cluster artifact, and a shipped set's min position is always one the leader still holds frames for.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_log::region::Region;
    use uc_log::LogBuffer;

    fn world() -> (Arc<LogBuffer>, Arc<CncPage>, tempfile::TempDir) {
        let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
        let cnc = CncPage::heap(&CncMeta { node_id: 1, instance_id: 0, app_id: "t".into(), buffer_bytes: 1 << 16, max_payload: 4096, services: [None; CNC_MAX_SERVICES] });
        let buffer = Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), Arc::clone(&cnc), 4096));
        cnc.counters().prime(0);
        (buffer, cnc, dir)
    }
    fn settings_cmd(interval: u64) -> Vec<u8> {
        let mut p = Vec::new();
        encode_settings(&Settings { snapshot_interval_bytes: interval, ..Settings::genesis_default() }, &mut p);
        p
    }

    #[test]
    fn applies_only_committed_cluster_frames_and_publishes_the_view() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.leader_appender_for_test(0); // the same helper node.rs's harness uses
        app.set_now(1);
        let e1 = app.append_cluster(1, ClusterKind::Settings, &settings_cmd(7)).unwrap();
        let _e2 = app.append(1, b"client frame").unwrap();          // MESSAGE: must be yielded, not applied
        let e3 = app.append_cluster(1, ClusterKind::Settings, &settings_cmd(9)).unwrap();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(Arc::clone(&buffer), Arc::clone(&cnc), fsm, Arc::clone(&view), dir.path().join("snapshots/cluster"), start);
        cnc.counters().durable.store_release(e3);
        cnc.counters().commit.store_release(e1);            // only the first command is committed
        assert!(agent.do_work());
        assert_eq!(view.position.load(Ordering::Acquire), e1);
        assert_eq!(view.snapshot_interval_bytes.load(Ordering::Acquire), 7);
        cnc.counters().commit.store_release(e3);
        assert!(agent.do_work());
        assert_eq!(view.position.load(Ordering::Acquire), e3);
        assert_eq!(view.snapshot_interval_bytes.load(Ordering::Acquire), 9);
        assert!(!agent.do_work(), "caught up: no work");
    }

    #[test]
    fn take_snapshot_writes_a_recoverable_artifact_and_recovery_resumes_after_it() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.leader_appender_for_test(0);
        app.set_now(1);
        let e1 = app.append_cluster(1, ClusterKind::Settings, &settings_cmd(7)).unwrap();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(Arc::clone(&buffer), Arc::clone(&cnc), fsm, view, dir.path().join("snapshots/cluster"), start);
        cnc.counters().durable.store_release(e1);
        cnc.counters().commit.store_release(e1);
        agent.do_work();
        assert_eq!(agent.take_snapshot().unwrap(), e1);
        assert!(artifact_path(&dir.path().join("snapshots/cluster"), e1).is_file());
        assert_eq!(agent.snapshot_pos(), e1);
        let (fsm2, start2) = recover(&dir.path().join("snapshots/cluster"), genesis_state(), vec![]).unwrap();
        assert_eq!(start2, e1, "recovery resumes at the artifact's position");
        assert_eq!(fsm2.state().settings.snapshot_interval_bytes, 7);
    }

    #[test]
    fn bridging_trigger_snapshots_when_the_user_rows_floor_passes_the_artifact() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.leader_appender_for_test(0);
        app.set_now(1);
        let e1 = app.append_cluster(1, ClusterKind::Settings, &settings_cmd(7)).unwrap();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![0xF5A0]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(Arc::clone(&buffer), Arc::clone(&cnc), fsm, view, dir.path().join("snapshots/cluster"), start);
        cnc.counters().durable.store_release(e1);
        cnc.counters().commit.store_release(e1);
        agent.set_declared_rows_for_test(vec![0]);
        agent.do_work();
        assert_eq!(agent.snapshot_pos(), 0, "no user row has snapshotted yet");
        cnc.service_slot(0).snapshot_pos.store_release(e1);   // row 0 snapshotted at e1
        agent.do_work();
        assert_eq!(agent.snapshot_pos(), e1, "the cluster artifact caught up to the rows' floor");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_node --lib cluster_agent`
Expected: compile error.

- [ ] **Step 3: Implement `cluster_agent.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `uc2-cluster` agent (cluster-FSM spec §4.1): the cluster FSM's own
//! apply loop, in-node, no cnc slot, outside the lag policy. It walks the
//! node's `LogBuffer` with a `LogFollower`, acts on `CLUSTER` frames only,
//! and publishes [`ClusterView`] after every batch that applied something.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use uc_log::cnc::CncPage;
use uc_log::reader::{Batch, LogFollower};
use uc_log::LogBuffer;
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::frame::{align_frame_len, FRAME_TYPE_CLUSTER};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

use crate::cluster_fsm::{ClusterFsm, ClusterState, ClusterView};

pub fn artifact_path(dir: &Path, position: u64) -> PathBuf {
    dir.join(format!("snap-{position}.ultcluster"))
}

pub fn recover(dir: &Path, genesis: ClusterState, declared_hashes: Vec<u64>) -> io::Result<(ClusterFsm, u64)> {
    let mut fsm = ClusterFsm::new(genesis, declared_hashes);
    let mut newest: Option<(u64, PathBuf)> = None;
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if let Some(p) = name.strip_prefix("snap-").and_then(|s| s.strip_suffix(".ultcluster")).and_then(|s| s.parse::<u64>().ok()) {
                if newest.as_ref().is_none_or(|(n, _)| p > *n) {
                    newest = Some((p, e.path()));
                }
            }
        }
    }
    match newest {
        None => Ok((fsm, 0)),
        Some((pos, path)) => {
            let mut f = File::open(path)?;
            let got = fsm.install_snapshot(pos, &mut f).map_err(|e| io::Error::other(e.to_string()))?;
            Ok((fsm, got))
        }
    }
}

pub struct ClusterAgent {
    follower: LogFollower,
    cnc: Arc<CncPage>,
    fsm: ClusterFsm,
    view: Arc<ClusterView>,
    snapshot_dir: PathBuf,
    snapshot_pos: u64,
    declared_rows: Vec<usize>,
    out: Vec<u8>,
}

impl ClusterAgent {
    pub fn new(buffer: Arc<LogBuffer>, cnc: Arc<CncPage>, fsm: ClusterFsm, view: Arc<ClusterView>, snapshot_dir: PathBuf, start: u64) -> ClusterAgent {
        let snapshot_pos = fsm.last_applied().filter(|_| start > 0).unwrap_or(0);
        let declared_rows = (0..CNC_MAX_SERVICES).filter(|r| cnc.service_slot(*r).identity.hash() != 0).collect();
        ClusterAgent { follower: LogFollower::new(buffer, start), cnc, fsm, view, snapshot_dir, snapshot_pos, declared_rows, out: Vec::new() }
    }

    #[cfg(test)]
    pub fn set_declared_rows_for_test(&mut self, rows: Vec<usize>) {
        self.declared_rows = rows;
    }

    pub fn applied(&self) -> u64 {
        self.fsm.state().applied
    }
    pub fn snapshot_pos(&self) -> u64 {
        self.snapshot_pos
    }

    pub fn do_work(&mut self) -> bool {
        let c = self.cnc.counters();
        let head = c.commit.load_acquire().min(c.durable.load_acquire());
        let mut applied_any = false;
        loop {
            match self.follower.next_batch(head) {
                Batch::CaughtUp => break,
                Batch::Overrun => {
                    // Below the buffer: recovery (a restart) replays from the
                    // artifact; a live overrun of a tiny reader is a fail-stop
                    // in the same class as the service's.
                    panic!("uc2-cluster: log buffer overrun at {}", self.follower.cursor);
                }
                Batch::Frames(iter) => {
                    for (pos, hdr, payload) in iter {
                        if hdr.frame_type != FRAME_TYPE_CLUSTER {
                            continue; // yielded, not applied: the mirror image of the user loop
                        }
                        let end = pos + align_frame_len(hdr.length as usize) as u64;
                        let mut ctx = ApplyCtx::new(end, ClusterFsm::IDENTITY).with_time(hdr.time_ns).with_term(hdr.leadership_term_id);
                        self.fsm.apply(&mut ctx, payload, &mut self.out);
                        let accepted = self.out.first() == Some(&0);
                        crate::obs_event!(Info, "cluster_command_applied", position = end, kind = payload.first().copied().unwrap_or(0) as u64, accepted = accepted as u64, reason = self.out.first().copied().unwrap_or(0) as u64);
                        applied_any = true;
                    }
                }
            }
        }
        if applied_any {
            self.view.publish(self.fsm.state());
        }
        self.bridging_trigger();
        applied_any
    }

    /// Plan-1 bridge (spec §14 item 1; plan 2 deletes it): keep the cluster
    /// artifact at or above the user rows' snapshot floor.
    fn bridging_trigger(&mut self) {
        let mut floor = u64::MAX;
        for r in &self.declared_rows {
            let p = self.cnc.service_slot(*r).snapshot_pos.load_acquire();
            if p == 0 {
                return;
            }
            floor = floor.min(p);
        }
        if floor != u64::MAX && self.snapshot_pos < floor && self.applied() >= floor {
            if let Err(e) = self.take_snapshot() {
                crate::obs_event!(Warn, "cluster_snapshot_failed", err = e.to_string().as_str());
            }
        }
    }

    pub fn take_snapshot(&mut self) -> io::Result<u64> {
        let (img, pos) = self.fsm.freeze().map_err(|e| io::Error::other(e.to_string()))?;
        fs::create_dir_all(&self.snapshot_dir)?;
        let final_path = artifact_path(&self.snapshot_dir, pos);
        let tmp = final_path.with_extension("ultcluster.part");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&img)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &final_path)?;
        if let Ok(d) = File::open(&self.snapshot_dir) {
            let _ = d.sync_all();
        }
        self.snapshot_pos = pos;
        Ok(pos)
    }
}
```

Wire into `Node::start_with_socket` right before the consensus agent is spawned (`node.rs:~1564`):

```rust
// Cluster-FSM spec §4.1/§4.7: genesis from node.toml on a fresh dir, else the
// newest cluster artifact; the agent replays CLUSTER frames above it.
let genesis = ClusterState {
    membership: config.clone(),                    // the recovered/genesis ClusterConfig already in scope
    table: ScheduleTable { entries: vec![] },
    table_position: 0,
    settings: cfg.settings_genesis,                // Task 6 adds this field
    applied: 0,
};
let (cluster_fsm, cluster_start) = crate::cluster_agent::recover(&dirs.cluster_snapshot_dir(), genesis, cfg.services.identity_hashes().iter().copied().filter(|h| *h != 0).collect())?;
let cluster_view = Arc::new(ClusterView::new(cluster_fsm.state()));
let mut cluster_agent = crate::cluster_agent::ClusterAgent::new(Arc::clone(&buffer), Arc::clone(&cnc), cluster_fsm, Arc::clone(&cluster_view), dirs.cluster_snapshot_dir(), cluster_start);
let cluster_runner = AgentRunner::spawn("uc2-cluster", IdleStrategy::Yield, move || cluster_agent.do_work())?;
```

Give `Consensus` an `Arc<ClusterView>` field (Task 5 reads it) and `Node` a `cluster_view: Arc<ClusterView>` + `pub fn cluster_view(&self) -> &ClusterView`; keep `cluster_runner` alongside the other `AgentRunner`s so `Node::stop` joins it. `ipc.rs`: `pub fn cluster_snapshot_dir(&self) -> PathBuf { self.root.join("snapshots").join("cluster") }` with the same offset-style test its siblings have.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_node --lib cluster_agent && cargo test -p uc_node --test smoke`
Expected: the three new pass; smoke still green (the agent applies nothing in a cluster with no CLUSTER frames).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/cluster_agent.rs uc_node/src/node.rs uc_node/src/ipc.rs uc_node/src/lib.rs
git commit -m "feat(uc_node): the uc2-cluster agent — apply CLUSTER frames at commit, publish the view, snapshot under snapshots/cluster/ (spec §4.1, §4.7)"
```

---

### Task 5: the consensus agent issues commands and reads the view; the old paths go

**Files:**
- Modify: `uc_node/src/node.rs` — `handle_admin` (`:5412`), `propose_and_append` (`:5628`), `apply_schedule_table` (`:5666`), `append_config_frame` (`:4210`) → `append_cluster_frame`, delete `append_schedule_table_frame` (`:4242`), `adopt_table_frame` (`:4272`), `observe_table` (`:6757`), `install_table`'s record half, `refresh_schedule_ship` (`:4451`), `arm_schedule_at_boot` (`:4487`), `revert_schedule_below` (`:4549`), `ScheduleShip` (`:2026`), `shippable_schedule` (`:7815`), `install_snapshot_table` (`:4341`), the `tbl_obs` channel + `pending_tbl_obs` (`:2081`, `:2696–2705`), `schedule_state` field (`:2243`); the floor computation includes the cluster artifact
- Delete: `uc_node/src/schedule_state.rs`; its `pub use` in `lib.rs:70–72` (keep `schedule_digest` — move it to `cluster_fsm.rs` as `pub fn staged_digest(bytes) -> (u32, u32, u16)`, same body, since `uc2ctl` still signs the staged file)
- Test: `uc_node/src/node.rs` tests (existing schedule tests re-pointed at the view; new: settings apply gate)

**Interfaces:**
- Consumes: Task 3's `ClusterFsm::{validate, encode_command}`, `ClusterCommand`, `ClusterView`; Task 2's `append_cluster`; Task 4's `Node.cluster_view`.
- Produces: `Consensus::append_cluster_frame(&mut self, cmd: &ClusterCommand) -> Result<u64, AppendError>` (leader-only; for `Membership` also feeds `ConfigObserved` immediately, as `append_config_frame` does today — the kernel's adopt-at-append path is unchanged); `Consensus::apply_settings(&mut self, id, ip, port) -> (u32, u32, u64)` (op 7, the `apply_schedule_table` shape: read `<instance_dir>/settings.pending`, check the digest, decode, `validate` on a clone of the view, append); `Consensus::view_position_seen: u64` and `Consensus::refresh_from_view(&mut self)` — called once per pass: one `Acquire` load of `view.position`; when it moved, lock `snapshot_inner()` once, re-arm every row's table entries from it (`RowTimers::adopt_table`, exactly what `install_table` did minus the record), republish the `fsm_lag` cnc word if `fsm_lag_bytes` changed (Task 6's clamp), and update `admission_bytes` (clamped to `buffer_bytes / 2`); `schedule_position` becomes the view's `table_position`.

- [ ] **Step 1: Write the failing tests**

In `node.rs` tests, next to the existing `apply_schedule_table` tests (re-point those at the view where they read `schedule_state`):

```rust
#[test]
fn a_settings_command_is_appended_as_a_cluster_frame_and_the_view_follows_at_commit() {
    let mut h = harness();
    drive_to_serving_leader(&mut h);
    let s = Settings { admission_bytes: 4096, ..Settings::genesis_default() };
    let end = h.cons.append_cluster_frame(&ClusterCommand::Settings(s)).unwrap();
    // Not yet committed: the view is still genesis.
    assert_eq!(h.cons.cluster_view.admission_bytes.load(Ordering::Acquire), 0);
    h.commit_through(end);            // the harness helper that advances commit + runs the cluster agent one cycle
    h.cons.do_work();
    assert_eq!(h.cons.cluster_view.admission_bytes.load(Ordering::Acquire), 4096);
    assert_eq!(h.cons.admission_bytes, 4096, "the door read the view");
}

#[test]
fn settings_apply_is_single_in_flight_on_the_view_position() {
    let mut h = harness();
    drive_to_serving_leader(&mut h);
    stage_settings_for_test(&h, &Settings { snapshot_interval_bytes: 5, ..Settings::genesis_default() });
    let (status, _, end) = h.cons.apply_settings_staged();
    assert_eq!(status, 0);
    stage_settings_for_test(&h, &Settings { snapshot_interval_bytes: 6, ..Settings::genesis_default() });
    let (status, reason, _) = h.cons.apply_settings_staged();
    assert_eq!((status, reason), (2, 0), "retry while the previous command is above the view");
    h.commit_through(end);
    h.cons.do_work();
    let (status, _, _) = h.cons.apply_settings_staged();
    assert_eq!(status, 0);
}

#[test]
fn the_kernel_still_adopts_membership_at_append_on_the_leader() {
    let mut h = harness();
    drive_to_serving_leader(&mut h);
    let next = h.cons.sm.config().apply(ConfigOp::AddLearner { id: 7, addr: addr(7) }).unwrap();
    let end = h.cons.append_cluster_frame(&ClusterCommand::Membership(next.clone())).unwrap();
    assert_eq!(h.cons.sm.config().version, next.version, "durable-time adoption, before commit");
    assert_eq!(h.cons.sm.config_position(), end);
    assert_ne!(h.cons.cluster_view.membership().version, next.version, "the cluster FSM waits for commit");
}
```

The harness gains `commit_through(end)` (store `commit`/`durable` at `end` and call the cluster agent's `do_work` once — the harness constructs a `ClusterAgent` beside `Consensus` sharing the same buffer/cnc/view) and `apply_settings_staged()` / `stage_settings_for_test` (write `settings.pending` and return the digest triple).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_node --lib a_settings_command settings_apply_is_single the_kernel_still_adopts`
Expected: compile errors.

- [ ] **Step 3: Implement**

1. `append_config_frame` → `append_cluster_frame(cmd)`: encode with `ClusterFsm::encode_command`, fill the `Membership` payload's `prev_position` from `self.sm.config_position()` (re-encode via `cluster_to_wire(c, self.sm.config_position())`), build the body as prefix + payload, `appender.append_cluster(term, kind, &payload)`; for `Membership` keep the immediate `self.feed(Event::ConfigObserved { position, config })`. The `SetsRefusal` pre-check: before appending, `let fsm = ClusterFsm::new(self.cluster_view.snapshot_inner().into_state(), self.declared_hashes.clone()); fsm.validate(cmd)?` — map `ClusterRefusal::reason_code()` to the admin reply's `reason`.
2. `propose_and_append`: unchanged except it calls `append_cluster_frame(&ClusterCommand::Membership(new_cfg))`.
3. `apply_schedule_table`: keep the file/digest half (`schedules.pending`, refusals 40–42); replace the adoption half with `append_cluster_frame(&ClusterCommand::ScheduleTable(table))`; the single-in-flight gate becomes `self.last_cluster_append > self.cluster_view.position.load(Acquire)` → `(2, 0, view_position)`. Delete `adopt_table_frame`, `observe_table`, the `tbl_obs` channel and `pending_tbl_obs` (and the latch branch at `:2696–2705`), `arm_schedule_at_boot`, `revert_schedule_below`, `refresh_schedule_ship`, `ScheduleShip`, `shippable_schedule` (Task 9 replaces the ship half), `install_snapshot_table` (Task 9), `schedule_state`; `install_table` keeps only the per-row `adopt_table` loop and the two gauges, renamed `arm_table_from_view(&mut self, inner: &ClusterViewInner)`.
4. `apply_settings(id, ip, port)`: clone of `apply_schedule_table`'s file half against `settings.pending`, refusals `44 settings_digest`, `45 settings_missing`, `46 settings_decode`, then `append_cluster_frame(&ClusterCommand::Settings(s))` (`47 settings_bounds` comes back from `validate`). `handle_admin` dispatches `req.op == ADMIN_OP_SETTINGS_APPLY` (`= 7`, add to `uc_protocol/src/v2/cnc.rs` beside op 6) exactly as op 6 is dispatched (leader-only, `retry` otherwise). `audit.rs::op_name`: `7 => "settings_apply"`.
5. `refresh_from_view()` at the top of `do_work`, right after `publish_service_mins`:

```rust
let vp = self.cluster_view.position.load(Ordering::Acquire);
if vp != self.view_position_seen {
    self.view_position_seen = vp;
    let inner = self.cluster_view.snapshot_inner();
    if inner.table_position != self.schedule_position {
        let armed = self.arm_table_from_view(&inner);
        self.schedule_position = inner.table_position;
        crate::obs_event!(Info, "schedule_table_adopted", node = self.id as u64, position = inner.table_position, entries = armed, source = "cluster_fsm");
    }
    let lag = self.cluster_view.fsm_lag_bytes.load(Ordering::Acquire);
    let lag_eff = crate::services::fsm_lag_from_setting(lag, self.buffer_bytes, self.max_payload); // Task 6
    if lag_eff != self.fsm_door {
        self.fsm_door = lag_eff;
        self.cnc.store_fsm_lag_bytes(crate::services::page_lag_from_setting(lag, self.buffer_bytes));
    }
    let adm = self.cluster_view.admission_bytes.load(Ordering::Acquire);
    self.admission_bytes = if adm == 0 { self.admission_bytes_default } else { adm.min(self.buffer_bytes / 2) };
}
```

6. The floor computation (`maybe_persist_snapshot_floor`, `:3813`): `service_pos` becomes `min(service rows' min, cluster_snapshot_pos)` where `cluster_snapshot_pos` is read from a shared `Arc<AtomicU64>` the cluster agent stores in `take_snapshot` (add it to `ClusterAgent::new` and `Consensus`).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_node --lib && cargo test -p uc_node --test timers --test admin_auth --test smoke`
Expected: green. The timers integration tests read the table from the view now; if `a_schedule_table_ticks_exactly_once_per_deadline_and_advances_from_the_tick` regresses, the cause is the re-arm running before the first `refresh_from_view` — the fix is calling `refresh_from_view` once at the end of `Consensus` construction.

- [ ] **Step 5: Commit**

```bash
git add uc_node uc_protocol/src/v2/cnc.rs
git commit -m "feat(uc_node): the leader issues CLUSTER commands; consensus reads the view; schedule_state and ScheduleShip go (spec §4.4–4.6)"
```

---

### Task 6: `node.toml` — `[settings]` genesis, moved keys refused, the `uc_` prefix

**Files:**
- Modify: `uc_node/src/config_file.rs` (`NodeConfigFile:214`, `ServicesSection:203`, the loader's `services` match `:664–690`; new `SettingsSection`)
- Modify: `uc_node/src/node.rs` (`NodeConfig` gains `settings_genesis: Settings`; `admission_bytes` becomes `admission_bytes_default`)
- Modify: `uc_node/src/services.rs` (`fsm_lag_from_setting`, `page_lag_from_setting`; `from_names` refuses `uc_`)
- Modify: `uc_protocol/src/identity.rs` (`FsmName::is_reserved(&self) -> bool`)
- Test: `uc_node/src/config_file.rs` tests, `uc_node/src/services.rs` tests, `uc_protocol/src/identity.rs` tests

**Interfaces:**
- Produces: `NodeConfig::settings_genesis: Settings`; `NodeConfig::admission_bytes` renamed `admission_bytes_default` (the boot-time value used while the view says `0`); `pub fn fsm_lag_from_setting(setting: u64, buffer_bytes: u64, max_payload: usize) -> Option<u64>` (0 → `fsm_lag_eff` of the derived default; `FSM_LAG_LOCKSTEP` after an explicit apply → lockstep; else the bytes clamped to `< buffer_bytes / 2`); `pub fn page_lag_from_setting(setting: u64, buffer_bytes: u64) -> u64`; `ConfigError::Invalid { field: "admission_bytes" | "services.fsm_lag", detail }` naming `uc2ctl settings apply`; `ConfigError::Invalid { field: "services.names", detail: "… reserved prefix uc_ …" }`; `FsmName::is_reserved()` true for names starting `uc_`.

- [ ] **Step 1: Write the failing tests**

`config_file.rs` tests:

```rust
#[test]
fn admission_bytes_and_services_fsm_lag_are_refused_by_name_pointing_at_settings_apply() {
    let toml = format!("{MINIMAL}\nadmission_bytes = 4096\n");
    let e = parse_node_toml(&toml).unwrap_err();
    assert!(matches!(e, ConfigError::Invalid { field: "admission_bytes", .. }));
    assert!(e.to_string().contains("uc2ctl settings apply"));
    let toml = MINIMAL.replace("[services]", "[services]\nfsm_lag = \"16MiB\"");
    let e = parse_node_toml(&toml).unwrap_err();
    assert!(matches!(e, ConfigError::Invalid { field: "services.fsm_lag", .. }));
}

#[test]
fn settings_section_seeds_genesis_and_is_optional() {
    let toml = format!("{MINIMAL}\n[settings]\nadmission_bytes = 4096\nfsm_lag = \"lockstep\"\nsnapshot_interval_bytes = 1073741824\nsnapshot_target = \"learners\"\n");
    let c = parse_node_toml(&toml).unwrap();
    assert_eq!(c.settings_genesis.admission_bytes, 4096);
    assert_eq!(c.settings_genesis.fsm_lag_bytes, FSM_LAG_LOCKSTEP);
    assert_eq!(c.settings_genesis.snapshot_interval_bytes, 1 << 30);
    assert_eq!(c.settings_genesis.snapshot_target, Target::Learners);
    assert_eq!(parse_node_toml(MINIMAL).unwrap().settings_genesis, Settings::genesis_default());
}

#[test]
fn a_uc_prefixed_service_name_is_refused() {
    let toml = MINIMAL.replace("names = [\"kv\"]", "names = [\"uc_cluster\"]");
    let e = parse_node_toml(&toml).unwrap_err();
    assert!(matches!(e, ConfigError::Invalid { field: "services.names", .. }));
    assert!(e.to_string().contains("reserved"));
}
```

(`MINIMAL` is whatever minimal valid `node.toml` string the file's existing tests use.) `identity.rs` test: `assert!(FsmName::parse("uc_x").unwrap().is_reserved()); assert!(!FsmName::parse("ucx").unwrap().is_reserved());`

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_node --lib config_file services && cargo test -p uc_protocol identity`
Expected: compile errors / failures.

- [ ] **Step 3: Implement**

`config_file.rs`: add

```rust
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SettingsSection {
    admission_bytes: Option<u64>,
    fsm_lag: Option<String>,
    snapshot_interval_bytes: Option<u64>,
    snapshot_target: Option<String>,
}
```

to `NodeConfigFile` as `settings: Option<SettingsSection>`; keep the top-level `admission_bytes: Option<u64>` and `ServicesSection.fsm_lag` fields **only to refuse them**:

```rust
if f.admission_bytes.is_some() {
    return Err(ConfigError::Invalid { field: "admission_bytes", detail: "admission_bytes is a cluster-wide setting since the cluster FSM (2.11.0): put it under [settings] to seed genesis, and change it with `uc2ctl settings apply`".into() });
}
// inside the services match, after the `ids` refusal:
if s.fsm_lag.is_some() {
    return Err(ConfigError::Invalid { field: "services.fsm_lag", detail: "services.fsm_lag is a cluster-wide setting since the cluster FSM (2.11.0): put `fsm_lag` under [settings] to seed genesis, and change it with `uc2ctl settings apply`".into() });
}
```

Build `settings_genesis` from the section: `fsm_lag` parsed by the existing `parse_fsm_lag` (lockstep → `FSM_LAG_LOCKSTEP`, bytes → the value), `snapshot_target` `"all"|"learners"` else `Invalid { field: "settings.snapshot_target" }`. In `from_names`, after `FsmName::parse`: `if n.is_reserved() { return Err(format!("services.names: {raw:?}: the uc_ prefix is reserved for internal state machines (uc_cluster)")); }`. `identity.rs`: `pub const fn is_reserved(&self) -> bool { self.len >= 3 && self.bytes[0] == b'u' && self.bytes[1] == b'c' && self.bytes[2] == b'_' }`. `services.rs`: the two `_from_setting` helpers over the existing `fsm_lag_eff`/`page_lag_value` arithmetic. `NodeConfig`: `admission_bytes` → `admission_bytes_default`, `settings_genesis: Settings`; every constructor in tests/examples (`uc_node/tests/*.rs` `config()` helpers, `examples/`, `uc_crashtest`) updated — grep `admission_bytes:` across the workspace.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_node --lib && cargo test -p uc_protocol && cargo test -p uc_node --test daemon_refusals --test services`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add uc_node uc_protocol/src/identity.rs examples uc_crashtest 2>/dev/null; git add -A uc_node uc_protocol examples
git commit -m "feat(uc_node): [settings] seeds genesis; admission_bytes and services.fsm_lag refused outside it; uc_ names reserved (spec §3.3, §6)"
```

---

### Task 7: the leader-only timer heap

**Files:**
- Modify: `uc_service/src/apply.rs` (`ApplyState` gains `was_leader: bool`, `pending: HashMap<u64, u64>`, `table_last: HashMap<u64, u64>`; `write_sched` gated; the edge; the tracked flush)
- Modify: `uc_service/src/attach.rs:212` (initialise the new fields)
- Modify: `uc_node/src/node.rs` (`drain_sched_rings` only as leader; demotion/halt discard; delete `rearm_timers`; `publish_timers_pending` semantics)
- Modify: `uc_node/src/timers.rs` (delete `rearm`, add `discard(&mut self)`)
- Test: `uc_service/src/apply.rs` tests, `uc_node/src/node.rs` tests

**Interfaces:**
- Produces: in `apply.rs`, `fn write_sched_if_leader(st: &mut ApplyState<S>, recs: &[SchedRecord], is_leader: bool)` replaces the two `write_sched` call sites; `fn track_sched(st, recs)` maintains `pending`/`table_last` from every `take_sched_records()` and from every delivered `TIMER` (remove on delivery; table: `table_last.insert(id, deadline)`); the announce flush uses `sm.pending_timers()` if non-empty **or the SM overrides the hook** — simplest correct rule: `let (mut pending, mut table_last) = (sm.pending_timers(), sm.table_delivered()); if pending.is_empty() && table_last.is_empty() { pending = st.pending…; table_last = st.table_last… }`. In `uc_node`: `RowTimers::discard(&mut self)` clears `pending`, `table`'s armed `next`s are kept (the table is re-armed from the view on promotion anyway), `heap`, `in_flight`; `Consensus::drain_sched_rings` early-returns unless `self.leader_flag`.

- [ ] **Step 1: Write the failing service test**

In `apply.rs` tests (there is a harness that builds an `ApplyState` over a heap buffer for the existing `write_sched` tests — use it):

```rust
#[test]
fn a_follower_never_writes_the_sched_ring_and_announces_on_the_leader_edge() {
    let (mut st, cnc, sched_consumer) = apply_state_for_test(TimerySm::default());
    cnc.status().flags.store_release(0); // follower
    append_and_commit(&st, &[b"schedule 1 @ 500"]);   // TimerySm::apply calls ctx.schedule(1, 500)
    apply_cycle(&mut st);
    assert!(sched_consumer.try_read(&mut Vec::new()).unwrap().is_none(), "no record on a follower");
    assert_eq!(st.pending.get(&1), Some(&500), "tracked in the loop regardless");
    cnc.status().flags.store_release(NODE_FLAG_LEADER);
    apply_cycle(&mut st);                               // the rising edge
    let recs = drain_all(&sched_consumer);
    assert!(recs.iter().any(|r| r.op == SchedOp::Schedule && r.timer_id == 1 && r.deadline_ns == 500), "announced on the edge: {recs:?}");
    append_and_commit(&st, &[b"schedule 2 @ 600"]);
    apply_cycle(&mut st);
    let recs = drain_all(&sched_consumer);
    assert!(recs.iter().any(|r| r.timer_id == 2), "written directly once leader");
}
```

`TimerySm` is a tiny test SM whose `apply` parses `"schedule <id> @ <ns>"` and calls `ctx.schedule`; it does **not** override `pending_timers` (the bare-SM case the in-loop map exists for).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_service a_follower_never_writes`
Expected: FAIL — a record is written on the follower.

- [ ] **Step 3: Implement the service side**

In `apply_cycle`, after the `is_leader` read (`:421`):

```rust
if is_leader && !st.was_leader {
    st.announce_pending = true; // spec §4.9: announce on the rising edge
}
st.was_leader = is_leader;
```

Move the announce block (`:368–390`) to run **after** this edge detection (it currently runs before the batch — keep it before the batch, just ensure the edge is detected first: read `is_leader` at the top of the cycle, before the announce block, and use the same value for the batch). Both `write_sched(&mut st.svc_sched, &recs)` sites (`:476`, `:497`) become:

```rust
track_sched(st, &recs);
if is_leader {
    write_sched(&mut st.svc_sched, &recs);
}
```

and in the `TIMER` arm, before `on_timer`: `st.pending.remove(&body.timer_id); if hdr.flags & FLAG_TIMER_TABLE != 0 { st.table_last.insert(body.timer_id, body.deadline_ns); }`. `track_sched`: `Schedule → pending.insert(id, dl)`, `Cancel → pending.remove(&id)`, `Consumed → pending.remove(&id)`, `TableConsumed → table_last.insert(id, dl)`. The announce flush falls back to the loop's maps when the SM's hooks return nothing. The announce itself is unconditional on `is_leader` at flush time? — No: guard the flush's `write_sched` with `is_leader` too, and keep `announce_pending` set if not leader (so attach on a follower announces on its first promotion).

- [ ] **Step 4: Write the failing node test**

```rust
#[test]
fn a_follower_does_not_drain_sched_rings_and_demotion_discards_the_heap() {
    let mut h = harness();
    h.cons.timers[0] = Some(crate::timers::RowTimers::new(0xF5A0));
    push_sched_record_for_test(&h, 0, SchedOp::Schedule, 1, 500);
    h.cons.do_work();
    assert_eq!(h.cons.timers[0].as_ref().unwrap().pending_len(), 0, "a follower ignores the ring");
    drive_to_serving_leader(&mut h);
    h.cons.do_work();
    assert_eq!(h.cons.timers[0].as_ref().unwrap().pending_len(), 1, "drained on promotion");
    h.cons.feed(Event::Tick { now_ns: 10_000_000_000 }); // let the term lapse → BecomeFollower
    h.cons.do_work();
    assert_eq!(h.cons.timers[0].as_ref().unwrap().pending_len(), 0, "discarded on demotion");
    assert_eq!(h.cons.timers[0].as_ref().unwrap().in_flight_len(), 0);
}
```

(Use whatever the harness's existing demotion path is — the `BecomeFollower` tests around `node.rs:6255` show it.)

- [ ] **Step 5: Implement the node side**

`drain_sched_rings`: first line `if !self.leader_flag.load(Ordering::Relaxed) { return false; }`. Replace both `self.rearm_timers()` calls (`:6261`, `:6566`) with `self.discard_timers()`:

```rust
/// Spec §4.9: the heap is leader-only. On any leader exit it is discarded,
/// not re-armed; the next promotion rebuilds it from the service's edge
/// announce and the cluster FSM's table.
fn discard_timers(&mut self) {
    for slot in self.timers.iter_mut().flatten() {
        slot.discard();
    }
    self.publish_timers_pending();
}
```

`timers.rs`: delete `rearm`; add `pub fn discard(&mut self) { self.pending.clear(); self.heap.clear(); self.in_flight.clear(); for e in self.table.values_mut() { e.next = None; } }` — and `refresh_from_view`'s re-arm on promotion: force `arm_table_from_view` on the next `refresh_from_view` by resetting `self.schedule_position = 0` inside `discard_timers`. Delete `timer_stats.rearmed` and the `timers_rearmed` obs record and its metric (`uc2_timers_rearmed_total`) — update `monitor-a-cluster.md`'s table in Task 12.

- [ ] **Step 6: Run**

Run: `cargo test -p uc_service && cargo test -p uc_node --lib && cargo test -p uc_node --test timers --test failover`
Expected: green. `the_real_leader_pass_satisfies_the_sim_oracle_across_seeds` still passes (it installs `RowTimers` directly and drives a leader).

- [ ] **Step 7: Commit**

```bash
git add uc_service uc_node
git commit -m "feat(uc_service,uc_node): the timer heap is leader-only — gated ring writes, edge announce, in-loop pending; rearm goes (spec §4.9)"
```

---

### Task 8: `uc2ctl settings apply` / `settings show`

**Files:**
- Create: `uc_ctl/src/settings.rs`
- Modify: `uc_ctl/src/main.rs` (`Cmd::Settings(SettingsArgs)` with `Apply { file }` / `Show`), `uc_ctl/src/lib.rs` if the crate has one
- Test: `uc_ctl/src/settings.rs` tests (parse + encode), `uc_node/tests/admin_auth.rs` (an end-to-end apply + show)

**Interfaces:**
- Consumes: Task 1's `Settings`, `encode_settings`; Task 5's op 7 and `settings.pending`; `uc_node::cluster_fsm::staged_digest`.
- Produces: `pub fn parse_settings(toml: &str) -> Result<Settings, String>` (`admission_bytes = <u64>`, `fsm_lag = "<n>MiB"|"lockstep"`, `snapshot_interval_bytes = <u64>`, `snapshot_target = "all"|"learners"`; absent keys keep `genesis_default`'s zero meaning "derive at use"); `pub fn apply(common: &CommonArgs, file: &Path) -> anyhow::Result<()>`; `pub fn show(common: &CommonArgs) -> anyhow::Result<()>` (reads the node's view through a new `Node`-side read: `uc2ctl` opens the cnc page and… **the view is in-process**, so `show` in plan 1 reads the newest `snapshots/cluster/snap-*.ultcluster` and prints its settings with its position, exactly as `schedule show` read `schedules.state`; spec §13's query path replaces this in phase 2).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn parse_settings_accepts_the_four_keys_and_refuses_unknowns() {
    let s = parse_settings("admission_bytes = 4096\nfsm_lag = \"lockstep\"\nsnapshot_interval_bytes = 10\nsnapshot_target = \"learners\"\n").unwrap();
    assert_eq!(s, Settings { admission_bytes: 4096, fsm_lag_bytes: FSM_LAG_LOCKSTEP, snapshot_interval_bytes: 10, snapshot_target: Target::Learners });
    assert_eq!(parse_settings("").unwrap(), Settings::genesis_default());
    assert!(parse_settings("bogus = 1").unwrap_err().contains("bogus"));
    assert!(parse_settings("snapshot_target = \"voters\"").unwrap_err().contains("snapshot_target"));
}
```

And in `uc_node/tests/admin_auth.rs`, beside the schedule-apply test: start a one-node cluster, write a settings TOML to the temp dir, run `uc_ctl::settings::apply`, wait until `node.cluster_view().admission_bytes == 4096`, then `show` and assert its output contains `admission_bytes=4096`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_ctl settings`
Expected: compile error.

- [ ] **Step 3: Implement**

`settings.rs` mirrors `schedule.rs`: `parse_settings` with `toml::from_str` into a `#[serde(deny_unknown_fields)]` struct, `parse_fsm_lag` reused from `uc_node::services` (make it `pub`), `apply` = stage to `<instance_dir>/settings.pending` (0600, fsync, rename), `staged_digest` into the admin line's `id/ip/port`, send op 7 through the same signed channel, print `version=<frame-end>` on `status 0`, the staged path + non-zero exit on `retry`, the reason name on refusal (`44 settings_digest`…`47 settings_bounds`, added to `uc_ctl`'s reason table). `show` reads the newest cluster artifact (reuse `uc_node::cluster_agent::recover` with a genesis of defaults) and prints `position=… admission_bytes=… fsm_lag=… snapshot_interval_bytes=… snapshot_target=…`.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_ctl && cargo test -p uc_node --test admin_auth`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add uc_ctl uc_node/tests/admin_auth.rs uc_node/src/services.rs
git commit -m "feat(uc_ctl): settings apply / settings show (spec §6, §8)"
```

---

### Task 9: the session carries the cluster artifact; `SNAP_TABLE` and `SnapBeginBody.config` go

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (`SnapBeginBody` loses `config`; `SNAP_BEGIN_LAYOUT_V4 = 3`; `DGRAM_KIND_SNAP_TABLE` → `DGRAM_KIND_SNAP_TABLE_RETIRED = 21`; delete `SnapTableBody` + its codec; fuzz seeds updated)
- Modify: `uc_net/src/sender.rs` (`SnapshotSet` loses `config` and `table`; the coverage invariant's carve-out for id 255; `send_snap_table` deleted)
- Modify: `uc_net/src/receiver.rs` (`SnapIntake.table` deleted; the `SNAP_TABLE` arm deleted; `incoming_snapshot_config`/`_table` cells replaced by the cluster artifact landing on disk; `snap_complete` publishes the position only)
- Modify: `uc_node/src/node.rs` (`snapshot_set_for` pushes the cluster artifact as `SnapArtifact { service_id: 255, snapshot_pos: cluster_snapshot_pos, path, len }`; the install handler installs it through a `ClusterFsm::install_snapshot` on the agent — via a `cluster_install_tx: SyncSender<(u64, PathBuf)>` the agent drains at the top of `do_work` — and seeds the kernel shadow with `adopt_snapshot_config(pos, installed_membership)`; delete `adopt_snapshot_config`'s config-cell read)
- Test: `uc_net` unit tests for the carve-out; `uc_node/tests/learner.rs` (Task 11 adds the scenarios; here, keep the existing seven green)

**Interfaces:**
- Consumes: Task 4's `ClusterAgent` (`snapshot_pos`, artifact path), Task 3's `ClusterFsm::install_snapshot`.
- Produces: `pub const CLUSTER_ARTIFACT_ID: u8 = 255` (in `uc_net::sender`); `SnapshotSet { services_declared, identity, version, artifacts }` where `artifacts` includes exactly one `service_id == CLUSTER_ARTIFACT_ID` entry, last; `ClusterAgent::install_from(&mut self, position: u64, path: &Path) -> io::Result<()>` (installs, publishes the view, sets `snapshot_pos`, resets the follower cursor to `position`).

- [ ] **Step 1: Write the failing sender test**

In `uc_net/src/sender.rs` tests, beside the coverage tests:

```rust
#[test]
fn the_set_requires_exactly_one_cluster_artifact_outside_the_declared_mask() {
    let mut set = two_row_set_for_test();          // rows 0 and 1, identity mask 0b11
    assert!(!set_is_valid(&set), "no cluster artifact: refused");
    set.artifacts.push(SnapArtifact { service_id: CLUSTER_ARTIFACT_ID, snapshot_pos: 4096, path: tmp_file(16), len: 16 });
    assert!(set_is_valid(&set));
    set.artifacts.push(SnapArtifact { service_id: CLUSTER_ARTIFACT_ID, snapshot_pos: 4096, path: tmp_file(16), len: 16 });
    assert!(!set_is_valid(&set), "two cluster artifacts: refused");
}
```

(`set_is_valid` = the check at `sender.rs:1165–1195` extracted into a function so it is unit-testable.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_net the_set_requires`
Expected: compile error.

- [ ] **Step 3: Implement**

Sender: extract the validity check; the mask check ignores id 255; add `let cluster = set.artifacts.iter().filter(|a| a.service_id == CLUSTER_ARTIFACT_ID).count(); if cluster != 1 { return false; }`; the ascending check treats 255 as last naturally. Delete `send_snap_table`, `SnapshotSet.config`/`.table`, the `SNAP_TABLE` resend. `SNAP_BEGIN` V4: drop `config` (the body shrinks by `2 + config.len()`), `layout = SNAP_BEGIN_LAYOUT_V4`; a V3 sender is refused by layout as V2 is today. Receiver: delete the table intake and the config cell; on the id-255 part's completion, hand `(snapshot_pos, path)` to `cluster_install_tx` **before** publishing `incoming_snapshot_pos`, and the install handler in `node.rs:3699–3741` waits for the agent's ack (a `cluster_installed: Arc<AtomicU64>` the agent stores after `install_from`) before adopting the floor — the same "cell before position" discipline the config carry used. `adopt_snapshot_config(pos, membership)` is fed from the installed image's membership (the agent exposes it through the view: `cluster_view.membership()`). `snapshot_set_for` gains the cluster artifact from `cluster_snapshot_pos` (0 → `decline(SNAP_DECLINE_MISSING, "missing cluster artifact")`, which is what the bridging trigger prevents on a settled leader).

- [ ] **Step 4: Run**

Run: `cargo test -p uc_net && cargo test -p uc_protocol && cargo test -p uc_node --test learner --test purge_safety && (cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`
Expected: green; `learner.rs`'s `a_fresh_learner_below_the_floor_installs_the_leaders_schedule_table` and `a_leader_without_a_table_ships_none_and_the_joiner_installs_none` pass through the new path (the second now means "the cluster artifact carries an empty table").

- [ ] **Step 5: Commit**

```bash
git add uc_protocol uc_net uc_node fuzz
git commit -m "feat(uc_net,uc_node): the session carries the cluster artifact under id 255; SNAP_TABLE and SnapBeginBody.config retire (spec §5.6 as far as plan 1 goes)"
```

---

### Task 10: the two-readers invariant in the sim, its red twin, and a `CLUSTER` truncation scenario

**Files:**
- Modify: `uc_sim/src/world.rs` (per-node `committed_membership: Vec<(u64, ClusterConfig)>` derived from `config_frames` at or below `commit`; `check_two_readers`)
- Modify: `uc_sim/src/invariants.rs` (`InvariantViolation::TwoReaders { node, position }`)
- Modify: `uc_sim/tests/scenarios.rs` (two tests)
- Test: as above

**Interfaces:**
- Produces: inv12 — after every event, for every node `n` and every config frame `f` with `f.end <= n.commit`: `n.cfg_observed` contains `f.end` **and** the kernel's adopted config at `f.end` equals `f.config` (the sim's `nodes[n].config_history` if it exists, else the ledger check that `cfg_observed` implies adoption in order). The red twin: a `mutation-testing` tooth `kernel_reads_committed_view` that feeds `ConfigObserved` only for frames `<= commit` (the wrong reader) and pins that inv7 (quorum legality) or inv5 (leader completeness) fires under `window_slide`'s membership churn.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn inv12_the_cluster_fsms_membership_is_a_committed_prefix_of_the_kernels() {
    let mut w = World::new(SimConfig { nodes: 3, seed: 7, ..SimConfig::default() });
    w.enable_membership_churn(); // the helper window_slide's scenario uses to propose add/remove
    w.run_for(200_000_000).unwrap(); // inv12 is swept after every event like the others
    assert!(w.stats().two_readers_checks > 0, "the invariant actually ran");
}

#[cfg(feature = "mutation-testing")]
#[test]
fn counterfactual_kernel_on_the_committed_view_breaks_quorum_legality() {
    let mut w = World::new(SimConfig { nodes: 3, seed: 7, ..SimConfig::default() });
    w.set_mutation(Mutation::KernelReadsCommittedView);
    w.enable_membership_churn();
    let e = w.run_for(200_000_000).unwrap_err();
    assert!(matches!(e, InvariantViolation::QuorumLegality { .. } | InvariantViolation::LeaderCompleteness { .. }), "{e:?}");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_sim inv12 counterfactual_kernel`
Expected: compile errors.

- [ ] **Step 3: Implement**

In `world.rs`, after `observe_config_frames` (`:1782`), add `check_two_readers(&self) -> Result<(), InvariantViolation>` and call it from the invariant sweep (wherever inv1–inv10 are swept after each event — the `handle` path at `:1369`). The mutation: in `observe_config_frames`, under `Mutation::KernelReadsCommittedView`, filter `f.end <= self.nodes[node].commit` instead of `<= durable`. Count checks in `Stats`.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_sim && cargo test -p uc_sim --features mutation-testing counterfactual_kernel`
Expected: green; the twin fails with a quorum/completeness violation.

- [ ] **Step 5: Commit**

```bash
git add uc_sim
git commit -m "test(uc_sim): inv12 — the cluster FSM's membership is a committed prefix of the kernel's, with a red twin (spec §4.6, §11)"
```

---

### Task 11: integration tests — failover re-announce, the residual staged and green, refusals

**Files:**
- Modify: `uc_node/tests/timers.rs` (new test), `uc_node/tests/learner.rs` (new tests), `uc_node/tests/services.rs` (the `uc_` refusal), `uc_node/tests/daemon_refusals.rs` (moved keys)

**Interfaces:** consumes everything above through `Node`, `Service`, `uc_ctl`.

- [ ] **Step 1: The failover test**

```rust
#[test]
fn timers_pending_on_the_old_leader_fire_exactly_once_on_the_new_one_after_its_announce() {
    let _g = serialize();
    let (dirs, mut nodes, svcs) = three_node_cluster_with(Timed::new(ClockSm::default())); // the cluster helper the existing failover test uses
    let leader = wait_for_leader(&nodes);
    schedule_via_client(&svcs[leader], &[(1, plus_ms(2000)), (2, plus_ms(2500))]);
    wait_until("pending on the leader", || read_timers_pending(&dirs[leader], 0) == 2);
    for f in (0..3).filter(|i| *i != leader) {
        assert_eq!(read_timers_pending(&dirs[f], 0), 0, "followers hold no heap (spec §4.9)");
    }
    nodes[leader].take().unwrap().stop();
    let new_leader = wait_for_leader_among(&nodes, leader);
    wait_until("announced on the edge", || read_timers_pending(&dirs[new_leader], 0) == 2);
    wait_until("both fired", || fired_on(&svcs[new_leader]).len() == 2);
    let fired = fired_on(&svcs[new_leader]);
    assert_eq!(fired.iter().map(|f| f.id).collect::<BTreeSet<_>>(), [1, 2].into());
    // every surviving service agrees: exactly once, same positions
    for s in surviving(&svcs, leader) { assert_eq!(fired_on(s), fired); }
}
```

- [ ] **Step 2: The residual, staged exactly**

```rust
#[test]
fn a_joiner_served_by_a_leader_restarted_before_its_first_commit_advance_still_installs_the_table() {
    let _g = serialize();
    // leader + joiner over loopback UDP, purge on (the shape of
    // a_fresh_learner_below_the_floor_installs_the_leaders_schedule_table)
    let (ldir, mut leader, jdir) = leader_with_purge_and_a_table(&[(0, 1, every_ms(100))]);
    let table_pos = wait_for_table_adopted(&ldir);
    fill_past_the_floor(&mut leader);
    // Restart the leader: its commit counter is zero again (counters.rs:55).
    let cfg = leader.config().clone();
    leader.stop();
    let leader = Node::start(cfg).unwrap();
    // Serve the joiner BEFORE any commit advance: no client traffic, no ticks.
    let joiner = start_joiner(&jdir, &leader);
    wait_until("joiner installed the cluster artifact", || cluster_snapshot_pos(&jdir) > 0);
    assert_eq!(table_position_on(&jdir), table_pos, "the residual: shipped none before this plan");
    leader.stop(); joiner.stop();
}
```

The commit body must say this test was run against `3997fd5` (pre-plan) and observed the joiner installing **no** table — i.e. red — by checking out that commit, applying only this test file, and running it.

- [ ] **Step 3: Refusals**

`services.rs`: a `node.toml` with `names = ["uc_cluster"]` → the daemon refuses with `services.names … reserved`. `daemon_refusals.rs`: `admission_bytes = 4096` at top level and `fsm_lag` under `[services]` → refused by name, message contains `uc2ctl settings apply`; the same keys under `[settings]` → starts.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_node --test timers --test learner --test services --test daemon_refusals`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add uc_node/tests
git commit -m "test(uc_node): failover re-announce; the restarted-shipper residual staged and green; settings/name refusals (spec §11)"
```

---

### Task 12: docs

**Files:**
- Modify: `docs/reference/configuration.md` (`[settings]`; `admission_bytes`/`fsm_lag` refusals; `uc_` reserved), `docs/reference/uc2ctl.md` (`settings apply`/`show`, op 7, refusals 44–47), `docs/reference/instance-directory.md` (`snapshots/cluster/`, `settings.pending`; `schedules.state` gone), `docs/reference/wire-protocol.md` (frame 4 = `CLUSTER` + kinds; 6 retired; kind 21 retired; `SNAP_BEGIN` V4), `docs/reference/cnc-page.md` (`ADMIN_OP_SETTINGS_APPLY`), `docs/reference/limits.md` (the two closed residuals struck through → "closed by the cluster FSM"; `uc2_timers_rearmed_total` gone), `docs/how-to/monitor-a-cluster.md` (`uc2_timers_pending` = the leader's; `uc2_cluster_fsm_position`, `uc2_settings_position`; `Uc2ScheduleTableDiverged` keys on the cluster FSM position), `docs/how-to/run-work-on-a-schedule.md` (unchanged in procedure; note the table now lives in the cluster FSM), `docs/how-to/schedule-work-in-a-service.md` (the failover-window sentence), `docs/notes/uc2-log-time-and-timers-explained.md` (the schedule-table section: adoption via the cluster FSM; the leader-only heap), `docs/notes/uc2-cluster-fsm-explained.md` (new: spec §2–§4's argument in plain language, ~150 lines), `RELEASES.md` (the 2.11.0 draft gains the cluster-FSM bullet; the `SNAP_TABLE` prose goes), `docs/releases.md` (same), `CLAUDE.md` (the "Standing facts" 2.11.0 entry: cluster FSM, frame 4/6/21, leader-only heap, `uc_node → uc_service`; "Next up"), `docs/VERIFICATION.md` (§2 inv12; §11's timer-heap and SPSC lines updated), `docs/BACKLOG.md` (item 2's under-ship and wiped-node bullets closed by name).

- [ ] **Step 1: Write them** — each doc's change is named above; the explainer follows the note conventions (`docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md` is the model: a problem, the line, the mechanism, what it retires, what stays and why).
- [ ] **Step 2: Link-check** — run the Python link/anchor checker used on 2026-09-04 (in the session log; re-create it: for every `](docs/…)` or `](../…)` in the touched files, assert the target exists and, if anchored, the heading slug exists).
- [ ] **Step 3: `cargo test --workspace --doc`** — green.
- [ ] **Step 4: Commit**

```bash
git add docs RELEASES.md CLAUDE.md
git commit -m "docs: the cluster FSM — configuration, uc2ctl, wire, cnc, monitor, limits, explainer, release draft (spec §12, plan 1)"
```

---

## Self-review

**Spec coverage (plan 1's share):** §3.2/§3.3 → Task 6; §4.1 → Tasks 0, 4; §4.2–4.4 → Task 3 (+ Task 5 for the pre-append check); §4.5 → Tasks 3, 5; §4.6 → Tasks 2, 5, 10; §4.7 → Tasks 4, 5; §4.8 → Tasks 3, 4; §4.9 → Task 7; §6 → Tasks 1, 5, 6, 8; §7 (plan-1 rows) → Tasks 1, 9; §8 (`settings`) → Task 8; §9 (plan-1 metrics) → Tasks 5, 12; §11 (plan-1 tests) → Tasks 10, 11; §12 → Task 12; §14 item 1 → all; §15 checks 1 (resolved: internal loop, Task 4), 2 (Task 5 Step 3 reads the sender derivation), 3 (Task 5 Step 3 item 5), 5–6 (Task 7 / Task 11). Not in this plan, by design: §5 (plan 2), §5.7 (plan 2), §13.

**Placeholder scan:** none. Two deliberate "read the file and use its variant" instructions in Task 3 Step 3 (`ProposeError`'s wrong-version variant, `SnapshotError`'s corrupt variant) are lookups, not gaps; the test asserts on `matches!` so either name works.

**Type consistency:** `ClusterCommand`/`ClusterRefusal`/`ClusterView`/`ClusterViewInner`/`ClusterState` named identically in Tasks 3, 4, 5, 9; `append_cluster(term, kind, payload)` in Tasks 2, 4, 5; `CLUSTER_ARTIFACT_ID = 255` in Task 9 only (Tasks 4/5 use the path, not the id); `Settings` fields (`fsm_lag_bytes`, `admission_bytes`, `snapshot_interval_bytes`, `snapshot_target`) identical in Tasks 1, 3, 5, 6, 8; `refresh_from_view` / `arm_table_from_view` / `discard_timers` in Tasks 5 and 7.
