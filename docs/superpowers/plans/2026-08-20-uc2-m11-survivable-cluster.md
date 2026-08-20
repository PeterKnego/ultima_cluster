# UC v2.4+ M11 — survivable cluster (backup/restore, quorum-loss, ENOSPC, flag-day) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** an operator can back up a node under load and restore it onto a new host (proven as a CI test, not a documented procedure), force a survivor out of quorum loss with the data-loss window stated, watch the disk-space wall before hitting it and survive `ENOSPC` as an asserted fail-stop, and execute a flag-day upgrade with a measured downtime number.

**Architecture:** four additive pieces over an unchanged consensus core. A `uc2_node::backup` module owns the artifact (a plain directory: `journal/` + `state/` + `snapshots/` + a `MANIFEST`), the **ordered copy rule** (`journal → state → snapshots`, which makes an under-load copy sound by monotonicity — see Task 1's proof note), and an offline `verify` that asserts the coverage invariant instead of trusting the operator. `uc2ctl` grows four offline subcommands (`backup`, `verify-backup`, `restore`, `force-single-member`); the force tool reuses the node's real boot-time config recovery (exported, not reimplemented) and writes a supremacy-safe `ConfigRecord` — quorum-of-1 then falls out of existing election logic. `ENOSPC` is already fail-stop through the journal writer's halt + M10's `agent_failstopped`/exit-1 chain; M11 asserts it end-to-end and adds a `free_disk_bytes` cnc field (reserved band, offset 3840) published by the daemon's outer loop, exported as a metric, and alerted on. The flag-day script composes M9's drain and `uc2ctl status`'s counters into stop-all → verify-equal-durable → upgrade → start-all → verify-serving, with downtime measured.

**Tech Stack:** Rust workspace (edition 2024); `uc2_node` (backup module, recovery export, cnc publisher), `uc2ctl` (new subcommands; gains a `uc2_node` dependency), `uc_protocol`+`uc2_log` (one pinned cnc field), `examples/uc2-crashtest` (multi-process survival + ENOSPC tests), `libc` (statvfs — already a dependency), bash (`scripts/`). **No new crate dependencies anywhere.**

**Spec:** `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §6 (M11), §1 (locked decisions), §3 (non-goals).

## Global Constraints

- **No consensus, wire-protocol, or cnc-layout changes outside the reserved band.** The one new cnc field sits at offset 3840 (free band is 3840..4096), pinned in BOTH `uc_protocol` and `uc2_log` with offset-assertion tests, doc comment updated (`cnc.rs:25`'s stale `3456..4096 reserved` note included).
- **The four polling agents gain no syscalls.** `statvfs` runs in the daemon's outer loop (~1s cadence), never in an agent duty cycle.
- **Force-config is offline-only and refuses loudly.** It takes the instance `flock` first (a running node = refusal), requires `--confirm-cluster <app-id>` to match `--app-id` exactly, refuses a tombstoned or non-member id, and prints the data-loss statement before writing. It never adds tombstones for the excluded peers (they wipe-and-rejoin later as fresh ids/learners; tombstones are permanent by design).
- **`verify` may heal, never hide.** `Journal::open` on the artifact may truncate a torn tail (identical to what restore-boot would do); verify prints what it healed. Everything else about verify is read-only.
- **Restore-scope rule stated wherever restore is documented:** restoring at most a MINORITY of voters against a live majority is safe (log/vote rollback is repaired/neutralized by the healthy quorum); restoring a majority is the quorum-loss procedure's domain and carries its data-loss statement.
- **No new deps**; `clippy --workspace --all-targets -- -D warnings` clean; TDD per task; journal-bearing tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`, scratch under `$HOME`, never `/tmp`.
- **Stage only your own files.** Branch: `uc2/m11-survivable-cluster`. Stage `Cargo.lock` explicitly when a manifest changes (uc2ctl's new dep will change it — name it in the commit).
- **Honest gates:** bars pre-committed in the gate doc before runs; local perf numbers are smoke; the flag-day downtime row is fleet-only; `v2.5.0` tags only after the fleet row passes (separate, user-approved step).
- Pre-existing intermittents (reconfig suite; `sigkill_mid_config_window` ~5%) — rerun in isolation, note, don't chase.

## File Structure

| File | Responsibility |
|---|---|
| `uc2_node/src/backup.rs` (new) | Artifact layout + `MANIFEST`; `backup_instance` (ordered copy), `verify_artifact`, `restore_artifact`. Pure lib, testable in-process. |
| `uc2_node/src/lib.rs` (modify) | `pub mod backup;` + `pub mod recovery;` |
| `uc2_node/src/recovery.rs` (new) | Thin pub wrapper exporting boot's real config recovery for offline tools: `recovered_config(instance_dir, seed) -> (ConfigRecord-shape, durable)`. |
| `uc2_node/src/node.rs` (modify) | `recover_config_record`/`rederive_config` become callable from `recovery.rs` (visibility only — no logic change). |
| `uc2ctl/src/main.rs` + `uc2ctl/Cargo.toml` (modify) | Subcommands `backup`, `verify-backup`, `restore`, `force-single-member`; new `uc2_node` dependency. |
| `uc_protocol/src/v2/cnc.rs` + `uc2_log/src/cnc.rs` (modify) | `CNC_OFF_FREE_DISK_BYTES = 3840` + accessors + offset pins both sides. |
| `uc2_node/src/bin/uc2-node.rs` (modify) | ~1s statvfs → `store_free_disk_bytes`. |
| `uc2_node/src/obs/metrics.rs` (modify) | `uc2_free_disk_bytes` family (omit when 0 = unpublished). |
| `packaging/prometheus/uc2-alerts.yml` (modify) | `Uc2DiskLow` rule. |
| `scripts/m10_alert_fire.sh` + `uc2_node/examples/m10_alerts.rs` (modify) | `Uc2DiskLow` builder/scenario + a YAML↔builders completeness cross-check (closes a latent silent-drop gap). |
| `examples/uc2-crashtest/` (modify) | New feature `survival-tests`; `tests/survival.rs` (backup-under-load→destroy→restore→rejoin; quorum-loss e2e); `tests/enospc.rs` behind `enospc-tests` (needs the fixture). |
| `scripts/enospc_fixture.sh` (new) | Creates/destroys a small loopback ext4 for the ENOSPC test (sudo). |
| `scripts/uc2_flag_day.sh` (new) | Stop-all → verify-equal-durable → upgrade-hook → start-all → verify-serving; measured downtime; `--local` process mode for testing. |
| `.github/workflows/nightly.yml` (modify) | `survival` job (+ ENOSPC fixture step). |
| `docs/how-to/back-up-a-cluster.md`, `docs/how-to/recover-from-quorum-loss.md`, `docs/how-to/upgrade-a-cluster.md` (new); `docs/reference/uc2ctl.md`, `docs/reference/instance-directory.md`, `docs/reference/cnc-page.md`, `docs/how-to/monitor-a-cluster.md`, `docs/how-to/diagnose-a-node.md`, `docs/how-to/README.md`, `docs/ops/uc2-runbook.md` (modify) | Operator docs. |
| `docs/benchmarks/uc2-m11-gate-<date>.md` (new, FIRST) + `uc2_node/examples/m11_gate.rs` (new) | Pre-committed decide rule + local gate harness. |

## As-built anchor map (read these before your task)

| Seam | Where |
|---|---|
| Durable/volatile split | `docs/reference/instance-directory.md` — durable: `journal/`, `state/`, `snapshots/`; volatile: `cnc2.dat`, `log.buf`, `*.ring`, `*.broadcast`, `instance.lock`. Boot recreates every volatile file unconditionally (`node.rs:523-562`), fresh random `instance_id` each boot. |
| Journal layout/recovery | segments `seg-{:020}.log`, last = active (`ultima_journal/src/journal/mod.rs:90-266`); torn tail = end-of-log (healed); sentinel + `truncate.intent` complete interrupted truncates; `Journal::first_seq/last_seq` (`:268-274`); `purge_before` (`:576-621`) never touches the active segment. |
| StableValue copy-safety | two-slot, gen-picked, one slot always intact (`ultima_journal/src/stable_value.rs:236-262`, `pick_slot :304-338`) — an arbitrary-instant file copy is always readable, worst case one generation stale. |
| State files | `state/{vote,term_map,output_progress,snapshot,config}.state` (`uc2_log/src/state.rs:98-104`); `NodeState::{config_record:218, store_config_record:227, snapshot_floor:196}`. |
| Snapshots | `snapshots/snap-<pos>.ultsnap`, tmp+fsync+rename atomic publish, `retain_newest(2)` (`uc2_service/src/snapshots.rs:56-119`); `parse_snap_pos :150`; `SnapshotStore::newest :72-86`. |
| Purge ordering (straddle hazard) | floor persisted durable-first, THEN async best-effort `ArchiveCmd::Purge` (`uc2_node/src/node.rs:2713-2756`); the hole-guard that already exists service-side: `replay_into` requires a covering snapshot when `first_meta > last_applied`, else `ServiceError::SnapshotRequired` (`uc2_service/src/replay.rs:62-114`). |
| Coverage primitives | `Archive::first_base` (`uc2_log/src/archive.rs:191`), `TailReader::first_meta` (`ultima_journal/src/journal/tail_reader.rs:156`), snap position from filename. |
| Config recovery at boot | `recover_config_record` (`uc2_node/src/node.rs:4678-4718`): genesis-seed → T5 revert if `position > durable` → forward `rederive_config` (`:4734-4760`, folds archived CONFIG frames with `wire.version > cur`). Both currently private. Test that pre-seeds a record directly: `uc2_node/tests/learner.rs:412`. |
| Tombstone semantics | permanent, boot refusal `node.rs:585-592`; never set for "not a member" (`uc2_consensus/src/election.rs:372-383`). |
| uc2ctl shape | `Cmd` subcommand enum + `CommonArgs{instance_dir, app_id}` (`uc2ctl/src/main.rs:108-125`); all existing commands need a LIVE node (cnc admin band); `run_status` prints `log: commit= durable= append=` (`:253`). |
| ENOSPC chain | journal writer halt-on-error (`ultima_journal/src/journal/writer.rs:405-445`) → `ArchiveError::Journal` → `archive.do_work(...).expect("archive fail-stop")` (`uc2_node/src/node.rs:1023`) → agent finished-flag → daemon `agent_failstopped` + exit 1 (`uc2-node.rs:134-145`) → systemd `Restart=on-failure`. Service snapshot publish is NOT fail-stop (drops + retries, `uc2_service/src/builder_agent.rs:51-77`) — documented asymmetry, out of scope to change. |
| cnc reserved band | free = 3840..4096; field pattern: `CNC_OFF_ADMISSION_BYTES` (`uc_protocol/src/v2/cnc.rs:126-131`) + const bound assert + pin tests `uc_protocol/src/v2/cnc.rs:430-442` AND `uc2_log/src/cnc.rs:998,1012`; accessor shape `uc2_log/src/cnc.rs:448-477`. Stale module-doc comment at `uc2_log/src/cnc.rs:25` to update. |
| Metrics/alerts extension | `CONTRACT_SERIES` (`uc2_node/src/obs/metrics.rs:34-97`) drives the m10 gate coverage row dynamically; `push_gauge` at `:112`; conditional-omission precedent: `uc2_leader_hint` (omit at `u64::MAX`). `scripts/m10_alert_fire.sh` `RULE_BUILDERS` dict (~:395-409) — NOT diffed against the YAML today (silent-drop gap; Task 5 closes it). |
| Daemon outer loop | `uc2-node.rs:104-202` — 100ms poll, derived events every 10th tick; holds `obs.cnc: Arc<CncPage>`; the natural statvfs home. |
| Quiesce/verify | `Node::stop_draining(deadline) -> DrainOutcome` (`node.rs:1483`); daemon exit codes 0/1/2; `uc2ctl status` works on a STOPPED node's leftover cnc2.dat (file outlives the process; header still valid). Fleet stop/start primitives: `m9_fleet_gate.py` `start_daemon`/`stop_daemon_timed`. |
| Crashtest pattern | `examples/uc2-crashtest`: real-process spawn via `env!("CARGO_BIN_EXE_uc2-crashtest-node")` (`tests/common/mod.rs:51-52`), `Reap` guard, feature-gated (`hard-crash-tests`), run by nightly `crashtest` job (`.github/workflows/nightly.yml`, ubuntu-latest — passwordless sudo + loop devices available). |
| Restore-safety argument | minority-restore rule (Global Constraints): a restored voter's stale vote can only matter in a term where the healthy majority has already moved on; its granted-alone vote certifies nothing at quorum 2-of-3. Restoring a majority = new truth = quorum-loss domain. |

---

### Task 0: Branch

- [ ] `git checkout -b uc2/m11-survivable-cluster` from current `main`. No worktree.

### Task 1: `uc2_node::backup` — artifact, ordered copy, verify

**Files:**
- Create: `uc2_node/src/backup.rs`; Modify: `uc2_node/src/lib.rs` (+`pub mod backup;`)
- Test: in-module unit tests + `uc2_node/tests/backup.rs`

**Interfaces (later tasks build on these exact names):**

```rust
pub struct BackupReport {
    pub journal_first_base: u64,   // 0 = journal unpurged/empty
    pub journal_last_pos: u64,     // recovered durable frontier of the COPY
    pub newest_snapshot: Option<u64>,
    pub snapshot_floor: u64,
    pub healed_torn_tail: bool,
    pub files: usize,
}
pub enum BackupError { /* thiserror: Io, NotAnInstanceDir, ArtifactExists, Hole { first_base: u64, newest_snapshot: Option<u64> }, ManifestMismatch(String), NotAnArtifact */ }

pub fn backup_instance(instance_dir: &Path, out: &Path) -> Result<BackupReport, BackupError>;
pub fn verify_artifact(artifact: &Path) -> Result<BackupReport, BackupError>;
```

**Semantics to implement exactly:**
- `backup_instance`: refuse `out` existing non-empty. Copy **in this order, one directory fully before the next: `journal/` → `state/` → `snapshots/`** (plain `fs::copy` per file; journal segments sorted by name; the active segment last within `journal/`). Then run `verify_artifact` on the copy and write `MANIFEST` (plain `key=value` lines, hand-formatted — no serde_json): `format=uc2-backup-v1`, `journal_first_base`, `journal_last_pos`, `newest_snapshot`, `snapshot_floor`, `healed_torn_tail`, `created_unix_ns`. The **ordering-rule why** goes in the module doc verbatim: *first_base only advances (purge), the newest snapshot position only advances (publish is atomic, retention keeps the newest 2, and purge only runs below a durably persisted floor that some retained snapshot covers) — so a snapshot copied AFTER the journal always covers any purge that happened BEFORE the journal copy. The reverse order can capture a snapshot set from before a purge that the journal copy then reflects: a hole.* The node may be RUNNING during backup: a torn active-segment tail is crash-equivalent and healed at verify; StableValue copies are always readable (two-slot).
- `verify_artifact`: (1) `Journal::open` the copy's `journal/` (may heal a torn tail — set `healed_torn_tail`, this is the ONE permitted mutation); read `first_base` via the archive-style first-record meta (open with `uc2_log::Archive::open` against the artifact dir — it wraps the journal exactly as boot does and exposes `first_base()`/`recovered_position()`); (2) open all five `state/*.state` StableValues read-only (decode = pass; both-slots-corrupt = fail); (3) list `snapshots/snap-*.ultsnap`, parse positions, ignore `*.tmp`; (4) **the coverage invariant**: if `first_base > 0` then `newest_snapshot` must exist and be `>= first_base`, else `BackupError::Hole{..}`; (5) if a `MANIFEST` exists, cross-check its recorded values (a re-verify of a shipped artifact must catch tampering/bitrot at the metadata level) — mismatch = `ManifestMismatch`.
- Restore is Task 2 (kept out of this task so the straddle tests gate cleanly on their own).

- [ ] **Step 1: failing tests** (`uc2_node/tests/backup.rs`; helpers: single node via the `lifecycle.rs` `single_node` pattern, purge-enabled config where noted):

```rust
#[test]
fn backup_of_a_stopped_node_verifies_and_reports_positions() { /* run single node + service, submit N, stop_draining, backup_instance, assert report.journal_last_pos > 0, verify_artifact(out) ok, MANIFEST parses */ }

#[test]
fn backup_of_a_running_node_under_load_verifies() { /* node+service+submit loop thread; backup_instance mid-load; assert Ok; healed_torn_tail may be either way — assert only verify passes */ }

#[test]
fn a_wrong_order_copy_across_a_purge_is_detected_as_a_hole() {
    // Anti-vacuity for the whole task. Drive a purge-enabled single node until
    // at least one purge lands (service publishes snapshots; PurgePolicy::BelowSnapshot).
    // Then build a BROKEN artifact by hand: copy snapshots/ FIRST, then force
    // another snapshot+purge cycle (submit more, wait for archive_first_base to
    // advance past the copied snapshots' newest pos), then copy journal/ + state/.
    // verify_artifact must return Err(BackupError::Hole{..}).
}

#[test]
fn ordered_backup_never_produces_a_hole_under_purge_churn() { /* loop >=5 purge cycles with backup_instance each cycle; every artifact verifies */ }

#[test]
fn manifest_tamper_is_caught() { /* flip journal_first_base in MANIFEST; verify -> ManifestMismatch */ }
```

- [ ] **Step 2:** `cargo test -p uc2_node --test backup` — FAIL (module absent).
- [ ] **Step 3:** implement `backup.rs` per the semantics block.
- [ ] **Step 4:** tests PASS; **Step 5:** workspace clippy; **Step 6:** commit `feat(backup): ordered-copy artifact + offline verify — the straddle rule asserted, not documented`.

### Task 2: `uc2ctl backup / verify-backup / restore`

**Files:** Modify `uc2ctl/src/main.rs`, `uc2ctl/Cargo.toml` (+`uc2_node = { path = "../uc2_node" }`), `Cargo.lock`; Create `uc2_node/src/backup.rs::restore_artifact` (same module); Test: extend `uc2_node/tests/backup.rs` + a uc2ctl-level smoke via `assert_cmd`? — **no new deps**: test the lib function; the CLI wiring is covered by the m11 gate's script row.

**Interfaces:**
```rust
pub fn restore_artifact(artifact: &Path, instance_dir: &Path) -> Result<BackupReport, BackupError>;
```
- `restore_artifact`: run `verify_artifact` first; refuse an `instance_dir` containing any of `journal/`, `state/`, `snapshots/` non-empty (volatile leftovers are fine — boot recreates them); copy the three dirs in; do NOT copy `MANIFEST` into the instance dir (leave beside? no — artifact stays intact, instance dir gets only the three dirs). The restored node's first boot does everything else (fresh cnc/instance_id, config recovery, rejoin).
- CLI: `uc2ctl backup --instance-dir D --out DIR`, `uc2ctl verify-backup DIR`, `uc2ctl restore DIR --instance-dir D` — clap variants in the existing `Cmd` enum style; print the `BackupReport` fields one per line (`journal_first_base=..` etc.); nonzero exit on error. These are OFFLINE commands: unlike every existing `uc2ctl` command they do not touch the cnc admin band — say so in their doc comments and in `docs/reference/uc2ctl.md` (Task 8).

- [ ] **Step 1: failing tests** — `restore_roundtrip_boots_and_serves` (backup a stopped single node, restore into a FRESH dir, `Node::start_with_socket` on it + service attach, assert it elects and a submitted+committed value from before the backup reads back) and `restore_refuses_a_dirty_target`.
- [ ] **Step 2:** FAIL; **Step 3:** implement + CLI; **Step 4:** PASS (`cargo test -p uc2_node --test backup`), `cargo run -p uc2ctl -- backup --help` renders; **Step 5:** clippy; **Step 6:** commit `feat(uc2ctl): offline backup / verify-backup / restore` (name `Cargo.lock` in the body).

### Task 3: survival crashtest — backup under load, destroy the host, restore, rejoin (CI)

**Files:** Modify `examples/uc2-crashtest/Cargo.toml` (`[features] survival-tests = []`); Create `examples/uc2-crashtest/tests/survival.rs`; Modify `.github/workflows/nightly.yml` (new `survival` job: `cargo test -p uc2-crashtest --features survival-tests`, timeout 30).

The spec's acceptance sentence, literally: *"A node is backed up under load, its host destroyed, a new host restored from the backup alone, and it rejoins and converges — as a CI test."* Multi-process, real binaries, the `tests/common/mod.rs` spawn pattern.

- [ ] **Step 1: failing test** (`#[cfg(feature = "survival-tests")]`):

```rust
#[test]
fn a_follower_backed_up_under_load_restores_onto_a_new_host_and_converges() {
    // 3 real node+service processes + a client submitting throughout.
    // 1. Identify a FOLLOWER; run uc2_node::backup::backup_instance against its
    //    LIVE instance dir (under load — this is the under-load half of the bar).
    // 2. SIGKILL that follower's processes and rm -rf its instance dir ("host destroyed").
    // 3. "New host": a different fresh dir path; restore_artifact; start node+service there
    //    with the same node id and the same bind addr (the config's members are seed-only
    //    — the durable config in the artifact owns membership).
    // 4. Converge: poll until the restored node's durable reaches the leader's commit
    //    (bounded 30s), then assert every response the client received BEFORE the kill
    //    reads back (linearizable read via the leader; the restored node must also
    //    serve a snapshot read at, or past, its pre-kill applied state).
    // 5. Client keeps running the whole time: zero acked-write loss cluster-wide.
}
```

- [ ] **Step 2:** FAIL (feature/file absent); **Step 3:** implement (reuse `common/mod.rs`; the backup call is in-process — the test links `uc2_node`); **Step 4:** `cargo test -p uc2-crashtest --features survival-tests` PASS 3 consecutive runs; **Step 5:** clippy + nightly.yml job added; **Step 6:** commit `test(survival): backup under load -> host destroyed -> restore -> rejoin, as CI`.

### Task 4: quorum-loss `force-single-member`

**Files:** Create `uc2_node/src/recovery.rs`; Modify `uc2_node/src/node.rs` (visibility: `recover_config_record` + `rederive_config` become `pub(crate)` callable from `recovery.rs`; NO logic change), `uc2_node/src/lib.rs`, `uc2ctl/src/main.rs`; Test: `uc2_node/tests/force_config.rs` + a quorum-loss e2e in `examples/uc2-crashtest/tests/survival.rs`.

**Interfaces:**
```rust
// uc2_node/src/recovery.rs
pub struct RecoveredConfig { pub version: u64, pub voters: Vec<(u32, SocketAddr)>, pub learners: Vec<(u32, SocketAddr)>, pub tombstones: Vec<u32>, pub durable: u64 }
pub fn recovered_config(instance_dir: &Path) -> io::Result<RecoveredConfig>;   // flock + Archive::open + NodeState::open + the REAL recover_config_record, read-only intent
pub struct ForceReport { pub old_version: u64, pub new_version: u64, pub durable: u64, pub dropped_peers: Vec<u32> }
pub fn force_single_member(instance_dir: &Path, node_id: u32) -> io::Result<ForceReport>;
```
- `force_single_member` mechanics (from the anchor map, every rule is load-bearing): take the exclusive flock (held = refuse: "a node is running"); recover the effective config exactly as boot would; refuse if `node_id` is tombstoned or not in voters∪learners; write via `NodeState::store_config_record`:
  `ConfigRecord { position: durable, prev_position: durable, config: forced, prev: forced }` where `forced = StoredConfig { version: recovered.version + 1, voters: [the survivor with its existing addr], learners: [], tombstones: recovered.tombstones /* UNCHANGED — Global Constraints */ }`. `position = durable` survives the T5 revert; `version = recovered+1` beats every archived CONFIG frame (rederive already folded all frames ≤ durable into `recovered.version`, and nothing exists above durable). Vote/term state untouched — quorum-of-1 falls out of `ElectionSm` reading the adopted config.
- CLI: `uc2ctl force-single-member --instance-dir D --app-id A --node-id N --confirm-cluster A2` — refuse unless `A2 == A`; before writing, print the data-loss statement (exact copy, tested): `forcing node {N} to a single-member cluster at durable position {durable}: any write acknowledged by the old quorum but not held in this node's journal is LOST; peers {ids} are dropped from the config and must be wiped and rejoined as fresh learners.` Then the `ForceReport`.
- e2e (in `survival.rs`, feature `survival-tests`): 3 nodes + client; SIGKILL two (leader among them) permanently; assert cluster stalled (no commits for 2s); stop survivor; `force_single_member`; restart survivor → elects itself, serves; **assert every client-acked write whose position ≤ the survivor's pre-force durable reads back** (the honest bar: acked-above-durable MAY be lost and the test prints how many were); then wipe one dead peer's dir, rejoin it as a fresh learner via `uc2ctl add-learner` + promote — back to 2 voters, converged.

- [ ] **Step 1: failing unit tests** (`force_config.rs`): `force_refuses_a_running_node` (hold the flock via a live `Node`), `force_refuses_a_tombstoned_id`, `force_refuses_confirm_mismatch` (CLI-level check lives in uc2ctl; test the lib refusals here + version/position math: build a 3-voter `config.state` at version 7 via `NodeState::store_config_record` (the `learner.rs:412` pattern), force, re-open with `recovered_config`, assert `version == 8`, sole voter, tombstones unchanged), `forced_single_node_boots_elects_and_serves` (in-process `Node::start_with_socket` after force → can_serve within 5s, submit+read).
- [ ] **Step 2:** FAIL; **Step 3:** implement (visibility change + recovery.rs + backup of `config.state`? no — StableValue's two slots already keep the previous generation; note that in the doc comment); **Step 4:** PASS + the survival.rs e2e; **Step 5:** clippy; **Step 6:** commit `feat(recovery): offline force-single-member — quorum-loss with the loss window stated`.

### Task 5: `free_disk_bytes` — cnc field, metric, alert, harness cross-check

**Files:** Modify `uc_protocol/src/v2/cnc.rs`, `uc2_log/src/cnc.rs`, `uc2_node/src/bin/uc2-node.rs`, `uc2_node/src/obs/metrics.rs`, `packaging/prometheus/uc2-alerts.yml`, `uc2_node/examples/m10_alerts.rs`, `scripts/m10_alert_fire.sh`, `docs/reference/cnc-page.md`.

- cnc: `pub const CNC_OFF_FREE_DISK_BYTES: usize = 3840;` (+ bound assert + "next free: 3904" doc note + fix the stale `3456..4096 reserved` module comments in both crates); `CncPage::{free_disk_bytes(), store_free_disk_bytes(v)}` in the `admission_bytes` accessor shape; pin tests in BOTH crates (`assert_eq!(CNC_OFF_FREE_DISK_BYTES, 3840)` + roundtrip).
- daemon: in the ~1s derived-events pass, `statvfs` the instance dir (via `libc::statvfs`; `f_bavail * f_frsize`) → `store_free_disk_bytes`; on statvfs error store nothing (leave last value) and rate-limited-warn once.
- metric: `uc2_free_disk_bytes` gauge, **omitted when the cnc field reads 0** (0 = never published — library/in-process users without the daemon; the `uc2_leader_hint` omission precedent). `CONTRACT_SERIES` gains the family — note in the entry's comment that the m10 gate coverage row reads it dynamically, and the m10_gate coverage scenario runs the real daemon? It does NOT (in-process ObsServer, no daemon loop) — so **the coverage row would fail on an omitted family**. Resolution, pinned here: `m10_gate`'s coverage cluster (and `m10_alerts`' scenarios) set the field explicitly via `sources.cnc.store_free_disk_bytes(...)` before scraping — one line in each harness, honest (the field is real, the writer just lives in the daemon).
- alert:
```yaml
  - alert: Uc2DiskLow
    expr: uc2_free_disk_bytes < 4 * uc2_journal_segment_bytes
    for: 2m
    labels: { severity: warning }
    annotations: { summary: "{{ $labels.instance }} has less than 4 journal segments of free disk — the archive fail-stops at ENOSPC; purge or grow the disk" }
```
- harness: `build_Uc2DiskLow` (synthetic-disclosed: heap-page sources with `store_free_disk_bytes(small)` + `uc2_journal_segment_bytes` real — the genuinely-broken-disk state is Task 6's fixture territory, disclosed) + **the completeness cross-check**: the Python step now parses `alert:` names out of the shipped YAML and fails loudly (`exit 1`, naming them) if any has no `RULE_BUILDERS` entry — closing the silent-drop gap found in exploration.

- [ ] **Step 1: failing tests** — offset pins both crates; encoder unit tests (`free_disk_omitted_when_zero`, `free_disk_present_when_stored`); lifecycle daemon test extension: the metrics-enabled daemon's scrape contains `uc2_free_disk_bytes` with a plausible (>0) value.
- [ ] **Step 2:** FAIL; **Step 3:** implement; **Step 4:** `cargo test -p uc_protocol -p uc2_log && cargo test -p uc2_node --lib --test lifecycle` + `scripts/m10_alert_fire.sh` 14/14 PASS (the cross-check counts too: break it deliberately once — delete the builder, expect loud FAIL — then restore; note the RED evidence in the report); **Step 5:** clippy + `promtool check rules`; **Step 6:** commit `feat(obs): free_disk_bytes cnc field + Uc2DiskLow — see the wall before ENOSPC hits it`.

### Task 6: ENOSPC asserted end-to-end

**Files:** Create `scripts/enospc_fixture.sh`, `examples/uc2-crashtest/tests/enospc.rs`; Modify `examples/uc2-crashtest/Cargo.toml` (`enospc-tests = []`), `.github/workflows/nightly.yml` (fixture step + `--features enospc-tests` in the survival job).

- `enospc_fixture.sh`: `create <dir> <size-mb>` → `fallocate` an image under `$HOME/.cache/uc2-enospc/`, `mkfs.ext4 -q`, `sudo mount -o loop` at `<dir>`, `sudo chown $USER`; `destroy <dir>` unmounts + removes. Probe `sudo -n true` first; exit 2 with a message if sudo is unavailable (test skips).
- `tests/enospc.rs` (gated on the feature AND `UC2_ENOSPC_DIR` env — unset = `eprintln!("skipped: ...")` + return, the elle-style env-gate): 3-node cluster, node 0's instance dir under `$UC2_ENOSPC_DIR` (64 MB fs, small journal segments via config so it fills in seconds), client load until node 0's daemon exits: assert **exit code 1**, stderr contains `agent_failstopped` and `archive fail-stop` and the io error text; assert the OTHER two keep committing (client keeps getting acks — leader was/moves off node 0); then `destroy`+`create` a bigger fs (or delete a filler file), restore the durable dirs? — no: **restart the same instance dir after freeing space** (the fixture pre-fills with a deletable ballast file so "space returned" = `rm ballast`): node 0 rejoins and converges. That is the spec's row verbatim: *fails in an asserted way, the cluster keeps serving, recovers when space is returned.*
- [ ] Steps: failing test → fixture → implement → `UC2_ENOSPC_DIR=... cargo test -p uc2-crashtest --features enospc-tests` PASS locally (this box has sudo; if it does not, report BLOCKED with the probe output) → clippy → commit `test(enospc): the wall is asserted — fail-stop named, cluster survives, recovery on space return`.

### Task 7: flag-day upgrade script

**Files:** Create `scripts/uc2_flag_day.sh`; Test: `--local` mode exercised by `uc2_node/examples/m11_gate.rs` (Task 9) — but the script itself lands here with a manual local run recorded.

Contract (env/args, elle_check.sh style, `set -euo pipefail`):
```
uc2_flag_day.sh --hosts "user@h1,user@h2,user@h3" --ssh-key K \
                --unit uc2-node --uc2ctl /usr/local/bin/uc2ctl \
                --instance-dir /srv/uc2/nX --app-id myapp \
                --upgrade-cmd 'sudo install -m755 /tmp/uc2-node.new /usr/local/bin/uc2-node'
```
Steps, each printed with a timestamp: (0) preflight: every host reachable, every node healthy (`uc2ctl status` parses, same `config: version=`); (1) operator confirms client traffic is stopped (`--yes-traffic-stopped` flag required — the script cannot verify what it cannot see; say so); (2) stop ALL nodes (`systemctl stop`, parallel), which drains (M9); (3) verify: `uc2ctl status` against each STOPPED node's leftover cnc — all `durable=` equal, else abort loudly (restart everything, exit 1 — the un-upgrade path must be stated); (4) run `--upgrade-cmd` on every host; (5) start all; (6) wait until every node reports the same `config: version=` and exactly one `leader=true can_serve=true` (bounded 60s); (7) print `DOWNTIME: <last-stop → first-can_serve> s` and exit 0. `--local` mode: `--hosts local:<cfg1,cfg2,cfg3>` drives plain `uc2-node --config` processes with SIGTERM instead of systemctl — same verify logic, for the gate harness and for dev boxes.
- [ ] Steps: write script → shellcheck-clean (if `shellcheck` present; else note) → manual `--local` 3-node run on this box, output pasted into the task report (downtime printed; not a bar — dev box) → docs cross-ref left to Task 8 → commit `feat(scripts): uc2_flag_day.sh — stop-all, verify-equal-durable, upgrade, start-all, measured downtime`.

### Task 8: operator docs

**Files:** Create `docs/how-to/back-up-a-cluster.md`, `docs/how-to/recover-from-quorum-loss.md`, `docs/how-to/upgrade-a-cluster.md`; Modify `docs/reference/uc2ctl.md` (four new commands + offline-vs-admin-band distinction), `docs/reference/instance-directory.md` (backup pointer), `docs/reference/cnc-page.md` (3840 row — done in Task 5, verify), `docs/how-to/monitor-a-cluster.md` (`uc2_free_disk_bytes` + `Uc2DiskLow` + the snapshot-publish-not-fail-stop asymmetry note), `docs/how-to/diagnose-a-node.md` ("is the disk about to fill" section), `docs/how-to/README.md` + `docs/ops/uc2-runbook.md` indexes ("Surviving failures" group).

Content requirements (house voice, failure-first): back-up doc carries the ordering rule AND its why, the under-load soundness argument, the verify-before-trust rule, and the **minority-restore rule** verbatim from Global Constraints; quorum-loss doc opens with the data-loss statement, walks force-single-member, then the wipe-and-rejoin path for repaired peers, and names what it is NOT (a substitute for backups; a thing to script around); upgrade doc wraps the script, states the flag-day property (mixed clusters stall commits — cite releases.md 0.5.0), and the traffic-stop prerequisite.
- [ ] Steps: write → every link resolves (`grep` targets exist) → `cargo test -p uc2_node the_packaged_example_config_is_valid` still green (untouched, sanity) → commit `docs: back up, survive quorum loss, upgrade — the M11 operator surface`.

### Task 9: the M11 gate

**Files:** Create `docs/benchmarks/uc2-m11-gate-<today>.md` (FIRST, its own commit), then `uc2_node/examples/m11_gate.rs`.

Pre-committed bar (write verbatim into the doc, then implement):

| # | Row | Bar | Adjudicated |
|---|---|---|---|
| 1 | Backup/restore CI | the Task 3 crashtest green **3/3 consecutive runs**, plus the straddle anti-vacuity test (wrong-order copy detected as `Hole`) green | local |
| 2 | Quorum-loss e2e | Task 4's e2e green 3/3: forced survivor serves; every acked write ≤ pre-force durable reads back; repaired peer rejoins as fresh learner | local |
| 3 | ENOSPC | Task 6's test green: named fail-stop, exit 1, cluster keeps committing, recovery on space return | local (fixture) |
| 4 | Disk-low observability | `uc2_free_disk_bytes` in a live daemon scrape; `m10_alert_fire.sh` **14/14** incl. `Uc2DiskLow`; the YAML↔builders cross-check trips on a deliberately removed builder (anti-vacuity, restored after) | local |
| 5 | Flag-day downtime | full stop-verify-upgrade-start on a real fleet: **downtime (last node stopped → cluster serving) ≤ 60 s**, binaries pre-staged (transfer excluded), zero acked loss across the flag day | **fleet only** |

`m11_gate.rs`: subcommands `all`/`survival`/`quorum`/`enospc`/`alerts`/`flagday-smoke` — rows 1-2 shell out to the crashtest suites (`cargo test -p uc2-crashtest --features survival-tests` x3), row 3 runs iff `UC2_ENOSPC_DIR` set (else prints SKIPPED and the row FAILs `all` unless `--allow-skip-enospc` — the gate run must run it), row 4 runs the harness + the deliberate-removal trip, `flagday-smoke` runs `uc2_flag_day.sh --local` and prints the downtime UNGATED. Bar printed first, per-row verdicts printed as completed (M10's lesson), `RESULT: PASS|FAIL`, exit codes 0/1.
- [ ] Steps: gate doc commit alone (`docs(bench): pre-commit the M11 survivable-cluster gate decide rule`) → harness → `cargo run -p uc2_node --release --example m11_gate -- all` rows 1-4 PASS → full local proof stack (`cargo test -p uc2_node`, workspace clippy) → commit `feat(bench): m11_gate — survival, quorum-loss, enospc, disk-low rows` → **STOP: the fleet flag-day row is a separate, user-approved step** (extend the m9/m10 fleet tooling then; not in this branch).

---

## Out of scope (named so they are not re-litigated)

- **Leadership transfer, mixed-version operation, admission-hold/quiesce-ingress API** (spec §1/§3; the backup design deliberately needs no quiesce).
- **Escalating the service snapshot-publish path to fail-stop** — documented asymmetry (Task 8), a candidate M12 follow-up.
- **Tombstoning force-dropped peers** — deliberately NOT done (Global Constraints; repaired peers rejoin as fresh ids).
- **The fleet flag-day run + v2.5.0 tagging** — post-branch, user-approved.

## Self-review (at plan-writing time)

- **Spec §6 coverage:** backup vs durable/volatile split → T1/T2; purge-straddle ordering rule asserted by the tool → T1 (anti-vacuity test); quorum-loss awkward-by-design with confirmation + loss window → T4; ENOSPC fail-stop asserted + warning ahead via cnc counter + M10 alert → T5/T6; flag-day script + measured number under a pre-committed bar → T7/T9 row 5; acceptance gate's three sentences → T3 (CI test), T6, T9 row 5. Gap check: none found.
- **Placeholder scan:** the test bodies in T1/T3/T4 use commented choreography where the exact wait-loops mirror named existing patterns (`lifecycle.rs single_node`, crashtest `common/mod.rs`) — the patterns are cited, the assertions are concrete. No TBDs.
- **Type consistency:** `BackupReport`/`BackupError` defined T1, consumed T2/T3; `recovered_config`/`force_single_member` defined T4 and consumed by its own e2e; `CNC_OFF_FREE_DISK_BYTES`/`store_free_disk_bytes` defined T5, consumed by T5's harness edits and T9 row 4; feature names `survival-tests`/`enospc-tests` consistent T3/T4/T6/T9.
- **Known risks:** T5's coverage-row interaction (omitted-when-0 family) is resolved in-plan (harnesses store the field); T6 depends on local sudo — BLOCKED protocol named; uc2ctl's new `uc2_node` dependency is a build-weight tradeoff accepted for a single admin binary.

## Execution log

(append rulings here as tasks complete — M7-M10 pattern)
