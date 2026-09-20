# Diff Replay Harness Implementation Plan (deliverable 1, plan A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `uc_diffreplay` — the crate that replays one corpus (snapshot + log span) through two FSM builds, diffs everything they do, attributes each difference, and confirms the set against a declared intent — plus the `project()` hook and the `examples/kv` integration that make it usable.

**Architecture:** A new publishable workspace crate `uc_diffreplay` with a library (corpus, trace, in-process replay driver, diff, attribution, confirm, report) and a CLI `uc2-diffreplay` that orchestrates two *app binaries* — each app binary embeds the driver behind a `replay` subcommand, so the harness never links the app's FSM and works across versions and languages. `uc_service` gains one provided trait method, `SnapshotStateMachine::project()`. A real node is spawned only by the `reconstruction` tests, which demonstrate the spec's §2.3 counterfactual through the genuine attach path.

**Tech Stack:** Rust 2024 / MSRV 1.89, `serde` + `serde_json` (traces, reports), `toml` (declarations), `clap` 4 (CLI), `anyhow`, `tempfile`; all already in `Cargo.lock`. No new external dependencies.

**Spec:** `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` (rev 5) — §4 (the loop and surfaces), §5.8–5.9 (projection, regression corpus), §6 (harness). This plan implements §11 items 3, 4, 4a, 4b, 12. Items 5–7, 13 are plan B (platform); item 8 is plan C; items 1, 2, 9 are plan D.

**Spec erratum this plan records (add to the spec's §6.3 as "as built" when this lands):** the `upgrade` and `determinism` modes do **not** run a 1-node cluster. `uc_service/src/replay.rs`'s module doc states *replay never publishes* — a service reconstructing from a journal writes no responses to the egress ring — so a real node would yield an empty output surface. Instead the harness crate ships an **in-process replay driver** (`uc_diffreplay::drive`) that walks the journal span with `uc_journal::TailReader`, calls `RawStateMachine::apply` with the recorded frame headers, and captures every surface; the app's own service binary embeds it behind a `replay` subcommand. The harness still treats the app as black-box (it only runs binaries), still needs no linking of two versions, and is still language-agnostic (a non-Rust app implements the same trace format). Only `reconstruction`'s real-attach test spawns a node.

**Spec erratum 2 (§4.2 ids surface):** `ApplyCtx::ids()` returns a fresh `IdGen` per call and exposes no mint count (`uc_service/src/traits.rs:131`). The "ids minted per apply" surface is therefore **observed indirectly** through state and responses, not captured directly. Capturing it directly needs an SDK change (a count on `ApplyCtx`) and is deferred to plan B or later.

**Spec erratum 3 (§6.1 corpus trimming):** §6.1's trimming is not implemented: bytes below P are retained because `Origin::Genesis` (reconstruction mode) replays from 0, and `state/` is retained so a corpus stays a valid `uc2ctl verify-backup` artifact. A trimmed export that drops the sub-P journal and `state/` is a follow-up for a corpus that will only ever run in `upgrade`/`determinism` mode.

## Global Constraints

- `rust-version = "1.89"`, `edition = "2024"` (root `Cargo.toml` `[workspace.package]`); the new crate inherits both via `.workspace = true`.
- `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` must pass after every task (CI's first steps).
- Tests write scratch **only** under `env!("CARGO_TARGET_TMPDIR")` via `tempfile` — never `/tmp` (CLAUDE.md "Local scratch").
- **No change to any apply hot loop.** The only `uc_service` edits are a provided trait method with a default body and three wrapper forwards; none is on the per-frame path.
- The new crate is `uc_diffreplay`, **publishable** (`publish` not set to false), versioned in lockstep at `2.12.0` like the other 13 — a decision recorded here; the release-order note in `docs/how-to/cut-a-release.md` §6 is updated in plan D.
- Canonical projection rule (spec §5.8): sorted keys, stable ordering, one record per line, no timestamps, no addresses. Two SMs with identical logical state must produce byte-identical projections.
- All positions are absolute log byte offsets; a corpus origin **P is an exclusive frontier** (the image covers every frame strictly below P; `uc_service/src/traits.rs` ~L391 `install_snapshot` doc).

---

## File structure

```
uc_diffreplay/                       NEW workspace member, publishable
  Cargo.toml
  README.md                          what it is, the four commands, the trace format
  src/lib.rs                         pub mod corpus, trace, drive, diff, attribute, confirm, report
  src/corpus.rs                      CorpusManifest, Corpus::{open, export, export_around}
  src/trace.rs                       Trace, Entry, EntryKind, Sched (+ serde_json I/O)
  src/drive.rs                       drive::<S>(), Origin, run_replay_cli::<S>(), project_artifact::<S>()
  src/diff.rs                        Profile, Divergence, Surface, LineDiff, diff()
  src/attribute.rs                   Declaration (TOML), Attribution, attribute()
  src/confirm.rs                     Verdict, confirm()
  src/report.rs                      Report, Report::{write_json, write_text, failed}
  src/bin/uc2-diffreplay.rs          CLI: corpus export | upgrade | determinism | reconstruction
  tests/common/mod.rs                in-process single-node helpers (copied from uc_service/tests/snapshot_build.rs)
  tests/corpus.rs                    manifest roundtrip; export from a real instance dir
  tests/drive.rs                     drive RegisterSm over a real corpus
  tests/diff_attribute_confirm.rs    pure-function tests over hand-built traces
  tests/upgrade_e2e.rs               CLI end to end with two RegisterSm binaries
  tests/reconstruction.rs            §2.3 demonstrated: driver-level AND real-attach
uc_service/src/traits.rs             + SnapshotStateMachine::project() (provided); TimerEvent::new
uc_service/src/tagged.rs             forward project()
uc_service/src/timed.rs              forward project() + pending-timer lines
uc_service/src/session.rs            forward project() + session-table lines
uc_lincheck/src/register.rs          project() for RegisterSm
uc_lincheck/src/bin/register-replay.rs   NEW test binary: RegisterSm behind `replay`/`project` (for the harness's own e2e)
examples/kv/src/lib.rs               project() for KvSm
examples/kv/src/bin/kv-service.rs    `replay` and `project` subcommands
examples/kv/Cargo.toml               + uc_diffreplay
examples/kv/tests/projection.rs      canonical-projection tests
examples/kv/tests/corpora/README.md  the regression-corpus convention
Cargo.toml                           + "uc_diffreplay" in members
docs/how-to/diff-replay.md           minimal how-to (plan D expands)
```

---

### Task 1: Crate scaffold

**Files:**
- Create: `uc_diffreplay/Cargo.toml`, `uc_diffreplay/src/lib.rs`, `uc_diffreplay/README.md`
- Modify: `Cargo.toml:3` (workspace `members`)

**Interfaces:**
- Produces: the crate `uc_diffreplay` with empty modules `corpus`, `trace`, `drive`, `diff`, `attribute`, `confirm`, `report`.

- [ ] **Step 1: Add the workspace member**

In the root `Cargo.toml`, line 3, add `"uc_diffreplay"` after `"uc_gateway"` in `members`.

- [ ] **Step 2: Write `uc_diffreplay/Cargo.toml`**

```toml
[package]
name = "uc_diffreplay"
description = "Diff replay for ultima_cluster state machines: replay one corpus (snapshot + log span) through two FSM builds, diff everything they do, attribute each difference, confirm against declared intent"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true
homepage.workspace = true
rust-version.workspace = true
keywords.workspace = true
categories.workspace = true

[[bin]]
name = "uc2-diffreplay"
path = "src/bin/uc2-diffreplay.rs"

[dependencies]
uc_service = { path = "../uc_service", version = "2.12.0" }
uc_protocol = { path = "../uc_protocol", version = "2.12.0" }
uc_journal = { workspace = true }
uc_node = { path = "../uc_node", version = "2.12.0" }
serde = { workspace = true }
serde_json = "1"
toml = "0.8"
anyhow = { workspace = true }
clap = { workspace = true }

[dev-dependencies]
uc_lincheck = { path = "../uc_lincheck" }
uc_client = { path = "../uc_client" }
tempfile = { workspace = true }
```

If `toml` in `Cargo.lock` is not `0.8`, use the version already locked (`grep -A1 'name = "toml"' Cargo.lock`) so no second copy is pulled.

- [ ] **Step 3: Write `src/lib.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Diff replay (spec `2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §4, §6):
//! replay the same input — a snapshot plus a log span — on different FSMs,
//! then compare the differences in their snapshots, outputs and logs.
//!
//! The pieces, in the order the loop runs them:
//! - [`corpus`] — the input: a backup artifact plus a `CORPUS` manifest.
//! - [`drive`] — the in-process replay driver an app's binary embeds.
//! - [`trace`] — what one run captured, on every surface.
//! - [`diff`] — two traces → a divergence profile.
//! - [`attribute`] — profile × declaration → each divergence named to an arm, or unexplained.
//! - [`confirm`] — attributed profile × declaration → verdicts.
//! - [`report`] — the attributed diff report, JSON and text.

pub mod attribute;
pub mod confirm;
pub mod corpus;
pub mod diff;
pub mod drive;
pub mod report;
pub mod trace;
```

Create each module file with just the license header and a one-line `//!` doc for now.

- [ ] **Step 4: Write `README.md`**

```markdown
# uc_diffreplay — diff replay for UC state machines

Replay the same input (a snapshot + a log span) on different FSMs, then
compare the differences in their snapshots, outputs and logs.

    uc2-diffreplay corpus export --instance-dir D --app-id A --row 0 --around POS --out CORPUS
    uc2-diffreplay upgrade       --corpus CORPUS --old ./svc-v1 --new ./svc-v2 --declare intent.toml --report r.json
    uc2-diffreplay determinism   --corpus CORPUS --bin ./svc --report r.json
    uc2-diffreplay reconstruction --corpus CORPUS --bin ./svc --report r.json

An app binary takes part by embedding the driver behind a `replay`
subcommand — see `examples/kv/src/bin/kv-service.rs`.

Spec: `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`.
```

- [ ] **Step 5: Build**

Run: `cargo build -p uc_diffreplay`
Expected: compiles (empty modules; the bin file does not exist yet — create `src/bin/uc2-diffreplay.rs` containing only `fn main() {}` so the manifest's `[[bin]]` resolves).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml uc_diffreplay
git commit -m "uc_diffreplay: crate scaffold (diff replay harness, plan A task 1)"
```

---

### Task 2: Corpus manifest and export

**Files:**
- Create: `uc_diffreplay/src/corpus.rs`, `uc_diffreplay/tests/common/mod.rs`, `uc_diffreplay/tests/corpus.rs`

**Interfaces:**
- Consumes: `uc_node::backup::backup_instance(instance_dir, out) -> Result<BackupReport, BackupError>` (`uc_node/src/backup.rs:537`); its artifact layout is `out/journal/` (`backup.rs:320`) and `out/snapshots/<row>/snap-<pos>.ultsnap`; `uc_service::snapshots::SnapshotStore::{open, newest, path_for}` (`uc_service/src/snapshots.rs:149+`).
- Produces:
  ```rust
  pub struct CorpusManifest { pub app_id: String, pub row: u8, pub origin: u64, pub end: u64, pub version: u32 }
  pub struct Corpus { pub dir: PathBuf, pub manifest: CorpusManifest }
  impl Corpus {
      pub fn open(dir: &Path) -> anyhow::Result<Corpus>;
      pub fn export(instance_dir: &Path, app_id: &str, row: u8, origin: u64, end: u64, version: u32, out: &Path) -> anyhow::Result<Corpus>;
      pub fn export_around(instance_dir: &Path, app_id: &str, row: u8, pos: u64, version: u32, out: &Path) -> anyhow::Result<Corpus>;
      pub fn journal_dir(&self) -> PathBuf;   // dir/journal
      pub fn artifact(&self) -> PathBuf;      // dir/snapshots/<row>/snap-<origin>.ultsnap
  }
  ```
  `version` is the FSM version that built the artifact (spec §6.1 — carried by hand until plan B's envelope stamp; `0` = unknown).

- [ ] **Step 1: Write the test helpers (`tests/common/mod.rs`)**

Copy these from `uc_service/tests/snapshot_build.rs:30-88` and adapt the imports; they are duplicated per test binary by convention in this repo.

```rust
#![allow(dead_code)]
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use uc_lincheck::RegisterSm;
use uc_node::{Node, NodeConfig};
use uc_service::StateMachine;

pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("diffreplay-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap()
}

pub fn node_config(dir: &Path, app_id: &str, fsm: &str) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: uc_node::FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(fsm),
    }
}

pub fn start_single_node(dir: &Path, app_id: &str, fsm: &str) -> Node {
    Node::start(node_config(dir, app_id, fsm)).unwrap()
}

pub fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// `uc2ctl snapshot` in process: command an instant, return its position P.
pub fn command_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match node.command_snapshot(false) {
            Ok(p) => return p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot refused: {e}"),
        }
    }
}

pub fn register_name() -> &'static str {
    <RegisterSm as StateMachine>::NAME
}
```

If `NodeConfig` no longer has a field named above (issue #37 removes `max_payload`), delete that line — match the struct at `uc_node/src/node.rs:182`.

- [ ] **Step 2: Write the failing manifest-roundtrip test (`tests/corpus.rs`)**

```rust
mod common;
use uc_diffreplay::corpus::{Corpus, CorpusManifest};

#[test]
fn manifest_roundtrips_through_a_corpus_dir() {
    let dir = common::tempdir();
    let m = CorpusManifest { app_id: "app".into(), row: 3, origin: 4096, end: 8192, version: 0x0102_0003 };
    std::fs::create_dir_all(dir.path().join("journal")).unwrap();
    m.write(dir.path()).unwrap();
    let c = Corpus::open(dir.path()).unwrap();
    assert_eq!(c.manifest, m);
    assert_eq!(c.journal_dir(), dir.path().join("journal"));
    assert_eq!(c.artifact(), dir.path().join("snapshots").join("3").join("snap-4096.ultsnap"));
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p uc_diffreplay --test corpus manifest_roundtrips -- --nocapture`
Expected: FAIL — `CorpusManifest` / `Corpus` not found.

- [ ] **Step 4: Implement `corpus.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! A corpus is a `uc2ctl backup` artifact plus a `CORPUS` manifest naming the
//! row, the origin P (exclusive frontier — the artifact to install), the end
//! Q, and the FSM version that built the artifact (spec §6.1).

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

const MANIFEST: &str = "CORPUS";
const FORMAT: &str = "uc2-corpus-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorpusManifest {
    pub app_id: String,
    pub row: u8,
    pub origin: u64,
    pub end: u64,
    pub version: u32,
}

impl CorpusManifest {
    /// `key=value` lines, the same hand-formatted style `uc_node::backup`'s
    /// `MANIFEST` uses (no serde on this file: it must stay greppable).
    pub fn write(&self, dir: &Path) -> anyhow::Result<()> {
        let text = format!(
            "format={FORMAT}\napp_id={}\nrow={}\norigin={}\nend={}\nversion={:#x}\n",
            self.app_id, self.row, self.origin, self.end, self.version
        );
        std::fs::write(dir.join(MANIFEST), text).context("write CORPUS")
    }

    pub fn read(dir: &Path) -> anyhow::Result<CorpusManifest> {
        let text = std::fs::read_to_string(dir.join(MANIFEST)).context("read CORPUS")?;
        let mut app_id = None;
        let (mut row, mut origin, mut end, mut version) = (None, None, None, None);
        for line in text.lines() {
            let Some((k, v)) = line.split_once('=') else { continue };
            match k {
                "format" if v != FORMAT => bail!("CORPUS format {v:?}, expected {FORMAT:?}"),
                "app_id" => app_id = Some(v.to_string()),
                "row" => row = Some(v.parse()?),
                "origin" => origin = Some(v.parse()?),
                "end" => end = Some(v.parse()?),
                "version" => version = Some(u32::from_str_radix(v.trim_start_matches("0x"), 16)?),
                _ => {}
            }
        }
        Ok(CorpusManifest {
            app_id: app_id.context("CORPUS: app_id")?,
            row: row.context("CORPUS: row")?,
            origin: origin.context("CORPUS: origin")?,
            end: end.context("CORPUS: end")?,
            version: version.context("CORPUS: version")?,
        })
    }
}

#[derive(Debug)]
pub struct Corpus {
    pub dir: PathBuf,
    pub manifest: CorpusManifest,
}

impl Corpus {
    pub fn open(dir: &Path) -> anyhow::Result<Corpus> {
        let manifest = CorpusManifest::read(dir)?;
        let c = Corpus { dir: dir.to_path_buf(), manifest };
        if !c.journal_dir().is_dir() {
            bail!("corpus has no journal/ at {}", c.journal_dir().display());
        }
        Ok(c)
    }

    pub fn journal_dir(&self) -> PathBuf {
        self.dir.join("journal")
    }

    pub fn artifact(&self) -> PathBuf {
        self.dir
            .join("snapshots")
            .join(self.manifest.row.to_string())
            .join(format!("snap-{}.ultsnap", self.manifest.origin))
    }

    /// `uc2ctl backup` into `out`, then stamp the CORPUS manifest. The node
    /// must be stopped (the backup verbs are offline — `uc_node::backup`).
    pub fn export(
        instance_dir: &Path,
        app_id: &str,
        row: u8,
        origin: u64,
        end: u64,
        version: u32,
        out: &Path,
    ) -> anyhow::Result<Corpus> {
        uc_node::backup::backup_instance(instance_dir, out)
            .map_err(|e| anyhow::anyhow!("backup: {e}"))?;
        let m = CorpusManifest { app_id: app_id.into(), row, origin, end, version };
        m.write(out)?;
        let c = Corpus::open(out)?;
        if !c.artifact().is_file() {
            bail!("no artifact for row {row} at origin {origin}: {}", c.artifact().display());
        }
        Ok(c)
    }

    /// Spec §6.1 `--around <pos>`: the newest complete artifact at or below
    /// `pos` is the origin; the end is `pos` itself (the caller widens it if
    /// the trigger needs a tail).
    pub fn export_around(
        instance_dir: &Path,
        app_id: &str,
        row: u8,
        pos: u64,
        version: u32,
        out: &Path,
    ) -> anyhow::Result<Corpus> {
        let store = uc_service::snapshots::SnapshotStore::open(instance_dir, row)?;
        let (origin, _) = store
            .newest(pos)?
            .with_context(|| format!("no complete artifact for row {row} at or below {pos}"))?;
        Corpus::export(instance_dir, app_id, row, origin, pos, version, out)
    }
}
```

If `uc_service::snapshots` is not `pub`, make the module public in `uc_service/src/lib.rs` (`pub mod snapshots;`) — it already exports free functions used by `uc_node`, so check its current visibility first.

- [ ] **Step 5: Run the roundtrip test**

Run: `cargo test -p uc_diffreplay --test corpus manifest_roundtrips`
Expected: PASS.

- [ ] **Step 6: Write the failing export test**

Append to `tests/corpus.rs`:

```rust
use uc_client::Client;
use uc_lincheck::{Cmd, CmdResp, RegisterSm};
use uc_service::{ServiceBuilder, ServiceConfig};

/// Drive a single node with RegisterSm: N writes, an instant at P, M more
/// writes. Returns (P, Q) with Q = the position after the last write.
pub fn build_register_history(dir: &std::path::Path, app_id: &str, n: u64, m: u64) -> (u64, u64) {
    let node = common::start_single_node(dir, app_id, common::register_name());
    let cfg = ServiceConfig::new(dir.to_path_buf(), app_id.to_string());
    let svc = ServiceBuilder::new(cfg, RegisterSm::default()).start_with_snapshots().unwrap();
    let client = Client::connect(dir, app_id).unwrap();
    for v in 0..n {
        let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap();
    }
    let p = common::command_instant(&node);
    // The instant completes when the row's artifact appears.
    let art = dir.join("snapshots").join("0").join(format!("snap-{p}.ultsnap"));
    common::wait_until(|| art.is_file());
    for v in n..n + m {
        let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap();
    }
    let q = node.snapshot_instant_position().max(p); // placeholder for "applied frontier" — replaced below
    client.shutdown();
    svc.stop();
    node.stop();
    (p, q)
}

#[test]
fn export_captures_artifact_and_journal() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _q) = build_register_history(inst.path(), "corp", 5, 5);
    // Q: use the journal's own extent — the end is "everything archived".
    let c = Corpus::export(inst.path(), "corp", 0, p, u64::MAX, 0, out.path()).unwrap();
    assert!(c.artifact().is_file());
    assert!(c.journal_dir().is_dir());
    assert_eq!(c.manifest.origin, p);
}
```

Replace the `q` line: `Node` has no "applied frontier" accessor in this plan's scope. Use `u64::MAX` as the end (the driver stops at the journal's last frame) — delete the `let q = …` line and return `(p, u64::MAX)`. The `_q` binding in the test then goes away too.

- [ ] **Step 7: Run it**

Run: `cargo test -p uc_diffreplay --test corpus export_captures -- --nocapture`
Expected: PASS. If `backup_instance` refuses a directory that still holds lock files from the just-stopped node, add `std::thread::sleep(Duration::from_millis(100))` after `node.stop()` and note it in the helper doc — the backup verbs are documented offline-only.

- [ ] **Step 8: fmt, clippy, commit**

```bash
cargo fmt --all && cargo clippy -p uc_diffreplay --all-targets -- -D warnings
git add uc_diffreplay
git commit -m "uc_diffreplay: corpus manifest + export from a backup artifact (task 2)"
```

---

### Task 3: Trace format

**Files:**
- Create: `uc_diffreplay/src/trace.rs`
- Test: unit tests in the same file

**Interfaces:**
- Produces:
  ```rust
  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Trace { pub row: u8, pub version: u32, pub origin: u64, pub end: u64,
                     pub projection_at_origin: Option<String>, pub projection_at_end: Option<String>,
                     pub entries: Vec<Entry> }
  pub struct Entry { pub pos: u64, pub kind: EntryKind, pub tag: Vec<u8>, pub response: Vec<u8>, pub sched: Vec<Sched> }
  pub enum EntryKind { Message, Timer { id: u64, deadline_ns: u64, table: bool } }
  pub struct Sched { pub op: String, pub id: u64, pub deadline_ns: u64 }
  impl Trace { pub fn write_json(&self, w: impl Write) -> anyhow::Result<()>; pub fn read_json(r: impl Read) -> anyhow::Result<Trace>; }
  ```
  `tag` = the first 4 bytes of the command payload (fewer if shorter) — the app-defined discriminant the attribution pass keys on (spec §4.5); opaque to the harness.

- [ ] **Step 1: Write the failing roundtrip test (in `trace.rs`, `#[cfg(test)]`)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn json_roundtrip_preserves_every_field() {
        let t = Trace {
            row: 0, version: 1, origin: 32, end: 96,
            projection_at_origin: Some("value=None\n".into()),
            projection_at_end: Some("value=Some(7)\n".into()),
            entries: vec![
                Entry { pos: 32, kind: EntryKind::Message, tag: vec![0, 7], response: vec![0], sched: vec![] },
                Entry { pos: 64, kind: EntryKind::Timer { id: 9, deadline_ns: 5, table: false }, tag: vec![],
                        response: vec![], sched: vec![Sched { op: "schedule".into(), id: 9, deadline_ns: 50 }] },
            ],
        };
        let mut buf = Vec::new();
        t.write_json(&mut buf).unwrap();
        assert_eq!(Trace::read_json(&buf[..]).unwrap(), t);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p uc_diffreplay --lib trace`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! What one replay captured, on every surface of spec §4.2 that the driver
//! can see directly: per-position responses and schedule records, and the
//! state projection at the origin and at the end.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Trace {
    pub row: u8,
    pub version: u32,
    pub origin: u64,
    pub end: u64,
    pub projection_at_origin: Option<String>,
    pub projection_at_end: Option<String>,
    pub entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub pos: u64,
    pub kind: EntryKind,
    /// First 4 bytes of the command payload — the app's discriminant.
    pub tag: Vec<u8>,
    pub response: Vec<u8>,
    pub sched: Vec<Sched>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    Message,
    Timer { id: u64, deadline_ns: u64, table: bool },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Sched {
    pub op: String,
    pub id: u64,
    pub deadline_ns: u64,
}

impl Trace {
    pub fn write_json(&self, w: impl Write) -> anyhow::Result<()> {
        Ok(serde_json::to_writer_pretty(w, self)?)
    }
    pub fn read_json(r: impl Read) -> anyhow::Result<Trace> {
        Ok(serde_json::from_reader(r)?)
    }
}
```

- [ ] **Step 4: Run it**

Run: `cargo test -p uc_diffreplay --lib trace`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_diffreplay/src/trace.rs
git commit -m "uc_diffreplay: trace format (task 3)"
```

---

### Task 4: `project()` on `SnapshotStateMachine`, forwarded by the wrappers

**Files:**
- Modify: `uc_service/src/traits.rs` (the `SnapshotStateMachine` trait, ~L372; `TimerEvent`, ~L30)
- Modify: `uc_service/src/tagged.rs:44-60`, `uc_service/src/timed.rs`, `uc_service/src/session.rs` (their `SnapshotStateMachine` impls)
- Modify: `uc_lincheck/src/register.rs:79-105` (`impl SnapshotStateMachine for RegisterSm`)
- Test: `uc_service/tests/projection.rs` (new)

**Interfaces:**
- Produces on `SnapshotStateMachine`:
  ```rust
  fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> { /* default: Err(Codec("project() not implemented")) */ }
  ```
  and `TimerEvent::new(id: u64, deadline_ns: u64, table: bool) -> TimerEvent` (needed by the driver in Task 5; `TimerEvent`'s fields are constructed in-crate today).

- [ ] **Step 1: Write the failing test (`uc_service/tests/projection.rs`)**

```rust
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine, StateMachine, Tagged, TimerEvent};

#[derive(Default)]
struct Plain(u64);
impl StateMachine for Plain {
    const NAME: &'static str = "plain";
    type Command = u64; type Response = u64; type Query = (); type QueryResponse = u64;
    fn apply(&mut self, _c: &mut ApplyCtx, cmd: u64) -> u64 { self.0 += cmd; self.0 }
    fn query(&self, _q: ()) -> u64 { self.0 }
    fn last_applied(&self) -> Option<u64> { None }
}
impl SnapshotStateMachine for Plain {
    type SnapshotHandle = u64;
    fn freeze(&self) -> Result<(u64, u64), SnapshotError> { Ok((self.0, 0)) }
    fn stream_snapshot(h: u64, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> { dst.write_all(&h.to_le_bytes())?; Ok(()) }
    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> {
        let mut b = [0u8; 8]; src.read_exact(&mut b)?; self.0 = u64::from_le_bytes(b); Ok(position)
    }
}

#[derive(Default)]
struct Projecting(Plain);
impl StateMachine for Projecting {
    const NAME: &'static str = "projecting";
    type Command = u64; type Response = u64; type Query = (); type QueryResponse = u64;
    fn apply(&mut self, c: &mut ApplyCtx, cmd: u64) -> u64 { self.0.apply(c, cmd) }
    fn query(&self, q: ()) -> u64 { self.0.query(q) }
    fn last_applied(&self) -> Option<u64> { None }
}
impl SnapshotStateMachine for Projecting {
    type SnapshotHandle = u64;
    fn freeze(&self) -> Result<(u64, u64), SnapshotError> { self.0.freeze() }
    fn stream_snapshot(h: u64, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> { Plain::stream_snapshot(h, dst) }
    fn install_snapshot(&mut self, p: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> { self.0.install_snapshot(p, src) }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> { writeln!(out, "total={}", self.0 .0)?; Ok(()) }
}

#[test]
fn default_project_is_a_named_refusal() {
    let mut out = Vec::new();
    let err = Plain::default().project(&mut out).unwrap_err();
    assert!(err.to_string().contains("project() not implemented"), "{err}");
}

#[test]
fn an_implemented_project_renders_and_tagged_forwards_it() {
    let mut sm = Tagged::<2, Projecting>::default();
    RawStateMachine::apply(&mut sm, &mut ApplyCtx::new(64, <Tagged<2, Projecting> as RawStateMachine>::IDENTITY), &bincode::serde::encode_to_vec(5u64, bincode::config::standard()).unwrap(), &mut Vec::new());
    let mut out = Vec::new();
    sm.project(&mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "total=5\n");
}

#[test]
fn timer_event_new_is_public() {
    let ev = TimerEvent::new(7, 100, true);
    assert_eq!(ev.id, 7);
    assert_eq!(ev.deadline_ns, 100);
    assert!(ev.table);
}
```

`uc_service`'s dev-dependencies already include `bincode` (it is a regular dependency).

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p uc_service --test projection`
Expected: FAIL — no method `project`, no `TimerEvent::new`.

- [ ] **Step 3: Add the provided method to the trait**

In `uc_service/src/traits.rs`, inside `pub trait SnapshotStateMachine`, after `install_snapshot`:

```rust
    /// Render the current state as **canonical, diffable text** — one record
    /// per line, sorted, stable, no timestamps or addresses — so two builds'
    /// states can be compared across a version boundary where the image bytes
    /// cannot be (diff replay spec §4.4, §5.8). Two SMs with identical logical
    /// state MUST produce byte-identical projections; the harness's
    /// `determinism` mode checks exactly that. O(state); never called on the
    /// apply path. Default: a named refusal, so a bare SM keeps working and a
    /// harness gets a clear answer.
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        let _ = out;
        Err(SnapshotError::Codec(
            "project() not implemented by this state machine (diff replay spec §5.8)".into(),
        ))
    }
```

And on `TimerEvent` (find `pub struct TimerEvent` near the top of `traits.rs`):

```rust
impl TimerEvent {
    /// Construct an event — for harnesses that dispatch recorded `TIMER`
    /// frames themselves (`uc_diffreplay::drive`). Production delivery
    /// constructs these in-crate.
    pub fn new(id: u64, deadline_ns: u64, table: bool) -> TimerEvent {
        TimerEvent { id, deadline_ns, table }
    }
}
```

If the fields are already `pub`, still add `new` (the driver uses it either way).

- [ ] **Step 4: Forward in the three wrappers**

`uc_service/src/tagged.rs`, inside `impl … SnapshotStateMachine for Tagged<ROW, S>`:

```rust
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        self.0.project(out)
    }
```

`uc_service/src/timed.rs`, in its `SnapshotStateMachine` impl — the pending timer set is replicated state (spec §5.6), so it is part of the projection, after the inner's lines:

```rust
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        self.inner.project(out)?;
        let mut pending: Vec<(u64, u64)> = self.pending_timers();
        pending.sort_unstable();
        for (id, deadline) in pending {
            writeln!(out, "timer id={id} deadline_ns={deadline}")?;
        }
        Ok(())
    }
```

Use whatever field name `Timed` gives its inner SM (read `timed.rs`; it forwards `apply` to it). `pending_timers()` is the `RawStateMachine` hook `Timed` already overrides.

`uc_service/src/session.rs`, in its `SnapshotStateMachine` impl — the session table is replicated (`SessionConfig` enforced at install):

```rust
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        self.inner.project(out)?;
        let mut rows: Vec<(u32, u32)> = self.sessions_for_projection();
        rows.sort_unstable();
        for (client, seq) in rows {
            writeln!(out, "session client={client} seq={seq}")?;
        }
        Ok(())
    }
```

Add a private `fn sessions_for_projection(&self) -> Vec<(u32, u32)>` on `Sessioned<S>` that copies `(client_id, last_seq)` out of whatever map `session.rs` keeps (read the struct; it holds a per-client last sequence for the FRESH/REPLAYED/EXPIRED tag). Do not expose the map itself.

- [ ] **Step 5: Implement `project()` for `RegisterSm`**

`uc_lincheck/src/register.rs`, inside `impl SnapshotStateMachine for RegisterSm`:

```rust
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
        writeln!(out, "value={:?}", self.value)?;
        writeln!(out, "last_applied={:?}", self.last_applied)?;
        Ok(())
    }
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p uc_service --test projection && cargo test -p uc_lincheck`
Expected: PASS, PASS.

- [ ] **Step 7: Workspace check and commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_service
git add uc_service/src/traits.rs uc_service/src/tagged.rs uc_service/src/timed.rs uc_service/src/session.rs uc_service/tests/projection.rs uc_lincheck/src/register.rs
git commit -m "uc_service: SnapshotStateMachine::project() — canonical state projection hook; wrappers forward (task 4)"
```

`cargo test -p uc_service` in full matters here: `Timed` and `Sessioned` have their own suites (`tests/timed.rs`, `tests/session.rs`) and the forwards must not disturb them.

---

### Task 5: The in-process replay driver

**Files:**
- Create: `uc_diffreplay/src/drive.rs`
- Test: `uc_diffreplay/tests/drive.rs` (uses `tests/corpus.rs`'s `build_register_history` — move that fn into `tests/common/mod.rs` first)

**Interfaces:**
- Consumes: `uc_journal::TailReader::{open, scan_from}` (`uc_journal/src/journal/tail_reader.rs:66,103` — `scan_from(start_meta, visit: FnMut(seq: u64, meta: u64, block: &[u8]) -> bool)`; `meta` is the block's base stream position, `block` is the raw frames concatenated as they lay in the ring); `uc_protocol::v2::frame::{read_header, align_frame_len, read_timer_body, HEADER_LEN, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER, FRAME_TYPE_PADDING, FLAG_TIMER_TABLE}`; `uc_service::snapshots::verify_snapshot_envelope(src, expected)` (`snapshots.rs:113`); `ApplyCtx::{new, with_time, with_term, take_sched_records_for_test}`; `uc_protocol::v2::ipc::{SchedRecord, SchedOp}`.
- Produces:
  ```rust
  pub enum Origin { Artifact, Genesis }
  pub fn drive<S: SnapshotStateMachine>(sm: S, corpus: &Corpus, origin: Origin) -> anyhow::Result<Trace>;
  pub fn project_artifact<S: SnapshotStateMachine>(sm: S, artifact: &Path, position: u64) -> anyhow::Result<String>;
  pub fn run_replay_cli<S: SnapshotStateMachine>(sm: S, corpus_dir: &Path, out: &Path, from_genesis: bool) -> anyhow::Result<()>;
  ```
  `Origin::Genesis` skips the install and scans from 0 — the `reconstruction` mode's counterfactual path (spec §2.3).

- [ ] **Step 1: Move `build_register_history` into `tests/common/mod.rs`** (it now returns `(p, u64::MAX)`), and make `tests/corpus.rs` call `common::build_register_history`.

- [ ] **Step 2: Write the failing driver test (`tests/drive.rs`)**

```rust
mod common;
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::drive::{Origin, drive};
use uc_diffreplay::trace::EntryKind;
use uc_lincheck::RegisterSm;

#[test]
fn driver_replays_the_span_above_the_origin_and_projects_both_ends() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "drv", 5, 3);
    let corpus = Corpus::export(inst.path(), "drv", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(RegisterSm::default(), &corpus, Origin::Artifact).unwrap();

    // Origin projection: the artifact at P holds writes 0..5 → value=Some(4).
    assert_eq!(t.projection_at_origin.as_deref(), Some("value=Some(4)\nlast_applied=Some(_)\n").map(|_| t.projection_at_origin.as_deref().unwrap()));
    assert!(t.projection_at_origin.as_deref().unwrap().starts_with("value=Some(4)\n"));
    // Exactly the 3 writes above P were applied, in order, each acked.
    let msgs: Vec<_> = t.entries.iter().filter(|e| matches!(e.kind, EntryKind::Message)).collect();
    assert_eq!(msgs.len(), 3, "{:?}", t.entries);
    assert!(msgs.windows(2).all(|w| w[0].pos < w[1].pos));
    assert!(t.projection_at_end.as_deref().unwrap().starts_with("value=Some(7)\n"));
    assert_eq!(t.origin, p);
}

#[test]
fn genesis_origin_replays_everything_from_zero() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "gen", 5, 3);
    let corpus = Corpus::export(inst.path(), "gen", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(RegisterSm::default(), &corpus, Origin::Genesis).unwrap();
    assert_eq!(t.projection_at_origin, None, "genesis has nothing installed to project");
    let msgs = t.entries.iter().filter(|e| matches!(e.kind, EntryKind::Message)).count();
    assert_eq!(msgs, 8);
    assert!(t.projection_at_end.as_deref().unwrap().starts_with("value=Some(7)\n"));
}
```

Simplify the first projection assertion to just the `starts_with` line — the `last_applied` value is a position the test does not know. Delete the `assert_eq!(t.projection_at_origin.as_deref(), …)` line.

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p uc_diffreplay --test drive`
Expected: FAIL — `drive` not defined.

- [ ] **Step 4: Implement `drive.rs`**

**Read `uc_service/src/replay.rs` in full before writing the block walk**, in particular how it advances past a `PADDING` frame inside an archived block and how it treats a frame whose end exceeds the block. The walk below mirrors `uc_log/src/reader.rs:127-153` (`FrameIter::next`) applied to a journal block; if `replay.rs` handles padding differently for archived blocks, follow `replay.rs`.

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The in-process replay driver (spec §6, plan A erratum): install the
//! corpus's artifact at P (or start from genesis), walk the journal span,
//! call `apply`/`on_timer` with the RECORDED headers — position, time_ns,
//! term — and capture every surface into a [`Trace`]. Deterministic by
//! construction: the same corpus through the same build yields the same
//! trace, which is what the `determinism` mode checks.
//!
//! Mirrors the dispatch rules of `uc_service/src/replay.rs` (MESSAGE above
//! the SM's frontier → apply; TIMER naming this identity → on_timer; anything
//! else skipped) without the live-rejoin machinery — there is no node here.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, bail};
use uc_journal::TailReader;
use uc_protocol::v2::frame::{
    FLAG_TIMER_TABLE, FRAME_TYPE_MESSAGE, FRAME_TYPE_PADDING, FRAME_TYPE_TIMER, HEADER_LEN,
    align_frame_len, read_header, read_timer_body,
};
use uc_protocol::v2::ipc::SchedOp;
use uc_service::snapshots::verify_snapshot_envelope;
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine, TimerEvent};

use crate::corpus::Corpus;
use crate::trace::{Entry, EntryKind, Sched, Trace};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Install the corpus artifact at P, replay `[P, end)`. The correct path.
    Artifact,
    /// Skip the install, replay `[0, end)` — the §2.3 counterfactual.
    Genesis,
}

fn project_string<S: SnapshotStateMachine>(sm: &S) -> anyhow::Result<String> {
    let mut out = Vec::new();
    sm.project(&mut out).map_err(|e| anyhow::anyhow!("project(): {e}"))?;
    String::from_utf8(out).context("projection is not UTF-8")
}

/// Install `artifact` (tagged `position`) into `sm`: strip and check the
/// framework envelope, then hand the payload to the SM. Returns the position
/// the SM reported, which must equal `position`.
fn install<S: SnapshotStateMachine>(sm: &mut S, artifact: &Path, position: u64) -> anyhow::Result<u64> {
    let mut f = BufReader::new(File::open(artifact).with_context(|| artifact.display().to_string())?);
    verify_snapshot_envelope(&mut f, position).map_err(|e| anyhow::anyhow!("envelope: {e}"))?;
    let got = sm
        .install_snapshot(position, &mut f)
        .map_err(|e| anyhow::anyhow!("install_snapshot({position}): {e}"))?;
    if got != position {
        bail!("install_snapshot returned {got}, expected {position}");
    }
    Ok(got)
}

pub fn project_artifact<S: SnapshotStateMachine>(mut sm: S, artifact: &Path, position: u64) -> anyhow::Result<String> {
    install(&mut sm, artifact, position)?;
    project_string(&sm)
}

pub fn drive<S: SnapshotStateMachine>(mut sm: S, corpus: &Corpus, origin: Origin) -> anyhow::Result<Trace> {
    let m = &corpus.manifest;
    let (start, projection_at_origin) = match origin {
        Origin::Artifact => {
            install(&mut sm, &corpus.artifact(), m.origin)?;
            (m.origin, Some(project_string(&sm)?))
        }
        Origin::Genesis => (0, None),
    };

    let reader = TailReader::open(&corpus.journal_dir())?;
    let identity = S::IDENTITY;
    let mut entries = Vec::new();
    let mut resp = Vec::with_capacity(256);
    let end = m.end;

    reader.scan_from(start, |_seq, base, block| {
        let mut off = 0usize;
        while off + HEADER_LEN <= block.len() {
            let hdr = read_header(&block[off..]);
            let len = hdr.length as usize;
            if len < HEADER_LEN || off + len > block.len() {
                // Torn or trailing bytes: the archive's own scan would have
                // ended here; so do we.
                return false;
            }
            let aligned = align_frame_len(len);
            let pos = base + off as u64;
            if pos >= end {
                return false;
            }
            if hdr.frame_type == FRAME_TYPE_PADDING {
                off += aligned;
                continue;
            }
            let payload = &block[off + HEADER_LEN..off + len];
            // The apply loop's idempotency guard, verbatim (uc_service/src/apply.rs:559).
            let above = Some(pos) > sm.last_applied();
            match hdr.frame_type {
                FRAME_TYPE_MESSAGE if above => {
                    let mut ctx = ApplyCtx::new(pos, identity)
                        .with_time(hdr.time_ns)
                        .with_term(hdr.leadership_term_id);
                    resp.clear();
                    sm.apply(&mut ctx, payload, &mut resp);
                    entries.push(Entry {
                        pos,
                        kind: EntryKind::Message,
                        tag: payload[..payload.len().min(4)].to_vec(),
                        response: resp.clone(),
                        sched: sched_of(&mut ctx),
                    });
                }
                FRAME_TYPE_TIMER if above => {
                    if let Some(body) = read_timer_body(payload) {
                        if body.identity_hash == identity.hash() {
                            let table = hdr.flags & FLAG_TIMER_TABLE != 0;
                            let mut ctx = ApplyCtx::new(pos, identity)
                                .with_time(hdr.time_ns)
                                .with_term(hdr.leadership_term_id);
                            sm.on_timer(&mut ctx, TimerEvent::new(body.timer_id, body.deadline_ns, table));
                            entries.push(Entry {
                                pos,
                                kind: EntryKind::Timer { id: body.timer_id, deadline_ns: body.deadline_ns, table },
                                tag: Vec::new(),
                                response: Vec::new(),
                                sched: sched_of(&mut ctx),
                            });
                        }
                    }
                }
                _ => {}
            }
            off += aligned;
        }
        true
    })?;

    Ok(Trace {
        row: m.row,
        version: S::VERSION,
        origin: start,
        end,
        projection_at_origin,
        projection_at_end: Some(project_string(&sm)?),
        entries,
    })
}

fn sched_of(ctx: &mut ApplyCtx) -> Vec<Sched> {
    ctx.take_sched_records_for_test()
        .into_iter()
        .map(|r| Sched {
            op: match r.op {
                SchedOp::Schedule => "schedule",
                SchedOp::Cancel => "cancel",
                SchedOp::Consumed => "consumed",
                SchedOp::TableConsumed => "table_consumed",
            }
            .into(),
            id: r.timer_id,
            deadline_ns: r.deadline_ns,
        })
        .collect()
}

/// What an app binary's `replay` subcommand calls (spec plan A erratum):
/// open the corpus, drive this SM, write the trace.
pub fn run_replay_cli<S: SnapshotStateMachine>(sm: S, corpus_dir: &Path, out: &Path, from_genesis: bool) -> anyhow::Result<()> {
    let corpus = Corpus::open(corpus_dir)?;
    let origin = if from_genesis { Origin::Genesis } else { Origin::Artifact };
    let trace = drive(sm, &corpus, origin)?;
    let f = File::create(out).with_context(|| out.display().to_string())?;
    trace.write_json(f)
}
```

Check `SchedOp`'s variant names against `uc_protocol/src/v2/ipc.rs:112` and match them exactly (the four above are what `traits.rs:~150-200` constructs). If `TailReader`'s visit closure signature differs from `(seq, meta, block)`, follow `tail_reader.rs:84`.

- [ ] **Step 5: Run the driver tests**

Run: `cargo test -p uc_diffreplay --test drive -- --nocapture`
Expected: PASS ×2. If the entry count is off by one, the culprit is the frontier rule: after `install_snapshot`, `RegisterSm` restores `last_applied` from its image (the cursor *below* P), so the frame starting exactly at P must be applied — `Some(pos) > last_applied()` does that. If it is off by *more*, the padding/alignment walk disagrees with `replay.rs` — go back to that file.

- [ ] **Step 6: fmt, clippy, commit**

```bash
cargo fmt --all && cargo clippy -p uc_diffreplay --all-targets -- -D warnings
git add uc_diffreplay
git commit -m "uc_diffreplay: in-process replay driver — install, walk, capture, project (task 5)"
```

---

### Task 6: Diff — two traces to a divergence profile

**Files:**
- Create: `uc_diffreplay/src/diff.rs`
- Test: `uc_diffreplay/tests/diff_attribute_confirm.rs` (started here, extended in tasks 7–8)

**Interfaces:**
- Produces:
  ```rust
  pub enum Surface { Response, Sched, ProjectionOrigin, ProjectionEnd }
  pub struct Divergence { pub pos: u64, pub tag: Vec<u8>, pub surface: Surface, pub a: Vec<u8>, pub b: Vec<u8> }
  pub struct LineDiff { pub removed: Vec<String>, pub added: Vec<String> }  // set difference of canonical lines
  pub struct Profile { pub entries: Vec<Divergence>, pub projection_origin: LineDiff, pub projection_end: LineDiff,
                       pub only_in_a: Vec<u64>, pub only_in_b: Vec<u64> }
  impl Profile { pub fn is_empty(&self) -> bool }
  impl LineDiff { pub fn is_empty(&self) -> bool }
  pub fn diff(a: &Trace, b: &Trace) -> anyhow::Result<Profile>;   // Err if origin/end differ
  ```
  Because projections are canonical (sorted lines), a **set** difference is the right diff — no LCS needed, no new dependency.

- [ ] **Step 1: Write the failing tests**

```rust
use uc_diffreplay::diff::{Surface, diff};
use uc_diffreplay::trace::{Entry, EntryKind, Sched, Trace};

fn trace(entries: Vec<(u64, &[u8], &[u8])>, proj_end: &str) -> Trace {
    Trace {
        row: 0, version: 1, origin: 32, end: 1000,
        projection_at_origin: Some("value=None\n".into()),
        projection_at_end: Some(proj_end.into()),
        entries: entries.into_iter().map(|(pos, tag, resp)| Entry {
            pos, kind: EntryKind::Message, tag: tag.to_vec(), response: resp.to_vec(), sched: vec![],
        }).collect(),
    }
}

#[test]
fn identical_traces_produce_an_empty_profile() {
    let a = trace(vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")], "value=Some(1)\n");
    let p = diff(&a, &a.clone()).unwrap();
    assert!(p.is_empty(), "{p:?}");
}

#[test]
fn one_changed_response_is_one_divergence_at_its_position() {
    let a = trace(vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")], "value=Some(1)\n");
    let b = trace(vec![(32, b"\x01", b"ok"), (64, b"\x02", b"OK")], "value=Some(1)\n");
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].pos, 64);
    assert_eq!(p.entries[0].tag, b"\x02");
    assert!(matches!(p.entries[0].surface, Surface::Response));
    assert!(p.projection_end.is_empty());
}

#[test]
fn projection_diff_is_a_line_set_difference() {
    let a = trace(vec![], "count=2\nkey=a version=1\nkey=b version=1\n");
    let b = trace(vec![], "count=2\nkey=a version=1 ttl=0\nkey=b version=1 ttl=0\n");
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.projection_end.removed, vec!["key=a version=1", "key=b version=1"]);
    assert_eq!(p.projection_end.added, vec!["key=a version=1 ttl=0", "key=b version=1 ttl=0"]);
}

#[test]
fn a_changed_sched_record_is_a_sched_divergence() {
    let mut a = trace(vec![(32, b"\x01", b"ok")], "");
    let mut b = a.clone();
    a.entries[0].sched = vec![Sched { op: "schedule".into(), id: 1, deadline_ns: 10 }];
    b.entries[0].sched = vec![Sched { op: "schedule".into(), id: 1, deadline_ns: 20 }];
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.entries.len(), 1);
    assert!(matches!(p.entries[0].surface, Surface::Sched));
}

#[test]
fn mismatched_spans_are_refused() {
    let a = trace(vec![], "");
    let mut b = a.clone();
    b.origin = 0;
    assert!(diff(&a, &b).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_diffreplay --test diff_attribute_confirm`
Expected: FAIL — `diff` not defined.

- [ ] **Step 3: Implement `diff.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Two traces → a divergence profile (spec §4.3): every position and surface
//! on which the two runs disagreed, and what each produced. Never a boolean.

use std::collections::BTreeMap;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::trace::{Entry, Trace};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    Response,
    Sched,
    ProjectionOrigin,
    ProjectionEnd,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    pub pos: u64,
    pub tag: Vec<u8>,
    pub surface: Surface,
    pub a: Vec<u8>,
    pub b: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct LineDiff {
    pub removed: Vec<String>,
    pub added: Vec<String>,
}

impl LineDiff {
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.added.is_empty()
    }
    /// Set difference over lines — projections are canonical (sorted, one
    /// record per line), so order carries no information and a multiset diff
    /// is exact.
    fn of(a: Option<&str>, b: Option<&str>) -> LineDiff {
        let count = |s: Option<&str>| {
            let mut m: BTreeMap<&str, usize> = BTreeMap::new();
            for l in s.unwrap_or("").lines() {
                *m.entry(l).or_default() += 1;
            }
            m
        };
        let (ma, mb) = (count(a), count(b));
        let mut d = LineDiff::default();
        for (l, &na) in &ma {
            let nb = mb.get(l).copied().unwrap_or(0);
            for _ in nb..na { d.removed.push((*l).to_string()); }
        }
        for (l, &nb) in &mb {
            let na = ma.get(l).copied().unwrap_or(0);
            for _ in na..nb { d.added.push((*l).to_string()); }
        }
        d
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Profile {
    pub entries: Vec<Divergence>,
    pub projection_origin: LineDiff,
    pub projection_end: LineDiff,
    /// Positions one side dispatched and the other did not — a frontier or
    /// identity disagreement, always worth a look.
    pub only_in_a: Vec<u64>,
    pub only_in_b: Vec<u64>,
}

impl Profile {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.projection_origin.is_empty()
            && self.projection_end.is_empty()
            && self.only_in_a.is_empty()
            && self.only_in_b.is_empty()
    }
}

pub fn diff(a: &Trace, b: &Trace) -> anyhow::Result<Profile> {
    if (a.origin, a.end, a.row) != (b.origin, b.end, b.row) {
        bail!(
            "traces cover different spans: a=[{}, {}) row {} vs b=[{}, {}) row {}",
            a.origin, a.end, a.row, b.origin, b.end, b.row
        );
    }
    let by_pos = |t: &Trace| -> BTreeMap<u64, &Entry> { t.entries.iter().map(|e| (e.pos, e)).collect() };
    let (ma, mb) = (by_pos(a), by_pos(b));
    let mut p = Profile {
        projection_origin: LineDiff::of(a.projection_at_origin.as_deref(), b.projection_at_origin.as_deref()),
        projection_end: LineDiff::of(a.projection_at_end.as_deref(), b.projection_at_end.as_deref()),
        ..Default::default()
    };
    for (&pos, ea) in &ma {
        let Some(eb) = mb.get(&pos) else {
            p.only_in_a.push(pos);
            continue;
        };
        if ea.response != eb.response {
            p.entries.push(Divergence { pos, tag: ea.tag.clone(), surface: Surface::Response, a: ea.response.clone(), b: eb.response.clone() });
        }
        if ea.sched != eb.sched {
            let enc = |s: &[crate::trace::Sched]| serde_json::to_vec(s).unwrap_or_default();
            p.entries.push(Divergence { pos, tag: ea.tag.clone(), surface: Surface::Sched, a: enc(&ea.sched), b: enc(&eb.sched) });
        }
    }
    for &pos in mb.keys() {
        if !ma.contains_key(&pos) {
            p.only_in_b.push(pos);
        }
    }
    Ok(p)
}
```

- [ ] **Step 4: Run**

Run: `cargo test -p uc_diffreplay --test diff_attribute_confirm`
Expected: PASS ×5.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p uc_diffreplay --all-targets -- -D warnings
git add uc_diffreplay
git commit -m "uc_diffreplay: diff — divergence profile over responses, sched, projections (task 6)"
```

---

### Task 7: Declaration and mechanical attribution

**Files:**
- Create: `uc_diffreplay/src/attribute.rs`
- Test: extend `uc_diffreplay/tests/diff_attribute_confirm.rs`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Deserialize)] pub struct Declaration { pub tags: BTreeMap<String, String>, pub touched: Touched, pub expect: Vec<Expect> }
  pub struct Touched { pub arms: Vec<String>, #[serde(default)] pub migration: bool }
  pub struct Expect { pub surface: String /* response|sched|projection_origin|projection_end */, #[serde(default)] pub arm: Option<String>, #[serde(default)] pub note: String }
  impl Declaration { pub fn from_toml(s: &str) -> anyhow::Result<Declaration>; pub fn arm_of(&self, tag: &[u8]) -> Option<&str>; }
  pub enum Attribution { Arm(String), Migration, Unexplained }
  pub struct Attributed { pub entries: Vec<(Divergence, Attribution)>, pub projection_origin: Option<Attribution>, pub projection_end: Option<Attribution> }
  pub fn attribute(p: &Profile, d: &Declaration) -> Attributed;
  ```
  Tags in TOML are hex of the payload's first bytes (`"01"`, `"0102"`). Attribution rule (spec §4.5, mechanical pass): a divergence whose tag maps to an arm in `touched.arms` → `Arm`; tag unmapped or arm untouched → `Unexplained`. Projection at origin: non-empty → `Migration` if `touched.migration`, else `Unexplained`. Projection at end: non-empty → `Arm` of the first touched arm if any, else `Migration` if `touched.migration`, else `Unexplained`.

- [ ] **Step 1: Write the failing tests** (append to `diff_attribute_confirm.rs`)

```rust
use uc_diffreplay::attribute::{Attribution, Declaration, attribute};

const DECL: &str = r#"
[tags]
"01" = "put"
"02" = "delete"
[touched]
arms = ["put"]
migration = true
[[expect]]
surface = "response"
arm = "put"
note = "put now acks with the new version"
[[expect]]
surface = "projection_origin"
note = "every entry gains ttl=0"
"#;

#[test]
fn declaration_parses_and_maps_tags() {
    let d = Declaration::from_toml(DECL).unwrap();
    assert_eq!(d.arm_of(b"\x01"), Some("put"));
    assert_eq!(d.arm_of(b"\x02"), Some("delete"));
    assert_eq!(d.arm_of(b"\x09"), None);
    assert!(d.touched.migration);
    assert_eq!(d.expect.len(), 2);
}

#[test]
fn touched_arm_attributes_untouched_arm_is_unexplained() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")], "x=1\n");
    let b = trace(vec![(32, b"\x01", b"OK"), (64, b"\x02", b"OK")], "x=1\n");
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(att.entries[0].1, Attribution::Arm(ref s) if s == "put"));
    assert!(matches!(att.entries[1].1, Attribution::Unexplained));
}

#[test]
fn origin_projection_diff_attributes_to_migration_when_declared() {
    let d = Declaration::from_toml(DECL).unwrap();
    let mut a = trace(vec![], "");
    let mut b = a.clone();
    a.projection_at_origin = Some("k=a\n".into());
    b.projection_at_origin = Some("k=a ttl=0\n".into());
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(att.projection_origin, Some(Attribution::Migration)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_diffreplay --test diff_attribute_confirm declaration`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `attribute.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Spec §4.5, the MECHANICAL attribution pass: partition the profile by
//! command tag and surface, the code change by touched arm (from the
//! declaration — drafted by the skill, owned by the developer), and
//! cross-tabulate. What this cannot name is `Unexplained`, and goes to the
//! semantic pass (the skill).

use std::collections::BTreeMap;

use anyhow::Context;
use serde::Deserialize;

use crate::diff::{Divergence, Profile};

#[derive(Deserialize, Debug, Clone)]
pub struct Declaration {
    /// Hex of the command payload's leading bytes → arm name.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub touched: Touched,
    #[serde(default)]
    pub expect: Vec<Expect>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Touched {
    #[serde(default)]
    pub arms: Vec<String>,
    /// The state diff at the origin is expected to be non-empty (an image
    /// migration, spec §4.4).
    #[serde(default)]
    pub migration: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Expect {
    /// `response` | `sched` | `projection_origin` | `projection_end`
    pub surface: String,
    #[serde(default)]
    pub arm: Option<String>,
    #[serde(default)]
    pub note: String,
}

impl Declaration {
    pub fn from_toml(s: &str) -> anyhow::Result<Declaration> {
        toml::from_str(s).context("declaration TOML")
    }
    /// Longest hex prefix of `tag` that has a mapping wins, so `"01"` and
    /// `"0102"` can coexist.
    pub fn arm_of(&self, tag: &[u8]) -> Option<&str> {
        let hex: String = tag.iter().map(|b| format!("{b:02x}")).collect();
        (1..=hex.len())
            .rev()
            .filter(|n| n % 2 == 0)
            .find_map(|n| self.tags.get(&hex[..n]).map(String::as_str))
    }
    fn touched(&self, arm: &str) -> bool {
        self.touched.arms.iter().any(|a| a == arm)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribution {
    Arm(String),
    Migration,
    Unexplained,
}

#[derive(Debug, Clone, Default)]
pub struct Attributed {
    pub entries: Vec<(Divergence, Attribution)>,
    pub projection_origin: Option<Attribution>,
    pub projection_end: Option<Attribution>,
}

pub fn attribute(p: &Profile, d: &Declaration) -> Attributed {
    let mut out = Attributed::default();
    for div in &p.entries {
        let att = match d.arm_of(&div.tag) {
            Some(arm) if d.touched(arm) => Attribution::Arm(arm.to_string()),
            _ => Attribution::Unexplained,
        };
        out.entries.push((div.clone(), att));
    }
    if !p.projection_origin.is_empty() {
        out.projection_origin = Some(if d.touched.migration { Attribution::Migration } else { Attribution::Unexplained });
    }
    if !p.projection_end.is_empty() {
        out.projection_end = Some(match d.touched.arms.first() {
            Some(arm) => Attribution::Arm(arm.clone()),
            None if d.touched.migration => Attribution::Migration,
            None => Attribution::Unexplained,
        });
    }
    out
}
```

- [ ] **Step 4: Run**

Run: `cargo test -p uc_diffreplay --test diff_attribute_confirm`
Expected: PASS ×8.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p uc_diffreplay --all-targets -- -D warnings
git add uc_diffreplay
git commit -m "uc_diffreplay: declaration + mechanical attribution by tag × touched arm (task 7)"
```

---

### Task 8: Confirm — the three-way gate

**Files:**
- Create: `uc_diffreplay/src/confirm.rs`
- Test: extend `uc_diffreplay/tests/diff_attribute_confirm.rs`

**Interfaces:**
- Produces:
  ```rust
  pub enum Verdict { Pass, Undeclared, Unexplained, Absent }
  pub struct Finding { pub surface: Surface, pub arm: Option<String>, pub pos: Option<u64>, pub verdict: Verdict, pub note: String }
  pub struct Verdicts { pub findings: Vec<Finding> }
  impl Verdicts { pub fn failed(&self) -> bool }
  pub fn confirm(att: &Attributed, d: &Declaration) -> Verdicts;
  ```
  Rule (spec §4.6): each attributed divergence is `Pass` if some `expect` matches its `(surface, arm)`, else `Undeclared`; each `Unexplained` attribution is `Unexplained`; each `expect` with no matching observed divergence is `Absent`.

- [ ] **Step 1: Write the failing tests** (append)

```rust
use uc_diffreplay::confirm::{Verdict, confirm};

#[test]
fn declared_and_observed_passes_undeclared_fails() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok")], "x=1\n");
    let b = trace(vec![(32, b"\x01", b"OK")], "x=1\n");
    let mut a2 = a.clone(); a2.projection_at_origin = Some("k=a\n".into());
    let mut b2 = b.clone(); b2.projection_at_origin = Some("k=a ttl=0\n".into());
    let v = confirm(&attribute(&diff(&a2, &b2).unwrap(), &d), &d);
    assert!(!v.failed(), "{:?}", v.findings);
    assert!(v.findings.iter().all(|f| matches!(f.verdict, Verdict::Pass)));
}

#[test]
fn an_observed_attributed_but_undeclared_diff_is_undeclared() {
    let mut d = Declaration::from_toml(DECL).unwrap();
    d.expect.clear(); // declare nothing
    let a = trace(vec![(32, b"\x01", b"ok")], "");
    let b = trace(vec![(32, b"\x01", b"OK")], "");
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    assert!(v.failed());
    assert!(v.findings.iter().any(|f| matches!(f.verdict, Verdict::Undeclared)));
}

#[test]
fn an_unexplained_diff_fails_as_unexplained() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x02", b"ok")], ""); // delete: not touched
    let b = trace(vec![(32, b"\x02", b"OK")], "");
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    assert!(v.findings.iter().any(|f| matches!(f.verdict, Verdict::Unexplained)));
}

#[test]
fn a_declared_but_absent_diff_fails_as_absent() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok")], "");
    let v = confirm(&attribute(&diff(&a, &a.clone()).unwrap(), &d), &d);
    assert!(v.failed());
    assert_eq!(v.findings.iter().filter(|f| matches!(f.verdict, Verdict::Absent)).count(), 2);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p uc_diffreplay --test diff_attribute_confirm confirm`
Expected: FAIL.

- [ ] **Step 3: Implement `confirm.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Spec §4.6: observed × attributed × declared → a verdict per entry.
//! A failure here is a FINDING, not a verdict on the change (S5).

use serde::Serialize;

use crate::attribute::{Attributed, Attribution, Declaration, Expect};
use crate::diff::Surface;

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Undeclared,
    Unexplained,
    Absent,
}

#[derive(Serialize, Debug, Clone)]
pub struct Finding {
    pub surface: Surface,
    pub arm: Option<String>,
    pub pos: Option<u64>,
    pub verdict: Verdict,
    pub note: String,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct Verdicts {
    pub findings: Vec<Finding>,
}

impl Verdicts {
    pub fn failed(&self) -> bool {
        self.findings.iter().any(|f| f.verdict != Verdict::Pass)
    }
}

fn surface_name(s: Surface) -> &'static str {
    match s {
        Surface::Response => "response",
        Surface::Sched => "sched",
        Surface::ProjectionOrigin => "projection_origin",
        Surface::ProjectionEnd => "projection_end",
    }
}

fn matches(e: &Expect, s: Surface, arm: Option<&str>) -> bool {
    e.surface == surface_name(s) && (e.arm.is_none() || e.arm.as_deref() == arm)
}

pub fn confirm(att: &Attributed, d: &Declaration) -> Verdicts {
    let mut v = Verdicts::default();
    let mut satisfied = vec![false; d.expect.len()];

    let mut judge = |surface: Surface, pos: Option<u64>, a: &Attribution, v: &mut Verdicts| {
        let arm = match a {
            Attribution::Arm(s) => Some(s.as_str()),
            Attribution::Migration => Some("migration"),
            Attribution::Unexplained => {
                v.findings.push(Finding { surface, arm: None, pos, verdict: Verdict::Unexplained, note: "no touched arm explains this".into() });
                return;
            }
        };
        match d.expect.iter().position(|e| matches(e, surface, arm)) {
            Some(i) => {
                satisfied[i] = true;
                v.findings.push(Finding { surface, arm: arm.map(String::from), pos, verdict: Verdict::Pass, note: d.expect[i].note.clone() });
            }
            None => v.findings.push(Finding { surface, arm: arm.map(String::from), pos, verdict: Verdict::Undeclared, note: "observed and attributed, but not declared".into() }),
        }
    };

    for (div, a) in &att.entries {
        judge(div.surface, Some(div.pos), a, &mut v);
    }
    if let Some(a) = &att.projection_origin {
        judge(Surface::ProjectionOrigin, None, a, &mut v);
    }
    if let Some(a) = &att.projection_end {
        judge(Surface::ProjectionEnd, None, a, &mut v);
    }
    for (i, e) in d.expect.iter().enumerate() {
        if !satisfied[i] {
            let surface = match e.surface.as_str() {
                "response" => Surface::Response,
                "sched" => Surface::Sched,
                "projection_origin" => Surface::ProjectionOrigin,
                _ => Surface::ProjectionEnd,
            };
            v.findings.push(Finding { surface, arm: e.arm.clone(), pos: None, verdict: Verdict::Absent, note: format!("declared but not observed: {}", e.note) });
        }
    }
    v
}
```

The `judge` closure borrows `satisfied` mutably and takes `v` as a parameter to avoid a double mutable borrow; if the borrow checker objects, turn it into a free `fn judge(d, satisfied: &mut [bool], v: &mut Verdicts, …)`.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_diffreplay --test diff_attribute_confirm`
Expected: PASS ×12.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p uc_diffreplay --all-targets -- -D warnings
git add uc_diffreplay
git commit -m "uc_diffreplay: confirm — the observed × attributed × declared gate (task 8)"
```

---

### Task 9: Report and CLI

**Files:**
- Create: `uc_diffreplay/src/report.rs`, `uc_diffreplay/src/bin/uc2-diffreplay.rs` (replace the stub)
- Create: `uc_lincheck/src/bin/register-replay.rs` (a test binary: `RegisterSm` behind `replay`/`project`, so the harness can e2e itself without the KV)
- Test: `uc_diffreplay/tests/upgrade_e2e.rs`

**Interfaces:**
- `report.rs`:
  ```rust
  #[derive(Serialize)] pub struct Report { pub mode: String, pub corpus: PathBuf, pub profile: Profile, pub findings: Vec<Finding>, pub summary: Summary }
  #[derive(Serialize)] pub struct Summary { pub pass: usize, pub undeclared: usize, pub unexplained: usize, pub absent: usize }
  impl Report { pub fn new(mode, corpus, profile, verdicts) -> Report; pub fn write_json(&self, w) -> Result<()>; pub fn write_text(&self, w) -> Result<()>; pub fn failed(&self) -> bool }
  ```
- CLI contract for app binaries (documented in README): `<bin> replay --corpus DIR --out TRACE.json [--from-genesis]` and `<bin> project --artifact FILE --position P`.
- `uc_lincheck` gains `[[bin]] name = "register-replay"` — add `uc_diffreplay` and `clap` as **dev**-dependencies… a `[[bin]]` cannot use dev-deps. Add them as regular optional dependencies behind a feature `replay-bin`, `required-features = ["replay-bin"]` on the bin. `uc_lincheck` is `publish = false`, so the extra deps are free.

- [ ] **Step 1: Write `report.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The attributed diff report (spec §6.4): designed to be read, not just
//! checked. JSON for the skill; text for a human.

use std::io::Write;
use std::path::PathBuf;

use serde::Serialize;

use crate::confirm::{Finding, Verdict, Verdicts};
use crate::diff::Profile;

#[derive(Serialize, Debug, Clone, Default)]
pub struct Summary {
    pub pass: usize,
    pub undeclared: usize,
    pub unexplained: usize,
    pub absent: usize,
}

#[derive(Serialize, Debug, Clone)]
pub struct Report {
    pub mode: String,
    pub corpus: PathBuf,
    pub profile: Profile,
    pub findings: Vec<Finding>,
    pub summary: Summary,
}

impl Report {
    pub fn new(mode: &str, corpus: PathBuf, profile: Profile, verdicts: Verdicts) -> Report {
        let mut s = Summary::default();
        for f in &verdicts.findings {
            match f.verdict {
                Verdict::Pass => s.pass += 1,
                Verdict::Undeclared => s.undeclared += 1,
                Verdict::Unexplained => s.unexplained += 1,
                Verdict::Absent => s.absent += 1,
            }
        }
        Report { mode: mode.into(), corpus, profile, findings: verdicts.findings, summary: s }
    }
    pub fn failed(&self) -> bool {
        self.summary.undeclared + self.summary.unexplained + self.summary.absent > 0
    }
    pub fn write_json(&self, w: impl Write) -> anyhow::Result<()> {
        Ok(serde_json::to_writer_pretty(w, self)?)
    }
    pub fn write_text(&self, mut w: impl Write) -> anyhow::Result<()> {
        writeln!(w, "diff replay — {} — corpus {}", self.mode, self.corpus.display())?;
        writeln!(w, "  divergences: {} entries, origin projection {}−/{}+, end projection {}−/{}+",
            self.profile.entries.len(),
            self.profile.projection_origin.removed.len(), self.profile.projection_origin.added.len(),
            self.profile.projection_end.removed.len(), self.profile.projection_end.added.len())?;
        for f in &self.findings {
            writeln!(w, "  {:<11} {:<18} arm={:<10} pos={:<8} {}",
                format!("{:?}", f.verdict), format!("{:?}", f.surface),
                f.arm.as_deref().unwrap_or("-"),
                f.pos.map(|p| p.to_string()).unwrap_or_else(|| "-".into()),
                f.note)?;
        }
        writeln!(w, "  {} pass, {} undeclared, {} unexplained, {} absent → {}",
            self.summary.pass, self.summary.undeclared, self.summary.unexplained, self.summary.absent,
            if self.failed() { "FAIL" } else { "PASS" })?;
        Ok(())
    }
}
```

- [ ] **Step 2: Write the test binary `uc_lincheck/src/bin/register-replay.rs`**

In `uc_lincheck/Cargo.toml` add:

```toml
[features]
replay-bin = ["dep:uc_diffreplay", "dep:clap", "dep:anyhow"]

[dependencies]
uc_diffreplay = { path = "../uc_diffreplay", optional = true }
clap = { workspace = true, optional = true }
anyhow = { workspace = true, optional = true }

[[bin]]
name = "register-replay"
path = "src/bin/register-replay.rs"
required-features = ["replay-bin"]
```

(Keep whatever `[dependencies]` already exist; merge.) Then:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `RegisterSm` behind the diff-replay app-binary contract — the harness's
//! own end-to-end fixture. Not a pattern to copy; see examples/kv for that.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uc_lincheck::RegisterSm;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    Replay {
        #[arg(long)] corpus: PathBuf,
        #[arg(long)] out: PathBuf,
        #[arg(long)] from_genesis: bool,
        /// Test knob: double every written value (a "v2" with changed semantics).
        #[arg(long)] double: bool,
    },
    Project {
        #[arg(long)] artifact: PathBuf,
        #[arg(long)] position: u64,
    },
}

fn main() -> anyhow::Result<()> {
    match Args::parse().cmd {
        Sub::Replay { corpus, out, from_genesis, double } => {
            if double {
                uc_diffreplay::drive::run_replay_cli(Doubling(RegisterSm::default()), &corpus, &out, from_genesis)
            } else {
                uc_diffreplay::drive::run_replay_cli(RegisterSm::default(), &corpus, &out, from_genesis)
            }
        }
        Sub::Project { artifact, position } => {
            print!("{}", uc_diffreplay::drive::project_artifact(RegisterSm::default(), &artifact, position)?);
            Ok(())
        }
    }
}

/// Same NAME, "v2" semantics: Write(v) stores 2·v. The §2.3 counterfactual
/// needs a build whose apply differs for old commands; this is the smallest.
struct Doubling(RegisterSm);
impl uc_service::StateMachine for Doubling {
    const NAME: &'static str = <RegisterSm as uc_service::StateMachine>::NAME;
    const VERSION: u32 = 2;
    type Command = uc_lincheck::Cmd;
    type Response = uc_lincheck::CmdResp;
    type Query = ();
    type QueryResponse = Option<u64>;
    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: uc_lincheck::Cmd) -> uc_lincheck::CmdResp {
        let cmd = match cmd { uc_lincheck::Cmd::Write(v) => uc_lincheck::Cmd::Write(v * 2), other => other };
        self.0.apply(ctx, cmd)
    }
    fn query(&self, q: ()) -> Option<u64> { self.0.query(q) }
    fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
}
impl uc_service::SnapshotStateMachine for Doubling {
    type SnapshotHandle = <RegisterSm as uc_service::SnapshotStateMachine>::SnapshotHandle;
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), uc_service::SnapshotError> { self.0.freeze() }
    fn stream_snapshot(h: Self::SnapshotHandle, dst: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> { RegisterSm::stream_snapshot(h, dst) }
    fn install_snapshot(&mut self, p: u64, src: &mut dyn std::io::Read) -> Result<u64, uc_service::SnapshotError> { self.0.install_snapshot(p, src) }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> { self.0.project(out) }
}
```

Check `uc_lincheck::Cmd`'s variants at `uc_lincheck/src/register.rs:25` and match the `Write` arm exactly; if `Cmd` is not `Clone`/has other variants that need mapping, map only `Write`.

- [ ] **Step 3: Write the CLI `src/bin/uc2-diffreplay.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2-diffreplay`: orchestrate diff replay over app binaries that
//! implement the `replay` contract (README). Exit 1 when the report fails.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use uc_diffreplay::attribute::{Declaration, attribute};
use uc_diffreplay::confirm::{Verdicts, confirm};
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::diff::diff;
use uc_diffreplay::report::Report;
use uc_diffreplay::trace::Trace;

#[derive(Parser)]
#[command(name = "uc2-diffreplay", version)]
struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Export a corpus from a (stopped) instance directory.
    Corpus {
        #[command(subcommand)]
        cmd: CorpusSub,
    },
    /// v_old vs v_new over one corpus, judged against a declaration.
    Upgrade {
        #[arg(long)] corpus: PathBuf,
        #[arg(long)] old: PathBuf,
        #[arg(long)] new: PathBuf,
        #[arg(long)] declare: PathBuf,
        #[arg(long)] report: PathBuf,
    },
    /// One binary, two processes: the profile must be empty.
    Determinism {
        #[arg(long)] corpus: PathBuf,
        #[arg(long)] bin: PathBuf,
        #[arg(long)] report: PathBuf,
    },
    /// One binary, two origins (artifact vs genesis): demonstrates §2.3.
    Reconstruction {
        #[arg(long)] corpus: PathBuf,
        #[arg(long)] bin: PathBuf,
        #[arg(long)] report: PathBuf,
    },
}

#[derive(Subcommand)]
enum CorpusSub {
    Export {
        #[arg(long)] instance_dir: PathBuf,
        #[arg(long)] app_id: String,
        #[arg(long)] row: u8,
        #[arg(long)] from: Option<u64>,
        #[arg(long)] to: Option<u64>,
        #[arg(long)] around: Option<u64>,
        #[arg(long, default_value_t = 0)] version: u32,
        #[arg(long)] out: PathBuf,
    },
}

/// Run `<bin> replay --corpus C --out T [--from-genesis] [extra…]` and read the trace back.
fn replay(bin: &Path, corpus: &Path, out: &Path, from_genesis: bool, extra: &[&str]) -> anyhow::Result<Trace> {
    let mut c = Command::new(bin);
    c.arg("replay").arg("--corpus").arg(corpus).arg("--out").arg(out);
    if from_genesis {
        c.arg("--from-genesis");
    }
    c.args(extra);
    let st = c.status().with_context(|| format!("spawn {}", bin.display()))?;
    if !st.success() {
        bail!("{} replay exited {st}", bin.display());
    }
    Trace::read_json(std::fs::File::open(out)?)
}

fn finish(report: Report, path: &Path) -> anyhow::Result<()> {
    report.write_json(std::fs::File::create(path)?)?;
    report.write_text(std::io::stdout())?;
    if report.failed() {
        std::process::exit(1);
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    match Args::parse().cmd {
        Sub::Corpus { cmd: CorpusSub::Export { instance_dir, app_id, row, from, to, around, version, out } } => {
            let c = match (from, to, around) {
                (Some(p), to, None) => Corpus::export(&instance_dir, &app_id, row, p, to.unwrap_or(u64::MAX), version, &out)?,
                (None, None, Some(pos)) => Corpus::export_around(&instance_dir, &app_id, row, pos, version, &out)?,
                _ => bail!("give --from [--to] or --around, not both"),
            };
            println!("corpus at {} — row {} origin {} end {}", c.dir.display(), c.manifest.row, c.manifest.origin, c.manifest.end);
            Ok(())
        }
        Sub::Upgrade { corpus, old, new, declare, report } => {
            let tmp = tempfile_dir(&report)?;
            let a = replay(&old, &corpus, &tmp.join("old.json"), false, &[])?;
            let b = replay(&new, &corpus, &tmp.join("new.json"), false, &[])?;
            let d = Declaration::from_toml(&std::fs::read_to_string(&declare)?)?;
            let profile = diff(&a, &b)?;
            let verdicts = confirm(&attribute(&profile, &d), &d);
            finish(Report::new("upgrade", corpus, profile, verdicts), &report)
        }
        Sub::Determinism { corpus, bin, report } => {
            let tmp = tempfile_dir(&report)?;
            let a = replay(&bin, &corpus, &tmp.join("run1.json"), false, &[])?;
            let b = replay(&bin, &corpus, &tmp.join("run2.json"), false, &[])?;
            let profile = diff(&a, &b)?;
            // The declaration is EMPTY by definition: any divergence is a defect.
            let d = Declaration::from_toml("[touched]\narms = []\n")?;
            let verdicts = confirm(&attribute(&profile, &d), &d);
            finish(Report::new("determinism", corpus, profile, verdicts), &report)
        }
        Sub::Reconstruction { corpus, bin, report } => {
            let tmp = tempfile_dir(&report)?;
            let art = replay(&bin, &corpus, &tmp.join("artifact.json"), false, &[])?;
            let mut gen = replay(&bin, &corpus, &tmp.join("genesis.json"), true, &[])?;
            // Compare end state only: the genesis run has no origin projection
            // and a different origin by construction. Align the spans for diff().
            gen.origin = art.origin;
            gen.projection_at_origin = art.projection_at_origin.clone();
            gen.entries.retain(|e| e.pos >= art.origin);
            let profile = diff(&art, &gen)?;
            let d = Declaration::from_toml("[touched]\narms = []\n")?;
            let verdicts = confirm(&attribute(&profile, &d), &d);
            let r = Report::new("reconstruction", corpus, profile, verdicts);
            // In this mode a NON-empty end-projection diff is the expected
            // demonstration (spec §6.2 part 1); report it, exit 0 either way.
            r.write_json(std::fs::File::create(&report)?)?;
            r.write_text(std::io::stdout())?;
            println!("reconstruction: end projections {}", if r.profile.projection_end.is_empty() { "AGREE (no semantic change below P)" } else { "DIVERGE — the §2.3 counterfactual" });
            Ok(())
        }
    }
}

fn tempfile_dir(beside: &Path) -> anyhow::Result<PathBuf> {
    let d = beside.with_extension("traces");
    std::fs::create_dir_all(&d)?;
    Ok(d)
}
```

Traces are written beside the report (`<report>.traces/`), never under `/tmp`.

- [ ] **Step 4: Write the failing e2e test (`tests/upgrade_e2e.rs`)**

```rust
mod common;
use std::path::PathBuf;
use std::process::Command;

use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::report::Report;

fn bin(name: &str) -> PathBuf {
    // Built by cargo for this test binary's profile: target/<profile>/<name>.
    let mut p = PathBuf::from(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    p.set_file_name(name);
    p
}

fn register_replay_bin() -> PathBuf {
    let p = bin("register-replay");
    assert!(p.exists(), "build it first: cargo build -p uc_lincheck --features replay-bin --bin register-replay ({})", p.display());
    p
}

#[test]
fn same_binary_twice_passes_with_an_empty_declaration() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "e2e", 4, 4);
    Corpus::export(inst.path(), "e2e", 0, p, u64::MAX, 0, out.path()).unwrap();
    let report = out.path().join("det.json");
    let st = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"))
        .args(["determinism", "--corpus"]).arg(out.path())
        .arg("--bin").arg(register_replay_bin())
        .arg("--report").arg(&report)
        .status().unwrap();
    assert!(st.success());
    let r: serde_json::Value = serde_json::from_reader(std::fs::File::open(&report).unwrap()).unwrap();
    assert_eq!(r["summary"]["pass"], 0);
    assert_eq!(r["summary"]["unexplained"], 0);
}

#[test]
fn upgrade_with_a_declared_absent_change_fails_with_absent() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "up", 4, 4);
    Corpus::export(inst.path(), "up", 0, p, u64::MAX, 0, out.path()).unwrap();
    let decl = out.path().join("intent.toml");
    std::fs::write(&decl, "[tags]\n\"00\" = \"write\"\n[touched]\narms = [\"write\"]\n[[expect]]\nsurface = \"projection_end\"\nnote = \"values doubled\"\n").unwrap();
    let report = out.path().join("up.json");
    let st = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"))
        .args(["upgrade", "--corpus"]).arg(out.path())
        .arg("--old").arg(register_replay_bin())
        .arg("--new").arg(register_replay_bin()) // same binary: nothing changes
        .arg("--declare").arg(&decl)
        .arg("--report").arg(&report)
        .status().unwrap();
    assert_eq!(st.code(), Some(1));
    let r: serde_json::Value = serde_json::from_reader(std::fs::File::open(&report).unwrap()).unwrap();
    assert_eq!(r["summary"]["absent"], 1);
}
```

`serde_json` is a regular dependency of the crate, so tests see it. The bincode encoding of `Cmd::Write(v)` starts with the variant index byte `0x00` — hence tag `"00"`. Verify with `bincode::serde::encode_to_vec(Cmd::Write(1), standard())[0]` if the enum order differs.

- [ ] **Step 5: Build the fixture binary and run**

Run:
```bash
cargo build -p uc_lincheck --features replay-bin --bin register-replay
cargo test -p uc_diffreplay --test upgrade_e2e -- --nocapture
```
Expected: PASS ×2. The `register-replay` binary lands next to `uc2-diffreplay` in the same target profile directory, which is what `bin()` assumes; if the test profile differs (`cargo test` builds `debug`), build the fixture with the same profile.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add uc_diffreplay uc_lincheck
git commit -m "uc_diffreplay: report + uc2-diffreplay CLI (corpus export, upgrade, determinism, reconstruction); register-replay fixture (task 9)"
```

---

### Task 10: `reconstruction` — the §2.3 counterfactual, demonstrated twice

**Files:**
- Test: `uc_diffreplay/tests/reconstruction.rs`

**Interfaces:**
- Consumes: `register-replay --double` (task 9), `uc_node::PurgePolicy::BelowSnapshot { slack_bytes }`, `uc_journal::TailReader::first_meta`, `uc_service::Service::query`.

This is the harness's first teeth-check (spec §6.2 part 1): a build whose semantics differ for old commands, replayed from the artifact vs from genesis, must disagree — at the driver level **and** through the real attach path.

- [ ] **Step 1: Driver-level demonstration**

```rust
mod common;
use std::process::Command;
use uc_diffreplay::corpus::Corpus;

fn register_replay_bin() -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    p.set_file_name("register-replay");
    assert!(p.exists(), "cargo build -p uc_lincheck --features replay-bin --bin register-replay");
    p
}

#[test]
fn a_changed_build_diverges_between_artifact_and_genesis_origins() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "rc", 5, 3);
    Corpus::export(inst.path(), "rc", 0, p, u64::MAX, 0, out.path()).unwrap();

    let run = |from_genesis: bool, name: &str| -> uc_diffreplay::trace::Trace {
        let t = out.path().join(name);
        let mut c = Command::new(register_replay_bin());
        c.arg("replay").arg("--corpus").arg(out.path()).arg("--out").arg(&t).arg("--double");
        if from_genesis { c.arg("--from-genesis"); }
        assert!(c.status().unwrap().success());
        uc_diffreplay::trace::Trace::read_json(std::fs::File::open(&t).unwrap()).unwrap()
    };
    let art = run(false, "art.json");
    let gen = run(true, "gen.json");
    // Artifact path: v1's state at P (value=4), then v2 doubles 5,6,7 → 14.
    assert!(art.projection_at_end.as_deref().unwrap().starts_with("value=Some(14)\n"), "{:?}", art.projection_at_end);
    // Genesis path: v2 doubles EVERYTHING → 14 as well for the last write…
    // …so use the origin-side evidence: the genesis run's state at P would be
    // value=Some(8), not the artifact's Some(4). Check via a write count that
    // makes the difference visible at the end: re-run with an odd tail.
    assert_eq!(gen.entries.len(), 8);
    assert_eq!(art.entries.len(), 3);
}
```

The last-write value happens to coincide (2·7 either way) — RegisterSm's state is a single register. Make the divergence visible by changing the history so the **last** command is not a `Write`: add a `Cmd::Read` (or whatever non-writing variant `uc_lincheck::Cmd` has at `register.rs:25`) as the final command in `build_register_history` via a new helper `build_register_history_ending_with_read`, so the end value is the last *write*, which differs (`Some(14)` on the artifact path vs `Some(14)` … still equal). **Better:** compare projections at a position where they differ — the `projection_at_origin` of the artifact run (`Some(4)`) against a genesis run driven only to `end = p` (`Corpus` with `end = p`): genesis-to-P yields `value=Some(8)`. Write it as:

```rust
#[test]
fn genesis_to_p_under_v2_is_not_v1s_state_at_p() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "rc2", 5, 0);
    let c = Corpus::export(inst.path(), "rc2", 0, p, p, 0, out.path()).unwrap();
    let t = out.path().join("gen.json");
    assert!(Command::new(register_replay_bin())
        .arg("replay").arg("--corpus").arg(&c.dir).arg("--out").arg(&t).arg("--double").arg("--from-genesis")
        .status().unwrap().success());
    let gen = uc_diffreplay::trace::Trace::read_json(std::fs::File::open(&t).unwrap()).unwrap();
    // v1 wrote 0..5 → its artifact holds Some(4). v2 replaying the same
    // frames from genesis holds Some(8): the counterfactual.
    assert!(gen.projection_at_end.as_deref().unwrap().starts_with("value=Some(8)\n"), "{:?}", gen.projection_at_end);
    let art = uc_diffreplay::drive::project_artifact(uc_lincheck::RegisterSm::default(), &c.artifact(), p).unwrap();
    assert!(art.starts_with("value=Some(4)\n"));
}
```

Replace the first test with this one (delete `a_changed_build_diverges_between_artifact_and_genesis_origins`).

- [ ] **Step 2: Run**

Run: `cargo test -p uc_diffreplay --test reconstruction genesis_to_p`
Expected: PASS — `Some(8)` vs `Some(4)`. If `drive` with `end == origin` yields no entries for the artifact path, that is correct; only the genesis path has entries here.

- [ ] **Step 3: Real-attach demonstration**

The same divergence through UC's own reconstruction path (`replay.rs:207` gap guard), with an in-process node and a "v2" `RegisterSm`:

```rust
use std::time::Duration;
use uc_client::Client;
use uc_lincheck::{Cmd, CmdResp, RegisterSm};
use uc_service::{ApplyCtx, ServiceBuilder, ServiceConfig, SnapshotError, SnapshotStateMachine, StateMachine};

/// Same NAME as RegisterSm, VERSION 2, Write(v) stores 2·v — a "v2" whose
/// apply differs for old commands. Duplicated from register-replay.rs.
#[derive(Default)]
struct Doubling(RegisterSm);
impl StateMachine for Doubling {
    const NAME: &'static str = <RegisterSm as StateMachine>::NAME;
    const VERSION: u32 = 2;
    type Command = Cmd; type Response = CmdResp; type Query = (); type QueryResponse = Option<u64>;
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> CmdResp {
        let cmd = match cmd { Cmd::Write(v) => Cmd::Write(v * 2), o => o };
        self.0.apply(ctx, cmd)
    }
    fn query(&self, q: ()) -> Option<u64> { self.0.query(q) }
    fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
}
impl SnapshotStateMachine for Doubling {
    type SnapshotHandle = <RegisterSm as SnapshotStateMachine>::SnapshotHandle;
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> { self.0.freeze() }
    fn stream_snapshot(h: Self::SnapshotHandle, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> { RegisterSm::stream_snapshot(h, dst) }
    fn install_snapshot(&mut self, p: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> { self.0.install_snapshot(p, src) }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> { self.0.project(out) }
}

/// v1 writes 0..5, instant at P, stop. Then attach v2 on the SAME instance
/// dir with the given purge policy and read the register.
fn v2_after_swap(purge: uc_node::PurgePolicy, app_id: &str) -> Option<u64> {
    let inst = common::tempdir();
    let dir = inst.path();
    // --- v1 era ---
    let mut cfg = common::node_config(dir, app_id, common::register_name());
    cfg.purge = purge;
    let node = uc_node::Node::start(cfg).unwrap();
    let svc = ServiceBuilder::new(ServiceConfig::new(dir.to_path_buf(), app_id.into()), RegisterSm::default())
        .start_with_snapshots().unwrap();
    let client = Client::connect(dir, app_id).unwrap();
    for v in 0..5u64 { let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap(); }
    let p = common::command_instant(&node);
    let art = dir.join("snapshots").join("0").join(format!("snap-{p}.ultsnap"));
    common::wait_until(|| art.is_file());
    if !matches!(purge, uc_node::PurgePolicy::Disabled) {
        // Purge moves on the complete set; wait for the journal's first
        // block to sit at or above P (TailReader::first_meta is the observable).
        let jr = uc_journal::TailReader::open(&dir.join("journal")).unwrap();
        common::wait_until(|| jr.first_meta().unwrap().unwrap_or(0) >= p);
    }
    client.shutdown();
    svc.stop();
    // --- flag day: swap the service binary, node keeps running ---
    let svc2 = ServiceBuilder::new(ServiceConfig::new(dir.to_path_buf(), app_id.into()), Doubling::default())
        .start_with_snapshots().unwrap();
    std::thread::sleep(Duration::from_millis(300)); // let reconstruction finish
    let v = svc2.query(());
    svc2.stop();
    node.stop();
    v
}

#[test]
fn real_attach_genesis_replay_computes_the_counterfactual_and_install_does_not() {
    // Purge disabled (the shipped default): v2 replays 0..5 from genesis, doubling → Some(8).
    let genesis = v2_after_swap(uc_node::PurgePolicy::Disabled, "ra1");
    // Purge below the set: v2 installs v1's artifact → Some(4), the true history.
    let installed = v2_after_swap(uc_node::PurgePolicy::BelowSnapshot { slack_bytes: 0 }, "ra2");
    assert_eq!(installed, Some(4), "artifact path must carry v1's state");
    assert_eq!(genesis, Some(8), "genesis path under v2 is the counterfactual");
    assert_ne!(genesis, installed, "§2.3: same binary, two paths, two states");
}
```

If `Service::query` needs the service to be caught up, replace the sleep with a `wait_until` on `svc2.query(()) == Some(_)`; if the purge floor does not move without a second instant or a cadence, command a second instant after the first completes and wait on `first_meta() >= p` again — read `docs/ops/uc2-runbook.md` "purge enablement" for the exact trigger and adjust; the test's assertion does not change.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_diffreplay --test reconstruction -- --nocapture --test-threads=1`
Expected: PASS ×2. This is the demonstration the spec's §2.3 evidence note says is missing — record the run in the commit message.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p uc_diffreplay --all-targets -- -D warnings
git add uc_diffreplay/tests/reconstruction.rs
git commit -m "uc_diffreplay: reconstruction — §2.3 counterfactual demonstrated at driver level and through the real attach path (task 10)"
```

---

### Task 11: `examples/kv` — projection, `replay`/`project` subcommands, canonical-projection tests

**Files:**
- Modify: `examples/kv/src/lib.rs` (`impl SnapshotStateMachine for KvSm`, ~L355-420), `examples/kv/src/bin/kv-service.rs:27-44,44-92`, `examples/kv/Cargo.toml`
- Create: `examples/kv/tests/projection.rs`

**Interfaces:**
- `KvSm::project` renders, in order: `count=<n>`, `cursor=<last_applied or none>`, `digest=<hex>`, then one line per entry in `OrdMap` order (already sorted by key): `key=<hex> version=<n> shape=value bytes=<hex>` or `key=<hex> version=<n> shape=list items=<hex,hex,…>`. `Sessioned` adds its `session client=… seq=…` lines after (task 4).
- `kv-service replay --corpus DIR --out TRACE [--from-genesis]` and `kv-service project --artifact FILE --position P`; no subcommand = attach, exactly as today.

- [ ] **Step 1: Write the failing canonical-projection test**

```rust
// examples/kv/tests/projection.rs
use kv_store::{KvSm, wire};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

fn apply(sm: &mut KvSm, pos: u64, frame: &[u8]) {
    let mut out = Vec::new();
    sm.apply(&mut ApplyCtx::new(pos, <KvSm as RawStateMachine>::IDENTITY), frame, &mut out);
}

fn project(sm: &KvSm) -> String {
    let mut out = Vec::new();
    sm.project(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn projection_is_canonical_regardless_of_insertion_order() {
    let mut a = KvSm::default();
    apply(&mut a, 32, &wire::encode_put(b"b", b"2"));
    apply(&mut a, 64, &wire::encode_put(b"a", b"1"));
    let mut b = KvSm::default();
    apply(&mut b, 32, &wire::encode_put(b"a", b"1"));
    apply(&mut b, 64, &wire::encode_put(b"b", b"2"));
    // Different positions → different cursor lines; compare the entry lines.
    let strip = |s: String| s.lines().filter(|l| !l.starts_with("cursor=")).collect::<Vec<_>>().join("\n");
    assert_eq!(strip(project(&a)), strip(project(&b)));
    assert!(project(&a).contains("key=61 version=1 shape=value bytes=31"));
}

#[test]
fn projection_survives_a_snapshot_roundtrip() {
    let mut a = KvSm::default();
    apply(&mut a, 32, &wire::encode_put(b"k", b"v"));
    apply(&mut a, 64, &wire::encode_append(b"l", b"x"));
    let (h, pos) = a.freeze().unwrap();
    let mut img = Vec::new();
    KvSm::stream_snapshot(h, &mut img).unwrap();
    let mut b = KvSm::default();
    b.install_snapshot(pos.max(96), &mut &img[..]).unwrap();
    assert_eq!(project(&a), project(&b));
}
```

Check `wire::encode_append` exists (`examples/kv/src/wire.rs:297`) and that `KvSm` is `Default` (`kv-service.rs:66` uses `KvSm::default()`); check `wire` is a `pub mod` of the crate.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p kv_store --test projection`
Expected: FAIL — `project` is the default refusal.

- [ ] **Step 3: Implement `KvSm::project`**

In `examples/kv/src/lib.rs`, inside `impl SnapshotStateMachine for KvSm`, after `install_snapshot`:

```rust
    /// Diff replay projection (spec §5.8): canonical text, one entry per
    /// line, in key order — `OrdMap` iterates sorted, so this is canonical
    /// for free. Same fields the image carries, human-readable.
    fn project(&self, out: &mut dyn Write) -> Result<(), SnapshotError> {
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        writeln!(out, "count={}", self.map.len())?;
        match self.last_applied {
            Some(c) => writeln!(out, "cursor={c}")?,
            None => writeln!(out, "cursor=none")?,
        }
        writeln!(out, "digest={:#018x}", self.digest)?;
        for (k, e) in self.map.iter() {
            match &e.shape {
                Shape::Value(v) => writeln!(out, "key={} version={} shape=value bytes={}", hex(k), e.version, hex(v))?,
                Shape::List(items) => {
                    let items: Vec<String> = items.iter().map(|i| hex(i)).collect();
                    writeln!(out, "key={} version={} shape=list items={}", hex(k), e.version, items.join(","))?
                }
            }
        }
        Ok(())
    }
```

Match `Shape`'s list variant name and item type to `lib.rs` (`Shape::List(...)` at ~L300); `e.version` is the entry's version field used by `stream_snapshot`.

- [ ] **Step 4: Run**

Run: `cargo test -p kv_store --test projection`
Expected: PASS ×2.

- [ ] **Step 5: Add the subcommands to `kv-service`**

`examples/kv/Cargo.toml`: add `uc_diffreplay = { path = "../../uc_diffreplay", version = "2.12.0" }` under `[dependencies]`.

`examples/kv/src/bin/kv-service.rs`: change `Args` to

```rust
#[derive(Parser)]
#[command(name = "kv-service", version)]
struct Args {
    /// The instance directory of the node to attach to.
    #[arg(long, required_unless_present = "cmd")]
    instance_dir: Option<PathBuf>,
    /// Application identity; must match the node's and the gateway's.
    #[arg(long, default_value = "kv")]
    app_id: String,
    /// How long to wait for the node's control page to appear.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
    #[command(subcommand)]
    cmd: Option<Sub>,
}

/// Diff replay (uc_diffreplay README): the app-binary contract.
#[derive(clap::Subcommand)]
enum Sub {
    /// Replay a corpus through this FSM in-process and write the trace.
    Replay {
        #[arg(long)] corpus: PathBuf,
        #[arg(long)] out: PathBuf,
        #[arg(long)] from_genesis: bool,
    },
    /// Install an artifact and print its canonical projection.
    Project {
        #[arg(long)] artifact: PathBuf,
        #[arg(long)] position: u64,
    },
}
```

and at the top of `main`:

```rust
    let args = Args::parse();
    // The same wrapper stack the live service runs — the envelope is part of
    // the behaviour being replayed.
    let sm = || Sessioned::new(KvSm::default(), SessionConfig::default());
    match args.cmd {
        Some(Sub::Replay { corpus, out, from_genesis }) => {
            return uc_diffreplay::drive::run_replay_cli(sm(), &corpus, &out, from_genesis);
        }
        Some(Sub::Project { artifact, position }) => {
            print!("{}", uc_diffreplay::drive::project_artifact(sm(), &artifact, position)?);
            return Ok(());
        }
        None => {}
    }
    let instance_dir = args.instance_dir.expect("clap: required unless a subcommand is given");
```

then replace every later `args.instance_dir` with `instance_dir`. Keep the rest of `main` byte-for-byte.

- [ ] **Step 6: Build and smoke the contract**

Run:
```bash
cargo build -p kv_store --bin kv-service
./$(ls -d ~/.cache/cargo-target 2>/dev/null || echo target)/debug/kv-service --help | grep -E "replay|project"
```
Expected: both subcommands listed. (Use whatever `CARGO_TARGET_DIR` this checkout builds into; `cargo metadata --format-version 1 | jq -r .target_directory` prints it.)

- [ ] **Step 7: Run the existing KV suites**

Run: `cargo test -p kv_store`
Expected: everything that passed before still passes (`sm_invariants`, `v2_lists`; `cluster.rs` tests that need release bins skip as before).

- [ ] **Step 8: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add examples/kv
git commit -m "examples/kv: project() + kv-service replay/project subcommands — the first worked diff-replay app (task 11)"
```

---

### Task 12: Regression-corpus convention with a worked KV example

**Files:**
- Create: `examples/kv/tests/corpora/README.md`, `examples/kv/tests/corpora/put-then-delete/{CORPUS,intent.toml,journal/…,snapshots/0/…}` (generated), `examples/kv/tests/regression_corpora.rs`

**Interfaces:**
- Convention (spec §5.9): `tests/corpora/<name>/` holds a corpus (as exported) plus `intent.toml` (the declaration). `regression_corpora.rs` replays each under the current build **against itself in `determinism` mode** and — where a `baseline.json` trace is checked in — in `upgrade` mode against that baseline with `intent.toml`.

- [ ] **Step 1: Generate the corpus**

Write a one-off test `examples/kv/tests/gen_corpus.rs` (ignored by default) that starts an in-process node with `ServicesConfig::single(KvSm::NAME)`, the `Sessioned<KvSm>` service, submits via `uc_client` the raw frames `wire::encode_put(b"a", b"1")`, `encode_put(b"b", b"2")`, commands an instant, then `encode_delete(b"a")`, stops, and exports with `Corpus::export(dir, "kv", 0, p, u64::MAX, KV_VERSION, "tests/corpora/put-then-delete")`. Run it once with `cargo test -p kv_store --test gen_corpus -- --ignored`, then commit the output. Keep the generator (ignored) so the corpus can be regenerated after a format change. Submitting raw bytes: `uc_client::Client` is typed over serde; for a raw-tier SM use `submit_to`/the raw path if one exists (`uc_client/src/client.rs:142`) or the `kv` CLI's own submission code — read `examples/kv/src/bin/kv.rs` and reuse its client path. The corpus must be small: check `du -sh tests/corpora/put-then-delete` — under 1 MiB, or trim the journal segment size in the node config (`journal_segment_bytes`) for the generator.

- [ ] **Step 2: Write `intent.toml`**

```toml
# Regression corpus "put-then-delete": two puts below the origin, one delete above it.
# Under the SAME build nothing may differ. Against baseline.json (if present),
# declare here what a change is expected to do.
[tags]
"01" = "put"
"02" = "delete"
[touched]
arms = []
```

Map the tag bytes to `wire.rs`'s op constants (`FORMAT_VERSION` is the first byte — so the tag's first byte is `01` for every command; use two bytes: `"01xx"` where `xx` is the op byte. Read `wire.rs:225-300` and write the real values).

- [ ] **Step 3: Write the regression test**

```rust
// examples/kv/tests/regression_corpora.rs
use std::path::{Path, PathBuf};
use std::process::Command;

fn corpora() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests").join("corpora");
    std::fs::read_dir(&root).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.join("CORPUS").is_file()).collect()
}

#[test]
fn every_regression_corpus_is_deterministic_under_this_build() {
    let kv_service = PathBuf::from(env!("CARGO_BIN_EXE_kv-service"));
    let diffreplay = which_uc2_diffreplay();
    for c in corpora() {
        let report = c.join("determinism.report.json");
        let st = Command::new(&diffreplay)
            .arg("determinism").arg("--corpus").arg(&c).arg("--bin").arg(&kv_service).arg("--report").arg(&report)
            .status().unwrap();
        assert!(st.success(), "determinism failed for {}", c.display());
        let _ = std::fs::remove_file(&report);
        let _ = std::fs::remove_dir_all(c.join("determinism.report.traces"));
    }
}

fn which_uc2_diffreplay() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_BIN_EXE_kv-service"));
    p.set_file_name("uc2-diffreplay");
    assert!(p.exists(), "cargo build -p uc_diffreplay first ({})", p.display());
    p
}
```

Add `.gitignore` lines in `tests/corpora/` for `*.report.json` and `*.report.traces/`.

- [ ] **Step 4: Run**

Run: `cargo build -p uc_diffreplay && cargo test -p kv_store --test regression_corpora`
Expected: PASS.

- [ ] **Step 5: Write `tests/corpora/README.md`**

```markdown
# Regression corpora

A corpus is a `uc2ctl backup`-shaped directory plus `CORPUS` (row, origin,
end, version) and `intent.toml` (the diff-replay declaration). Each one is a
recorded input that once mattered — a bug's trigger, a migration's shape —
kept so every future build is replayed over it (spec §5.9, §6.5.1).

`regression_corpora.rs` runs every corpus here in `determinism` mode on each
`cargo test`. To pin a behaviour across versions, add `baseline.json` (a trace
from the version you trust) and the test will also run `upgrade` mode against
it with `intent.toml`.

Regenerate a corpus with `cargo test --test gen_corpus -- --ignored` after a
wire or image format change; a corpus without its `intent.toml` is a recording,
not a test.
```

- [ ] **Step 6: Commit**

```bash
git add examples/kv/tests
git commit -m "examples/kv: regression-corpus convention + put-then-delete corpus (task 12)"
```

---

### Task 13: Minimal how-to, README polish, workspace gates

**Files:**
- Create: `docs/how-to/diff-replay.md`
- Modify: `uc_diffreplay/README.md` (trace format section), `docs/how-to/README.md` (index line)

- [ ] **Step 1: Write `docs/how-to/diff-replay.md`**

```markdown
# Diff replay an FSM change

Replay the same input — a snapshot plus a log span — through the old and the
new build of your state machine, diff everything they did, and confirm the
differences are the ones you meant. The full model is the spec
(`docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`, §4);
this page is the commands.

## 1. Make your service binary replayable

Add `replay` and `project` subcommands that call
`uc_diffreplay::drive::run_replay_cli` / `project_artifact` with the **same
wrapper stack** your live service uses (`Sessioned`, `Timed`, …) —
`examples/kv/src/bin/kv-service.rs` is the worked example. Implement
`SnapshotStateMachine::project()`: canonical text, sorted, one record per line.

## 2. Capture a corpus

    uc2ctl snapshot …                                  # a complete set at P
    <stop the node>
    uc2-diffreplay corpus export --instance-dir /srv/uc2/n0 --app-id kv --row 0 \
        --from P --out ./corpus                        # or --around <pos> for a bug

## 3. Declare what you intend

`intent.toml` — which arms the change touched, what should differ:

    [tags]        # first bytes of your command encoding → arm name
    "0101" = "put"
    [touched]
    arms = ["put"]
    migration = true                                    # the image format changed
    [[expect]]
    surface = "projection_origin"
    note = "every entry gains ttl=0"
    [[expect]]
    surface = "response"
    arm = "put"
    note = "put acks now carry ttl"

## 4. Run

    uc2-diffreplay upgrade --corpus ./corpus --old ./kv-service-1.0 --new ./kv-service-1.1 \
        --declare intent.toml --report report.json

Exit 0 = every difference is attributed and declared, every declaration was
observed. Anything else exits 1 and the report names it: `Undeclared`,
`Unexplained`, or `Absent`.

## Also

    uc2-diffreplay determinism    --corpus C --bin ./kv-service   # same build twice; must be empty
    uc2-diffreplay reconstruction --corpus C --bin ./kv-service   # artifact vs genesis origin (spec §2.3)
```

- [ ] **Step 2: Add the index line** to `docs/how-to/README.md` in alphabetical position: `- [Diff replay an FSM change](diff-replay.md)`.

- [ ] **Step 3: README — trace format**

Append to `uc_diffreplay/README.md`:

```markdown
## The trace an app binary writes

JSON, `uc_diffreplay::trace::Trace`: `row`, `version`, `origin`, `end`,
`projection_at_origin`, `projection_at_end`, and `entries[]` of
`{ pos, kind: "Message" | { "Timer": { id, deadline_ns, table } }, tag, response, sched[] }`.
A non-Rust app produces the same JSON and takes part in every mode.
```

- [ ] **Step 4: Full workspace gates**

Run:
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p uc_diffreplay -p uc_service -p uc_lincheck -p kv_store
cargo clippy -p uc_lincheck --features replay-bin --all-targets -- -D warnings
```
Expected: all clean. The last line matters: a feature-gated binary escapes `--workspace` clippy (this bit the tree on 2026-09-10).

- [ ] **Step 5: Commit**

```bash
git add docs/how-to/diff-replay.md docs/how-to/README.md uc_diffreplay/README.md
git commit -m "docs: diff replay how-to; uc_diffreplay README trace format (task 13)"
```

---

## Self-review

**Spec coverage (deliverable 1, plan A's items):**
- §6.1 corpus + `--around` → Tasks 2, 9. ✔
- §6.2 three modes → Task 9 (CLI), Task 10 (`reconstruction` part 1, both levels), e2e for `determinism`/`upgrade`. ✔ Part 2 (verify the refusal) is plan C by design.
- §6.3 black-box → the driver erratum in the header; app binaries are still black-box to the harness. ✔
- §6.4 attributed diff report → Task 9. ✔
- §4.2 surfaces: responses, sched, projections → Tasks 3, 5, 6. `on_committed` emission sequence — **not captured**: the driver has no output handler. Gap: add an optional `RawOutputHandler` recorder to `drive` in a follow-up task (or plan C); recorded here, not silently dropped. Ids — erratum 2. Probe queries — the driver projects instead; a `--probe` query list is a natural follow-up.
- §4.3–4.6 diff / attribute / confirm → Tasks 6, 7, 8. ✔
- §5.8 projection hook → Task 4; KV → Task 11. ✔
- §5.9 regression corpus → Task 12. ✔
- §6.5.1 bug fix flow → `--around` + §5.9; no separate task needed. ✔

**Placeholder scan:** the driver test in Task 5 had a malformed assertion, replaced inline with the `starts_with` form; Task 10's first test was replaced with `genesis_to_p…` because the single-register SM hides the divergence at the end position. No "TBD"/"similar to" remains; every code step has code.

**Type consistency:** `Corpus::export(instance_dir, app_id, row, origin, end, version, out)` used identically in Tasks 2, 5, 9, 10, 12; `drive(sm, &corpus, Origin)` in 5, 10; `run_replay_cli(sm, &corpus_dir, &out, from_genesis)` in 9, 11; `Declaration::from_toml`, `attribute(&Profile, &Declaration) -> Attributed`, `confirm(&Attributed, &Declaration) -> Verdicts`, `Report::new(mode, corpus, profile, verdicts)` consistent across 7–9. `Surface` lives in `diff.rs` and is re-used by `confirm.rs`. `TimerEvent::new` (Task 4) is what Task 5 calls.
