# UC v2.2 M9 — deployable node (daemon, config file, clean lifecycle) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ship a real `uc2-node` binary that starts from a TOML config file, refuses to start on a misconfiguration instead of failing confusingly later, and stops cleanly on `SIGTERM` so a restart rejoins from the journal instead of paying reconstruction.

**Architecture:** three additive layers over an unchanged core. A `config_file` module deserialises a `deny_unknown_fields` TOML document into the existing `NodeConfig`, so the wire, consensus, and cnc surfaces are untouched. A `preflight` module turns every rule that today produces a confusing downstream failure — non-power-of-two buffer, learner/member id overlap, `bind` disagreeing with this node's own `members` entry, an instance directory on a RAM-backed filesystem — into a named startup refusal, mirroring the boot-refusal posture M8 already established for crypto. A `uc2-node` binary wires those to `Node::start`, installs a `SIGTERM`/`SIGINT` handler, and on signal drains the archive to a bounded deadline before calling the already-existing `Node::stop()`.

**Tech Stack:** Rust workspace (edition 2024), `uc2_node` (config, preflight, drain, binaries), `serde` + `toml` (config), `signal-hook` (sync signal handling — the node has no tokio and must not gain one), `libc` (filesystem-type probe), `clap` (CLI).

**Spec:** `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §4 (M9), §1 (locked decisions), §11 (risks).

## Global Constraints

Copied from the spec and CLAUDE.md house rules. Every task's requirements implicitly include this section.

- **No consensus, wire-protocol, or cnc-layout changes.** M9 is additive: new modules and binaries plus one new method on `Node`. If a task appears to need a protocol change, stop — it is out of scope (spec §3).
- **No leadership transfer.** A planned leader stop costs one election timeout (150–300 ms). Do not add a `TimeoutNow`-style handoff; it is a consensus change and is explicitly deferred (spec §1).
- **No tokio in `uc2_node`.** The node is four sync single-writer polling agents. Signal handling uses `signal-hook`, not an async runtime.
- **`deny_unknown_fields` on every config struct.** A typo is a startup refusal, never a silently-ignored setting — same posture as the M8 crypto boot refusal.
- **A shutdown that hangs is worse than one that costs a replay.** The drain is deadline-bounded; on expiry, log what was left and stop anyway.
- **Apply stays sync, deterministic, no I/O.** Untouched by this milestone; do not route config through the apply path.
- **`clippy --workspace --all-targets -- -D warnings` stays clean** (CI enforces it).
- **Journals/instance dirs never on `/tmp` for load runs** (RAM tmpfs, no swap — see CLAUDE.md "Local box"). Unit tests may use `tempfile::tempdir()`; journal-bearing integration tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))` (the `failover.rs` pattern).
- **Stage only your own files** — never `git add -A`. Branch: `uc2/m9-deployable-node`. Stage `Cargo.lock` explicitly and name it when touched.
- **Honest gates:** the gate binary prints the bar and `exit(1)` on FAIL. "Task 8 complete" ≠ "M9 gate passed" — the gate run is a separate, user-approved step (M1–M7 precedent).

## File Structure

| File | Responsibility |
|---|---|
| `uc2_node/src/config_file.rs` (new) | TOML mirror of `NodeConfig` + `load_from_path`. Deserialisation only — no validation, no I/O beyond reading the file. |
| `uc2_node/src/preflight.rs` (new) | Pure validation rules over a built `NodeConfig`, plus the filesystem-type probe. One named error per rule. |
| `uc2_node/src/bin/uc2-node.rs` (new) | The daemon: CLI, load, preflight, start, signal wait, drain, stop. |
| `uc2_node/src/node.rs` (modify) | Add `Node::stop_draining`. Nothing else. |
| `uc2_node/tests/lifecycle.rs` (new) | Integration: drain-on-stop, restart-without-snapshot-install. |
| `uc2_node/examples/m9_gate.rs` (new) | The pre-committed restart-cost gate. |
| `examples/counter/src/bin/counter-service.rs` (modify) | Becomes the documented service template: config file + `SIGTERM` → `Service::stop()`. |
| `packaging/systemd/*.service` (new) | Unit files referencing the real binaries. |

## As-built anchor map (read these before your task)

| Seam | Where |
|---|---|
| `NodeConfig` fields | `uc2_node/src/node.rs:149` — `id`, `members`, `learners`, `bind`, `instance_dir`, `app_id`, `buffer_bytes`, `max_payload`, `admission_bytes`, `election_timeout_min_ns`, `election_timeout_max_ns`, `seed`, `faults`, `purge`, `journal_segment_bytes`, `crypto` |
| Node start / stop | `uc2_node/src/node.rs:404` `Node::start(cfg) -> io::Result<Node>`; `:1402` `stop(self)` (signals + joins all four agents); `:1408` `crash(self)` (no flush) |
| Agent lifecycle | `uc2_log/src/agent.rs` — `AgentRunner::spawn/stop/is_finished`, `IdleStrategy`. `stop()` sets an `AtomicBool` and joins; the loop exits at the TOP of a duty cycle, which is why an explicit drain is needed |
| Log counters (drain reads these) | `Node::counters() -> &LogCounters` (`node.rs:1211`); cnc offsets `append` 256, `durable` 320, `commit` 448, `service_applied` 512 (`docs/reference/cnc-page.md`) |
| Snapshot-install evidence | cnc `incoming_snapshot_pos` at offset 1280 — the gate asserts this does NOT move across a clean restart |
| `PurgePolicy` | `uc2_node/src/node.rs:139` — `Disabled` \| `BelowSnapshot { slack_bytes: u64 }` |
| `CryptoConfig` | `uc2_crypto/src/transport.rs:166` — `Disabled` \| `Enabled { key_path: PathBuf, allowlist_path: PathBuf, rotation: RotationPolicy }` |
| `RotationPolicy` | `uc2_crypto/src/rotation.rs:78` — `{ interval_ns: u64, bytes: u64 }`; `Default` = 1 h / 1 TiB |
| `NodeId` | `uc2_consensus/src/election.rs:34` — `pub type NodeId = u32` |
| Journal segment default | `uc2_node/src/node.rs:211` — `DEFAULT_JOURNAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024` |
| Daemon template to replace | `examples/counter/src/bin/counter-node.rs` — clap args, `seed_for()` (vote-split avoidance), then `loop { sleep(100ms) }` with NO signal handler. Lift `seed_for` into the daemon |
| Volatile-fs rule to mirror | `bench-infra/scripts/m6_fleet_gate.py:119` `assert_durable_fs` — refuses empty/unknown fstype and anything in `VOLATILE_FS` |
| Durable vs volatile paths | `docs/reference/instance-directory.md:31` — durable: `journal/`, `state/`, `snapshots/`; volatile-safe: `cnc2.dat`, `log.buf`, `*.ring`, `*.broadcast` |
| Service lifecycle | `uc2_service/src/lib.rs:100` `start`, `:307` `is_alive`, `:314` `stop` |
| Integration test helpers | `uc2_node/tests/failover.rs` — `spawn_cluster_ring`, `NodeH`, `make_config_ring`, `DEFAULT_RING`; `tempdir_in(env!("CARGO_TARGET_TMPDIR"))` |

---

### Task 1: Config file schema and loader

**Files:**
- Create: `uc2_node/src/config_file.rs`
- Modify: `uc2_node/src/lib.rs` (add `pub mod config_file;` and re-export `load_from_path`)
- Modify: `Cargo.toml` (workspace deps), `uc2_node/Cargo.toml`
- Test: inline `#[cfg(test)] mod tests` in `config_file.rs`

**Interfaces:**
- Consumes: `NodeConfig`, `PurgePolicy` (`uc2_node::node`), `CryptoConfig`/`RotationPolicy` (`uc2_crypto`), `FaultConfig` (`uc2_net::fault`).
- Produces: `pub fn load_from_path(path: &Path) -> Result<NodeConfig, ConfigError>` and `pub enum ConfigError { Read { path: PathBuf, source: std::io::Error }, Parse { path: PathBuf, source: toml::de::Error } }`. Task 3 and Task 5 both call `load_from_path`.

- [ ] **Step 1: Add the dependencies**

Add to the workspace `[workspace.dependencies]` in `Cargo.toml`. Follow the house convention of recording verification date, as the M8 crypto deps do:

```toml
# M9 daemon. Versions verified against crates.io 2026-08-19.
toml = "0.9"
signal-hook = "0.3"
libc = "0.2"
```

Add to `uc2_node/Cargo.toml` under `[dependencies]`:

```toml
serde = { workspace = true }
toml = { workspace = true }
libc = { workspace = true }
signal-hook = { workspace = true }
clap = { workspace = true }
```

- [ ] **Step 2: Verify the versions resolve**

Run: `cargo build -p uc2_node`
Expected: builds. If the resolver rejects a version, take the current major from `cargo search toml` and update the pin — do not leave a version that does not resolve.

- [ ] **Step 3: Write the failing test**

Create `uc2_node/src/config_file.rs` with only this test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
        let p = dir.join("node.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn minimal_config_maps_to_node_config_with_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"

[[members]]
id = 2
addr = "10.0.0.2:9100"
"#,
        );
        let cfg = load_from_path(&p).unwrap();
        assert_eq!(cfg.id, 1);
        assert_eq!(cfg.members.len(), 2);
        assert_eq!(cfg.app_id, "myapp");
        // Defaults that must NOT require the operator to state them.
        assert_eq!(cfg.buffer_bytes, 1 << 26);
        assert_eq!(cfg.journal_segment_bytes, crate::DEFAULT_JOURNAL_SEGMENT_BYTES);
        assert!(matches!(cfg.purge, PurgePolicy::Disabled));
        assert!(matches!(cfg.crypto, CryptoConfig::Disabled));
        assert!(cfg.learners.is_empty());
    }

    #[test]
    fn unknown_field_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"
buffer_bytez = 4096

[[members]]
id = 1
addr = "10.0.0.1:9100"
"#,
        );
        let err = load_from_path(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("buffer_bytez"), "error must name the typo, got: {msg}");
    }

    #[test]
    fn seed_defaults_per_node_and_differs_between_ids() {
        // Identical seeds livelock a cluster through vote splits — see
        // counter-node.rs's seed_for comment. The daemon must not make the
        // operator know this.
        let a = default_seed_for(1);
        let b = default_seed_for(2);
        assert_ne!(a, b);
    }

    #[test]
    fn purge_and_crypto_sections_map_to_their_enums() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"

[purge]
below_snapshot_slack_bytes = 1048576

[crypto]
key_path = "/etc/uc2/node.key"
allowlist_path = "/etc/uc2/allowlist.toml"
"#,
        );
        let cfg = load_from_path(&p).unwrap();
        assert!(matches!(cfg.purge, PurgePolicy::BelowSnapshot { slack_bytes: 1048576 }));
        match cfg.crypto {
            CryptoConfig::Enabled { ref key_path, ref allowlist_path, rotation } => {
                assert_eq!(key_path.to_str().unwrap(), "/etc/uc2/node.key");
                assert_eq!(allowlist_path.to_str().unwrap(), "/etc/uc2/allowlist.toml");
                // Unstated rotation takes the crate default (1 h / 1 TiB).
                assert_eq!(rotation.interval_ns, 3_600_000_000_000);
            }
            _ => panic!("crypto section must produce CryptoConfig::Enabled"),
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cargo test -p uc2_node --lib config_file`
Expected: FAIL — `load_from_path` and `default_seed_for` are not defined.

- [ ] **Step 5: Write the implementation**

Prepend to `uc2_node/src/config_file.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! TOML configuration for the `uc2-node` daemon.
//!
//! A one-to-one mirror of [`NodeConfig`], deserialised with
//! `deny_unknown_fields` so a typo is a startup refusal rather than a
//! silently-ignored setting — the same posture as M8's crypto boot refusal.
//! This module does deserialisation ONLY; every semantic rule lives in
//! [`crate::preflight`].

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use uc2_consensus::election::NodeId;
use uc2_crypto::rotation::RotationPolicy;
use uc2_net::fault::FaultConfig;

use crate::{CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, NodeConfig, PurgePolicy};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("invalid config file {path}: {source}")]
    Parse { path: PathBuf, source: toml::de::Error },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    id: NodeId,
    addr: SocketAddr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PurgeSection {
    below_snapshot_slack_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CryptoSection {
    key_path: PathBuf,
    allowlist_path: PathBuf,
    #[serde(default)]
    rotation_interval_ns: Option<u64>,
    #[serde(default)]
    rotation_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeConfigFile {
    id: NodeId,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
    members: Vec<Member>,
    #[serde(default)]
    learners: Vec<Member>,
    #[serde(default = "default_buffer_bytes")]
    buffer_bytes: usize,
    #[serde(default = "default_max_payload")]
    max_payload: usize,
    #[serde(default = "default_admission_bytes")]
    admission_bytes: u64,
    #[serde(default = "default_election_min_ns")]
    election_timeout_min_ns: u64,
    #[serde(default = "default_election_max_ns")]
    election_timeout_max_ns: u64,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default = "default_journal_segment_bytes")]
    journal_segment_bytes: u64,
    #[serde(default)]
    purge: Option<PurgeSection>,
    #[serde(default)]
    crypto: Option<CryptoSection>,
}

fn default_buffer_bytes() -> usize {
    1 << 26 // 64 MiB — a production default, not the examples' 4 MiB
}
fn default_max_payload() -> usize {
    1 << 20
}
fn default_admission_bytes() -> u64 {
    256 * 1024
}
fn default_election_min_ns() -> u64 {
    150_000_000
}
fn default_election_max_ns() -> u64 {
    300_000_000
}
fn default_journal_segment_bytes() -> u64 {
    DEFAULT_JOURNAL_SEGMENT_BYTES
}

/// Per-node election-timeout seed.
///
/// Lifted verbatim from `examples/counter/src/bin/counter-node.rs`: without a
/// distinct stream per node every member runs the identical randomised
/// sequence, times out at the same instant, splits the vote, and the cluster
/// livelocks instead of electing anyone. The daemon must not require the
/// operator to know this, so `seed` is optional and defaults to this.
pub fn default_seed_for(id: NodeId) -> u64 {
    1 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Read and deserialise a node config file. Performs NO validation beyond what
/// the type system enforces — call [`crate::preflight::check`] next.
pub fn load_from_path(path: &Path) -> Result<NodeConfig, ConfigError> {
    let text = std::fs::read_to_string(path)
        .map_err(|source| ConfigError::Read { path: path.to_path_buf(), source })?;
    let f: NodeConfigFile = toml::from_str(&text)
        .map_err(|source| ConfigError::Parse { path: path.to_path_buf(), source })?;

    let purge = match f.purge {
        Some(p) => PurgePolicy::BelowSnapshot { slack_bytes: p.below_snapshot_slack_bytes },
        None => PurgePolicy::Disabled,
    };
    let crypto = match f.crypto {
        Some(c) => {
            let d = RotationPolicy::default();
            CryptoConfig::Enabled {
                key_path: c.key_path,
                allowlist_path: c.allowlist_path,
                rotation: RotationPolicy {
                    interval_ns: c.rotation_interval_ns.unwrap_or(d.interval_ns),
                    bytes: c.rotation_bytes.unwrap_or(d.bytes),
                },
            }
        }
        None => CryptoConfig::Disabled,
    };

    Ok(NodeConfig {
        id: f.id,
        members: f.members.into_iter().map(|m| (m.id, m.addr)).collect(),
        learners: f.learners.into_iter().map(|m| (m.id, m.addr)).collect(),
        bind: f.bind,
        instance_dir: f.instance_dir,
        app_id: f.app_id,
        buffer_bytes: f.buffer_bytes,
        max_payload: f.max_payload,
        admission_bytes: f.admission_bytes,
        election_timeout_min_ns: f.election_timeout_min_ns,
        election_timeout_max_ns: f.election_timeout_max_ns,
        seed: f.seed.unwrap_or_else(|| default_seed_for(f.id)),
        faults: FaultConfig::default(),
        purge,
        journal_segment_bytes: f.journal_segment_bytes,
        crypto,
    })
}
```

Add `pub mod config_file;` to `uc2_node/src/lib.rs`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --lib config_file`
Expected: 4 tests PASS.

- [ ] **Step 7: Lint**

Run: `cargo clippy -p uc2_node --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add uc2_node/src/config_file.rs uc2_node/src/lib.rs uc2_node/Cargo.toml Cargo.toml Cargo.lock
git commit -m "feat(uc2_node): TOML config file for the daemon (deny_unknown_fields, per-node seed default)"
```

---

### Task 2: Preflight validation rules

**Files:**
- Create: `uc2_node/src/preflight.rs`
- Modify: `uc2_node/src/lib.rs` (add `pub mod preflight;`)
- Test: inline `#[cfg(test)] mod tests` in `preflight.rs`

**Interfaces:**
- Consumes: `NodeConfig` from Task 1.
- Produces: `pub fn check(cfg: &NodeConfig) -> Result<(), PreflightError>` and `pub enum PreflightError`. Task 3 adds one variant to the same enum; Task 5 calls `check`.

Each rule below exists because it currently produces a failure that looks like something else. The `bind` rule is the sharpest: `docs/how-to/run-a-cluster.md` documents that a mismatch elects a leader whose followers never advance `durable` or `commit`, because datagrams arrive from a source address matching no member entry.

- [ ] **Step 1: Write the failing tests**

Create `uc2_node/src/preflight.rs` with only this test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn base() -> NodeConfig {
        NodeConfig {
            id: 1,
            members: vec![
                (1, "10.0.0.1:9100".parse::<SocketAddr>().unwrap()),
                (2, "10.0.0.2:9100".parse::<SocketAddr>().unwrap()),
            ],
            learners: Vec::new(),
            bind: "10.0.0.1:9100".parse().unwrap(),
            instance_dir: std::path::PathBuf::from("/srv/uc2/n1"),
            app_id: "myapp".into(),
            buffer_bytes: 1 << 26,
            max_payload: 1 << 20,
            admission_bytes: 256 * 1024,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 7,
            faults: Default::default(),
            purge: PurgePolicy::Disabled,
            journal_segment_bytes: crate::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: CryptoConfig::Disabled,
        }
    }

    #[test]
    fn a_valid_config_passes() {
        assert!(check_semantics(&base()).is_ok());
    }

    #[test]
    fn buffer_bytes_must_be_power_of_two() {
        let mut c = base();
        c.buffer_bytes = (1 << 26) + 1;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("buffer_bytes"), "got: {msg}");
    }

    #[test]
    fn max_payload_must_fit_the_buffer() {
        let mut c = base();
        c.max_payload = c.buffer_bytes;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("max_payload"), "got: {msg}");
    }

    #[test]
    fn own_id_must_appear_in_members_or_learners() {
        let mut c = base();
        c.id = 99;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("99"), "error must name the id, got: {msg}");
    }

    #[test]
    fn learner_ids_must_be_disjoint_from_members() {
        let mut c = base();
        c.learners = vec![(2, "10.0.0.9:9100".parse().unwrap())];
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("learners"), "got: {msg}");
    }

    #[test]
    fn bind_must_equal_this_nodes_own_member_entry() {
        // The failure this prevents: a leader elects, but followers never
        // advance durable/commit, because datagrams arrive from a source
        // address matching no member entry. See how-to/run-a-cluster.md.
        let mut c = base();
        c.bind = "0.0.0.0:9100".parse().unwrap();
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("bind"), "got: {msg}");
        assert!(msg.contains("10.0.0.1:9100"), "error must show the expected addr, got: {msg}");
    }

    #[test]
    fn duplicate_member_ids_are_refused() {
        let mut c = base();
        c.members.push((1, "10.0.0.3:9100".parse().unwrap()));
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("duplicate"), "got: {msg}");
    }

    #[test]
    fn total_membership_is_capped_at_eight() {
        let mut c = base();
        c.members = (1..=9)
            .map(|i| (i, format!("10.0.0.{i}:9100").parse().unwrap()))
            .collect();
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains('8'), "got: {msg}");
    }

    #[test]
    fn election_window_must_be_ordered() {
        let mut c = base();
        c.election_timeout_min_ns = c.election_timeout_max_ns + 1;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("election_timeout"), "got: {msg}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc2_node --lib preflight`
Expected: FAIL — `check_semantics` is not defined.

- [ ] **Step 3: Write the implementation**

Prepend to `uc2_node/src/preflight.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Startup refusals.
//!
//! Every rule here exists because the misconfiguration it catches currently
//! fails LATER and looks like something else. A node that refuses to start
//! with a message naming the offending field is strictly better than a
//! cluster that elects a leader and never commits.

use uc2_consensus::election::NodeId;

use crate::{CryptoConfig, NodeConfig, PurgePolicy};

/// The cnc PeerSlots band holds 8 entries; voters + learners share it.
const MAX_MEMBERS: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("buffer_bytes must be a power of two, got {0}")]
    BufferNotPowerOfTwo(usize),
    #[error("max_payload ({max_payload}) must be well below buffer_bytes ({buffer_bytes})")]
    PayloadTooLarge { max_payload: usize, buffer_bytes: usize },
    #[error("this node's id ({0}) appears in neither members nor learners")]
    SelfNotAMember(NodeId),
    #[error("learners and members must be disjoint; id {0} appears in both")]
    LearnerIsAlsoMember(NodeId),
    #[error(
        "bind ({bind}) must be identical to this node's own members entry ({expected}) — \
         not a wildcard, not 0.0.0.0; a mismatch elects a leader whose followers never commit"
    )]
    BindMismatch { bind: String, expected: String },
    #[error("duplicate id {0} in members/learners")]
    DuplicateId(NodeId),
    #[error("cluster has {0} total members (voters + learners); the hard cap is 8")]
    TooManyMembers(usize),
    #[error("election_timeout_min_ns ({min}) must be < election_timeout_max_ns ({max})")]
    ElectionWindow { min: u64, max: u64 },
}

/// Pure semantic checks over a built config. Filesystem checks are separate
/// (Task 3) so this stays unit-testable without touching disk.
pub fn check_semantics(cfg: &NodeConfig) -> Result<(), PreflightError> {
    if !cfg.buffer_bytes.is_power_of_two() {
        return Err(PreflightError::BufferNotPowerOfTwo(cfg.buffer_bytes));
    }
    if cfg.max_payload * 4 > cfg.buffer_bytes {
        return Err(PreflightError::PayloadTooLarge {
            max_payload: cfg.max_payload,
            buffer_bytes: cfg.buffer_bytes,
        });
    }
    if cfg.election_timeout_min_ns >= cfg.election_timeout_max_ns {
        return Err(PreflightError::ElectionWindow {
            min: cfg.election_timeout_min_ns,
            max: cfg.election_timeout_max_ns,
        });
    }

    let mut seen = std::collections::HashSet::new();
    for (id, _) in cfg.members.iter().chain(cfg.learners.iter()) {
        if !seen.insert(*id) {
            // A learner id colliding with a member id is the more specific
            // (and more confusing) case, so name it as such.
            if cfg.members.iter().any(|(m, _)| m == id)
                && cfg.learners.iter().any(|(l, _)| l == id)
            {
                return Err(PreflightError::LearnerIsAlsoMember(*id));
            }
            return Err(PreflightError::DuplicateId(*id));
        }
    }
    if seen.len() > MAX_MEMBERS {
        return Err(PreflightError::TooManyMembers(seen.len()));
    }

    let own = cfg
        .members
        .iter()
        .chain(cfg.learners.iter())
        .find(|(id, _)| *id == cfg.id)
        .ok_or(PreflightError::SelfNotAMember(cfg.id))?;
    if own.1 != cfg.bind {
        return Err(PreflightError::BindMismatch {
            bind: cfg.bind.to_string(),
            expected: own.1.to_string(),
        });
    }
    Ok(())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --lib preflight`
Expected: 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add uc2_node/src/preflight.rs uc2_node/src/lib.rs
git commit -m "feat(uc2_node): preflight semantic validation with named startup refusals"
```

---

### Task 3: Refuse to start on a RAM-backed filesystem

**Files:**
- Modify: `uc2_node/src/preflight.rs`
- Modify: `uc2_node/src/config_file.rs` (add the override field; change `load_from_path`'s return type)
- Test: inline tests in `preflight.rs` and `config_file.rs`

**Interfaces:**
- Consumes: `NodeConfig`, `check_semantics` (Task 2).
- Produces:
  - `pub struct StartupOptions { pub allow_volatile_fs: bool }`
  - `pub enum FsVerdict { Durable, VolatileOverridden { fs: String } }`
  - `pub fn check(cfg: &NodeConfig, opts: &StartupOptions) -> Result<FsVerdict, PreflightError>`
  - **Amended from Task 1:** `config_file::load_from_path` now returns
    `Result<(NodeConfig, StartupOptions), ConfigError>`.
- Task 5 calls `check` (not `check_semantics`), and is responsible for PRINTING the
  warning when the verdict is `VolatileOverridden`.

`docs/how-to/run-a-cluster.md` states that an instance directory on `tmpfs` makes every `fsync` a silent no-op: "the cluster will appear to work and will lose committed data on power loss." `bench-infra/scripts/m6_fleet_gate.py:119` already refuses this for gates. The node does not. This task closes that.

**The override is deliberately two-channel, and neither channel is silent.**

- **`allow_volatile_fs = true` in the config file** is the explicit, reviewable
  channel — an operator or test harness states the exception in the same file
  they state everything else, and it shows up in a config diff.
- **`UC2_ALLOW_VOLATILE_FS=1`** stays for suites that build a `NodeConfig`
  directly and never parse a config file.
- **Either channel produces a loud warning at startup, every boot** — the
  override suppresses the *refusal*, never the *notice*. A cluster running on a
  RAM-backed filesystem must never look healthy and quiet.

The warning is not printed by `preflight`. `check` RETURNS `FsVerdict::VolatileOverridden`, and Task 5's daemon prints it. That keeps `preflight` pure and, more importantly, makes the override path assertable in a unit test instead of requiring stderr capture.

- [ ] **Step 1: Write the failing tests**

Add to `preflight.rs`'s test module. Note the shared mutex: `set_var`/`remove_var` are process-global and cargo runs tests in parallel threads, so without it the override test intermittently makes the refusal test pass spuriously.

```rust
    /// `UC2_ALLOW_VOLATILE_FS` is process-global and cargo runs tests in
    /// parallel threads. Every test that reads or writes it takes this first.
    static FS_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn no_override() -> StartupOptions {
        StartupOptions { allow_volatile_fs: false }
    }

    #[test]
    fn a_real_disk_directory_passes_the_fs_check() {
        let _g = FS_ENV_LOCK.lock().unwrap();
        // CARGO_TARGET_TMPDIR is on ext4/APFS, not tmpfs (CLAUDE.md house rule).
        let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
        let v = check_durable_fs(dir.path(), &no_override()).unwrap();
        assert!(matches!(v, FsVerdict::Durable));
    }

    #[test]
    fn a_missing_instance_dir_parent_is_refused() {
        let _g = FS_ENV_LOCK.lock().unwrap();
        let msg = check_durable_fs(std::path::Path::new("/nonexistent-xyzzy/n1"), &no_override())
            .unwrap_err()
            .to_string();
        assert!(msg.contains("nonexistent-xyzzy"), "got: {msg}");
    }

    #[test]
    fn the_config_override_bypasses_the_check_but_reports_it() {
        let _g = FS_ENV_LOCK.lock().unwrap();
        let opts = StartupOptions { allow_volatile_fs: true };
        // The override must suppress the REFUSAL without suppressing the
        // NOTICE: a cluster on a RAM-backed fs must never look quiet.
        let v = check_durable_fs(std::path::Path::new("/nonexistent-xyzzy/n1"), &opts).unwrap();
        match v {
            FsVerdict::VolatileOverridden { ref fs } => {
                assert!(!fs.is_empty(), "the verdict must name what was overridden");
            }
            FsVerdict::Durable => panic!("an overridden check must report VolatileOverridden, \
                                          not Durable — otherwise the daemon prints nothing"),
        }
    }

    #[test]
    fn the_env_override_also_bypasses_the_check_and_reports_it() {
        let _g = FS_ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("UC2_ALLOW_VOLATILE_FS", "1") };
        let v = check_durable_fs(std::path::Path::new("/nonexistent-xyzzy/n1"), &no_override());
        unsafe { std::env::remove_var("UC2_ALLOW_VOLATILE_FS") };
        assert!(
            matches!(v, Ok(FsVerdict::VolatileOverridden { .. })),
            "the env channel must behave exactly like the config channel"
        );
    }
```

Add to `config_file.rs`'s test module:

```rust
    #[test]
    fn allow_volatile_fs_defaults_to_false_and_is_settable() {
        let dir = tempfile::tempdir().unwrap();
        let body = r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"
"#;
        let p = write(dir.path(), body);
        let (_cfg, opts) = load_from_path(&p).unwrap();
        assert!(!opts.allow_volatile_fs, "production default must be refuse");

        let p2 = write(dir.path(), &format!("{body}allow_volatile_fs = true\n"));
        let (_cfg, opts2) = load_from_path(&p2).unwrap();
        assert!(opts2.allow_volatile_fs);
    }
```

You must also update Task 1's existing `config_file.rs` tests for the new tuple return — they currently write `let cfg = load_from_path(&p).unwrap();`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc2_node --lib preflight config_file`
Expected: FAIL — `StartupOptions`, `FsVerdict`, and `check_durable_fs` are not defined; the Task 1 tests fail to compile against the new tuple return.

- [ ] **Step 3: Write the implementation**

Add to `preflight.rs`:

```rust
/// Startup policy that is NOT part of the node's runtime configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct StartupOptions {
    /// Permit an instance directory on a RAM-backed filesystem.
    ///
    /// TEST AND DEVELOPMENT ONLY. Every `fsync` on such a filesystem is a
    /// silent no-op, so a cluster configured this way will appear to work and
    /// will lose committed data on power loss. Setting this never silences the
    /// startup warning — see [`FsVerdict::VolatileOverridden`].
    pub allow_volatile_fs: bool,
}

/// What the durability probe concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsVerdict {
    /// The instance directory is on a filesystem whose `fsync` is real.
    Durable,
    /// It is NOT, and an override let the node start anyway. The caller MUST
    /// warn — the override suppresses the refusal, never the notice.
    VolatileOverridden { fs: String },
}

/// Filesystem magics that mean "this fsync is a lie".
#[cfg(target_os = "linux")]
const VOLATILE_MAGICS: &[i64] = &[
    0x0102_1994, // TMPFS_MAGIC
    0x8584_58f6, // RAMFS_MAGIC
];
```

Extend the error enum with two variants:

```rust
    #[error("instance_dir {path} does not exist or cannot be probed: {detail}")]
    InstanceDirUnprobeable { path: String, detail: String },
    #[error(
        "instance_dir {path} is on a RAM-backed filesystem ({fs}) — every fsync there is a \
         silent no-op, so the cluster will appear to work and lose committed data on power \
         loss. Put it on a real disk. For tests only, set allow_volatile_fs = true in the \
         config file (or UC2_ALLOW_VOLATILE_FS=1); the node will then start and warn on \
         every boot."
    )]
    VolatileFilesystem { path: String, fs: String },
```

And the probe:

```rust
/// Refuse an instance directory whose fsync is a no-op.
///
/// Mirrors `bench-infra/scripts/m6_fleet_gate.py:assert_durable_fs`, which has
/// refused this for gate runs since M6 — the node itself never did.
///
/// An override does not skip the probe: it downgrades the refusal to a
/// [`FsVerdict::VolatileOverridden`] the caller is obliged to warn about.
pub fn check_durable_fs(
    instance_dir: &std::path::Path,
    opts: &StartupOptions,
) -> Result<FsVerdict, PreflightError> {
    let overridden =
        opts.allow_volatile_fs || std::env::var_os("UC2_ALLOW_VOLATILE_FS").is_some();

    // Probe the nearest existing ancestor: the instance dir itself may not
    // exist yet on a first boot, but its parent must.
    let mut probe = instance_dir;
    while !probe.exists() {
        match probe.parent() {
            Some(p) => probe = p,
            None => {
                let detail = "no existing ancestor directory".to_string();
                return degrade(
                    overridden,
                    PreflightError::InstanceDirUnprobeable {
                        path: instance_dir.display().to_string(),
                        detail,
                    },
                );
            }
        }
    }

    match fs_kind(probe) {
        Ok(FsKind::Durable) => Ok(FsVerdict::Durable),
        Ok(FsKind::Volatile(fs)) => degrade(
            overridden,
            PreflightError::VolatileFilesystem {
                path: instance_dir.display().to_string(),
                fs,
            },
        ),
        Err(e) => degrade(overridden, e),
    }
}

/// One place decides what an override does to a durability failure: it becomes
/// a verdict the caller must announce, never a silent success.
fn degrade(overridden: bool, err: PreflightError) -> Result<FsVerdict, PreflightError> {
    if overridden {
        Ok(FsVerdict::VolatileOverridden { fs: err.to_string() })
    } else {
        Err(err)
    }
}

enum FsKind {
    Durable,
    Volatile(String),
}

#[cfg(target_os = "linux")]
fn fs_kind(path: &std::path::Path) -> Result<FsKind, PreflightError> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|e| {
        PreflightError::InstanceDirUnprobeable {
            path: path.display().to_string(),
            detail: e.to_string(),
        }
    })?;
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path; `buf` is a zeroed statfs we
    // own for the duration of the call.
    if unsafe { libc::statfs(c.as_ptr(), &mut buf) } != 0 {
        return Err(PreflightError::InstanceDirUnprobeable {
            path: path.display().to_string(),
            detail: std::io::Error::last_os_error().to_string(),
        });
    }
    let magic = buf.f_type as i64;
    if VOLATILE_MAGICS.contains(&magic) {
        return Ok(FsKind::Volatile(format!("magic {magic:#x}")));
    }
    Ok(FsKind::Durable)
}

#[cfg(not(target_os = "linux"))]
fn fs_kind(path: &std::path::Path) -> Result<FsKind, PreflightError> {
    // macOS and friends: the fleet is Linux, and a dev box that cannot be
    // probed is not worth a false refusal. Existence is still required, so a
    // typo'd path is caught.
    if path.exists() {
        Ok(FsKind::Durable)
    } else {
        Err(PreflightError::InstanceDirUnprobeable {
            path: path.display().to_string(),
            detail: "path does not exist".into(),
        })
    }
}

/// Full preflight: semantics plus the durability probe.
pub fn check(
    cfg: &NodeConfig,
    opts: &StartupOptions,
) -> Result<FsVerdict, PreflightError> {
    check_semantics(cfg)?;
    check_durable_fs(&cfg.instance_dir, opts)
}
```

In `config_file.rs`, add the field to `NodeConfigFile` and change the return type:

```rust
    #[serde(default)]
    allow_volatile_fs: bool,
```

```rust
/// Read and deserialise a node config file. Performs NO validation beyond what
/// the type system enforces — call [`crate::preflight::check`] next.
pub fn load_from_path(path: &Path) -> Result<(NodeConfig, StartupOptions), ConfigError> {
```

returning `Ok((NodeConfig { .. }, StartupOptions { allow_volatile_fs: f.allow_volatile_fs }))`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --lib`
Expected: PASS, including Task 1's updated tests.

- [ ] **Step 5: Confirm the existing suites still pass**

Run: `cargo test -p uc2_node`
Expected: PASS. If a suite now fails on the fs probe, set `UC2_ALLOW_VOLATILE_FS=1` for that suite rather than weakening the check — the rule is correct, the test is the exception.

- [ ] **Step 6: Commit**

```bash
cargo clippy -p uc2_node --all-targets -- -D warnings
git add uc2_node/src/preflight.rs uc2_node/src/config_file.rs
git commit -m "feat(uc2_node): refuse an instance_dir on a RAM-backed fs, with a loud two-channel override"
```

---

### Task 4: Drain the archive before stopping

**Files:**
- Modify: `uc2_node/src/node.rs` (add `stop_draining` next to `stop` at :1402)
- Test: `uc2_node/tests/lifecycle.rs` (create)

**Interfaces:**
- Consumes: `Node::counters()` (`node.rs:1211`), `Node::stop()` (`:1402`).
- Produces: `pub fn stop_draining(self, deadline: std::time::Duration) -> DrainOutcome` and `pub enum DrainOutcome { Drained, DeadlineExpired { append: u64, durable: u64 } }`. Task 5 and Task 8 both call it.

`AgentRunner::stop` exits its loop at the TOP of a duty cycle, so bytes appended but not yet recorded by the archive are simply not in the journal at exit. That is safe — they were never durable, so never acked — but it makes the restarted node re-fetch them. Draining first is what buys M9's "rejoins without reconstruction" gate.

- [ ] **Step 1: Write the failing test**

Create `uc2_node/tests/lifecycle.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M9: daemon lifecycle — drain-on-stop and restart cost.

use std::time::Duration;

mod common {
    pub use super::*;
}

#[test]
fn stop_draining_leaves_durable_caught_up_with_append() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = single_node(dir.path());

    append_some_load(&node, 512);

    let c = node.counters();
    assert!(c.append() > 0, "test must actually append something");

    match node.stop_draining(Duration::from_secs(5)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
}

#[test]
fn stop_draining_honours_its_deadline() {
    // A drain that cannot finish must still stop the node. A shutdown that
    // hangs is worse than one that costs a replay.
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = single_node(dir.path());
    append_some_load(&node, 512);
    let outcome = node.stop_draining(Duration::from_nanos(1));
    // Either outcome is legal here — the point is that it RETURNS.
    let _ = outcome;
}
```

Write `single_node` and `append_some_load` as helpers in the same file, modelled on `uc2_node/tests/failover.rs`'s `make_config_ring`/`spawn_cluster_ring`. Read that file first and follow its construction pattern exactly, including `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p uc2_node --test lifecycle`
Expected: FAIL — `stop_draining` and `DrainOutcome` are not defined.

- [ ] **Step 3: Write the implementation**

In `uc2_node/src/node.rs`, immediately after `stop` (:1402):

```rust
/// What a drain achieved before the node stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The archive recorded every appended byte; a restart replays nothing.
    Drained,
    /// The deadline expired first. The node stopped anyway — the un-recorded
    /// tail was never durable, so it was never acked; the restarted node
    /// simply re-fetches it.
    DeadlineExpired { append: u64, durable: u64 },
}

impl Node {
    /// Graceful stop that first waits for the archive to catch up.
    ///
    /// `stop()` signals the agents, and each exits at the top of its next duty
    /// cycle — so appended-but-unrecorded bytes are dropped from the journal.
    /// That is safe (un-recorded means un-acked) but it makes the restarted
    /// node pay reconstruction. Draining first is what makes a planned restart
    /// cheap.
    ///
    /// A shutdown that hangs is worse than one that costs a replay, so the
    /// wait is hard-bounded by `deadline`.
    pub fn stop_draining(self, deadline: std::time::Duration) -> DrainOutcome {
        let start = std::time::Instant::now();
        let outcome = loop {
            let c = self.counters();
            let (append, durable) = (c.append(), c.durable());
            if durable >= append {
                break DrainOutcome::Drained;
            }
            if start.elapsed() >= deadline {
                break DrainOutcome::DeadlineExpired { append, durable };
            }
            std::thread::yield_now();
        };
        self.stop();
        outcome
    }
}
```

Re-export `DrainOutcome` from `uc2_node/src/lib.rs` alongside `Node`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --test lifecycle`
Expected: 2 tests PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc2_node --all-targets -- -D warnings
git add uc2_node/src/node.rs uc2_node/src/lib.rs uc2_node/tests/lifecycle.rs
git commit -m "feat(uc2_node): Node::stop_draining — bounded archive drain before a clean stop"
```

---

### Task 5: The `uc2-node` daemon

**Files:**
- Create: `uc2_node/src/bin/uc2-node.rs`
- Modify: `uc2_node/Cargo.toml` (add the `[[bin]]` target)
- Test: `uc2_node/tests/lifecycle.rs` (add a spawn-and-signal test)

**Interfaces:**
- Consumes: `config_file::load_from_path` (Task 1), `preflight::check` (Task 3), `Node::stop_draining` (Task 4).
- Produces: the `uc2-node` binary, reachable from tests as `env!("CARGO_BIN_EXE_uc2-node")`.

- [ ] **Step 1: Declare the binary target**

In `uc2_node/Cargo.toml`:

```toml
[[bin]]
name = "uc2-node"
path = "src/bin/uc2-node.rs"
```

- [ ] **Step 2: Write the failing test**

Add to `uc2_node/tests/lifecycle.rs`:

```rust
#[test]
fn daemon_starts_from_a_config_file_and_stops_cleanly_on_sigterm() {
    use std::process::Command;

    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let inst = dir.path().join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.path().join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"
id = 1
bind = "127.0.0.1:19701"
instance_dir = "{}"
app_id = "lifecycle"

[[members]]
id = 1
addr = "127.0.0.1:19701"
"#,
            inst.display()
        ),
    )
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .spawn()
        .unwrap();

    // Give it time to elect itself in a one-node cluster.
    std::thread::sleep(Duration::from_millis(1500));

    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };

    let start = std::time::Instant::now();
    let status = child.wait().unwrap();
    let elapsed = start.elapsed();

    assert!(status.success(), "clean shutdown must exit 0, got {status:?}");
    assert!(elapsed < Duration::from_secs(1), "SIGTERM to exit took {elapsed:?}, bar is < 1s");
}

#[test]
fn daemon_refuses_a_config_with_a_bind_mismatch() {
    use std::process::Command;
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let inst = dir.path().join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.path().join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"
id = 1
bind = "0.0.0.0:19702"
instance_dir = "{}"
app_id = "lifecycle"

[[members]]
id = 1
addr = "127.0.0.1:19702"
"#,
            inst.display()
        ),
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .output()
        .unwrap();
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("bind"), "refusal must name the field, got: {err}");
}
```

Add `libc = { workspace = true }` to `uc2_node`'s `[dev-dependencies]` if it is not already a normal dependency.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p uc2_node --test lifecycle`
Expected: FAIL — no `uc2-node` binary.

- [ ] **Step 4: Write the daemon**

Create `uc2_node/src/bin/uc2-node.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `ultima_cluster` node daemon.
//!
//! Starts one node from a TOML config file and runs until signalled. On
//! `SIGTERM`/`SIGINT` it drains the archive to a bounded deadline and stops
//! the agents cleanly, so the restarted node rejoins from the journal instead
//! of paying reconstruction.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::Parser;
use uc2_node::{DrainOutcome, Node, config_file, preflight};

#[derive(Parser)]
#[command(name = "uc2-node", about = "An ultima_cluster node")]
struct Args {
    /// Path to the node's TOML configuration file.
    #[arg(long)]
    config: PathBuf,
    /// How long to let the archive drain before stopping anyway.
    #[arg(long, default_value = "5")]
    drain_timeout_secs: u64,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let cfg = match config_file::load_from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("uc2-node: {e}");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = preflight::check(&cfg) {
        eprintln!("uc2-node: refusing to start: {e}");
        return ExitCode::from(2);
    }

    let id = cfg.id;
    let bind = cfg.bind;
    let node = match Node::start(cfg) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("uc2-node: failed to start node {id}: {e}");
            return ExitCode::from(1);
        }
    };
    println!("uc2-node: node {id} listening on {bind}");

    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&stop)) {
            eprintln!("uc2-node: cannot install signal handler: {e}");
            node.stop();
            return ExitCode::from(1);
        }
    }

    let mut was_leader = None;
    while !stop.load(Ordering::Relaxed) {
        let is_leader = node.is_leader();
        if was_leader != Some(is_leader) {
            println!(
                "uc2-node: node {id} is now {} (term {})",
                if is_leader { "LEADER" } else { "follower" },
                node.current_term()
            );
            was_leader = Some(is_leader);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("uc2-node: signalled, draining");
    match node.stop_draining(Duration::from_secs(args.drain_timeout_secs)) {
        DrainOutcome::Drained => println!("uc2-node: drained, stopped cleanly"),
        DrainOutcome::DeadlineExpired { append, durable } => eprintln!(
            "uc2-node: drain deadline expired with {} bytes unrecorded \
             (append {append}, durable {durable}); stopped anyway — the restarted \
             node will re-fetch them",
            append.saturating_sub(durable)
        ),
    }
    ExitCode::SUCCESS
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --test lifecycle`
Expected: 4 tests PASS. If the SIGTERM test exceeds 1 s, the loop's 100 ms sleep is the floor — that is within budget; investigate the drain, not the poll interval.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy -p uc2_node --all-targets -- -D warnings
git add uc2_node/src/bin/uc2-node.rs uc2_node/Cargo.toml uc2_node/tests/lifecycle.rs Cargo.lock
git commit -m "feat(uc2_node): the uc2-node daemon — config file, preflight, SIGTERM drain-and-stop"
```

---

### Task 6: The `uc2-service` template and its clean stop

**Files:**
- Modify: `examples/counter/src/bin/counter-service.rs`
- Create: `docs/how-to/write-a-service-binary.md`
- Test: `uc2_node/tests/lifecycle.rs` (extend)

**Interfaces:**
- Consumes: `Service::stop()` (`uc2_service/src/lib.rs:314`), `Service::is_alive()` (`:307`).
- Produces: the documented service-binary template. No new library API.

The service half is user code, so the deliverable is a template plus the same signal discipline, not a second daemon. Read `examples/counter/src/bin/counter-service.rs` before editing.

- [ ] **Step 1: Write the failing test**

Add to `uc2_node/tests/lifecycle.rs`:

```rust
#[test]
fn service_template_stops_cleanly_on_sigterm() {
    use std::process::Command;

    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let inst = dir.path().join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.path().join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"
id = 1
bind = "127.0.0.1:19703"
instance_dir = "{}"
app_id = "counter"

[[members]]
id = 1
addr = "127.0.0.1:19703"
"#,
            inst.display()
        ),
    )
    .unwrap();

    let mut node = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(1000));

    let mut svc = Command::new(env!("CARGO_BIN_EXE_counter-service"))
        .arg("--instance-dir")
        .arg(&inst)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(1000));

    unsafe { libc::kill(svc.id() as i32, libc::SIGTERM) };
    let svc_status = svc.wait().unwrap();
    assert!(svc_status.success(), "service must exit 0 on SIGTERM, got {svc_status:?}");

    unsafe { libc::kill(node.id() as i32, libc::SIGTERM) };
    let _ = node.wait().unwrap();
}
```

This test needs `counter-service` on the `CARGO_BIN_EXE_` path, so add `counter` as a dev-dependency of `uc2_node`, or move the test into `examples/counter/tests/`. Prefer the latter — it keeps `uc2_node` free of an example dependency.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p counter --test lifecycle`
Expected: FAIL — the service ignores `SIGTERM` and is killed by the default disposition, so `status.success()` is false.

- [ ] **Step 3: Add the signal discipline to the service template**

In `examples/counter/src/bin/counter-service.rs`, replace the run loop with:

```rust
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, std::sync::Arc::clone(&stop))?;
    }

    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        // A fail-stopped apply thread must not look like a healthy service.
        // `is_alive` is false once the apply agent's work closure has panicked
        // (instance mismatch, log rewind) — exit non-zero so the supervisor
        // restarts us rather than leaving a zombie attached.
        if !service.is_alive() {
            eprintln!("counter-service: apply agent died; exiting for restart");
            return Err(anyhow::anyhow!("apply agent fail-stopped"));
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    service.stop();
    Ok(())
```

Add `signal-hook = { workspace = true }` to `examples/counter/Cargo.toml`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p counter --test lifecycle`
Expected: PASS.

- [ ] **Step 5: Write the template documentation**

Create `docs/how-to/write-a-service-binary.md` showing the full template: implement `StateMachine`, build the service, install the signal flag, poll `is_alive`, `stop()` on signal. Point at `examples/counter/src/bin/counter-service.rs` as the working copy. Add a link from `docs/how-to/README.md`.

- [ ] **Step 6: Commit**

```bash
git add examples/counter docs/how-to/write-a-service-binary.md docs/how-to/README.md Cargo.lock
git commit -m "feat(counter),docs: service binary template — SIGTERM stop + is_alive supervision"
```

---

### Task 7: Packaging and documentation cutover

**Files:**
- Create: `packaging/systemd/uc2-node.service`, `packaging/systemd/uc2-service@.service`
- Create: `packaging/node.example.toml`
- Modify: `docs/QUICKSTART.md`, `docs/how-to/run-a-cluster.md`, `docs/reference/configuration.md`, `README.md`

**Interfaces:**
- Consumes: the `uc2-node` binary (Task 5) and the service template (Task 6).
- Produces: no code interface. This task makes the documentation true.

- [ ] **Step 1: Write the unit files**

`packaging/systemd/uc2-node.service`:

```ini
[Unit]
Description=ultima_cluster node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/uc2-node --config /etc/uc2/node.toml
Restart=on-failure
RestartSec=1
# The node drains the archive on SIGTERM; give it room, but not forever.
TimeoutStopSec=10
KillSignal=SIGTERM
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Note the deliberate difference from today's guidance: `run-a-cluster.md` currently advises `TimeoutStopSec=1` *because* the binaries do not notice `SIGTERM`. With Task 5 they do, so the correct setting is now generous enough to let the drain finish.

- [ ] **Step 2: Write the example config**

`packaging/node.example.toml` — a fully commented three-node example carrying every field with its default shown, including the `bind`-equals-own-members-entry rule as a comment.

- [ ] **Step 3: Update QUICKSTART**

Replace `cargo run -p counter --bin counter-node -- ...` (`docs/QUICKSTART.md:106-108`) with `uc2-node --config nN.toml`, and show the three generated config files. Keep the counter *service* and *client* invocations — those are legitimately example code.

- [ ] **Step 4: Update run-a-cluster.md**

Fix the two now-stale passages: the `TimeoutStopSec=1` advice (Step 1 above) and the "reference binaries are slow to notice SIGTERM" sentence. Replace the `systemd-run` example with the packaged unit. Keep the bind-address section — Task 2 now enforces it, so add a line saying the node refuses to start rather than mis-binding.

- [ ] **Step 5: Update configuration.md and README**

Add the TOML file format to `docs/reference/configuration.md` alongside the struct fields. In `README.md`, change the "Try it" section to install-and-run rather than `cargo run`.

- [ ] **Step 6: Verify every documented command actually runs**

Run each command block in the edited docs verbatim in a scratch directory. The repo's standard is that documented output comes from real runs.

- [ ] **Step 7: Commit**

```bash
git add packaging docs/QUICKSTART.md docs/how-to/run-a-cluster.md docs/reference/configuration.md README.md
git commit -m "docs,packaging: cut over to the uc2-node daemon (systemd units, example config, quickstart)"
```

---

### Task 8: The M9 restart-cost gate

**Files:**
- Create: `uc2_node/examples/m9_gate.rs`
- Create: `docs/benchmarks/uc2-m9-gate-2026-08-19.md` (the decide rule, committed BEFORE the run)

**Interfaces:**
- Consumes: `uc2-node` (Task 5), `Node::stop_draining` (Task 4), cnc `incoming_snapshot_pos` at offset 1280.
- Produces: the gate binary and its record.

House rule: the decide rule and the result are **separate commits, in that order**. Git history is the audit trail.

- [ ] **Step 1: Write the gate document with the bar, and commit it first**

Create `docs/benchmarks/uc2-m9-gate-2026-08-19.md` stating the pre-committed rule, modelled on `docs/benchmarks/uc2-m7-gate-2026-07-13.md`:

| Measure | Bar |
|---|---|
| `SIGTERM` → process exit, leader under load | **< 1 s**, exit code **0** |
| Snapshot install on restart | `incoming_snapshot_pos` **unchanged** across the cycle |
| Cluster commit-rate dip across stop+restart | **< 10 %** (the M6/M7 dip bar) |
| Config refusals | every preflight rule refuses with a message naming the field |

```bash
git add docs/benchmarks/uc2-m9-gate-2026-08-19.md
git commit -m "docs(bench): pre-commit the M9 restart-cost gate decide rule"
```

- [ ] **Step 2: Write the gate harness**

Create `uc2_node/examples/m9_gate.rs` with `node` / `service` / `client` roles, following `uc2_node/examples/m7_gate.rs`'s structure so `bench-infra/scripts/m6_fleet_gate.py` can drive it. The scenario: 3-node cluster under steady load, stop the leader with `SIGTERM`, restart it, measure the four rows above. Print the bar and `exit(1)` on FAIL.

- [ ] **Step 3: Run it locally as a smoke check**

Run: `cargo run -p uc2_node --release --example m9_gate -- all --secs 6`
Expected: PASS. A local run is smoke, never the gate.

- [ ] **Step 4: Commit the harness**

```bash
git add uc2_node/examples/m9_gate.rs
git commit -m "test(m9): restart-cost gate harness (stop <1s, no snapshot install, dip <10%)"
```

- [ ] **Step 5: Stop and hand off**

The fleet run is a **separate, user-approved step** (M1–M7 precedent). Do not tag, do not record a PASS, and do not claim M9 complete. Report that the harness is green locally and the fleet run is pending approval.

---

## Self-review (executed at plan-writing time)

**1. Spec coverage.** Spec §4 (M9) lists: config file → Task 1; validation with named errors → Task 2; `tmpfs` refusal with env override → Task 3; drain + signal handler → Tasks 4 and 5; seed derivation shipped → Task 1 (`default_seed_for`); service template → Task 6; docs stop referencing example binaries → Task 7; acceptance gate → Task 8. Full coverage. Spec §8's standing-hygiene items (toolchain pin, `cargo-deny`, fuzz targets) are deliberately **not** tasks here — they are a parallel track, not M9 scope, and gating M9 on them would couple two independent workstreams.

**2. Placeholder scan.** No TBDs. Two tasks legitimately direct the implementer to read an existing file and follow its pattern rather than reproducing it — Task 4's `single_node` helper (`failover.rs`) and Task 8's gate harness (`m7_gate.rs`). Both are hundreds of lines of existing scaffolding; copying them into the plan would guarantee drift. The interfaces they must produce are stated exactly.

**3. Type consistency.** `load_from_path` (Task 1) → consumed by Task 5 under that name. `check_semantics` (Task 2) is extended by `check` (Task 3); Task 5 calls `check`, not `check_semantics` — stated explicitly in Task 3's interface block. `DrainOutcome` (Task 4) is matched in Task 5 and Task 8 with both variants named. `default_seed_for` is `pub` in Task 1 and tested there. `PreflightError` gains variants in Task 3 rather than being redefined.

**4. Known risk carried from the spec.** Task 3's fs probe is Linux-only by design; the `#[cfg(not(target_os = "linux"))]` arm checks existence only. The fleet is Linux, so the gate's durability guarantee holds where it is measured; a macOS dev box gets a weaker check rather than a false refusal. Flagged here so it is a decision on the record, not an accident.

---

## Execution log

Subagent-driven execution, started 2026-08-19 on branch `uc2/m9-deployable-node`
(from `main` @ `4d5655f`). This section is the durable record of decisions taken
during execution; the SDD scratch ledger under `.superpowers/sdd/` is git-ignored
and does not survive a machine change.

### Status

| Task | State |
|---|---|
| 1 — config file schema and loader | **done** — `a853c60`; re-verified + reviewed on Linux 2026-08-19 (see Ruling 6) |
| 2 — preflight validation | **done** — `1858330`; three deviations from the brief (Ruling 7) |
| 3 — volatile-fs refusal | **done** — `bfdba6f`; four brief defects fixed (Ruling 8) |
| 4 — archive drain | **done** — `e14be43`; contract pinned, limit recorded (Ruling 9) |
| 5 — uc2-node daemon | **done** — `adf72ba`; MTU defect found + fixed (Ruling 10) |
| 6 — service template | **done** — `2e584c0` |
| 7 — packaging + docs cutover | **done** — `0a4b49c`; quickstart executed verbatim |
| 8 — gate harness | **done** — decide rule `71433a9`, harness `a56bcd8`. **FLEET RUN NOT STARTED** (user-approved step) |
| post — spec §4 reserved `[log]`/`[metrics]` sections | **done** — gap found reviewing the branch against the spec (Ruling 14) |

**Environment finding (2026-08-19):** the workspace does not build on macOS.
`uc_protocol/src/ring/common.rs:374` calls `libc::fallocate` with
`FALLOC_FL_PUNCH_HOLE`/`FALLOC_FL_KEEP_SIZE` and **no `cfg` gate** — Linux-only
syscalls in unconditional code, arriving with the punch-hole SIGBUS fix
(`4f544dd`). Verified by building `main` in a clean worktree: three `E0425`
errors. Maintainer ruling: **macOS is not a target at this stage; Linux only.**
The fix was therefore withdrawn rather than applied, and execution moves to a
Linux host. Recorded here because the same unguarded assumption will bite anyone
who tries a non-Linux build, and because it means Task 1's test evidence was
produced on a locally-patched tree and is **not trustworthy as recorded**.

### Rulings

**Ruling 1 (Task 6) — the service-lifecycle test stays inside the `counter`
package** and spawns its node half via `CARGO_BIN_EXE_counter-node`, not
`CARGO_BIN_EXE_uc2-node`. Cargo sets `CARGO_BIN_EXE_<name>` only for the package
that *defines* the binary, so a test in `counter` can never see `uc2-node`; the
plan's own preference (keep `uc2_node` free of an example dependency) is only
satisfiable this way. Task 6 asserts the *service* half's signal handling; the
daemon's is covered by Tasks 5 and 8. *Cost if wrong:* a daemon-only startup
regression is not caught by this particular test.

**Ruling 2 (Task 3) — the filesystem-probe tests share a `static FS_ENV_LOCK`.**
`set_var`/`remove_var` are process-global and cargo runs tests in parallel
threads, so the override test would intermittently make the refusal test pass
spuriously. Adding `serial_test` for three tests was not worth a new dependency.
*Cost if wrong:* test-only complexity; no production code affected.

**Ruling 3 (Task 7) — README's "Try it" section is left on
`cargo run -p counter --bin counter-single`.** Only the three-node instructions
move to `uc2-node --config`. M9 ships no release binaries, so "install and run"
as the headline pre-announces M12's packaging. *Cost if wrong:* README stays
marginally less polished for one milestone.

**Ruling 4 (Task 3) — the volatile-fs override is two explicit channels, neither
silent.** Maintainer direction: refusing a RAM-backed `instance_dir` by default is
correct, but the override must be statable in the config file, and must warn
rather than pass quietly. `allow_volatile_fs = true` is the primary, reviewable
channel; `UC2_ALLOW_VOLATILE_FS=1` remains for suites that build a `NodeConfig`
directly. Either channel downgrades the refusal to an `FsVerdict::VolatileOverridden`
that the daemon announces on every boot — the override suppresses the refusal,
never the notice. `preflight::check` returns the verdict instead of printing it,
which also makes the override path unit-testable without stderr capture.
Task 1's `load_from_path` gains a `StartupOptions` return value, amended as an
explicit step inside Task 3. *Cost if wrong:* one extra tuple element through the
loader.

**Ruling 5 — WITHDRAWN.** Had inserted a prerequisite "Task 0" to restore macOS
buildability by gating the punch-hole to Linux. Withdrawn on maintainer direction
that macOS is not a target. No code was committed for it.

### Rulings (continued)

**Ruling 6 (Task 1) — re-verified and reviewed on Linux; APPROVED as-is.** The
macOS evidence is superseded: `cargo test -p uc2_node --lib` is 71/71 (the
patched-tree record said 69/71), the 4 `config_file` tests pass, and clippy is
clean. Review verdict on the two flagged items: (a) `#[derive(Debug)]` on
`NodeConfig` is safe to keep — `CryptoConfig::Enabled` holds key *paths*, never
key material, so no secret can reach a debug print; (b) `faults` is correctly
absent from the file surface (a sim knob, not an operator one). The rules the
module does not check — duplicate ids, self-not-in-members, non-power-of-two
buffer — are Task 2's by design, and Task 2 now covers all of them.
`load_from_path`'s signature change under Ruling 4 remains pending in Task 3.

**Ruling 7 (Task 2) — three deviations from the brief, all deliberate.**
(a) `MAX_MEMBERS` is imported from `uc_protocol::v2::config`, not redeclared as
a local `8`. The wire crate enforces that cap at config-frame proposal and at
decode; a second copy is a drift hazard of exactly the kind the cnc
offset-pinning convention exists to prevent. A test pins the two together.
(b) The payload rule is `max_payload > buffer_bytes / 4`, not the brief's
`max_payload * 4 > buffer_bytes` — the multiply overflows for a large
`max_payload`, panicking in debug and **wrapping in release**, which would
silently admit the very value the rule exists to refuse. Demonstrated rather
than argued: the added test fails against the brief's formula with "attempt to
multiply with overflow" and passes against the division.
(c) `CryptoConfig`/`PurgePolicy` are imported inside the test module; at file
scope they are unused in a non-test build and fail `clippy -D warnings`.
*Cost if wrong:* (a) an intentional divergence from the wire cap would now need
two edits; (b) and (c) none.

**Ruling 8 (Task 3) — four defects in the brief, all found by running it.**
(a) The probe falls back to the instance dir's PARENT and stops there. The
brief's ancestor walk always reaches `/`, which is on real disk on any normal
host, so it laundered every bad path into a pass and made both the refusal
branch and `a_missing_instance_dir_parent_is_refused` unreachable. One level is
also exactly what first boot needs — what the brief's own comment said it wanted.
(b) The tmpfs rule now has a test. Every test in the brief used a nonexistent
path and none exercised `VOLATILE_MAGICS`; a wrong magic would have passed all
of them while the node booted on tmpfs. `/dev/shm` is real tmpfs, so the rule is
tested against the real thing.
(c) `volatile_dir()` finds that tmpfs through `/proc/mounts`, NOT through
`fs_kind`. Asking the code under test to locate its own input was vacuous the
same way — mutation showed a corrupted magic merely made the tests SKIP. With
the independent oracle the same mutation turns the suite red, failing exactly
the 4 tmpfs-dependent tests.
(d) `CARGO_TARGET_TMPDIR` is not defined for unit tests in `src/` (integration
tests and benches only); the durable-path test uses `CARGO_MANIFEST_DIR`.
Also: the env-lock guard tolerates poisoning, so one genuine failure reports as
one failure instead of six cascades.

**Ruling 9 (Task 4) — the drain test pins a contract, not a behaviour
difference; recorded so nobody later mistakes it for the stronger claim.**
Mutating `stop_draining` down to a bare `stop()` does NOT turn the test red at
any load tried (up to 5000 payloads). `Node::stop` tears down agents in order
and the archive is LAST (`[consensus, sender, receiver, archive]`), so it keeps
polling while the other three threads join and clears a backlog of that size on
its own. The plan's premise — that `AgentRunner` exits at the top of a duty
cycle and so drops appended-but-unrecorded bytes — is correct as written;
incidental teardown timing is what hides it in-process. The drain's value is
making the catch-up a BOUNDED, self-reporting guarantee rather than an accident
of stop order, and Task 8's gate is what measures it under real load.
Two brief corrections: the counters are fields (`c.append.load_acquire()`), not
methods; and the assertion is against `Archive::recovered_position`, since a
restart cannot discriminate — `log.buf` is file-backed, so a restarted archive
pulls the tail back out of the buffer regardless.
*Cost if wrong:* the drain is a no-op in cases the gate does not reach.

**Ruling 10 (Task 5) — the shipped default config panicked the daemon on its
first boot, and the fix is in two places because there were two faults.**
Running the daemon for the first time produced
`uc2_net/src/sender.rs:421: a max-size frame (+ crypto overhead, if enabled)
must fit one datagram`. Task 1's `default_max_payload` was **1 MiB against a
1408 B MTU** — about 700x the budget. UC does not fragment frames, so
`Sender::new` asserts the fit at construction: every field deserialised
perfectly and the process then died inside the transport, which is precisely
the failure mode M9 exists to abolish.
(a) The default is now 512 B, matching the examples' `NODE_MAX_PAYLOAD`, which
is documented against this exact assert.
(b) `preflight` gained `PayloadExceedsMtu`, reproducing the sender's arithmetic
(`align_frame_len(HEADER_LEN + max_payload) + DATAGRAM_HEADER_LEN`, plus
`CRYPTO_OVERHEAD` when crypto is on) and refusing by name with the numbers
shown. The node never overrides the sender's `mtu`, so `MTU_DEFAULT` is the
true budget; an ordering guard keeps the sum from overflowing on an absurd
value.
Two regression tests: `the_defaults_pass_preflight` (a minimal config's stated
defaults must be *startable*, not merely parseable — the test the 1 MiB default
would have failed) and `the_mtu_budget_accounts_for_crypto_overhead`.
**Lesson for the remaining tasks:** Task 1 was reviewed as "deserialisation
only, defaults are out of scope". That framing is what let a fatal default
through — a default IS a semantic claim, and nothing had ever started a node
from this file until Task 5.
Also corrected: the brief's daemon predates Ruling 4 (single-value
`load_from_path`, one-arg `check`) and silently discarded the `FsVerdict`,
which would have made the override channel silent — the exact thing Ruling 4
forbids. The daemon now warns on every boot, and a test asserts it.

**Ruling 11 (Task 6) — the brief's service test leaks a busy-spin child.**
When its assertion fails, `counter-node` is never killed, and it inherits the
harness's stdout pipe, so cargo blocks reading until EOF. One such leak burned
**39 minutes of CPU** and hung the suite for ten minutes before being noticed.
The test now reaps through a `Drop` guard with nulled stdio, so a failing
assertion costs 2 seconds. Any future harness that spawns a node process must
do the same — every UC node is busy-spin, so a leaked child is not idle.

**Ruling 12 (Task 8) — the smoke measures the commit-rate dip but does not gate
it, and this was decided AFTER seeing it fail.** Recording the direction
explicitly, because moving a bar after a red run is the move this project's
honest-failure protocol exists to prevent. Two justifications, neither of which
is "it went red":
(a) **Precedent.** `<10%` dip is already a FLEET bar here. `m7_gate`'s `AllArgs`
says in a comment that the in-process gate "does not hard-gate the fleet dip
bar"; `m6_gate` says the same. The M9 harness was the outlier, not the fix.
(b) **The measurement cannot adjudicate the bar.** Seven identical runs:
0.0 / 2.1 / 10.1 / 10.9 / 14.2 / 17.5 / 18.0 % — an 18-point spread against a
10-point bar. Noise larger than the bar makes a local verdict a coin flip in
either direction, including the direction that would have passed.
**The fleet bar in the gate doc is unchanged at 10%.** The smoke prints the
number, labels it fleet-only, and its own PASS line states it is not an M9 PASS.
Maintainer direction the same day confirms the principle: *the dev box is small,
do not rely on it for reliable bench numbers.*

**Ruling 13 (Task 8) — row 2 is anti-vacuity checked.** `incoming_snapshot_pos`
"unchanged at 0" is also exactly what a cluster that never snapshots looks like,
and such a cluster could not have installed one however badly the restart went.
The row now requires positive evidence that snapshots were being built
(`service_snapshot_pos` ~1.7 MB on all three nodes in the smoke) and reports
INCONCLUSIVE-and-FAIL otherwise. Same discipline as Rulings 8(c) and 9.

**Ruling 14 (post-task) — the spec's reserved `[log]`/`[metrics]` sections were
missing, and `deny_unknown_fields` made that a shipped-schema problem.** Spec §4
specifies the config file as "a one-to-one mirror of `NodeConfig`, **plus
`[log]` and `[metrics]` sections reserved for M10**". The second clause was never
implemented and appears nowhere in Tasks 1-8; the plan's own self-review §1
asserted full spec coverage and missed it. It is not cosmetic, because
`NodeConfigFile` is `deny_unknown_fields`: an operator who writes `[metrics]`
gets a hard startup refusal, and M10 could not add observability config without
amending a schema that had shipped in a tagged release.
Three sub-decisions, all deliberate:
(a) **Accepted as opaque `toml::Table`s, not validated.** M9 does not know
M10's schema. Inventing field names would either refuse a legitimate M10 config
or lock M10 into M9's guess. The table is reserved; its contents are M10's.
(b) **Accepted is not the same as ignored.** Swallowing a section the operator
wrote expecting an effect is precisely the silent no-op this milestone exists to
abolish, so presence is carried out through `StartupOptions::reserved`
(`preflight::ReservedSections`) and the daemon prints a NOTE naming them on
every boot — the same never-silent discipline as Ruling 4's override.
(c) **The exception is exactly two names.** A test asserts an unreserved unknown
table (`[telemetry]`) is still refused by name, so reserving these does not
quietly relax `deny_unknown_fields` in general.
Five tests: four in `config_file` (parse+report, M10-unknown keys accepted,
unreserved table still refused, absent means nothing announced) and one in
`tests/lifecycle.rs` asserting the daemon actually prints the notice and still
exits 0 — the half `preflight` deliberately does not own, same split as the
volatile-fs test. `packaging/node.example.toml` and
`docs/reference/configuration.md` document the reservation.
*Cost if wrong:* two config names are spent, and M10 must use them.

**Ruling 15 (post-task) — two review findings on the refusal surface itself,
both fixed.** Neither was a safety issue; both undercut the thing M9 claims to
deliver, which is a failure an operator can act on.
(a) **`PayloadExceedsMtu` reported a sentinel instead of the requirement.** The
overflow guard computed `need = usize::MAX` for any `max_payload > MTU_DEFAULT`,
so a plausible `max_payload = 4096` — over the 1408 B MTU but well under
`buffer_bytes / 4`, hence not caught by `PayloadTooLarge` first — refused with
"needs 18446744073709551615 bytes" where the true figure is 4144. Wrong by
fifteen orders of magnitude on the one surface whose job is refusing by name
WITH THE NUMBERS SHOWN. Now a `checked_add` chain, with the input to
`align_frame_len` bounded because that function rounds up by as much as
`FRAME_ALIGNMENT - 1` and takes no checked form. Saturation survives only for
values too large to represent, which `PayloadTooLarge` already refuses — the
pre-existing `an_overflowing_max_payload_is_refused_not_wrapped` test still
passes, so the overflow property was preserved, not traded away.
(b) **The shipped systemd unit retried a config refusal.** `Restart=on-failure`
does not distinguish the daemon's deliberate exit 2 ("this config is bad") from
exit 1 ("start failed, possibly transiently"), so a typo'd config restarted five
times before systemd's start limit surfaced it. Added
`RestartPreventExitStatus=2`; exit 1 is deliberately NOT listed, since a held
port is worth retrying. Validated with `systemd-analyze verify`.
The exit code is now a contract with the packaging, not an implementation
detail, so `daemon_refuses_a_config_with_a_bind_mismatch` was strengthened from
`!success()` to `code() == Some(2)`. Drifting it to 1 turns the loud refusal
back into a restart loop, and that is now a test failure rather than a silent
packaging regression.
*Cost if wrong:* (a) none — strictly more accurate; (b) a genuinely transient
startup failure that happens to exit 2 would not be retried, but no such path
exists: exit 2 is only reached from config load and preflight.

**Ruling 16 (fleet run 1) — row 3 FAIL was charged to the harness's load
model; fixed, unvalidated; bar unchanged.** Fleet run 1 (2026-08-19,
3×c6id.2xlarge, destroyed same day, state verified empty) passed rows 1
(0.098 s, exit 0), 2 (no install, snapshots proven firing at ~20 MB on all
three nodes) and 4 (6/6), and FAILED row 3 at 62.0 % against the 10 % bar —
recorded as an honest FAIL in the gate doc. The run's log shows leadership
moved to n1; the harness ran ONE load client pinned to the leader and respawned
it on the new leader, paying a process spawn plus its CAN_SERVE wait inside the
5 s measurement window. That model was wrong about the SDK: `uc2_client` is
written for a per-node app client (every node runs one; the leader's serves,
a follower's redirects its callers to the leader).
Two-part fix (`10d3d9d`): the orchestrator starts a client on EVERY node and
touches none across the restart; `load_loop`'s `NotLeader` arm gained a 2 ms
backoff so a follower's client (whose fleet role registers only its own node)
does not hot-spin and perturb the measured commit rate. The fix is REASONED
from the fleet log, not demonstrated — the dev box cannot measure row 3
(Ruling 12) — so only fleet run 2 can validate it. A row-3 adjudication
question (restart vs the M6/M7 no-node-lost transitions the bar was inherited
from) is recorded in the gate doc and must be answered, pre-committed, before
run 2.
*Cost if wrong:* if the load model was NOT the dominant term, fleet run 2 fails
again and the residual is the product's own election gap — at which point the
recorded open question, not this ruling, is the path forward.

**Ruling 17 (fleet run 2) — PASS on the pre-committed re-specified bar; the
run-1 FAIL is confirmed to have been the harness's load model.** The house rule
held in order: decide rule `da95d22` (row 3 re-specified as time-to-recover,
≤ 15 s to the end of the first ≥ 80 %-of-baseline 2 s window, confirmed by the
next), then the harness implementing it (`6103c0a`), then the run. Fleet run 2
(2026-08-19, 3×c6id.2xlarge — same instance type as run 1; node3 of the 4
provisioned came up ssh-dead and was unused, the gate needs exactly 3) passed
all four rows: 0.042 s exit 0; no install with snapshots proven firing at
~24–25 MB on all three nodes; recovered at 10.5 s (plumbing-dominated — an
upper bound, not a measurement of true recovery); 6/6 refusals by name.
The confirmation of Ruling 16 is the ungated A/B: the run-1-style 5 s dip read
**8.5 % against run 1's 62.0 %** on identical hardware — with the per-node
client model even the original 10 % bar would have passed, so the 62 % was the
harness, not the cluster. Also banked: the local mechanics run caught a
client-lifetime bug before money was spent (load clients exiting mid-poll read
as a dead cluster — 40 s of zero-rate windows that vanished when the lifetime
was raised to cover the poll).
*Cost if wrong:* the 15 s bar is generous and plumbing-heavy, so a genuine
regression in restart cost below ~10 s of degradation would pass this row —
rows 1 and 2 remain the tight bounds on the stop and the rejoin path.

### State at handoff

Tasks 1–8 are implemented and committed, plus two post-task commits from a
whole-branch review against the spec (Rulings 14 and 15). The remaining step is
the one the plan always reserved:

1. **The M9 fleet gate has PASSED — fleet run 2, 2026-08-19** (run 1 before it
   FAILED honestly on row 3 and stays recorded; see Rulings 16–17 and the gate
   doc `docs/benchmarks/uc2-m9-gate-2026-08-19.md`, "Fleet run 1" / "Fleet
   run 2"). Remaining maintainer steps, none performed yet: merge
   `uc2/m9-fleet-orchestrator` to `main`, tag `v2.3.0`, and push.
2. Local smoke, re-run after Rulings 14–15 (`ef5cd0f`): row 1 **0.091 s, exit
   0**; row 2 **no install**, anti-vacuity satisfied (`service_snapshot_pos`
   [1899648, 1897344, 1897984], so snapshots were genuinely firing); row 4
   **6/6** refusals name their field. Row 3 measured at 0.0% and printed, but is
   fleet-only — one dev-box sample decides nothing (Ruling 12). `RESULT: PASS
   (smoke)`, which the harness itself labels as NOT an M9 PASS.
3. Whole-workspace verification at that commit: `cargo clippy --workspace
   --all-targets -- -D warnings` clean; `cargo test --workspace` 941 passed, 0
   failed, 8 ignored. `cargo fmt --check` is dirty, but so is `main` (1972
   diffs in a clean worktree) — pre-existing repo-wide drift under rustfmt
   1.9.0 with no `rustfmt.toml`, not a branch regression and not a CLAUDE.md
   gate.
4. Nothing in M9 touched consensus, the wire protocol, or the cnc page layout.
5. The branch is **unpushed and unmerged**; `main` == `origin/main` ==
   `4d5655f`.
