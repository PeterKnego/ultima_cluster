# FSM Identity (named rows, `ApplyCtx`, `IdGen`, per-FSM version) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every state machine an identity declared in code (`const NAME` + `const VERSION`), bind it to the cluster-wide row so a mis-attached service and a mis-declared cluster are refused **by name**, introduce the `ApplyCtx` apply signature once, and ship `IdGen` (deterministic, per-apply IDs) as its first consumer — one combined cnc 3.1 + wire 0.7.0 flag day.

**Architecture:** A `core`-only `uc_protocol::identity` module (name rules, frozen FNV-1a 64 hash, packed version) is the single source every crate uses. The node writes each row's name + hash into that slot's reserved cnc line at boot (inside `CncPage::init`, before the header CRC); the service writes its version into the status line at attach; a service finds its row by scanning names. `SNAP_BEGIN`/`SNAP_DONE` carry `[u64; 8]` identity hashes and `[u32; 8]` versions in row order and the receiver compares positionally. Disk, rings, artifact routing, the client engine and the gateway are untouched. `RawStateMachine::apply` takes `&mut ApplyCtx` (position now; time fields later); `ctx.ids()` yields the `IdGen`.

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89 — no features newer than that; `is_multiple_of`/`offset_of!` are already used in-tree); `uc_protocol` (core-only leaf), `uc_log` (cnc page), `uc_service`, `uc_node`, `uc_net`, `uc_client`, `uc_ctl`; `fuzz/` (separate workspace, nightly); Python fleet driver `bench-infra/scripts/m14_fleet_gate.py`.

**Spec:** `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` (binding; §2.1 records the cut, §3.3 the `ApplyCtx`, §7 the version). Read it end-to-end before Task 0. Task 0 records the as-built deltas this plan discovered.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all` (enforced by CI: `cargo fmt --all -- --check`), `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals` (the CI `test` job, `.github/workflows/ci.yml:44-65`). `fuzz/` is outside the workspace: after Task 3 and Task 6, `(cd fuzz && cargo +nightly check)` must pass.
- **Frozen constants, never to change after they ship**: the FNV-1a 64 hash of a name (§3.2), the `IdGen` Feistel permutation and its three round constants (§3.4), the packed-version layout `major:8 ‖ minor:8 ‖ patch:16`. Each is pinned by a golden-vector test whose comment says so.
- **Name rules, verbatim (§3.1)**: 1..=32 bytes, lowercase ASCII letters, digits, `_`, `-`; first byte a letter.
- **One type, one row.** Identity is per state-machine type, so two rows cannot host the same type. Harnesses that run one type at N rows use `uc_service::Tagged<const ROW: u8, S>` (Task 5), whose names are `fsm0`..`fsm7`. Production never needs it.
- **`[services]` is required** in `node.toml` (§4.1); `ids` is refused by field name with a pointer to `names`. Programmatic configs use `ServicesConfig::single(<Sm>::NAME)` / `from_names(..)`; `ServicesConfig::default()` no longer exists.
- **No wire field other than `SnapBeginBody` changes.** `SNAP_DONE` echoes the same struct (`uc_net/src/receiver.rs:2196-2216`), so it moves with it.
- **`ApplyCtx` is `#[non_exhaustive]`**, not `Copy`, with a public `position` and a private identity; only `ApplyCtx::new(position, identity)` constructs it, and `apply` takes it as **`&mut ApplyCtx`** (spec §3.3: the scheduler's `schedule`/`cancel` push into it later; a `&` would force interior mutability on the hot path). `IdGen` is `!Send`.
- **Fleet spend is user-gated** (Task 10's gate run). Local numbers are smoke, never a gate (CLAUDE.md).
- **Never write scratch to `/tmp`**; Elle under `$HOME/.cache/uc2-elle*`.
- Commit subjects: `type(scope): imperative summary`, as in `git log --oneline -30`.
- Every new or changed test is **watched red first** (the step says what to revert or stub); the commit message of the task names the test.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` §4.2, §3.3, §6 | as-built errata (line assignment, `Tagged`, SNAP_DONE, `ServicesConfig::single`) | 0 |
| `uc_protocol/src/identity.rs` (new), `uc_protocol/src/lib.rs` | `FsmName`, `NameError`, `fnv1a_64`, `FsmIdentity`, `pack_version`/`unpack_version`/`VersionDisplay` | 1 |
| `uc_protocol/src/v2/cnc.rs` | `CNC_V2_VERSION` 3.1; `CNC_SVC_OFF_VERSION`, `CNC_SVC_OFF_NAME`, `CNC_SVC_NAME_LEN`, `CNC_SVC_OFF_IDENTITY_HASH`; offset tests | 2 |
| `uc_log/src/cnc.rs` | `ServiceStatusLine`, `ServiceIdentityLine`, `ServiceSlot` re-shaped; `CncMeta.services`; `init` writes names; accessors | 2 |
| `uc_service/src/traits.rs`, `uc_service/src/ids.rs` (new), `session.rs`, `apply.rs`, `replay.rs`, `lib.rs` | `ApplyCtx`, `NAME`/`VERSION`/`IDENTITY`, `IdGen`, ctx-building call sites, forwarding | 3 |
| every `impl StateMachine for` / `impl RawStateMachine for` in the tree (list in Task 3) | `const NAME`, `ctx: &mut ApplyCtx` | 3 |
| `uc_node/src/services.rs`, `config_file.rs`, `node.rs` (boot), all `ServicesConfig::default()/from_ids` sites | names in config; `single`/`from_names`/`from_cli`; `[services]` required; names into `CncMeta` | 4 |
| `uc_service/src/config.rs`, `attach.rs`, `lib.rs`, `tagged.rs` (new); every `.service_id(` site; `--service-id` flags | attach by name; `UnknownFsm`; version written; `Tagged`; binaries take `--fsm` | 5 |
| `uc_protocol/src/v2/datagram.rs`, `uc_protocol/src/version.rs`, `uc_net/src/{sender,receiver}.rs`, `uc_node/src/node.rs` (snapshot path), `fuzz/README.md` | wire 0.7.0 body; positional identity + version checks; refusal detail; new counter | 6 |
| `uc_node/src/obs/metrics.rs`, `uc_ctl/src/main.rs`, `docs/ops/uc2-runbook.md` (alert rule), `bench-infra/ansible/roles/run/tasks/main.yml` | name+row labels, `uc2_service_identity_hash`/`uc2_service_version`, `uc2_snapshot_refused_version_total`, `uc2ctl status` | 7 |
| `uc_client/src/{engine,client,pipelined,error}.rs`, `examples/counter/src/bin/counter-client.rs` | `fsm(name) -> u8`, `declared_names()`, `ClientError::UnknownFsm` | 8 |
| `uc_node/tests/lincheck_v2/mod.rs`, `uc_node/tests/services.rs`, `uc_node/tests/learner.rs`, `examples/uc_crashtest/tests/common/mod.rs`, `bench-infra/scripts/m14_fleet_gate.py`, `scripts/elle_check.sh` | capstones by name; the order-mismatch and version-mismatch negative scenarios; fleet driver | 9 |
| `RELEASES.md`, `docs/releases.md`, `docs/reference/{wire-protocol,cnc-page,configuration,semver-policy,limits}.md`, `docs/how-to/{run-a-cluster,upgrade-a-cluster,monitor-a-cluster,diagnose-a-node,change-cluster-membership}.md`, `docs/ops/uc2-runbook.md`, `QUICKSTART.md`, `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md` (new), `docs/benchmarks/uc2-fsm-identity-gate-<date>.md` (new), `docs/BACKLOG.md` | docs sweep, explainer, release writeup, gate doc | 10 |

---

### Task 0: Spec as-built errata

**Files:**
- Modify: `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` (§3.3 end, §4.2 table, §5, §6, §11)

Recon of the tree (2026-09-02) found four things the spec's text does not say; record them so the spec stays the binding record.

- [ ] **Step 1: §4.2 line assignment.** Replace the §4.2 table's two service-written rows with this (the identity hash is *node*-written, on line 7 beside the name, because the node already knows the name at boot and line 7 has one writer; the status line's second word takes the version, the only service-originated fact):

```markdown
| slot `+448..+480` (line 7) | `name`, NUL-padded | 32 B | **node, in `CncPage::init`** (before the header CRC) |
| slot `+480..+488` (line 7) | `identity_hash` u64 | 8 B | node, in `CncPage::init` |
| slot `+488..+512` | reserved (zero) | 24 B | — |
| slot `+8` (status line 0) | `version` u32 (§7), stored as a u64 word | 8 B | service, at attach (same line and writer as `status`) |
```

- [ ] **Step 2: §3.3 append** after "mechanically.":

```markdown
**One type, one row.** Identity is per type, so a harness that runs one
state machine type at several rows (apply_bench, the two-FSM lincheck
capstones, m12_gate's fleet rows) wraps it in `uc_service::Tagged<const
ROW: u8, S>`, a zero-cost forwarding newtype whose `NAME` is `fsm{ROW}`.
`ServicesConfig::tagged(n)` declares `fsm0..fsm{n-1}`. A production
deployment never needs it — two rows running the same logic on one log
compute the same state twice.
```

- [ ] **Step 3: §5 append** after "Artifacts route by row, as today.":

```markdown
`SNAP_DONE` echoes the same `SnapBeginBody` (`uc_net/src/receiver.rs`,
`snap_send_done`), so it carries the 0.7.0 layout with no separate change.
The receiver learns its own per-row versions through a closure the node
installs with `set_snapshot_intake` (the services write them into the cnc
page at attach; `uc_net` has no cnc dependency).
```

- [ ] **Step 4: §4.1 append**: "`ServicesConfig::single(name)` is the one-FSM programmatic form; `from_names(&[..], lag)` the general one; `from_cli` requires `--services` (absent is a refusal, as the section is)."

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md
git commit -m "docs(spec): FSM identity as-built errata — line-7 hash, Tagged, SNAP_DONE, single()"
```

---

### Task 1: `uc_protocol::identity` — name rules, frozen hash, packed version

**Files:**
- Create: `uc_protocol/src/identity.rs`
- Modify: `uc_protocol/src/lib.rs:16-21` (add `pub mod identity;` between `error_codes` and `magic`; re-export `pub use identity::{FsmIdentity, FsmName};`)

**Interfaces:**
- Produces (all `core`-only, no `std`):
  - `pub const FSM_NAME_MAX_LEN: usize = 32;`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct FsmName { bytes: [u8; 32], len: u8 }` with `pub const fn parse(s: &str) -> Result<FsmName, NameError>`, `pub const fn parse_or_panic(s: &str) -> FsmName`, `pub fn as_str(&self) -> &str`, `pub const fn hash(&self) -> u64`, `pub const fn padded(&self) -> [u8; 32]`, `pub fn from_padded(b: &[u8; 32]) -> Option<FsmName>`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum NameError { Empty, TooLong(usize), BadFirstByte(u8), BadByte(u8) }` + `Display`
  - `pub const fn fnv1a_64(bytes: &[u8]) -> u64`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct FsmIdentity { pub name: FsmName, pub version: u32 }` with `pub const fn parse(name: &str, version: u32) -> FsmIdentity` (panics on a bad name), `pub const fn hash(&self) -> u64`, `pub const fn fold32(&self) -> u32`
  - `pub const fn pack_version(major: u8, minor: u8, patch: u16) -> u32`, `pub const fn unpack_version(v: u32) -> (u8, u8, u16)`, `pub struct VersionDisplay(pub u32)` (`Display`: `"1.2.3"`, `0` → `"unversioned"`)

- [ ] **Step 1: Write the failing tests** at the bottom of the new `uc_protocol/src/identity.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_rules_table() {
        for ok in ["a", "kv", "orders", "order-book_v2", "a234567890123456789012345678901"] {
            assert!(FsmName::parse(ok).is_ok(), "{ok}");
        }
        assert_eq!(FsmName::parse(""), Err(NameError::Empty));
        assert_eq!(FsmName::parse("a23456789012345678901234567890123"), Err(NameError::TooLong(33)));
        assert_eq!(FsmName::parse("1abc"), Err(NameError::BadFirstByte(b'1')));
        assert_eq!(FsmName::parse("_abc"), Err(NameError::BadFirstByte(b'_')));
        assert_eq!(FsmName::parse("Orders"), Err(NameError::BadFirstByte(b'O')));
        assert_eq!(FsmName::parse("ord ers"), Err(NameError::BadByte(b' ')));
        assert_eq!(FsmName::parse("ordérs"), Err(NameError::BadByte(0xC3)));
        assert_eq!(FsmName::parse("kv").unwrap().as_str(), "kv");
    }

    /// FROZEN: FNV-1a 64 published vectors. Never change these.
    #[test]
    fn fnv1a_64_golden_vectors() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x8594_4171_f739_67e8);
        // The name hash is the hash of exactly the name's bytes, no padding.
        assert_eq!(FsmName::parse("a").unwrap().hash(), fnv1a_64(b"a"));
    }

    #[test]
    fn padded_roundtrip_and_zero_line_is_none() {
        let n = FsmName::parse("orders").unwrap();
        let p = n.padded();
        assert_eq!(&p[..6], b"orders");
        assert!(p[6..].iter().all(|&b| b == 0));
        assert_eq!(FsmName::from_padded(&p), Some(n));
        assert_eq!(FsmName::from_padded(&[0u8; 32]), None);
        let mut bad = p;
        bad[3] = b' ';
        assert_eq!(FsmName::from_padded(&bad), None, "a corrupt line is not a name");
    }

    #[test]
    fn identity_is_const_and_fold32_is_stable() {
        const ID: FsmIdentity = FsmIdentity::parse("orders", pack_version(1, 2, 3));
        assert_eq!(ID.name.as_str(), "orders");
        assert_eq!(ID.version, 0x0102_0003);
        let h = ID.hash();
        assert_eq!(ID.fold32(), (h >> 32) as u32 ^ h as u32);
        assert_eq!(unpack_version(ID.version), (1, 2, 3));
        assert_eq!(VersionDisplay(ID.version).to_string(), "1.2.3");
        assert_eq!(VersionDisplay(0).to_string(), "unversioned");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_protocol identity`
Expected: compile error (`identity` module does not exist).

- [ ] **Step 3: Implement** `uc_protocol/src/identity.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FSM identity (spec `2026-09-02-uc2-fsm-identity-design.md` §3): the name a
//! state machine declares in code, its FROZEN hash, and the packed per-FSM
//! version. `core`-only — the node, the service SDK, the client, the cnc page
//! and the wire all use exactly these rules and this hash.

use core::fmt;

/// Bytes, not chars: a name is ASCII, so the two agree.
pub const FSM_NAME_MAX_LEN: usize = 32;

/// A validated FSM name: 1..=32 bytes of `[a-z0-9_-]`, first byte a letter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmName {
    bytes: [u8; FSM_NAME_MAX_LEN],
    len: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameError {
    Empty,
    TooLong(usize),
    BadFirstByte(u8),
    BadByte(u8),
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameError::Empty => write!(f, "FSM name is empty"),
            NameError::TooLong(n) => write!(f, "FSM name is {n} bytes, max {FSM_NAME_MAX_LEN}"),
            NameError::BadFirstByte(b) => write!(f, "FSM name must start with a-z, got {:?}", *b as char),
            NameError::BadByte(b) => write!(f, "FSM name may contain only a-z 0-9 _ -, got {:?}", *b as char),
        }
    }
}

/// FNV-1a 64 over `bytes`. FROZEN: this is what goes on the wire and into
/// `IdGen`; changing it is a flag day.
pub const fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i] as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    h
}

impl FsmName {
    pub const fn parse(s: &str) -> Result<FsmName, NameError> {
        let b = s.as_bytes();
        if b.is_empty() {
            return Err(NameError::Empty);
        }
        if b.len() > FSM_NAME_MAX_LEN {
            return Err(NameError::TooLong(b.len()));
        }
        if !b[0].is_ascii_lowercase() {
            return Err(NameError::BadFirstByte(b[0]));
        }
        let mut out = [0u8; FSM_NAME_MAX_LEN];
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-') {
                return Err(NameError::BadByte(c));
            }
            out[i] = c;
            i += 1;
        }
        Ok(FsmName { bytes: out, len: b.len() as u8 })
    }

    /// For `const` contexts (the trait's provided `IDENTITY`): a bad name is a
    /// compile-time error at the first use.
    pub const fn parse_or_panic(s: &str) -> FsmName {
        match Self::parse(s) {
            Ok(n) => n,
            Err(_) => panic!(
                "invalid FSM NAME: 1..=32 bytes of [a-z0-9_-], starting with a letter"
            ),
        }
    }

    pub fn as_str(&self) -> &str {
        // ASCII by construction, so this cannot fail.
        core::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("")
    }

    /// Hash of exactly the name's bytes (no padding).
    pub const fn hash(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut i = 0;
        while i < self.len as usize {
            h ^= self.bytes[i] as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
            i += 1;
        }
        h
    }

    /// The 32-byte NUL-padded form the cnc slot line carries.
    pub const fn padded(&self) -> [u8; FSM_NAME_MAX_LEN] {
        self.bytes
    }

    /// Inverse of [`padded`](Self::padded). All-zero (an undeclared row) or a
    /// line that fails the rules is `None` — a shared-memory page never panics
    /// an attacher.
    pub fn from_padded(b: &[u8; FSM_NAME_MAX_LEN]) -> Option<FsmName> {
        let len = b.iter().position(|&c| c == 0).unwrap_or(FSM_NAME_MAX_LEN);
        if len == 0 {
            return None;
        }
        let s = core::str::from_utf8(&b[..len]).ok()?;
        FsmName::parse(s).ok()
    }
}

/// `major:8 ‖ minor:8 ‖ patch:16` — the same packing as `ProtocolVersion`
/// (Aeron's `SemanticVersion` is 8/8/8; both order as integers). FROZEN.
pub const fn pack_version(major: u8, minor: u8, patch: u16) -> u32 {
    ((major as u32) << 24) | ((minor as u32) << 16) | patch as u32
}

pub const fn unpack_version(v: u32) -> (u8, u8, u16) {
    ((v >> 24) as u8, (v >> 16) as u8, v as u16)
}

/// `Display` for a packed version: `"1.2.3"`, or `"unversioned"` for `0`.
pub struct VersionDisplay(pub u32);

impl fmt::Display for VersionDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return f.write_str("unversioned");
        }
        let (a, b, c) = unpack_version(self.0);
        write!(f, "{a}.{b}.{c}")
    }
}

/// What a state machine type IS: its name (identity) and its logic version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmIdentity {
    pub name: FsmName,
    pub version: u32,
}

impl FsmIdentity {
    pub const fn parse(name: &str, version: u32) -> FsmIdentity {
        FsmIdentity { name: FsmName::parse_or_panic(name), version }
    }
    pub const fn hash(&self) -> u64 {
        self.name.hash()
    }
    /// The 32-bit fold `IdGen` mixes in (spec §3.4).
    pub const fn fold32(&self) -> u32 {
        let h = self.hash();
        (h >> 32) as u32 ^ h as u32
    }
}
```

(`fnv1a_64` and `FsmName::hash` duplicate the loop because a `const fn` cannot take a slice of a struct field through a temporary in MSRV 1.89 without extra borrow ceremony; the golden test asserts they agree.)

- [ ] **Step 4: Register the module** in `uc_protocol/src/lib.rs`: add `pub mod identity;` after `pub mod error_codes;` and `pub use identity::{FsmIdentity, FsmName};` beside the existing `pub use`. Add to the crate doc comment's list of `core`-only modules (`lib.rs:8-10`): "`identity.rs`".

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p uc_protocol identity`
Expected: 4 passed.

- [ ] **Step 6: fmt + clippy, commit**

```bash
cargo fmt --all && cargo clippy -p uc_protocol --all-targets -- -D warnings
git add uc_protocol/src/identity.rs uc_protocol/src/lib.rs
git commit -m "feat(uc_protocol): identity module — FsmName rules, frozen FNV-1a 64, packed version (spec §3.1-3.2)"
```

---

### Task 2: cnc 3.1 — names and hashes on line 7, version in the status line

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs:52-58` (`CNC_V2_VERSION`), `:239-289` (slot layout comment + consts), the `offsets_do_not_overlap` test (`:660-672`)
- Modify: `uc_log/src/cnc.rs:41-47` (`CncMeta`), `:166-211` (`ServiceSlot` + pins + pack/unpack), `:343-365` (`init`), accessors near `:521-562`
- Modify: every `CncMeta { .. }` literal — find with `grep -rn "CncMeta {" --include=*.rs . | grep -v /target/` (expected: `uc_node/src/node.rs:657`, `uc_service/src/attach.rs` test, `uc_client/src/{engine,pipelined}.rs` tests, `uc_node/examples/apply_bench.rs`, plus any others the grep shows) — add `services: [None; CNC_MAX_SERVICES],` for now (Task 4 makes the node pass real names).

**Interfaces:**
- Consumes: `uc_protocol::identity::FsmName` (Task 1).
- Produces:
  - `uc_protocol::v2::cnc`: `CNC_V2_VERSION = (3 << 24) | (1 << 16)`; `CNC_SVC_OFF_VERSION: usize = 8`; `CNC_SVC_OFF_NAME: usize = 448`; `CNC_SVC_NAME_LEN: usize = 32`; `CNC_SVC_OFF_IDENTITY_HASH: usize = 480`.
  - `uc_log::cnc::ServiceStatusLine { load_acquire() -> u64, store_release(u64), version() -> u32, store_version(u32) }` (the `status` field's new type; existing `slot.status.load_acquire()` / `store_release(..)` call sites compile unchanged).
  - `uc_log::cnc::ServiceIdentityLine { name() -> Option<FsmName>, hash() -> u64 }` (the former `reserved` field, now `identity`).
  - `uc_log::cnc::CncMeta.services: [Option<FsmName>; CNC_MAX_SERVICES]` written by `init`.
  - `CncPage::service_names(&self) -> [Option<FsmName>; CNC_MAX_SERVICES]`, `CncPage::row_of(&self, name: &FsmName) -> Option<u8>`.

- [ ] **Step 1: Write the failing tests**

In `uc_protocol/src/v2/cnc.rs`'s `offsets_do_not_overlap` test, append:

```rust
        // FSM identity (cnc 3.1): version word in the status line, name +
        // hash on the once-reserved line 7. Both inside the 512 B slot.
        assert_eq!(CNC_V2_VERSION, (3 << 24) | (1 << 16));
        assert_eq!(CNC_SVC_OFF_VERSION, 8);
        assert_eq!(CNC_SVC_OFF_NAME, 448);
        assert_eq!(CNC_SVC_NAME_LEN, 32);
        assert_eq!(CNC_SVC_OFF_IDENTITY_HASH, CNC_SVC_OFF_NAME + CNC_SVC_NAME_LEN);
        assert!(CNC_SVC_OFF_IDENTITY_HASH + 8 <= CNC_SERVICE_SLOT_STRIDE);
        assert_eq!(CNC_SVC_OFF_NAME, CNC_SVC_OFF_RESERVED, "line 7 is the identity line");
```

In `uc_log/src/cnc.rs` tests module, add:

```rust
    #[test]
    fn init_writes_names_and_hashes_on_line_seven_and_attachers_find_rows() {
        use uc_protocol::identity::FsmName;
        let dir = tempdir();
        let kv = FsmName::parse("kv").unwrap();
        let orders = FsmName::parse("orders").unwrap();
        let mut services = [None; CNC_MAX_SERVICES];
        services[0] = Some(kv);
        services[1] = Some(orders);
        let meta = CncMeta {
            node_id: 1,
            instance_id: 7,
            app_id: "app".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services,
        };
        let page = CncPage::create_file(&dir.path().join("cnc2.dat"), &meta).unwrap();
        assert_eq!(page.service_slot(0).identity.name(), Some(kv));
        assert_eq!(page.service_slot(0).identity.hash(), kv.hash());
        assert_eq!(page.service_slot(1).identity.name(), Some(orders));
        assert_eq!(page.service_slot(2).identity.name(), None);
        assert_eq!(page.row_of(&orders), Some(1));
        assert_eq!(page.row_of(&FsmName::parse("nope").unwrap()), None);
        // The version word is the service's: zero at boot, settable, read back.
        assert_eq!(page.service_slot(1).status.version(), 0);
        page.service_slot(1).status.store_version(0x0102_0003);
        assert_eq!(page.service_slot(1).status.version(), 0x0102_0003);
        // And it shares line 0 with `status` without disturbing it.
        page.service_slot(1).status.store_release(pack_service_status(1, true, 3));
        assert_eq!(unpack_service_status(page.service_slot(1).status.load_acquire()), (1, true, 3));
        assert_eq!(page.service_slot(1).status.version(), 0x0102_0003);
        // A reopened page sees the names (they are bytes on the file).
        let again = CncPage::open_file(&dir.path().join("cnc2.dat"), "app").unwrap();
        assert_eq!(again.service_names()[1], Some(orders));
    }
```

(`tempdir()` is whatever helper the existing tests in that file use — read the test module's imports first and use the same one.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_protocol offsets_do_not_overlap` → FAIL (`CNC_SVC_OFF_VERSION` undefined). `cargo test -p uc_log init_writes_names` → compile error.

- [ ] **Step 3: Implement `uc_protocol/src/v2/cnc.rs`**

Replace the `CNC_V2_VERSION` definition and its doc:

```rust
/// Packed like `uc_protocol::ProtocolVersion`: `(major << 24) | (minor << 16) | patch`.
/// 3.1 (FSM identity): each service slot's line 7 carries the row's name +
/// identity hash (node-written at boot) and the status line's second word the
/// attached service's version. A 3.0 attacher does not read either, and a 3.1
/// attacher on a 3.0 page finds no names — both refuse by name. Flag day by
/// policy (`docs/reference/semver-policy.md`).
pub const CNC_V2_VERSION: u32 = (3 << 24) | (1 << 16);
```

Update the slot layout comment (`:249-262`) lines for `+0` and `+448`:

```
//   +0   status          u64 = service_id (bits 0..8) | attached (bit 8)
//                              | incarnation (bits 32..64)    writer: service (attach/detach)
//   +8   version         u64 (low 32 = packed FSM version)   writer: service (attach)
//   ...
//   +448 name            [u8; 32] NUL-padded FSM name          writer: node (init, boot-once)
//   +480 identity_hash   u64 FNV-1a 64 of the name             writer: node (init, boot-once)
//   +488 reserved (zero)
```

Add after `CNC_SVC_OFF_RESERVED`:

```rust
/// cnc 3.1: the attached service's packed version (`identity::pack_version`),
/// low 32 bits of the status line's second word. `0` = unversioned/absent.
pub const CNC_SVC_OFF_VERSION: usize = 8;
/// cnc 3.1: line 7 — the row's FSM name, NUL-padded to 32 B, then its hash.
pub const CNC_SVC_OFF_NAME: usize = 448;
pub const CNC_SVC_NAME_LEN: usize = 32;
pub const CNC_SVC_OFF_IDENTITY_HASH: usize = 480;
const _: () = assert!(CNC_SVC_OFF_NAME == CNC_SVC_OFF_RESERVED);
const _: () = assert!(CNC_SVC_OFF_IDENTITY_HASH + 8 <= CNC_SERVICE_SLOT_STRIDE);
```

- [ ] **Step 4: Implement `uc_log/src/cnc.rs`**

`CncMeta`:

```rust
pub struct CncMeta {
    pub node_id: u32,
    pub instance_id: u128,
    pub app_id: String,
    pub buffer_bytes: u64,
    pub max_payload: u32,
    /// cnc 3.1: the row → name map, written into each slot's line 7 by
    /// `init`, before the header. `None` = row undeclared. A harness page
    /// (`ServicesConfig::none_for_tests`) is all `None`.
    pub services: [Option<FsmName>; CNC_MAX_SERVICES],
}
```

Replace the `ServiceSlot` block (`:166-194`) with:

```rust
/// cnc 3.1: the slot's line 0 — `status` (word 0) and the attached service's
/// packed version (word 1). One writer (the service, at attach/detach).
#[repr(C)]
pub struct ServiceStatusLine {
    status: AtomicU64,
    version: AtomicU64,
    _pad: [u64; 6],
}
impl ServiceStatusLine {
    pub fn load_acquire(&self) -> u64 {
        self.status.load(Ordering::Acquire)
    }
    pub fn store_release(&self, v: u64) {
        self.status.store(v, Ordering::Release)
    }
    pub fn version(&self) -> u32 {
        self.version.load(Ordering::Acquire) as u32
    }
    pub fn store_version(&self, v: u32) {
        self.version.store(v as u64, Ordering::Release)
    }
}
const _: () = assert!(std::mem::size_of::<ServiceStatusLine>() == 64);
const _: () = assert!(std::mem::offset_of!(ServiceStatusLine, version) == cnc::CNC_SVC_OFF_VERSION);

/// cnc 3.1: the slot's line 7 — the row's name (NUL-padded) and its FNV-1a
/// hash, written ONCE by the node in `init`, before the header is published,
/// and never again. Read-only for every attacher.
#[repr(C)]
pub struct ServiceIdentityLine {
    name: [u8; cnc::CNC_SVC_NAME_LEN],
    hash: AtomicU64,
    _pad: [u64; 3],
}
impl ServiceIdentityLine {
    pub fn name(&self) -> Option<FsmName> {
        FsmName::from_padded(&self.name)
    }
    pub fn hash(&self) -> u64 {
        self.hash.load(Ordering::Acquire)
    }
}
const _: () = assert!(std::mem::size_of::<ServiceIdentityLine>() == 64);
const _: () = assert!(
    std::mem::offset_of!(ServiceIdentityLine, hash) == cnc::CNC_SVC_OFF_IDENTITY_HASH - cnc::CNC_SVC_OFF_NAME
);

#[repr(C)]
pub struct ServiceSlot {
    pub status: ServiceStatusLine,
    pub applied: PaddedAtomicU64,
    pub epoch: PaddedAtomicU64,
    pub output_completed: PaddedAtomicU64,
    pub snapshot_pos: PaddedAtomicU64,
    pub heartbeat_ns: PaddedAtomicU64,
    pub lag_waits: PaddedAtomicU64,
    pub identity: ServiceIdentityLine,
}
```

Keep every existing `const _: () = assert!(offset_of!(ServiceSlot, ..))` line, replacing the `reserved` one with `assert!(std::mem::offset_of!(ServiceSlot, identity) == cnc::CNC_SVC_OFF_NAME)`. Add `use uc_protocol::identity::FsmName;` and `use std::sync::atomic::{AtomicU64, Ordering};` if not already imported.

In `init` (`:343`), **before** `cnc::write_cnc_header(page, &header, &meta.app_id)`, add:

```rust
        // cnc 3.1: names + hashes on each slot's line 7, BEFORE the header —
        // an attacher that passes `validate` must already see them.
        for (row, name) in meta.services.iter().enumerate() {
            let base = cnc::CNC_OFF_SERVICE_SLOTS + row * cnc::CNC_SERVICE_SLOT_STRIDE;
            let (n, h) = match name {
                Some(n) => (n.padded(), n.hash()),
                None => ([0u8; cnc::CNC_SVC_NAME_LEN], 0u64),
            };
            page[base + cnc::CNC_SVC_OFF_NAME..base + cnc::CNC_SVC_OFF_NAME + cnc::CNC_SVC_NAME_LEN]
                .copy_from_slice(&n);
            page[base + cnc::CNC_SVC_OFF_IDENTITY_HASH..base + cnc::CNC_SVC_OFF_IDENTITY_HASH + 8]
                .copy_from_slice(&h.to_le_bytes());
        }
```

(`page` is `self.page_mut()`, already bound in `init`; the slot band is zeroed by file creation, so the loop only writes declared rows' bytes — writing the `None` arm too is harmless and keeps re-init explicit.)

Add accessors beside `service_slot`:

```rust
    /// cnc 3.1: every row's name (`None` = undeclared), straight off line 7.
    pub fn service_names(&self) -> [Option<FsmName>; CNC_MAX_SERVICES] {
        let mut out = [None; CNC_MAX_SERVICES];
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = self.service_slot(i).identity.name();
        }
        out
    }
    /// The row declared under `name`, if any.
    pub fn row_of(&self, name: &FsmName) -> Option<u8> {
        (0..CNC_MAX_SERVICES).find(|&i| self.service_slot(i).identity.name() == Some(*name)).map(|i| i as u8)
    }
```

- [ ] **Step 5: Fix every `CncMeta { .. }` literal** (grep above) by adding `services: [None; CNC_MAX_SERVICES],` (import `CNC_MAX_SERVICES` from `uc_protocol::v2::cnc` where needed).

- [ ] **Step 6: Run to verify passes; whole-workspace check**

Run: `cargo test -p uc_protocol offsets_do_not_overlap && cargo test -p uc_log` → PASS. Then the Global Constraints command set. Expected: green — nothing reads the new fields yet, and `status.load_acquire()`/`store_release()` call sites (`uc_service/src/attach.rs`, `uc_service/src/lib.rs`, `uc_ctl/src/main.rs:589`, `uc_node/src/obs/metrics.rs:206`, `uc_node/src/node.rs` liveness words) compile unchanged through `ServiceStatusLine`'s methods. If any site used `slot.status` as a `PaddedAtomicU64` by type name, change the type to `ServiceStatusLine`.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs $(git diff --name-only)
git commit -m "feat(cnc): 3.1 — row name + hash on slot line 7 at boot, service version in the status line (spec §4.2)"
```

---

### Task 3: `ApplyCtx`, `NAME`/`VERSION`/`IDENTITY`, `IdGen` — and every state machine in the tree

**Files:**
- Modify: `uc_service/src/traits.rs` (both traits, the blanket impl), `uc_service/src/session.rs:166-273` (`Sessioned` impl), `uc_service/src/apply.rs:390` and `uc_service/src/replay.rs:168` (the two ctx-building call sites — confirm with `grep -rn "\.apply(" uc_service/src` that there is no third), `uc_service/src/lib.rs:64-71` (re-exports)
- Create: `uc_service/src/ids.rs`
- Modify (mechanical, one `const NAME` line + `position: u64` → `ctx: &mut ApplyCtx` reading `ctx.position`): `examples/counter/src/lib.rs:52`, `uc_lincheck/src/register.rs:48`, `uc_lincheck/src/list_append.rs:38`, `uc_client/tests/roundtrip.rs:44`, `uc_client/tests/pipelined.rs:34`, `uc_gateway/examples/m12_gate.rs:462,547,1604`, `uc_node/examples/apply_bench.rs:82`, `uc_node/examples/m5_gate.rs:233,267`, `uc_node/examples/m6_gate.rs:185`, `uc_node/examples/m7_gate.rs:277`, `uc_node/examples/m9_gate.rs:149`, `uc_node/examples/m10_gate.rs:260`, `uc_node/examples/m10_alerts.rs:501,1028`, `uc_node/examples/read_profile.rs:538`, `uc_node/tests/services.rs:157,356`, `uc_node/tests/obs_http.rs:253`, `uc_node/tests/learner.rs:672`, `uc_node/tests/backup.rs:812`, `uc_node/tests/crypto_adversarial.rs:1087`, `uc_node/tests/crypto_cluster.rs:70`, `uc_node/tests/query_barrier.rs:42`, `uc_node/tests/lincheck_v2/mod.rs:1335,1403`, `uc_service/src/apply.rs:826`, `uc_service/tests/session.rs:283`, `uc_service/tests/raw_contract.rs:25,79`, `uc_service/tests/reconstruction.rs:46`, `uc_service/tests/query.rs:49`, `uc_service/tests/output.rs:54`, `uc_service/tests/apply.rs:37`, and `fuzz/src/lib.rs:35,94` (outside the workspace — check with `cd fuzz && cargo +nightly check`).

**Interfaces:**
- Consumes: `uc_protocol::identity::{FsmIdentity, FsmName}` (Task 1).
- Produces (`uc_service`, re-exported at the crate root):
  - `#[non_exhaustive] pub struct ApplyCtx { pub position: u64, identity: FsmIdentity }`, `ApplyCtx::new(position: u64, identity: FsmIdentity) -> ApplyCtx`, `ApplyCtx::identity(&self) -> FsmIdentity`, `ApplyCtx::ids(&self) -> IdGen`.
  - `RawStateMachine { const NAME: &'static str; const VERSION: u32 = 0; const IDENTITY: FsmIdentity = FsmIdentity::parse(Self::NAME, Self::VERSION); fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>); fn query(..); fn last_applied(..) }`.
  - `StateMachine` mirrors: `const NAME`, `const VERSION: u32 = 0`, `fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Self::Command) -> Self::Response`.
  - `pub struct IdGen` (`!Send`): `IdGen::new(position: u64, identity: FsmIdentity) -> IdGen`, `IdGen::next(&mut self) -> u128`, `IdGen::ordinal(&self) -> u32`.
  - `pub(crate) fn permute(a: u64, b: u64) -> u128` / `unpermute(x: u128) -> (u64, u64)` in `ids.rs` (tests only).

Names given to in-tree state machines (used again in Tasks 4-5 and 9; keep them exactly): `CounterSm` → `"counter"`; `RegisterSm` → `"register"`; `ListAppendSm` → `"list-append"`; every test/gate `CountSm` → `"count"`; `SpinCountSm` → `"spin"`; `RawCountSm`/`RawCount` → `"raw"`; `RegSm` → `"reg"`; `NoopSm` → `"noop"`; `SlowSm` → `"slow"`; `SlowCountSm` → `"slow-count"`; `ProfileSm` → `"profile"`; `SumSm` → `"sum"`; `RestoreCountSm` → `"restore-count"`; `Corrupt<RegisterSm>` → `"corrupt"`; `InstallCounting` → `"install-counting"`; `ClearsOutSm` → `"clears-out"`; `Counter`/`Echo` (raw_contract) → `"counter"`/`"echo"`; fuzz `NoopSm`/`EchoSm` → `"noop"`/`"echo"`.

- [ ] **Step 1: Write the failing tests** — new file `uc_service/src/ids.rs` ends with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::identity::FsmIdentity;

    const ORDERS: FsmIdentity = FsmIdentity::parse("orders", 0);
    const KV: FsmIdentity = FsmIdentity::parse("kv", 0);

    /// FROZEN golden vectors. Fill the `EXPECTED` values from the first run
    /// (`cargo test -p uc_service ids::tests::golden -- --nocapture` prints
    /// them), then NEVER change them: a replica on a different build must mint
    /// the same IDs.
    #[test]
    fn golden() {
        let mut g = IdGen::new(0, ORDERS);
        let a = g.next();
        let b = g.next();
        let mut h = IdGen::new(u64::MAX, KV);
        let c = h.next();
        eprintln!("golden: {a:#034x} {b:#034x} {c:#034x}");
        const EXPECTED: [u128; 3] = [0, 0, 0]; // <- pin after the first run
        assert_eq!([a, b, c], EXPECTED);
    }

    #[test]
    fn permutation_is_a_bijection() {
        for &(a, b) in &[(0u64, 0u64), (1, 0), (0, 1), (u64::MAX, u64::MAX), (0xdead_beef, 0xcafe_babe)] {
            assert_eq!(unpermute(permute(a, b)), (a, b));
        }
        // Exhaustive on a small sub-domain: 2^12 inputs, no collision.
        let mut seen = std::collections::HashSet::new();
        for a in 0..64u64 {
            for b in 0..64u64 {
                assert!(seen.insert(permute(a, b)));
            }
        }
    }

    #[test]
    fn consecutive_ordinals_and_positions_share_no_visible_structure() {
        let mut g = IdGen::new(1000, ORDERS);
        let x = g.next();
        let y = g.next();
        assert_ne!(x >> 64, y >> 64, "high halves differ");
        assert_ne!(x as u64, y as u64, "low halves differ");
        let z = IdGen::new(1001, ORDERS).next();
        assert_ne!(x, z);
        assert_eq!(g.ordinal(), 2);
    }

    #[test]
    fn two_identities_mint_disjoint_series_and_version_is_not_an_input() {
        let a = IdGen::new(5, ORDERS).next();
        let b = IdGen::new(5, KV).next();
        assert_ne!(a, b);
        const ORDERS_V2: FsmIdentity = FsmIdentity::parse("orders", 0x0200_0000);
        assert_eq!(IdGen::new(5, ORDERS_V2).next(), a, "an upgrade must not change what a replay mints");
    }

    #[test]
    fn same_inputs_same_series() {
        let mut g1 = IdGen::new(42, ORDERS);
        let mut g2 = IdGen::new(42, ORDERS);
        for _ in 0..5 {
            assert_eq!(g1.next(), g2.next());
        }
    }
}
```

And in `uc_service/tests/raw_contract.rs`, add a test proving the context is the only route to a generator and that `Sessioned` forwards it:

```rust
#[test]
fn ctx_ids_is_the_only_generator_and_sessioned_forwards_the_context() {
    use uc_service::{ApplyCtx, RawStateMachine, SessionConfig, Sessioned};
    struct Minter { seen: Vec<u128>, last: Option<u64> }
    impl RawStateMachine for Minter {
        const NAME: &'static str = "minter";
        fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) {
            let mut ids = ctx.ids();
            self.seen.push(ids.next());
            self.last = Some(ctx.position);
            out.extend_from_slice(&ctx.position.to_le_bytes());
        }
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> { self.last }
    }
    let direct = {
        let mut m = Minter { seen: vec![], last: None };
        let mut out = Vec::new();
        m.apply(&mut ApplyCtx::new(64, Minter::IDENTITY), &[], &mut out);
        m.seen[0]
    };
    let mut s = Sessioned::new(Minter { seen: vec![], last: None }, SessionConfig::default());
    let mut cmd = Vec::new();
    cmd.extend_from_slice(&1u64.to_le_bytes()); // client_id
    cmd.extend_from_slice(&1u64.to_le_bytes()); // seq
    let mut out = Vec::new();
    s.apply(&mut ApplyCtx::new(64, <Sessioned<Minter> as RawStateMachine>::IDENTITY), &cmd, &mut out);
    assert_eq!(<Sessioned<Minter> as RawStateMachine>::NAME, "minter");
    assert_eq!(s.inner().seen[0], direct, "same position, same identity → same ID through the wrapper");
    assert_eq!(s.last_applied(), Some(64));
}
```

(`Sessioned::inner()` — add `pub fn inner(&self) -> &S` to `session.rs` if absent; it is a one-line accessor.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_service ids` → compile error (no `ids` module). `cargo test -p uc_service --test raw_contract ctx_ids` → compile error (`ApplyCtx` undefined).

- [ ] **Step 3: Implement `uc_service/src/ids.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Deterministic IDs (spec §3.4): one generator per apply call, built from
//! the frame's position and the FSM's identity; the same series on every
//! replica, whether it replayed the journal or installed a snapshot.

use std::marker::PhantomData;

use uc_protocol::identity::FsmIdentity;

// FROZEN round constants — a change is a flag day (spec §3.4).
const K0: u64 = 0x9E37_79B9_7F4A_7C15;
const K1: u64 = 0xD1B5_4A32_D192_ED03;
const K2: u64 = 0x8CB9_2BA7_2F3D_8DD7;

/// murmur3's 64-bit finalizer.
#[inline]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// Three-round Feistel over the two 64-bit halves. A Feistel network is a
/// bijection for ANY round function, so distinct inputs give distinct IDs by
/// construction. FROZEN.
#[inline]
pub(crate) fn permute(mut a: u64, mut b: u64) -> u128 {
    a ^= fmix64(b ^ K0);
    b ^= fmix64(a ^ K1);
    a ^= fmix64(b ^ K2);
    ((a as u128) << 64) | b as u128
}

#[cfg(test)]
pub(crate) fn unpermute(x: u128) -> (u64, u64) {
    let (mut a, mut b) = ((x >> 64) as u64, x as u64);
    a ^= fmix64(b ^ K2);
    b ^= fmix64(a ^ K1);
    a ^= fmix64(b ^ K0);
    (a, b)
}

/// The ID generator for ONE apply call. Obtain it from
/// [`ApplyCtx::ids`](crate::ApplyCtx::ids); never keep one across calls — a
/// stashed generator reintroduces the lifetime-counter divergence spec §3.4
/// describes, and the type is `!Send` so the obvious stash into a
/// `Send` state machine fails to compile.
pub struct IdGen {
    position: u64,
    fold: u32,
    ordinal: u32,
    _not_send: PhantomData<*const ()>,
}

impl IdGen {
    pub fn new(position: u64, identity: FsmIdentity) -> IdGen {
        IdGen { position, fold: identity.fold32(), ordinal: 0, _not_send: PhantomData }
    }

    /// The next ID in this apply call's series. Input: `position ‖ ordinal ‖
    /// fold32(identity)`; output: the frozen permutation of it.
    pub fn next(&mut self) -> u128 {
        let o = self.ordinal;
        self.ordinal = o.checked_add(1).expect("IdGen: more than 2^32 IDs in one apply call");
        permute(self.position, ((o as u64) << 32) | self.fold as u64)
    }

    /// How many IDs this generator has minted.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
}
```

- [ ] **Step 4: Implement the trait change** in `uc_service/src/traits.rs`. Add at the top:

```rust
use uc_protocol::identity::FsmIdentity;

use crate::ids::IdGen;

/// Everything the framework knows about the committed frame being applied
/// (spec §3.3). Built by the apply loop and by journal replay, once per
/// frame; a state machine constructs one only in its own unit tests.
/// `#[non_exhaustive]`: the timestamps/scheduler design adds fields here
/// without changing `apply`'s signature again.
#[non_exhaustive]
#[derive(Debug)]
pub struct ApplyCtx {
    /// The frame's absolute byte position (the idempotency key).
    pub position: u64,
    identity: FsmIdentity,
}

impl ApplyCtx {
    pub fn new(position: u64, identity: FsmIdentity) -> ApplyCtx {
        ApplyCtx { position, identity }
    }
    pub fn identity(&self) -> FsmIdentity {
        self.identity
    }
    /// The deterministic ID generator for THIS apply call (spec §3.4).
    pub fn ids(&self) -> IdGen {
        IdGen::new(self.position, self.identity)
    }
}
```

`StateMachine` (`:27-47`): add before `type Command`:

```rust
    /// The FSM's identity — the same wherever this type attaches (spec §3).
    const NAME: &'static str;
    /// Packed semantic version of this FSM's logic (`identity::pack_version`);
    /// `0` = unversioned. Equality-checked cluster-wide, never an ID input.
    const VERSION: u32 = 0;
```

and change `fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response;` to `fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Self::Command) -> Self::Response;` (doc: "`ctx.position` is the frame's absolute byte position, the idempotency key; `ctx.ids()` the deterministic ID generator").

`RawStateMachine` (`:55-64`): add `const NAME`, `const VERSION: u32 = 0`, and

```rust
    /// Provided; evaluated (and validated) at first use — a bad `NAME` is a
    /// compile-time error where `IDENTITY` is first named.
    const IDENTITY: FsmIdentity = FsmIdentity::parse(Self::NAME, Self::VERSION);
```

and `fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>);`.

Blanket impl (`:69-92`): add `const NAME: &'static str = S::NAME; const VERSION: u32 = S::VERSION;` and change the apply signature + `StateMachine::apply(self, ctx, cmd)`.

`session.rs:166`: `impl<S: RawStateMachine> RawStateMachine for Sessioned<S>` gains `const NAME: &'static str = S::NAME; const VERSION: u32 = S::VERSION;`; `fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>)` with `let position = ctx.position;` as the first line (so the body is otherwise untouched) and `self.inner.apply(ctx, body, &mut resp);` at `:231`. Add `pub fn inner(&self) -> &S { &self.inner }` if absent.

`apply.rs:390`: `let mut ctx = ApplyCtx::new(pos, S::IDENTITY); sm.apply(&mut ctx, payload, &mut st.resp_buf);` (`S` is `apply_cycle`'s type parameter). `replay.rs:168`: `guard.apply(&mut ApplyCtx::new(pos, S::IDENTITY), &payload[off + HEADER_LEN..off + total], &mut scratch);`. `apply.rs:826` test SM: add `const NAME: &'static str = "count";` and the ctx signature.

`lib.rs`: `pub mod ids;` and extend the re-export: `pub use crate::traits::{ApplyCtx, ...}; pub use crate::ids::IdGen;`.

- [ ] **Step 5: Update every state machine impl** (the list under **Files**, with the names table above). The edit per typed SM is exactly:

```rust
impl StateMachine for CountSm {
    const NAME: &'static str = "count";
    type Command = ...;
    ...
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> Resp {
        let position = ctx.position; // then the body as before
```

Import `ApplyCtx` from `uc_service` (`use uc_service::ApplyCtx;` or the fully-qualified `uc_service::ApplyCtx` where the file uses full paths, e.g. `uc_lincheck/src/register.rs`). Raw SMs (`RawStateMachine`) take `ctx: &mut ApplyCtx` the same way.

- [ ] **Step 6: Run to verify passes**

Run: `cargo test -p uc_service ids` → prints the three golden values; pin them into `EXPECTED`, re-run → 5 passed. `cargo test -p uc_service --test raw_contract` → PASS. Then the full Global Constraints set, plus `(cd fuzz && cargo +nightly check)`.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A uc_service uc_lincheck uc_node uc_client uc_gateway examples fuzz/src
git commit -m "feat(uc_service): ApplyCtx apply signature, const NAME/VERSION/IDENTITY, IdGen via ctx.ids() (spec §3.3-3.4); every in-tree SM named"
```

---

### Task 4: `ServicesConfig` by name; `[services]` required; the node publishes names at boot

**Files:**
- Modify: `uc_node/src/services.rs` (the `ServicesConfig` struct, `Default`, `from_ids` → `from_names`, `from_cli`, new accessors, tests `:322-471`)
- Modify: `uc_node/src/config_file.rs:188-197` (`ServicesSection`), `:44-80` (`ConfigError` gains `ServicesChoiceRequired`), `:653-676` (parse block), `:770-786` (`MINIMAL` fixture gains a `[services]`), tests `:1338-1372`
- Modify: `uc_node/src/node.rs:657-664` (`CncMeta { .. services: cfg.services.service_names() }`), `:9979` (test)
- Modify every `ServicesConfig::default()` / `from_ids(` site: `uc_node/src/node.rs:8618`, `uc_node/tests/backup.rs:845,885,925,969,1178,1258`, `uc_node/tests/obs_http.rs:284`, `uc_node/tests/crypto_adversarial.rs:195,1176`, `uc_node/examples/m10_alerts.rs:663,983,1086,1355`, `uc_node/examples/m10_gate.rs:591`, `uc_node/tests/lincheck_v2/mod.rs:262-268`, `uc_node/tests/services.rs:64-66` (`ids` helper → `names`), `uc_gateway/examples/m12_gate.rs:1643-1648` (`services_from_flags`), `examples/uc_crashtest/src/bin/uc_crashtest-node.rs:117` — (`grep -rn "ServicesConfig::default()\|from_ids(" --include=*.rs . | grep -v /target/` is the authoritative list).

**Interfaces:**
- Consumes: `FsmName` (Task 1); every SM's `NAME` (Task 3).
- Produces (`uc_node::ServicesConfig`, still `Copy`):
  - `pub fn from_names(names: &[&str], fsm_lag: Option<FsmLag>) -> Result<Self, String>` — refusals, each a full sentence starting `services.names`: empty; more than 8; invalid (`"services.names: {name:?}: {NameError}"`); duplicate.
  - `pub fn single(name: &str) -> Self` (panics on an invalid name — programmatic use only).
  - `pub fn tagged(n: u8) -> Self` — `fsm0..fsm{n-1}` (Task 5's `Tagged` harness rows).
  - `pub fn from_cli(names: Option<&str>, fsm_lag: Option<&str>) -> Result<Self, String>` — `names` absent is `Err("--services is required: a comma-separated list of FSM names in row order, e.g. --services kv,orders")`.
  - `pub fn name_of(&self, row: u8) -> Option<FsmName>`, `pub fn row_of(&self, name: &str) -> Option<u8>`, `pub fn service_names(&self) -> [Option<FsmName>; 8]`, `pub fn identity_hashes(&self) -> [u64; 8]` (0 for undeclared rows), `pub fn count(&self) -> u8`, `pub fn with_lag(self, lag: Option<FsmLag>) -> Self` (harness helper: same names, another lag).
  - `declared()`, `is_declared()`, `ids()`, `ring_ids()`, `ring_mask()`, `resolve_lag()`, `page_lag_value()`, `validate()`, `none_for_tests()` keep their signatures; `declared()` is now `(1 << count) - 1` (contiguous rows).
  - `ConfigError::ServicesChoiceRequired` with message `"[services] section is required: names = [\"<fsm>\", ...] in row order, identical on every node (FSM identity, 2.11)"`.
  - `[services] ids = [...]` → `ConfigError::Invalid { field: "services.ids", detail: "services.ids was replaced by services.names (FSM identity): list the FSM names in row order, e.g. names = [\"kv\", \"orders\"]" }`.

- [ ] **Step 1: Write the failing tests.** Replace `services.rs`'s `default_is_fsm_zero_with_unset_lag_resolving_to_a_quarter_buffer`, `from_ids_builds_the_bitmask_in_any_order`, `from_ids_refusals_are_named`, `from_cli_absent_is_default_and_both_flags_parse`, `from_cli_refuses_by_flag_name` with:

```rust
    #[test]
    fn from_names_assigns_rows_in_list_order_and_single_is_one_row() {
        let c = ServicesConfig::from_names(&["orders", "kv"], None).unwrap();
        assert_eq!(c.count(), 2);
        assert_eq!(c.declared(), 0b11);
        assert_eq!(c.row_of("orders"), Some(0));
        assert_eq!(c.row_of("kv"), Some(1));
        assert_eq!(c.row_of("nope"), None);
        assert_eq!(c.name_of(1).unwrap().as_str(), "kv");
        assert_eq!(c.name_of(2), None);
        assert_eq!(c.identity_hashes()[0], FsmName::parse("orders").unwrap().hash());
        assert_eq!(c.identity_hashes()[2], 0);
        assert_eq!(c.ids().collect::<Vec<_>>(), vec![0, 1]);
        let s = ServicesConfig::single("count");
        assert_eq!((s.count(), s.declared(), s.resolve_lag(1 << 24)), (1, 0b1, FsmLag::Bounded(1 << 22)));
        assert_eq!(ServicesConfig::tagged(3).row_of("fsm2"), Some(2));
    }

    #[test]
    fn from_names_refusals_are_named() {
        let e = |n: &[&str]| ServicesConfig::from_names(n, None).unwrap_err();
        assert!(e(&[]).contains("services.names must not be empty"), "{}", e(&[]));
        assert!(e(&["a", "a"]).contains("duplicate FSM name \"a\""));
        assert!(e(&["1abc"]).contains("services.names: \"1abc\": FSM name must start with a-z"));
        let nine: Vec<String> = (0..9).map(|i| format!("f{i}")).collect();
        let nine: Vec<&str> = nine.iter().map(String::as_str).collect();
        assert!(e(&nine).contains("at most 8 FSMs"));
    }

    #[test]
    fn from_cli_requires_services_and_parses_both_flags() {
        let e = ServicesConfig::from_cli(None, None).unwrap_err();
        assert!(e.starts_with("--services is required"), "{e}");
        let c = ServicesConfig::from_cli(Some("kv, orders"), Some("lockstep")).unwrap();
        assert_eq!(c.row_of("orders"), Some(1));
        assert_eq!(c.resolve_lag(1 << 24), FsmLag::Lockstep);
        assert!(ServicesConfig::from_cli(Some("Kv"), None).unwrap_err().starts_with("--services"));
        assert!(ServicesConfig::from_cli(Some("kv"), Some("16 MiB")).unwrap_err().starts_with("--fsm-lag"));
    }
```

(keep `none_for_tests_declares_nothing_but_still_rings_fsm_zero`, `lag_validation_*`, `service_mins_*`, `parse_fsm_lag_table`, `fsm_lag_eff_table`, `report_ceiling_*` as they are, updating any `from_ids(&[0,1]..)` inside them to `from_names(&["a","b"], ..)`).

In `config_file.rs` tests, replace `services_section_absent_means_fsm_zero_and_the_default_bound`, `services_section_parses_ids_and_a_byte_size_lag`, `services_section_parses_lockstep`, and the `services_refusals_name_the_field` table with:

```rust
    #[test]
    fn services_section_is_required_like_crypto_and_admin() {
        let body = MINIMAL.replace("[services]\nnames = [\"sm\"]\n", "");
        assert!(!body.contains("[services]"));
        let err = load_str(&body).unwrap_err();
        assert!(matches!(err, ConfigError::ServicesChoiceRequired), "{err}");
        assert!(err.to_string().contains("[services] section is required"));
    }

    #[test]
    fn services_section_parses_names_in_row_order_and_a_lag() {
        let body = MINIMAL.replace(
            "names = [\"sm\"]",
            "names = [\"kv\", \"orders\"]\nfsm_lag = \"16MiB\"",
        );
        let (cfg, _) = load_str(&body).unwrap();
        assert_eq!(
            cfg.services,
            ServicesConfig::from_names(&["kv", "orders"], Some(FsmLag::Bounded(16 << 20))).unwrap()
        );
    }

    #[test]
    fn services_ids_is_refused_with_a_pointer_to_names() {
        let body = MINIMAL.replace("names = [\"sm\"]", "ids = [0, 1]");
        let err = load_str(&body).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { field: "services.ids", .. }), "{err}");
        assert!(err.to_string().contains("replaced by services.names"), "{err}");
    }

    #[test]
    fn services_refusals_name_the_field() {
        for (repl, needle, field) in [
            ("names = []", "services.names must not be empty", "services.names"),
            ("names = [\"a\", \"a\"]", "duplicate FSM name", "services.names"),
            ("names = [\"Orders\"]", "must start with a-z", "services.names"),
            ("names = [\"sm\"]\nfsm_lag = \"16 MiB\"", "services.fsm_lag must be", "services.fsm_lag"),
        ] {
            let body = MINIMAL.replace("names = [\"sm\"]", repl);
            let err = load_str(&body).unwrap_err();
            match err {
                ConfigError::Invalid { field: f, detail } => {
                    assert_eq!(f, field, "{repl}");
                    assert!(detail.contains(needle), "{repl}: {detail}");
                }
                other => panic!("{repl}: {other}"),
            }
        }
    }
```

and add `[services]\nnames = ["sm"]\n` to the `MINIMAL` fixture (after `[admin]\nauth = "none"`).

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_node --lib services::tests config_file::tests` → compile errors (`from_names`, `count`, `ServicesChoiceRequired` undefined).

- [ ] **Step 3: Implement `services.rs`.** Replace the struct, `Default`, `from_ids`, `from_cli`, `declared` (keep the rest):

```rust
use uc_protocol::identity::FsmName;

/// The declared FSM set (row → name, in `[services] names` order) + lag
/// policy. Static per node; must match cluster-wide (checked by name on the
/// snapshot path, exported for alerting). There is no default: a node names
/// its FSMs or refuses to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServicesConfig {
    names: [Option<FsmName>; CNC_MAX_SERVICES],
    count: u8,
    fsm_lag: Option<FsmLag>,
}

impl ServicesConfig {
    pub fn from_names(names: &[&str], fsm_lag: Option<FsmLag>) -> Result<Self, String> {
        if names.is_empty() {
            return Err("services.names must not be empty: list the FSM names in row order".into());
        }
        if names.len() > CNC_MAX_SERVICES {
            return Err(format!("services.names: at most {CNC_MAX_SERVICES} FSMs per log, got {}", names.len()));
        }
        let mut out = [None; CNC_MAX_SERVICES];
        for (i, raw) in names.iter().enumerate() {
            let n = FsmName::parse(raw).map_err(|e| format!("services.names: {raw:?}: {e}"))?;
            if out[..i].contains(&Some(n)) {
                return Err(format!("services.names: duplicate FSM name {raw:?}"));
            }
            out[i] = Some(n);
        }
        Ok(Self { names: out, count: names.len() as u8, fsm_lag })
    }

    /// One FSM at row 0. Programmatic use (tests, harnesses); panics on an
    /// invalid name, which is a bug at the call site, not a config error.
    pub fn single(name: &str) -> Self {
        Self::from_names(&[name], None).expect("a valid FSM name")
    }

    /// `fsm0..fsm{n-1}`: the rows `uc_service::Tagged<ROW, S>` attaches to.
    pub fn tagged(n: u8) -> Self {
        let names: Vec<String> = (0..n).map(|i| format!("fsm{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        Self::from_names(&refs, None).expect("fsm0..fsm7 are valid names")
    }

    pub fn from_cli(names: Option<&str>, fsm_lag: Option<&str>) -> Result<Self, String> {
        let lag = match fsm_lag {
            None => None,
            Some(raw) => Some(parse_fsm_lag(raw.trim()).map_err(|d| format!("--fsm-lag {raw:?}: {d}"))?),
        };
        let Some(list) = names else {
            return Err("--services is required: a comma-separated list of FSM names in row order, e.g. --services kv,orders".into());
        };
        let parts: Vec<&str> = list.split(',').map(str::trim).collect();
        Self::from_names(&parts, lag).map_err(|d| format!("--services {list:?}: {d}"))
    }

    #[doc(hidden)]
    pub fn none_for_tests() -> Self {
        Self { names: [None; CNC_MAX_SERVICES], count: 0, fsm_lag: None }
    }

    pub fn count(&self) -> u8 { self.count }
    pub fn with_lag(mut self, lag: Option<FsmLag>) -> Self { self.fsm_lag = lag; self }
    pub fn declared(&self) -> u64 { (1u64 << self.count) - 1 }
    pub fn name_of(&self, row: u8) -> Option<FsmName> { self.names.get(row as usize).copied().flatten() }
    pub fn row_of(&self, name: &str) -> Option<u8> {
        let n = FsmName::parse(name).ok()?;
        self.names.iter().position(|x| *x == Some(n)).map(|i| i as u8)
    }
    pub fn service_names(&self) -> [Option<FsmName>; CNC_MAX_SERVICES] { self.names }
    pub fn identity_hashes(&self) -> [u64; CNC_MAX_SERVICES] {
        let mut h = [0u64; CNC_MAX_SERVICES];
        for (i, n) in self.names.iter().enumerate() {
            if let Some(n) = n { h[i] = n.hash(); }
        }
        h
    }
    // is_declared / ids / ring_ids / ring_mask / resolve_lag / page_lag_value / validate: unchanged.
}
```

Update the module doc (`:4-6`) and `FsmLag`'s untouched. Delete `impl Default for ServicesConfig`.

- [ ] **Step 4: Implement `config_file.rs`.** `ServicesSection { names: Vec<String>, #[serde(default)] ids: Option<Vec<u8>>, #[serde(default)] fsm_lag: Option<String> }` — `ids` is accepted by serde only so the loader can refuse it by name. Parse block:

```rust
    let services = match f.services {
        None => return Err(ConfigError::ServicesChoiceRequired),
        Some(s) => {
            if s.ids.is_some() {
                return Err(ConfigError::Invalid {
                    field: "services.ids",
                    detail: "services.ids was replaced by services.names (FSM identity): list the FSM names in row order, e.g. names = [\"kv\", \"orders\"]".into(),
                });
            }
            let fsm_lag = /* unchanged */;
            let refs: Vec<&str> = s.names.iter().map(String::as_str).collect();
            let cfg = ServicesConfig::from_names(&refs, fsm_lag).map_err(|detail| ConfigError::Invalid { field: "services.names", detail })?;
            cfg.validate(f.buffer_bytes as u64).map_err(|detail| ConfigError::Invalid { field: "services.fsm_lag", detail })?;
            cfg
        }
    };
```

Add the `ServicesChoiceRequired` variant beside `AdminChoiceRequired` with the message from **Interfaces**. Since `names: Vec<String>` is now a required field of the section, a `[services]` with only `fsm_lag` is a serde parse error naming `names` — acceptable and covered by `deny_unknown_fields` style.

- [ ] **Step 5: Node boot + every call site.** `node.rs:657`: `services: cfg.services.service_names(),` in the `CncMeta` literal. Every `ServicesConfig::default()` → `ServicesConfig::single(<the SM that test attaches>::NAME)` (e.g. `obs_http.rs` → `NoopSm::NAME`; `crypto_adversarial.rs`/`crypto_cluster.rs` → `CountSm::NAME`; `backup.rs` → `RestoreCountSm::NAME` where a service attaches, `none_for_tests()` where none does — read each test); every `from_ids(&[0, 1], lag)` → `from_names(&[A::NAME, B::NAME], lag)` with the two types that test actually attaches (`m10_alerts.rs:983,1086` → `[NoopSm::NAME, SlowSm::NAME]`; `backup.rs:1178,1258` → read the test; `services.rs` helper `ids(&[..])` becomes `names(&[..])` taking `&[&str]`; `lincheck_v2/mod.rs:262-268` → `FsmSet::Single => ServicesConfig::single(RegisterSm::NAME)`, `FsmSet::Two { lag } => ServicesConfig::tagged(2)` with `lag` — Task 9 finishes the harness). `m12_gate.rs` `services_from_flags` and `uc_crashtest-node.rs` compile unchanged (they call `from_cli`); their callers now must pass `--services` — the crashtest `spawn_node`/`spawn_node_with` (`examples/uc_crashtest/tests/common/mod.rs:220-262`) gain `.arg("--services").arg("counter")` (the counter service's name) and `m14_fleet_gate.py` is Task 9.

- [ ] **Step 6: Run to verify passes**, then the Global Constraints set. Note for the two-FSM tests in `uc_node/tests/services.rs` that attach `CountSm` twice: in this task services still attach by `service_id`, so declare `names(&["count", "fsm1"], lag)` and keep `.service_id(1)` on the second attach — green now, and Task 5 replaces that second attach with `Tagged<1, CountSm>` (whose name is `fsm1`) without touching the config again.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A uc_node uc_gateway/examples examples/uc_crashtest
git commit -m "feat(node): [services] names in row order, required like [crypto]/[admin]; names + hashes published on the cnc page at boot (spec §4.1-4.2)"
```

---

### Task 5: Attach by name, version written at attach, `Tagged`, binaries take `--fsm`

**Files:**
- Modify: `uc_service/src/config.rs:12-48,69-114` (drop `service_id` + its setter; `ServiceNotDeclared` → `UnknownFsm`; `AlreadyAttached` gains the name), `uc_service/src/attach.rs:59-222`, `uc_service/src/lib.rs:264` (`SnapshotStore::open(&cfg.instance_dir, attached.service_id)`), `uc_service/src/snapshots.rs` (unchanged signature — it takes the row)
- Create: `uc_service/src/tagged.rs`
- Modify every `.service_id(` site: `uc_gateway/examples/m12_gate.rs:772,778,1765`, `uc_node/tests/learner.rs:722`, `uc_node/tests/backup.rs:1184,1262,1268`, `uc_node/tests/services.rs:178,236,259,292,433,472`, `uc_node/examples/apply_bench.rs:178`, `uc_node/tests/lincheck_v2/mod.rs:343`, `uc_node/examples/m10_alerts.rs:989,1093,1096`, `examples/counter/src/bin/counter-service.rs:54`, `examples/uc_crashtest/src/bin/uc_crashtest-service.rs:60`
- Modify the `--service-id` flags: `examples/counter/src/bin/counter-service.rs:31-34` (delete the flag — the binary IS `CounterSm`), `examples/uc_crashtest/src/bin/uc_crashtest-service.rs:43-46` (delete), `uc_gateway/examples/m12_gate.rs:225-229` (→ `--fsm <name>`), `examples/uc_crashtest/tests/common/mod.rs:245-260` (`spawn_service_id` → `spawn_service_named(dir, name)`)

**Interfaces:**
- Consumes: `CncPage::row_of`, `ServiceStatusLine::store_version` (Task 2); `S::IDENTITY` (Task 3); `ServicesConfig::tagged` (Task 4).
- Produces:
  - `ServiceConfig { instance_dir, app_id, snapshot_policy }` — no `service_id`.
  - `ServiceError::UnknownFsm { name: String, declared: Vec<String> }` — `"FSM {name:?} is not declared on this node (declared, in row order: {declared:?}); add it to [services] names on the node, or attach the service that is"`.
  - `ServiceError::AlreadyAttached { name: String, row: u8 }` — `"another process already holds FSM {name:?} at row {row} on this instance dir (service.{row}.lock)"`.
  - `pub struct Tagged<const ROW: u8, S>(pub S)` implementing `StateMachine` (when `S: StateMachine`) and `SnapshotStateMachine` by forwarding, with `NAME = TAGGED_NAMES[ROW as usize]` and `TAGGED_NAMES: [&str; 8] = ["fsm0", ..., "fsm7"]`; `Default` when `S: Default`.
  - Binaries: `counter-service` attaches as `"counter"` (no flag); `uc_crashtest-service` attaches as `"counter"` too (it runs `CounterSm`; `--sessioned` unchanged); `m12_gate service --fsm count|spin|raw|fsm<N>` (`fsm<N>` = `Tagged<N, CountSm>`), replacing `--service-id`/`--raw-sm`/`--work-spin`'s selection role (`--work-spin` stays as the spin count for `spin`).

- [ ] **Step 1: Write the failing tests.** In `uc_node/tests/services.rs`, rewrite `an_undeclared_id_is_refused_by_name_and_a_second_attach_on_the_same_id_is_refused` (`:230`) as:

```rust
#[test]
fn an_unknown_name_is_refused_by_name_and_a_second_attach_of_the_same_fsm_is_refused() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["count", "fsm1"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP), SlowCountSm::default())
        .start()
        .err()
        .expect("slow-count is not declared");
    match &err {
        uc_service::ServiceError::UnknownFsm { name, declared } => {
            assert_eq!(name, "slow-count");
            assert_eq!(declared, &["count".to_string(), "fsm1".to_string()]);
        }
        other => panic!("{other:?}"),
    }
    assert!(err.to_string().contains("FSM \"slow-count\" is not declared"), "{err}");

    let svc1 = start_service::<Tagged<1, CountSm>>(dir.path());
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP), Tagged::<1, CountSm>::default())
        .start()
        .err()
        .expect("fsm1 is held");
    assert!(matches!(&err, uc_service::ServiceError::AlreadyAttached { row: 1, .. }), "{err:?}");
    svc1.stop();
    let svc1b = start_service::<Tagged<1, CountSm>>(dir.path());
    assert_eq!(svc1b.epoch(), 2);
    // The version word is the service's: Tagged forwards CountSm's (0 here).
    assert_eq!(open_cnc(dir.path()).service_slot(1).status.version(), 0);
    svc1b.stop();
    node.stop();
}
```

with `start_service` re-typed as `pub fn start_service<S: StateMachine + Default>(dir: &Path) -> uc_service::Service<S>` (no id argument), and delete `an_out_of_range_service_id_is_a_named_refusal_not_a_shift_overflow_panic` (`:286`, there is no id to be out of range; the shift it guarded is gone). Add a version test beside it:

```rust
#[test]
fn attach_writes_the_declared_version_into_the_slot() {
    #[derive(Default)]
    struct V(CountSm);
    impl StateMachine for V {
        const NAME: &'static str = "count";
        const VERSION: u32 = uc_protocol::identity::pack_version(1, 4, 0);
        type Command = <CountSm as StateMachine>::Command;
        type Response = <CountSm as StateMachine>::Response;
        type Query = <CountSm as StateMachine>::Query;
        type QueryResponse = <CountSm as StateMachine>::QueryResponse;
        fn apply(&mut self, ctx: &mut ApplyCtx, c: Self::Command) -> Self::Response { self.0.apply(ctx, c) }
        fn query(&self, q: Self::Query) -> Self::QueryResponse { self.0.query(q) }
        fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
    }
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["count"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc = start_service::<V>(dir.path());
    assert_eq!(open_cnc(dir.path()).service_slot(0).status.version(), 0x0104_0000);
    svc.stop();
    node.stop();
}
```

And a `Tagged` unit test in `uc_service/src/tagged.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawStateMachine;
    #[derive(Default)]
    struct Inner(u64);
    impl crate::StateMachine for Inner {
        const NAME: &'static str = "inner";
        const VERSION: u32 = 7;
        type Command = u64; type Response = u64; type Query = (); type QueryResponse = u64;
        fn apply(&mut self, _c: &mut crate::ApplyCtx, cmd: u64) -> u64 { self.0 += cmd; self.0 }
        fn query(&self, _q: ()) -> u64 { self.0 }
        fn last_applied(&self) -> Option<u64> { None }
    }
    #[test]
    fn tagged_renames_and_forwards_version_and_logic() {
        assert_eq!(<Tagged<3, Inner> as RawStateMachine>::NAME, "fsm3");
        assert_eq!(<Tagged<3, Inner> as RawStateMachine>::VERSION, 7);
        let mut t = Tagged::<3, Inner>::default();
        assert_eq!(crate::StateMachine::apply(&mut t, &crate::ApplyCtx::new(1, <Tagged<3, Inner> as RawStateMachine>::IDENTITY), 5), 5);
    }
}
```

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_node --test services an_unknown_name` → compile error (`UnknownFsm`, `Tagged` undefined).

- [ ] **Step 3: Implement.** `config.rs`: delete the `service_id` field, its initialiser and the `service_id()` setter; replace the two error variants per **Interfaces**. `tagged.rs`:

```rust
//! `Tagged<ROW, S>`: run one state-machine type at several rows (harnesses
//! only — spec §3.3 "one type, one row"). Zero-cost forwarding newtype whose
//! `NAME` is `fsm<ROW>`; `ServicesConfig::tagged(n)` declares the rows.
use crate::{ApplyCtx, SnapshotStateMachine, StateMachine};
use crate::config::SnapshotError;

pub const TAGGED_NAMES: [&str; 8] = ["fsm0", "fsm1", "fsm2", "fsm3", "fsm4", "fsm5", "fsm6", "fsm7"];

#[derive(Default)]
pub struct Tagged<const ROW: u8, S>(pub S);

impl<const ROW: u8, S: StateMachine> StateMachine for Tagged<ROW, S> {
    const NAME: &'static str = TAGGED_NAMES[ROW as usize];
    const VERSION: u32 = S::VERSION;
    type Command = S::Command; type Response = S::Response; type Query = S::Query; type QueryResponse = S::QueryResponse;
    #[inline] fn apply(&mut self, ctx: &mut ApplyCtx, cmd: S::Command) -> S::Response { self.0.apply(ctx, cmd) }
    #[inline] fn query(&self, q: S::Query) -> S::QueryResponse { self.0.query(q) }
    #[inline] fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
}
impl<const ROW: u8, S: StateMachine + SnapshotStateMachine> SnapshotStateMachine for Tagged<ROW, S> {
    type SnapshotHandle = S::SnapshotHandle;
    fn freeze(&self) -> Result<(S::SnapshotHandle, u64), SnapshotError> { self.0.freeze() }
    fn stream_snapshot(h: S::SnapshotHandle, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> { S::stream_snapshot(h, dst) }
    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> { self.0.install_snapshot(position, src) }
}
```

(`const NAME: &'static str = TAGGED_NAMES[ROW as usize];` indexes a const array in a const context — allowed; `ROW >= 8` fails to compile at first use.) Register `pub mod tagged; pub use tagged::Tagged;` in `lib.rs`.

`attach.rs:63-90` becomes:

```rust
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), &cfg.app_id)?;
    let meta = cnc.meta();
    let instance_id = meta.instance_id;

    // 1b. Find our row BY NAME (spec §4.3). A harness page (`none_for_tests`:
    // `services_declared == 0` and no names) rings row 0 for whoever attaches.
    let names = cnc.service_names();
    let harness = cnc.services_declared() == 0 && names.iter().all(Option::is_none);
    let row: u8 = if harness {
        0
    } else {
        cnc.row_of(&S::IDENTITY.name).ok_or_else(|| ServiceError::UnknownFsm {
            name: S::IDENTITY.name.as_str().to_string(),
            declared: names.iter().flatten().map(|n| n.as_str().to_string()).collect(),
        })?
    };
    let declared = match cnc.services_declared() { 0 => 1, d => d };
    let lag_mode = lag_mode_for(&cnc);
    let lock_path = dir.join(format!("service.{row}.lock"));
    // ... unchanged, with `cfg.service_id` → `row` everywhere below, and
    // `AlreadyAttached { name: S::IDENTITY.name.as_str().to_string(), row }`.
```

After the `s.status.store_release(pack_service_status(row, true, ..))` line add `s.status.store_version(S::VERSION);`. `Attached.service_id = row`. `lib.rs:264`: `SnapshotStore::open(&cfg.instance_dir, attached.service_id)` — move the call after `attach()` if it precedes it.

- [ ] **Step 4: Sweep the call sites.** Every `ServiceConfig::new(..).service_id(id)` → `ServiceConfig::new(..)`, and where `id != 0` the *type* becomes `Tagged<id, T>` (or the distinct type that row hosts). `m10_alerts.rs:1096`: `svc1` runs `SlowSm` — its name is `"slow"`, and that example's node config (`:1086`) lists `[NoopSm::NAME, SlowSm::NAME]`. `apply_bench.rs:176-180`: N rows → `ServicesConfig::tagged(n)` on the page (`:148` writes the mask; the bench builds its own `CncMeta` — pass `ServicesConfig::tagged(a.fsms).service_names()`), and spawn with a `match id { 0 => spawn::<Tagged<0, RawCount>>(..), 1 => .., 7 => .., _ => unreachable!() }` (the const-generic row cannot be a runtime value). `m12_gate.rs`: `--fsm <name>`; the service arm matches `("count", spin == 0)` → `CountSm`, `("spin", _)` → `SpinCountSm::with_spin`, `("raw", _)` → `RawCountSm`, `name.strip_prefix("fsm")` → `Tagged<N, CountSm>` by a `match` over `0..8`; the node arm's `--services` is already names (Task 4). `counter-service.rs` / `uc_crashtest-service.rs`: delete the flag and the `.service_id(..)`. `examples/uc_crashtest/tests/common/mod.rs:245-260`: `spawn_service_id(dir, id)` → `spawn_service_named(dir, name: &str)` is no longer needed (the service binary runs one type); the two-FSM crashtest scenarios (Task 9) get a second binary flag `--tagged <row>` that wraps `CounterSm` in `Tagged<row, CounterSm>` — implement that flag now in `uc_crashtest-service.rs` (`#[arg(long)] tagged: Option<u8>`, `match` over `0..8`), so Task 9 only edits tests.

- [ ] **Step 5: Run to verify passes** — `cargo test -p uc_node --test services`, `cargo test -p uc_service`, then the Global Constraints set. `uc_node/tests/services.rs`'s two-FSM tests now attach `CountSm` at row 0 and `Tagged<1, CountSm>` at row 1 with `names(&["count", "fsm1"], lag)`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(uc_service): attach by NAME — ServiceConfig loses service_id, UnknownFsm refusal, version written at attach, Tagged<ROW, S> for harness rows (spec §4.3, §7)"
```

---

### Task 6: Wire 0.7.0 — `SNAP_BEGIN` carries identity + version per row; positional check by name

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs:167-176` (`SNAP_BEGIN_FIXED_LEN`, layout const), `:243-314` (`SnapBeginBody`, writer, reader), tests `:756-841`
- Modify: `uc_protocol/src/version.rs:65` (`CURRENT` → `0.7.0`), test `:101-104`
- Modify: `uc_net/src/sender.rs:131-144` (`SnapshotSet` gains `identity`, `version`), `:162` region (`SnapSession` gains both), `:1139-1146` (invariant: `services_declared == mask_of(identity)`), `:1250-1261` and `:1390-1412` (`send_snap_begin` takes both arrays)
- Modify: `uc_net/src/receiver.rs:548-562` (stats: `snap_refused_version_mismatch`, `identity_refusal`), `:638-746` (`SnapIntake` stores both arrays; `own_identity`, `own_versions`), `:1113-1120` (`set_snapshot_intake`), `:1834-1855` (the check), `:2153-2216` (`SNAP_DONE` echo), tests `:4987-5850`
- Modify: `uc_node/src/node.rs:1030-1037` (install the closure), `:1522-1540` (`snapshot_session_refusals` → a triple), `:2970-2990` (edge detector logs by name), `:6225-6299` (`snapshot_set_for` fills both arrays), `:9968-10039` (tests)
- Modify: `uc_net/tests/snapshot_session.rs:232-262` (`forge_begin`), `:446-500` (refusal tests)
- Modify: `fuzz/README.md:154` (row text); the target itself is unchanged.

**Interfaces:**
- Consumes: `ServicesConfig::identity_hashes()` (Task 4); `ServiceStatusLine::version()` (Task 2).
- Produces:
  - `uc_protocol::v2::datagram`: `SNAP_BEGIN_FIXED_LEN = 122`; `SNAP_BEGIN_LAYOUT_V3: u8 = 2` (`SNAP_BEGIN_LAYOUT_V2 = 1` stays, as the refused legacy value); `SnapBeginBody { session: u32, layout: u8, service_id: u8, snapshot_pos: u64, total_len: u64, identity: [u64; 8], version: [u32; 8], config: Vec<u8> }` (no `services_declared` field) + `pub fn declared_mask(&self) -> u64`. Layout LE: `session 0..4, layout 4, service_id 5, 6..8 zero, snapshot_pos 8..16, total_len 16..24, identity 24..88 (8 × u64), version 88..120 (8 × u32), config_len u16 120..122, config 122..`.
  - `uc_protocol::version::CURRENT = ProtocolVersion::new(0, 7, 0)`.
  - `uc_net::sender::SnapshotSet { services_declared: u64, identity: [u64; 8], version: [u32; 8], config, artifacts }`.
  - `uc_net::receiver::FollowerReceiver::set_snapshot_intake(&mut self, snap_root: PathBuf, own_identity: [u64; 8], own_versions: Arc<dyn Fn() -> [u32; 8] + Send + Sync>, incoming: Option<IncomingSnapshotSignal>)`.
  - `FollowerStats { snap_refused_declared_mismatch, snap_refused_version_mismatch: AtomicU64, identity_refusal: Mutex<Option<IdentityRefusal>> }` with `pub struct IdentityRefusal { pub row: u8, pub ours: u64, pub theirs: u64, pub ours_version: u32, pub theirs_version: u32, pub kind: RefusalKind }`, `pub enum RefusalKind { Identity, Version }`.
  - `uc_node`: `snapshot_session_refusals() -> (u64, u64, u64)` (legacy, identity, version); the `snapshot_session_refused` obs record gains `row`, `ours`, `theirs` (names where known, else `hash:0x…`), `ours_version`, `theirs_version`.

- [ ] **Step 1: Write the failing tests.** In `datagram.rs` replace the three SNAP_BEGIN tests with:

```rust
    #[test]
    fn snap_begin_body_070_roundtrips_and_pins_layout() {
        assert_eq!(SNAP_BEGIN_FIXED_LEN, 122);
        assert_eq!(SNAP_BEGIN_LAYOUT_V3, 2);
        let mut identity = [0u64; 8];
        identity[0] = 0x1111_2222_3333_4444;
        identity[1] = 0x5555_6666_7777_8888;
        let mut version = [0u32; 8];
        version[1] = 0x0102_0003;
        let b = SnapBeginBody {
            session: 9, layout: SNAP_BEGIN_LAYOUT_V3, service_id: 1, snapshot_pos: 4096,
            total_len: 77, identity, version, config: vec![],
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(&buf[0..4], &9u32.to_le_bytes());
        assert_eq!(buf[4], 2);
        assert_eq!(buf[5], 1);
        assert_eq!(&buf[24..32], &identity[0].to_le_bytes());
        assert_eq!(&buf[32..40], &identity[1].to_le_bytes());
        assert_eq!(&buf[92..96], &version[1].to_le_bytes());
        assert_eq!(&buf[120..122], &0u16.to_le_bytes());
        assert_eq!(read_snap_begin_body(&buf), Some(b.clone()));
        assert_eq!(b.declared_mask(), 0b11);
        assert_eq!(read_snap_begin_body(&buf[..SNAP_BEGIN_FIXED_LEN - 1]), None);
    }

    #[test]
    fn a_wire_060_shaped_snap_begin_is_too_short_and_a_layout_one_body_decodes() {
        // The exact 34-byte fixed part 0.6.0 sent: below the 0.7.0 fixed length.
        let legacy = [0u8; 34];
        assert_eq!(read_snap_begin_body(&legacy), None, "34 bytes is below the 0.7.0 fixed part");
        // A 122-byte body with layout 1 DOES decode — the receiving node
        // refuses it by name (`peer wire ≤ 0.6.0`), not the decoder.
        let b = SnapBeginBody { session: 1, layout: SNAP_BEGIN_LAYOUT_V2, service_id: 0, snapshot_pos: 0, total_len: 1, identity: [0; 8], version: [0; 8], config: vec![] };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(read_snap_begin_body(&buf).unwrap().layout, 1);
    }

    #[test]
    fn snap_begin_config_rides_past_the_fixed_part() {
        let b = SnapBeginBody { session: 1, layout: SNAP_BEGIN_LAYOUT_V3, service_id: 0, snapshot_pos: 0, total_len: 1, identity: [1; 8], version: [0; 8], config: vec![1, 2, 3, 4] };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN + 4];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(&buf[120..122], &4u16.to_le_bytes());
        assert_eq!(read_snap_begin_body(&buf), Some(b));
        assert_eq!(read_snap_begin_body(&buf[..buf.len() - 1]), None);
    }
```

`version.rs`: rename the test to `current_is_the_fsm_identity_wire` asserting `ProtocolVersion::new(0, 7, 0)`.

`uc_net/tests/snapshot_session.rs`: `forge_begin(layout, identity: [u64; 8], version: [u32; 8])`; rewrite `a_mismatched_declared_set_refuses_the_session` as `a_mismatched_identity_refuses_the_session_and_names_the_row`:

```rust
#[test]
fn a_mismatched_identity_refuses_the_session_and_names_the_row() {
    let mut h = build(FaultConfig::default(), &["a"]); // the follower's own row 0 is "a"
    let st = h.follower.stats();
    let mut theirs = [0u64; 8];
    theirs[0] = uc_protocol::identity::FsmName::parse("b").unwrap().hash();
    h.forge_begin(SNAP_BEGIN_LAYOUT_V3, theirs, [0; 8]);
    h.pump_until("the identity refusal is counted", |_| st.snap_refused_declared_mismatch.load(Ordering::Relaxed) > 0);
    let r = st.identity_refusal.lock().unwrap().clone().expect("detail recorded");
    assert_eq!((r.row, r.kind), (0, RefusalKind::Identity));
    assert_eq!(r.ours, uc_protocol::identity::FsmName::parse("a").unwrap().hash());
    assert_eq!(r.theirs, theirs[0]);
    assert_eq!(st.snap_refused_legacy_peer.load(Ordering::Relaxed), 0);
    assert!(!h.follower_snap_dir.join("0").exists());
}

#[test]
fn same_names_in_a_different_row_order_are_refused_positionally() {
    let mut h = build(FaultConfig::default(), &["a", "b"]);
    let st = h.follower.stats();
    let (ha, hb) = (name_hash("a"), name_hash("b"));
    let mut theirs = [0u64; 8];
    theirs[0] = hb;
    theirs[1] = ha;
    h.forge_begin(SNAP_BEGIN_LAYOUT_V3, theirs, [0; 8]);
    h.pump_until("refused", |_| st.snap_refused_declared_mismatch.load(Ordering::Relaxed) > 0);
    let r = st.identity_refusal.lock().unwrap().clone().unwrap();
    assert_eq!((r.row, r.ours, r.theirs), (0, ha, hb));
}

#[test]
fn a_version_mismatch_is_refused_only_when_both_sides_report_one() {
    let mut h = build_with_versions(FaultConfig::default(), &["a"], [0x0100_0000, 0, 0, 0, 0, 0, 0, 0]);
    let st = h.follower.stats();
    let ours = [name_hash("a"), 0, 0, 0, 0, 0, 0, 0];
    // Their row 0 is unversioned: not a mismatch.
    h.forge_begin(SNAP_BEGIN_LAYOUT_V3, ours, [0; 8]);
    h.pump_until("intake opened", |_| h.follower_snap_dir.join("0").exists());
    assert_eq!(st.snap_refused_version_mismatch.load(Ordering::Relaxed), 0);
    // Their row 0 is 2.0.0 against our 1.0.0: refused, by row, both versions.
    h.forge_begin(SNAP_BEGIN_LAYOUT_V3, ours, [0x0200_0000, 0, 0, 0, 0, 0, 0, 0]);
    h.pump_until("version refusal", |_| st.snap_refused_version_mismatch.load(Ordering::Relaxed) > 0);
    let r = st.identity_refusal.lock().unwrap().clone().unwrap();
    assert_eq!((r.row, r.kind, r.ours_version, r.theirs_version), (0, RefusalKind::Version, 0x0100_0000, 0x0200_0000));
}
```

(`build(cfg, &[names])` replaces `build(cfg, &[ids])` — it calls `set_snapshot_intake` with `identity_hashes_of(names)` and a closure returning the `versions` array; `build_with_versions` is the same with an explicit array; `name_hash(s)` = `FsmName::parse(s).unwrap().hash()`. Read the existing `build` at the top of that file and thread the two new arguments through.) `a_layout_zero_begin_is_refused_as_a_wire_050_peer` becomes `a_layout_one_begin_is_refused_as_a_pre_070_peer` forging `SNAP_BEGIN_LAYOUT_V2`; `a_too_short_0_5_0_begin_body_is_refused_as_a_legacy_peer` forges 34 bytes.

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_protocol snap_begin` → FAIL (`SNAP_BEGIN_LAYOUT_V3` undefined); `cargo test -p uc_net --test snapshot_session` → compile errors.

- [ ] **Step 3: Implement `datagram.rs`.** Constants and struct per **Interfaces** (update the doc comment `:243-260` to describe 0.7.0: "`identity[r]` is the sender's row-`r` FSM identity hash, `0` = undeclared; `version[r]` its attached service's packed version, `0` = unknown; the receiver compares both **positionally** and refuses by name — spec §5"). Writer:

```rust
pub fn write_snap_begin_body(buf: &mut [u8], b: &SnapBeginBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4] = b.layout;
    buf[5] = b.service_id;
    buf[6..8].fill(0);
    buf[8..16].copy_from_slice(&b.snapshot_pos.to_le_bytes());
    buf[16..24].copy_from_slice(&b.total_len.to_le_bytes());
    for (i, h) in b.identity.iter().enumerate() {
        buf[24 + i * 8..32 + i * 8].copy_from_slice(&h.to_le_bytes());
    }
    for (i, v) in b.version.iter().enumerate() {
        buf[88 + i * 4..92 + i * 4].copy_from_slice(&v.to_le_bytes());
    }
    buf[120..122].copy_from_slice(&(b.config.len() as u16).to_le_bytes());
    if !b.config.is_empty() {
        buf[122..122 + b.config.len()].copy_from_slice(&b.config);
    }
}
```

Reader mirrors it (length checks against `SNAP_BEGIN_FIXED_LEN` + `config_len` at `120..122`). `declared_mask`: `self.identity.iter().enumerate().fold(0, |m, (i, h)| if *h != 0 { m | (1 << i) } else { m })`.

- [ ] **Step 4: Implement `uc_net`.** `SnapshotSet`/`SnapSession` carry `identity`/`version`; `try_open_snap_session` adds `set.services_declared == mask_of(&set.identity)` to its refusal condition (where `mask_of` folds non-zero entries — put it next to the struct as `pub fn identity_mask(identity: &[u64; 8]) -> u64`); `send_snap_begin(.., identity: &[u64; 8], version: &[u32; 8], config)` fills the body. Receiver: fields `own_identity: [u64; 8]`, `own_versions: Option<Arc<dyn Fn() -> [u32; 8] + Send + Sync>>`; `SnapIntake` stores `identity`/`version` (used by the `SNAP_DONE` echo and by `next_declared_id(intake.declared_mask, ..)`); the check at `:1834-1855`:

```rust
        if b.layout != SNAP_BEGIN_LAYOUT_V3 {
            // "peer wire ≤ 0.6.0" — a body whose discriminator we do not speak.
            self.stats.snap_refused_legacy_peer.fetch_add(1, Ordering::Relaxed);
            self.snap_drop_intake_from(from);
            return;
        }
        let bit = 1u64.checked_shl(b.service_id as u32).unwrap_or(0);
        if b.identity != self.own_identity || b.declared_mask() & bit == 0 {
            let row = (0..8).find(|&r| b.identity[r] != self.own_identity[r]).unwrap_or(b.service_id as usize) as u8;
            *self.stats.identity_refusal.lock().unwrap() = Some(IdentityRefusal {
                row, ours: self.own_identity[row as usize], theirs: b.identity[row as usize],
                ours_version: 0, theirs_version: 0, kind: RefusalKind::Identity,
            });
            self.stats.snap_refused_declared_mismatch.fetch_add(1, Ordering::Relaxed);
            self.snap_drop_intake_from(from);
            return;
        }
        if let Some(own) = &self.own_versions {
            let ours = own();
            if let Some(r) = (0..8).find(|&r| ours[r] != 0 && b.version[r] != 0 && ours[r] != b.version[r]) {
                *self.stats.identity_refusal.lock().unwrap() = Some(IdentityRefusal {
                    row: r as u8, ours: self.own_identity[r], theirs: b.identity[r],
                    ours_version: ours[r], theirs_version: b.version[r], kind: RefusalKind::Version,
                });
                self.stats.snap_refused_version_mismatch.fetch_add(1, Ordering::Relaxed);
                self.snap_drop_intake_from(from);
                return;
            }
        }
```

(`identity_refusal` is a `Mutex` touched only on the refusal path; the hot path never locks it.) Update the stats doc comments: the declared-set counter's doc now says "identity (name) mismatch at some row"; add the new counter's.

- [ ] **Step 5: Implement `uc_node`.** `node.rs:1030`: `receiver.set_snapshot_intake(snap_root.clone(), cfg.services.identity_hashes(), { let cnc = Arc::clone(&cnc); Arc::new(move || { let mut v = [0u32; 8]; for r in 0..8 { v[r] = cnc.service_slot(r).status.version(); } v }) }, Some(..))`. `snapshot_set_for`: build `identity: services.identity_hashes()` and `version` from `cnc.service_slot(id).status.version()` per declared row, `services_declared: mask` unchanged. `snapshot_session_refusals()` returns the triple. The edge detector at `:2970-2990` reads `stats.identity_refusal` when a count moved and emits `snapshot_session_refused` with `row`, `ours` = `services.name_of(row)` or `hash:0x…`, `theirs` = the name among `services.service_names()` whose hash equals `theirs`, else `hash:0x…`, `ours_version`/`theirs_version` via `VersionDisplay`. Update the `node.rs` unit tests at `:9968-10039` for the new `SnapshotSet` fields.

- [ ] **Step 6: Run to verify passes** — `cargo test -p uc_protocol`, `cargo test -p uc_net`, `cargo test -p uc_node --lib`, then the Global Constraints set and `(cd fuzz && cargo +nightly check)`. Update `fuzz/README.md:154`'s row: "Since wire 0.7.0 that includes SNAP_BEGIN's per-row identity hashes and versions".

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(wire): 0.7.0 — SNAP_BEGIN/SNAP_DONE carry per-row identity hashes + versions; receiver compares positionally and records the refusing row (spec §5, §7)"
```

---

### Task 7: Observability — labels by name, identity/version gauges, the new counter, `uc2ctl status`, the alert rule

**Files:**
- Modify: `uc_node/src/obs/metrics.rs:176-236` (`ServiceRow` gains `name`, `identity_hash`, `version`; labels `service="<name>",row="<r>"`), `:362-372` (`uc_services_declared` help text), `:84-90` + `:692-702` (register + emit `uc2_snapshot_refused_version_total`), new gauges `uc2_service_identity_hash{service,row}` and `uc2_service_version{service,row}`; tests near `:1019` and `:1245-1256`
- Modify: `uc_ctl/src/main.rs:556-600` (the services table)
- Modify: `docs/ops/uc2-runbook.md` (the alert-rule block that mentions `service=` — add `Uc2ServiceIdentityDrift` / `Uc2ServiceVersionDrift`), `bench-infra/ansible/roles/run/tasks/main.yml` (the line that mentions `service=`, if it is a rule or dashboard fragment — read it first)

**Interfaces:**
- Consumes: `ServiceIdentityLine::{name, hash}`, `ServiceStatusLine::version` (Task 2); `snapshot_session_refusals() -> (u64, u64, u64)` (Task 6).
- Produces: per-FSM samples labeled `service="orders",row="1"`; gauges `uc2_service_identity_hash`, `uc2_service_version`; counter `uc2_snapshot_refused_version_total`; `uc2ctl status` lines `  row=1 name=orders version=1.2.0 hash=0x… attached=… epoch=… incarnation=… applied=… lag=… snapshot_pos=… heartbeat_age=…`.

- [ ] **Step 1: Write the failing tests.** In `metrics.rs`'s test module (find the existing per-service render test near `:1019`, which builds a page with `store_services_declared(0b11)`), build the page through `CncMeta` with names `["kv", "orders"]`, set row 1's version to `0x0102_0003`, render, and assert:

```rust
        assert!(text.contains("uc_service_attached{service=\"orders\",row=\"1\"} 0\n"), "{text}");
        assert!(text.contains(&format!("uc2_service_identity_hash{{service=\"orders\",row=\"1\"}} {}\n", FsmName::parse("orders").unwrap().hash())), "{text}");
        assert!(text.contains("uc2_service_version{service=\"orders\",row=\"1\"} 16908291\n"), "{text}");
        assert!(text.contains("# HELP uc2_snapshot_refused_version_total"), "{text}");
```

and extend the refusal-counter test at `:1245-1256` with the version counter at `1`. In `uc_ctl`, there is no test harness for `status` output; the step below is verified by running it against a dev cluster (`cargo run -p uc_ctl -- status --instance-dir D --app-id A`) and pasting the output into the commit message.

- [ ] **Step 2: Run to verify fails** — `cargo test -p uc_node --lib obs::metrics` → FAIL (labels lack `row`, gauges absent).

- [ ] **Step 3: Implement.** `service_rows`: `let name = slot.identity.name().map(|n| n.as_str().to_string()).unwrap_or_default(); labels: format!("service=\"{name}\",row=\"{id}\"")`, plus `identity_hash: slot.identity.hash()`, `version: slot.status.version() as u64`. Two `push_gauge_with_services` families after `uc_service_lag_waits` (help: "FNV-1a 64 of the row's declared FSM name; must be identical on every node — alert on `count by (row) (uc2_service_identity_hash) > 1`" and "Packed semantic version of the attached service (0 = none/unversioned); alert on `count by (row, service) (uc2_service_version > 0) > 1`"). Change `uc_services_declared`'s help to "Bitmask of declared rows (contiguous from 0)…". Register and emit the new counter from `snapshot_session_refusals().2`. `uc2ctl status`: print `name=` (from `identity.name()`), `version=` (`VersionDisplay`), `hash=` per row, `row=` first.

- [ ] **Step 4: Alert rule + runbook.** Add to the runbook's alert block:

```yaml
- alert: Uc2ServiceIdentityDrift
  expr: count by (row) (count by (row, uc2_service_identity_hash) (uc2_service_identity_hash)) > 1
  for: 1m
  annotations: { summary: "row {{ $labels.row }} names different FSMs on different nodes — snapshot sessions between them are refused by name" }
- alert: Uc2ServiceVersionDrift
  expr: count by (row) (count by (row, uc2_service_version) (uc2_service_version > 0)) > 1
  for: 5m
  annotations: { summary: "row {{ $labels.row }} runs different FSM versions across nodes" }
```

(the metric-VALUE grouping trick is what Prometheus needs to compare values across instances; if the runbook's rules use a different style, match it). Mention both in the runbook's per-FSM section beside the existing `declared-set mismatch` remedy.

- [ ] **Step 5: Run to verify passes**; Global Constraints; run `uc2ctl status` against a local one-node cluster and check the row line.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(obs): per-FSM labels by name+row, identity-hash and version gauges, version-refusal counter, uc2ctl status by name; drift alert rules (spec §4.5)"
```

---

### Task 8: Client — `fsm(name) -> u8`, `declared_names()`, `ClientError::UnknownFsm`

**Files:**
- Modify: `uc_client/src/engine.rs:203` (`SendHalf` keeps `names: [Option<FsmName>; 8]` read once at `:352-366` from `cnc.service_names()`), `:478` (`declared()` unchanged; add `fsm()` and `declared_names()`), `uc_client/src/client.rs:125-160` and `uc_client/src/pipelined.rs:216-265` (forward both), `uc_client/src/error.rs:78-92` (new variant)
- Modify: `examples/counter/src/bin/counter-client.rs:44-52` (`--service-id <u8>` → `--fsm <name>`, resolved through `client.fsm(..)`)
- Tests: `uc_client/tests/roundtrip.rs` (a resolution test against a real node), `uc_client/src/engine.rs` unit tests near `:1031` (page built with names)

**Interfaces:**
- Consumes: `CncPage::service_names` (Task 2).
- Produces: `Client::fsm(&self, name: &str) -> Result<u8, ClientError>`, `Client::declared_names(&self) -> Vec<FsmName>` (row order), same on `PipelinedClient` and `SendHalf`; `ClientError::UnknownFsm { name: String, declared: Vec<String> }` — `"FSM {name:?} is not declared on the attached node (declared, in row order: {declared:?})"`. Every existing `u8`-taking method is unchanged.

- [ ] **Step 1: Write the failing test** in `uc_client/tests/roundtrip.rs` (it already boots a node + `CountSm` service — reuse its fixture; the node config becomes `ServicesConfig::single(CountSm::NAME)`):

```rust
#[test]
fn fsm_resolves_a_name_to_its_row_and_refuses_an_unknown_one() {
    let f = fixture(); // whatever the file's node+service+client setup is called
    assert_eq!(f.client.fsm("count").unwrap(), 0);
    assert_eq!(f.client.declared_names().iter().map(|n| n.as_str()).collect::<Vec<_>>(), ["count"]);
    match f.client.fsm("orders") {
        Err(uc_client::ClientError::UnknownFsm { name, declared }) => {
            assert_eq!(name, "orders");
            assert_eq!(declared, ["count"]);
        }
        other => panic!("{other:?}"),
    }
    let row = f.client.fsm("count").unwrap();
    let r: u64 = f.client.submit_to(row, &Cmd::Add(1)).unwrap();
    assert_eq!(r, 1);
}
```

- [ ] **Step 2: Run to verify fails** — `cargo test -p uc_client --test roundtrip fsm_resolves` → compile error.

- [ ] **Step 3: Implement.** In `SendHalf::open` after the declared-mask parse: `let names = cnc.service_names();` stored on the struct.

```rust
    /// The row declared under `name` on the attached node (spec §6).
    pub fn fsm(&self, name: &str) -> Result<u8, ClientError> {
        let wanted = FsmName::parse(name).ok();
        self.names
            .iter()
            .position(|n| n.is_some() && *n == wanted)
            .map(|i| i as u8)
            .ok_or_else(|| ClientError::UnknownFsm {
                name: name.to_string(),
                declared: self.declared_names().iter().map(|n| n.as_str().to_string()).collect(),
            })
    }
    /// Declared FSM names in row order.
    pub fn declared_names(&self) -> Vec<FsmName> {
        self.names.iter().flatten().copied().collect()
    }
```

Forward from `Client` and `PipelinedClient`. `counter-client.rs`: `#[arg(long)] fsm: Option<String>` → `let row = match args.fsm { Some(n) => client.fsm(&n)?, None => 0 };` where the old code used `service_id.unwrap_or(0)`.

- [ ] **Step 4: Run to verify passes**; Global Constraints.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A uc_client examples/counter
git commit -m "feat(uc_client): fsm(name) -> row, declared_names(), UnknownFsm; counter-client --fsm (spec §6)"
```

---

### Task 9: Capstones by name, the negative scenarios, the fleet driver

**Files:**
- Modify: `uc_node/tests/lincheck_v2/mod.rs:198-270` (`FsmSet::Two` → `ServicesConfig::tagged(2)` with the lag; `spawn_service`/`spawn_service1` attach `SM` and `Tagged<1, SM1>`), `:1335,1403` (the two test SMs already named in Task 3)
- Modify: `uc_node/tests/lin_v2.rs`, `lin_partition_v2.rs` (only where they name ids: `submit_to(1, ..)` stays — rows are unchanged)
- Modify: `examples/uc_crashtest/tests/common/mod.rs:264-285` (`spawn_node_with_services(dir, "counter,fsm1", lag)`), `tests/hard_crash.rs` two-FSM scenarios (second service via `--tagged 1`)
- Modify: `uc_node/tests/learner.rs` — new `a_joiner_whose_rows_are_named_in_the_other_order_is_refused_by_name_and_stalls` and `a_joiner_running_another_fsm_version_is_refused_with_both_versions`
- Modify: `bench-infra/scripts/m14_fleet_gate.py:271-297` (names), `scripts/elle_check.sh` (`--services` args if it passes any; the `quiet_two_fsm` pass)
- Modify: `uc_node/tests/services.rs` `two_fsms_apply_the_same_log_and_fsm_zero_answers_the_client` and friends — already on `Tagged<1, CountSm>` from Task 5; confirm.

**Interfaces:**
- Consumes: `Tagged`, `ServicesConfig::tagged`, `--tagged <row>` on the crashtest service bin (Task 5); `snapshot_session_refusals()` + `identity_refusal` (Task 6).
- Produces: capstones green under names; two negative scenarios in `learner.rs`; the fleet driver's `node_args`/`service_args` by name.

- [ ] **Step 1: Write the failing negative scenarios** in `uc_node/tests/learner.rs` (model them on that file's existing learner-join + snapshot tests — read `SumSm` at `:672` and the join helper first):

```rust
/// Spec §8: same names, other order → every snapshot session is refused BY
/// NAME at the first differing row, the joiner stalls, and the counters +
/// obs record say so. Nothing is installed.
#[test]
fn a_joiner_whose_rows_are_named_in_the_other_order_is_refused_by_name_and_stalls() {
    let leader_cfg = ServicesConfig::from_names(&["sum", "fsm1"], None).unwrap();
    let joiner_cfg = ServicesConfig::from_names(&["fsm1", "sum"], None).unwrap();
    let (cluster, joiner) = boot_leader_and_below_floor_learner(leader_cfg, joiner_cfg); // the file's helper shape
    wait_until("refusal counted", || joiner.snapshot_session_refusals().1 > 0);
    let r = joiner.receiver_stats().identity_refusal.lock().unwrap().clone().unwrap();
    assert_eq!(r.row, 0);
    assert_eq!(r.ours, FsmName::parse("fsm1").unwrap().hash());
    assert_eq!(r.theirs, FsmName::parse("sum").unwrap().hash());
    assert!(!joiner.instance_dir().join("snapshots").join("0").join(format!("snap-{}.ultsnap", cluster.floor())).exists());
    let rec = last_obs_record("snapshot_session_refused");
    assert!(rec.contains("\"ours\":\"fsm1\"") && rec.contains("\"theirs\":\"sum\""), "{rec}");
}

#[test]
fn a_joiner_running_another_fsm_version_is_refused_with_both_versions() {
    // Same names both sides; the joiner's service declares VERSION 2.0.0
    // against the leader's 1.0.0 — two newtypes over SumSm differing only in
    // the const, attached to the leader and the joiner respectively.
    struct SumV1(SumSm);
    struct SumV2(SumSm);
    macro_rules! forward_raw {
        ($t:ident, $v:expr) => {
            impl uc_service::RawStateMachine for $t {
                const NAME: &'static str = "sum";
                const VERSION: u32 = $v;
                fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) { self.0.apply(ctx, cmd, out) }
                fn query(&self, q: &[u8], out: &mut Vec<u8>) { self.0.query(q, out) }
                fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
            }
        };
    }
    forward_raw!(SumV1, pack_version(1, 0, 0));
    forward_raw!(SumV2, pack_version(2, 0, 0));
    let cfg = ServicesConfig::from_names(&["sum"], None).unwrap();
    let (cluster, joiner) = boot_leader_and_below_floor_learner_with::<SumV1, SumV2>(cfg, cfg);
    let _ = &cluster;
    wait_until("version refusal counted", || joiner.snapshot_session_refusals().2 > 0);
    let r = joiner.receiver_stats().identity_refusal.lock().unwrap().clone().unwrap();
    assert_eq!((r.kind, r.ours_version, r.theirs_version), (RefusalKind::Version, pack_version(2, 0, 0), pack_version(1, 0, 0)));
}
```

(`receiver_stats()` / `last_obs_record` — use whatever accessors the file already has for counters and log records; if none exists for the receiver stats, expose `Node::receiver_stats(&self) -> Arc<FollowerStats>` as `#[doc(hidden)]`, the way `snapshot_session_refusals` is exposed.)

- [ ] **Step 2: Run to verify they fail** — the refusal counter never moves before Task 6's check exists? It does exist (Task 6); the tests fail red on the *config* until `from_names` in different order boots two nodes — run them, they must fail at `identity_refusal.is_none()` if you temporarily revert the receiver's positional check to a set comparison (do that revert locally, watch red, restore).

- [ ] **Step 3: Harness edits.** `lincheck_v2/mod.rs`: `FsmSet::Two { lag } => ServicesConfig::tagged(2)` re-lagged via a `with_lag(lag)` helper on `ServicesConfig` (add it: `pub fn with_lag(mut self, lag: Option<FsmLag>) -> Self`), `spawn_service::<SM>` at row 0 must be… **note**: row 0 is `fsm0`, so under `FsmSet::Two` both services are `Tagged<0, SM>` and `Tagged<1, SM1>`; under `Single`, `ServicesConfig::single(SM::NAME)` and a plain `SM`. Crashtest: `spawn_node_with_services(dir, "counter,fsm1", lag)` and the second service `spawn_service_tagged(dir, 1)` → `--tagged 1`. `m14_fleet_gate.py`: `fsms` entries become `(name, spin)` with `name = "spin" if spin else ("count" if i == 0 else f"fsm{i}")`; `node_args` joins names; `service_args` passes `--fsm name --work-spin spin`. `scripts/elle_check.sh`: grep it for `--services`/`--service-id` and apply the same mapping.

- [ ] **Step 4: Run the capstones** (dev-box smoke, per CLAUDE.md never a gate): `cargo test -p uc_node --test lin_v2 two_fsm`, `cargo test -p uc_node --test lin_partition_v2 minority_partition_and_heal_two_fsm`, `cargo test -p uc_crashtest --features hard-crash-tests`, `scripts/elle_check.sh` (`ELLE_DIR=$HOME/.cache/uc2-elle`), plus the full `cargo test -p uc_node`. Paste each command's summary line into the commit message.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "test: two-FSM capstones by name via Tagged; order- and version-mismatch joiners refused by name (spec §9); fleet driver + elle by name"
```

---

### Task 10: Docs sweep, explainer, release writeup, gate doc

**Files:**
- Modify: `docs/reference/wire-protocol.md` (`CURRENT` 0.7.0; the `SNAP_BEGIN` row + body table), `docs/reference/cnc-page.md:67-68,124` (3.1; slot lines 0 and 7), `docs/reference/configuration.md:33,78-114` (`[services] names`, required, `ids` refused), `docs/reference/semver-policy.md` (the flag-day list: 0.7.0 + cnc 3.1; the API-break note and the open major/minor decision), `docs/reference/limits.md` (8 FSMs, one type per row, name rules), `docs/how-to/run-a-cluster.md:159-201,275` (names, required section, one service binary per name), `docs/how-to/upgrade-a-cluster.md:268-320` (a 2.11 section: cnc 3.1 + wire 0.7.0, the `ids`→`names` edit, the two new refusals + remedies), `docs/how-to/monitor-a-cluster.md:104` (labels `service="<name>",row=`; the two drift alerts), `docs/how-to/diagnose-a-node.md:72` (`UnknownFsm`), `docs/how-to/change-cluster-membership.md:123`, `docs/ops/uc2-runbook.md` (cnc decode of line 0 word 1 and line 7; `uc2ctl status` sample), `QUICKSTART.md` (`[services] names = ["counter"]`; `counter-service` without `--service-id`), `README.md` (one line under scope/limits if it lists FSM ids), `docs/VERIFICATION.md` §11 (the two negative scenarios), `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` (an erratum block at the top pointing here for `[services]`, `service_id`, SNAP_BEGIN), `docs/BACKLOG.md` item 2 (→ DONE with the release), item 3 (→ "the static half shipped in 2.11; see §7")
- Create: `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`, `docs/benchmarks/uc2-fsm-identity-gate-<date>.md`
- Modify: `RELEASES.md` (new top section), `docs/releases.md` (new top entry + release-evidence row), `Cargo.toml` workspace `version` + every path dep's `version = "2.10.0"` (per `docs/how-to/cut-a-release.md`), `CLAUDE.md` (version line, "Next up", the standing facts: wire 0.7.0, cnc 3.1, `[services]` required, identity in code)

**Interfaces:** consumes everything above; produces the tag's writeup. **The maintainer decides the version number** (spec §10: major vs minor for the breaking trait change) — ask before editing `Cargo.toml`; write the docs with `2.11.0` as the placeholder string and replace on the answer.

- [ ] **Step 1: The explainer** `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md` — plain language (the maintainer's standing preference), sections: why identity is in code and not config; why the row stays and what "named rows" refuses that M14 did not; what the version promises and does not (§7, Aeron's two halves); `IdGen` — why per-apply scoping is the whole correctness story, the retry rule under `Sessioned`, the cross-FSM fold rule, why no Snowflake; the §2.1 comparison table verbatim; the two refusals an operator can see and their remedies.

- [ ] **Step 2: The gate doc skeleton** `docs/benchmarks/uc2-fsm-identity-gate-<date>.md` with **bars pre-committed before any run** (honest-failure protocol): rows = the M14 gate's a/b/e (steady-window, `m14_fleet_gate.py`, names substituted) with the bar "within the same-source rebuild resolution measured by `scripts/hop1_ab.sh` on the day (record the number first)", plus one row "learner join with names at a two-FSM cluster completes" (pass/fail). Leave the results table empty until the user green-lights the fleet run (user-gated spend).

- [ ] **Step 3: `RELEASES.md` section** (newest first, per CLAUDE.md's required shape): feature bullets — FSM identity in code (`→` explainer + `docs/reference/configuration.md`), `ApplyCtx` + `IdGen` (`→` explainer + SDK docs), per-FSM version (`→` explainer §version), refusals by name + drift alerts (`→` upgrade how-to, monitor how-to); a breaking-changes bullet (trait signature, `ServiceConfig::service_id` gone, `[services]` required, `--service-id` flags gone, wire 0.7.0 + cnc 3.1 flag day); performance bullet → the gate doc. Then the matching `docs/releases.md` entry with the release-evidence row.

- [ ] **Step 4: The sweep.** Work the file list above top to bottom; for each, grep the file for `service_id`, `ids = [`, `--service-id`, `0.6.0`, `cnc 3.0`, `services_declared` and rewrite each hit. `docs/reference/wire-protocol.md`'s `SNAP_BEGIN` body table gets the 0.7.0 layout from Task 6's **Interfaces** verbatim.

- [ ] **Step 5: Verify** — `grep -rn "service_id\|--service-id\|ids = \[" docs QUICKSTART.md README.md examples/*/README.md | grep -v superpowers | grep -v "row\b"` returns only the historical M14 spec/plan text and release notes; `cargo test --workspace --doc` passes (doc examples compile with the new signature).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "docs: FSM identity — explainer, reference/how-to sweep, RELEASES + releases entry, gate doc with pre-committed bars"
```

- [ ] **Step 7 (user-gated): fleet run + version bump + tag** — follow `docs/how-to/cut-a-release.md` once the maintainer picks the version number and green-lights the fleet run; fill the gate doc's results from the run; only then bump `Cargo.toml` versions and tag.

---

## Self-review (run before handing this plan over)

- **Spec coverage.** §3.1-3.2 → T1; §3.3 (`ApplyCtx`, consts, forwarding) → T3; §3.4 (`IdGen`, rules) → T3 + explainer T10; §4.1 → T4; §4.2 → T2 (+ T0 erratum); §4.3 → T5; §4.4 (disk unchanged) → no task, asserted by T5's untouched `SnapshotStore`; §4.5 → T7; §5 → T6; §6 → T8 (client) and the gateway's "unchanged" is asserted by T6 leaving `uc_gateway` untouched; §7 → T3 (`VERSION` const), T5 (written at attach), T6 (checked), T7 (exported); §8 failure modes → tests in T4 (required section, `ids`), T5 (`UnknownFsm`, `AlreadyAttached`), T6/T9 (order, version), T2 (cnc version); §9 → T1-T9 tests + T9 capstones + T10 gate; §10 → T10; §11 → T10 backlog lines; §12 order = task order.
- **Placeholders.** The only deliberately deferred values are the `IdGen` golden vectors (pinned from the first run, by design) and the release version string (the maintainer's decision, spec §10).
- **Type consistency.** `ApplyCtx::new(u64, FsmIdentity)`, `ctx.ids() -> IdGen`, `IdGen::next() -> u128`, `FsmName::{parse, as_str, hash, padded, from_padded}`, `FsmIdentity::{parse, hash, fold32}`, `ServicesConfig::{from_names, single, tagged, from_cli, name_of, row_of, service_names, identity_hashes, count, with_lag}`, `CncMeta.services`, `ServiceStatusLine::{version, store_version}`, `ServiceIdentityLine::{name, hash}`, `CncPage::{service_names, row_of}`, `SnapBeginBody::{identity, version, declared_mask}`, `set_snapshot_intake(root, [u64; 8], Arc<dyn Fn() -> [u32; 8]>, incoming)`, `IdentityRefusal`/`RefusalKind`, `snapshot_session_refusals() -> (u64, u64, u64)`, `Client::{fsm, declared_names}`, `ClientError::UnknownFsm`, `Tagged<const ROW: u8, S>`, `TAGGED_NAMES` — used with these exact names in every task above.
