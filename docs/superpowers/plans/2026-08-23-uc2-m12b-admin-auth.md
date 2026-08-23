# M12b — Admin authentication, audit log, explicit-choice config: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Membership-changing admin operations require a named HMAC key distinct from filesystem access and are recorded in an append-only audit log; `[crypto]` and `[admin]` become explicit choices a `node.toml` must make or the daemon refuses to start.

**Architecture:** The existing cnc admin slot (`CNC_OFF_ADMIN_REQ = 3584`, 32 bytes used) is joined by a new 64-byte `CNC_OFF_ADMIN_AUTH = 3904` line in the reserved band carrying an HMAC-SHA256 tag over `app_id ‖ instance_id ‖ seq ‖ nonce ‖ op ‖ id ‖ ip ‖ port ‖ expiry_ns`, an expiry, and a key-name hash; `uc2ctl` writes the auth line *before* the request's `seq` release-store so the consensus agent's acquire on `seq` orders both. The node verifies **first** in `handle_admin` (leader and follower alike) against an `AdminPolicy` (`Filesystem` = today's behaviour; `Hmac { keys, ttl }`) that the **daemon** builds from `[admin]` and hands to `Node::start_with(cfg, StartOpts)`; library users (in-process tests, gates) keep `Node::start`/`start_with_socket`, which default to `Filesystem` — so no `NodeConfig` literal changes. Every admin request — accepted, refused, retry — is appended to `<instance_dir>/audit.jsonl` (O_APPEND + fsync) *before* the response line is published, and mirrored as an `obs_event!`. The crypto lives in `uc2_crypto::admin` (sign/verify/key files; `hmac` 0.12 is already in the lockfile via `hkdf`); `uc2ctl` gains `--admin-key`, `gen-admin-key`, `audit`. `[crypto]` gains a required `enabled`; absent `[crypto]` or `[admin]` is a named `ConfigError`.

**Tech Stack:** Rust edition 2024; RustCrypto `hmac` + `sha2` (in-family); `clap`/`toml`/`serde` already used by the daemon and `uc2ctl`; no new crates beyond promoting `hmac` to a direct dependency.

**Spec:** `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §3.3 and §5 — read both first. Production-readiness umbrella §7. This plan's one deviation from §5.2 (no "recent `(seq, nonce)` ring") is argued in Task 3 and amended into the spec in Task 7.

## Global Constraints

- No consensus change, no wire-protocol change, **no cnc layout change outside the reserved band** (`3904..4096`) — the new line occupies `3904..3968`; pin it in BOTH `uc_protocol` and `uc2_log` with offset-assertion tests (CLAUDE.md rule). No new remote-admin surface: admin stays local through the cnc slot; the gateway carries no admin ops.
- `NodeConfig` gains NO field (36 struct-literal sites in tests/gates); the admin policy travels via `Node::start_with(cfg, StartOpts)`.
- Exit-code contract of `uc2-node` unchanged: `2` = config/preflight refusal (named, `uc2-node: …` on stderr), `1` = runtime start failure, `0` = clean stop.
- Verification before every commit: `cargo clippy --workspace --all-targets -- -D warnings` (plus `-p uc2_service --features ultima_db`, `-p uc2_service --features apply-profile`, `-p uc2_gateway --features test-util`), `cargo test -p <crate>` for the touched crates, and at the end `cargo test --workspace --exclude uc2_node` + the `uc2_node` fast set (`--lib --test smoke --test failover --test learner --test purge_safety --test query_barrier`) + `--test reconfig` (it drives the admin slot).
- Tests write only under `CARGO_TARGET_TMPDIR` (`tempdir()` helpers as in `uc2_node/tests/lincheck_v2/mod.rs`); never `/tmp`. SPDX header on every new file.
- The audit log is append-only; **record before respond**; one `fsync` per record (admin ops are rare — document the cost).
- Key files: 32 raw bytes, refused if group/world-accessible (`mode & 0o077 != 0`, the same rule `uc2_crypto::identity::Identity::load` applies) — factor that check into one shared helper.
- Dev box is not a bench; nothing here is a perf gate.

## File structure

| Path | Responsibility |
|---|---|
| `uc_protocol/src/v2/cnc.rs` | `CNC_OFF_ADMIN_AUTH = 3904`, its layout doc, static assert, pin-test line |
| `uc2_log/src/cnc.rs` | `AdminAuth { tag, expiry_ns, key_name_hash }`, `read_admin_auth`/`write_admin_auth`, roundtrip+offset-pin test |
| `uc2_crypto/src/admin.rs` (new) | `AdminKey`, `AdminPolicy`, `fnv1a64`, canonical message, `sign`/`verify`, `load_key_file`, `generate_key_file`, shared `check_key_file_perms` (moved out of `identity.rs`) |
| `uc2_node/src/node.rs` | `StartOpts`, `Node::start_with`, `handle_admin` verify-first + reason codes 20–23, `on_config_proposal` audit hook |
| `uc2_node/src/audit.rs` (new) | `AuditLog` (O_APPEND + fsync), `AuditRecord`, `AuditOutcome`; `obs::log::format_line` refactor |
| `uc2_node/src/config_file.rs`, `src/bin/uc2-node.rs` | `CryptoSection.enabled`, `AdminSection`, `ConfigError::{CryptoChoiceRequired, AdminChoiceRequired, AdminKeys}`, `StartupOptions.admin`, policy build in the daemon |
| `uc2ctl/src/main.rs` | `--admin-key`/`--admin-key-name` on `CommonArgs`, `gen-admin-key`, `audit`, reason strings 20–23 |
| `uc2ctl/tests/admin_auth_bin.rs` (new) | end-to-end: in-process `Node::start_with(Hmac)` + the real `uc2ctl` binary |
| `uc2_node/tests/admin_auth.rs` (new) | in-process verify/audit tests |
| `packaging/node.example.toml`, `bench-infra/scripts/m9_fleet_gate.py` (`render_config`) | explicit `[crypto] enabled = …` + `[admin]` |
| `docs/reference/configuration.md`, `docs/how-to/change-cluster-membership.md`, `docs/how-to/upgrade-a-cluster.md`, `docs/how-to/encrypt-node-traffic.md`, `docs/reference/instance-directory.md`, `docs/ops/uc2-runbook.md`, `docs/benchmarks/uc2-m12-gate-2026-08-22.md`, spec §5 | docs + gate rows + amendment |

---

### Task 1: The cnc admin-auth line (pinned in both crates)

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (after `CNC_OFF_FREE_DISK_BYTES`'s doc; the pin test ~449-461)
- Modify: `uc2_log/src/cnc.rs` (structs near `AdminReq`/`AdminResp` ~162-180; accessors near 512-600; tests near 1017-1060)

**Interfaces:**
- Produces:
  ```rust
  // uc_protocol::v2::cnc
  pub const CNC_OFF_ADMIN_AUTH: usize = 3904;   // tag [u8;32] @+0, expiry_ns u64 @+32, key_name_hash u64 @+40, 16 reserved
  // uc2_log::cnc
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct AdminAuth { pub tag: [u8; 32], pub expiry_ns: u64, pub key_name_hash: u64 }
  impl AdminAuth { pub const ZERO: AdminAuth; pub fn is_zero(&self) -> bool }
  impl CncPage { pub fn read_admin_auth(&self) -> AdminAuth; pub fn write_admin_auth(&self, a: &AdminAuth); }
  ```
  Discipline (document on both accessors): the admin writer calls `write_admin_auth` BEFORE `write_admin_req` (whose `seq` store is the release); the consensus agent calls `read_admin_auth` only AFTER `read_admin_req` returned `Some` (its `seq` load is the acquire), so the auth bytes are ordered by the same seqlock. `write_admin_auth(&ZERO)` clears the line (the writer clears it after the response is read, so a later filesystem-policy request never carries a stale tag).

- [ ] **Step 1: Write the failing tests.** In `uc2_log/src/cnc.rs` tests, next to `free_disk_bytes_roundtrip_and_offset_pin`:

```rust
#[test]
fn admin_auth_roundtrip_and_offset_pin() {
    let (page, raw) = fresh_page_for_tests(); // whatever helper the sibling pin tests use
    let a = AdminAuth { tag: [0xA5; 32], expiry_ns: 0x1122_3344_5566_7788, key_name_hash: 0xDEAD_BEEF_CAFE_F00D };
    page.write_admin_auth(&a);
    assert_eq!(page.read_admin_auth(), a);
    assert_eq!(&raw[3904..3936], &[0xA5u8; 32]);
    assert_eq!(&raw[3936..3944], &0x1122_3344_5566_7788u64.to_le_bytes());
    assert_eq!(&raw[3944..3952], &0xDEAD_BEEF_CAFE_F00Du64.to_le_bytes());
    assert!(raw[3952..3968].iter().all(|&b| b == 0));
    page.write_admin_auth(&AdminAuth::ZERO);
    assert!(page.read_admin_auth().is_zero());
}
```
In `uc_protocol/src/v2/cnc.rs`'s pin test add `assert_eq!(CNC_OFF_ADMIN_AUTH, 3904);`.
- [ ] **Step 2: Run** — `cargo test -p uc2_log admin_auth` → FAIL (unresolved); `cargo test -p uc_protocol` → FAIL.
- [ ] **Step 3: Implement** — `uc_protocol`: the const with a doc comment in the style of `CNC_OFF_ADMIN_REQ` (layout per byte, writer = uc2ctl, reader = consensus agent, "Next free reserved-band offset after this line: 3968") and `const _: () = assert!(CNC_OFF_ADMIN_AUTH + 64 <= CNC_PAGE_LEN);`. `uc2_log`: the struct + `ZERO`/`is_zero`; `write_admin_auth` writes tag, expiry LE, hash LE as plain byte stores (no seq word of its own — ordering comes from `req.seq`); `read_admin_auth` plain loads. Keep the module's existing raw-slice style.
- [ ] **Step 4: Run** — both crates' tests PASS; `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(cnc): CNC_OFF_ADMIN_AUTH = 3904 — admin-request HMAC line, pinned in uc_protocol and uc2_log"`.

---

### Task 2: `uc2_crypto::admin` — keys, canonical message, sign/verify, key files

**Files:**
- Create: `uc2_crypto/src/admin.rs`
- Modify: `uc2_crypto/src/lib.rs` (`pub mod admin;`, `CryptoError` variants), `uc2_crypto/src/identity.rs` (use the shared perms helper), `uc2_crypto/Cargo.toml` (`hmac = "0.12"` direct dep; add to `[workspace.dependencies]` as `hmac = "0.12"` and reference `{ workspace = true }`)

**Interfaces:**
- Produces:
  ```rust
  pub const ADMIN_KEY_LEN: usize = 32;
  pub const ADMIN_TAG_LEN: usize = 32;
  pub fn fnv1a64(name: &str) -> u64;                       // key_name_hash
  pub struct AdminKey { pub name: String, pub name_hash: u64, secret: Zeroizing<[u8; 32]> }
  impl AdminKey { pub fn new(name: &str, secret: [u8; 32]) -> Self; pub fn load(name: &str, path: &Path) -> Result<Self, CryptoError>; }
  #[derive(Clone)] pub enum AdminPolicy { Filesystem, Hmac { keys: Arc<Vec<AdminKey>>, ttl: Duration } }
  impl AdminPolicy { pub fn key_by_hash(&self, h: u64) -> Option<&AdminKey>; }
  /// The bytes the tag covers — one canonical layout, both sides:
  /// app_id (u16 LE len ++ bytes) ‖ instance_id u128 LE ‖ seq u64 ‖ nonce u64 ‖ op u32 ‖ id u32 ‖ ip u32 ‖ port u16 ‖ expiry_ns u64
  pub struct AdminMessage<'a> { pub app_id: &'a str, pub instance_id: u128, pub seq: u64, pub nonce: u64, pub op: u32, pub id: u32, pub ip: u32, pub port: u16, pub expiry_ns: u64 }
  impl AdminMessage<'_> { pub fn canonical_bytes(&self) -> Vec<u8>; }
  pub fn sign(key: &AdminKey, m: &AdminMessage<'_>) -> [u8; 32];          // HMAC-SHA256
  pub fn verify(key: &AdminKey, m: &AdminMessage<'_>, tag: &[u8; 32]) -> bool;  // constant-time (hmac::Mac::verify_slice)
  pub fn check_key_file_perms(path: &Path) -> Result<(), CryptoError>;      // the 0o077 rule, moved from identity.rs
  pub fn generate_key_file(path: &Path) -> Result<(), CryptoError>;         // 32 random bytes, create_new, mode 0600
  ```
  `CryptoError` gains `AdminKeyLength { path: String, len: usize }`, `AdminKeyExists { path: String }`.

- [ ] **Step 1: Write the failing tests** (in `admin.rs` `#[cfg(test)]`): (a) `fnv1a64("ops-alice")` equals a hand-computed constant (compute once with a throwaway script and pin it; also `fnv1a64("") == 0xcbf29ce484222325`); (b) a fixed HMAC vector: key = `[0x0b; 32]`, message = `AdminMessage{ app_id: "myapp", instance_id: 1, seq: 7, nonce: 9, op: 1, id: 2, ip: 0x7f000001, port: 9100, expiry_ns: 1_000 }` → `sign` equals `hmac::Mac` computed inline in the test over `canonical_bytes()` (asserts our canonical layout is what we think: also assert `canonical_bytes().len() == 2 + 5 + 16 + 8 + 8 + 4 + 4 + 4 + 2 + 8`); (c) `verify` false on any flipped byte of tag or message; (d) `AdminKey::load` refuses a 0o644 file (`KeyFilePermissions`), a 31-byte file (`AdminKeyLength`), accepts 0o600/32 bytes; (e) `generate_key_file` creates 0600/32 bytes and refuses to overwrite (`AdminKeyExists`); (f) `identity.rs`'s existing permission test still passes after the helper move. Use `tempfile` under `CARGO_TARGET_TMPDIR`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (canonical bytes exactly as documented; `sign` via `Hmac::<Sha256>::new_from_slice`; `verify` via `verify_slice`; `AdminKey` zeroizes; `identity.rs` calls `check_key_file_perms`). **Step 4: Run** `cargo test -p uc2_crypto` PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(crypto): uc2_crypto::admin — named HMAC-SHA256 admin keys, canonical request message, key files (shared 0600 rule)"`.

---

### Task 3: Node verifies first — `AdminPolicy`, `Node::start_with`, reason codes

**Files:**
- Modify: `uc2_node/src/node.rs` (`Node` fields ~344/1707; `start`/`start_with_socket` ~427-435; `handle_admin` 3452-3480; the admin-poll step ~2011-2014)
- Modify: `uc2_node/src/lib.rs` (re-export `uc2_crypto::admin::{AdminPolicy, AdminKey}` and `StartOpts`)
- Test: `uc2_node/tests/admin_auth.rs` (new)

**Interfaces:**
- Produces:
  ```rust
  #[derive(Default)] pub struct StartOpts { pub socket: Option<std::net::UdpSocket>, pub admin: AdminPolicy /* Default = Filesystem */ }
  impl Node {
      pub fn start_with(cfg: NodeConfig, opts: StartOpts) -> io::Result<Node>;  // the one real constructor
      pub fn start(cfg) -> io::Result<Node>                 // = start_with(cfg, StartOpts::default())
      pub fn start_with_socket(cfg, sock) -> io::Result<Node> // = start_with(cfg, StartOpts { socket: Some(sock), ..Default::default() })
  }
  // reason codes (uc2_node::admin_reason, pub consts, documented next to REASON_MALFORMED_OP = 11):
  pub const REASON_AUTH_MISSING: u32 = 20;   // Hmac policy, auth line zero
  pub const REASON_AUTH_BAD_TAG: u32 = 21;
  pub const REASON_AUTH_EXPIRED: u32 = 22;   // expiry_ns <= now (unix ns) — also covers a far-future expiry > now + 2*ttl (refuse: clock games)
  pub const REASON_AUTH_UNKNOWN_KEY: u32 = 23;
  ```
  `AdminPolicy` is stored on `Node` (the consensus agent's state) as `admin: AdminPolicy`; `handle_admin` becomes:

```rust
fn handle_admin(&mut self, req: AdminReq) {
    // M12b: authenticate FIRST — leader and follower alike — so a follower never
    // forwards an unauthenticated proposal. Filesystem policy = legacy (the
    // instance dir's permissions are the boundary); the auth line is ignored.
    if let Err(reason) = self.verify_admin(&req) {
        self.audit(AuditOutcome::Refused(reason), &req, None);
        self.write_admin_reply(req.seq, 1, reason, self.cnc.config_version());
        return;
    }
    // … existing body unchanged, except: every write_admin_reply here is preceded by
    // self.audit(outcome, &req, actor) (Task 4) …
}

fn verify_admin(&self, req: &AdminReq) -> Result<Option<String> /* actor key name */, u32> {
    match &self.admin {
        AdminPolicy::Filesystem => Ok(None),
        AdminPolicy::Hmac { keys, ttl } => {
            let auth = self.cnc.read_admin_auth();
            if auth.is_zero() { return Err(REASON_AUTH_MISSING); }
            let now = unix_ns();
            if auth.expiry_ns <= now || auth.expiry_ns > now + 2 * ttl.as_nanos() as u64 { return Err(REASON_AUTH_EXPIRED); }
            let key = keys.iter().find(|k| k.name_hash == auth.key_name_hash).ok_or(REASON_AUTH_UNKNOWN_KEY)?;
            let meta = self.cnc.meta();
            let m = AdminMessage { app_id: &meta.app_id, instance_id: meta.instance_id, seq: req.seq, nonce: req.nonce,
                                   op: req.op, id: req.id, ip: req.ip, port: req.port, expiry_ns: auth.expiry_ns };
            if !uc2_crypto::admin::verify(key, &m, &auth.tag) { return Err(REASON_AUTH_BAD_TAG); }
            Ok(Some(key.name.clone()))
        }
    }
}
```
  **Why no `(seq, nonce)` replay ring (deviation from spec §5.2, ruled):** the tag covers `seq`, and the consensus agent only ever acts on `seq > last_admin_seq` (`read_admin_req(self.last_admin_seq)`), so a captured request cannot be re-presented with its original `seq`, and re-presenting it with a higher `seq` invalidates the tag; across a node restart `last_admin_seq` resets but `instance_id` changes, which the tag also covers. A ring would therefore never refuse anything the existing checks don't — document this in `verify_admin`'s doc comment and amend the spec in Task 7. `expiry` still bounds the window in which a live, correctly-sequenced request could be delayed and then applied.

- [ ] **Step 1: Write the failing tests** — `uc2_node/tests/admin_auth.rs`, single-node in-process cluster (copy `make_config` from `lincheck_v2/mod.rs` with `n = 1`, `tempdir()` under `CARGO_TARGET_TMPDIR`), a helper that mirrors `uc2ctl`'s flow without the binary:

```rust
fn admin_request(cnc: &CncPage, op: u32, id: u32, addr: (u32, u16), auth: Option<(&AdminKey, Duration /*ttl*/ , Option<[u8;32]> /*override tag*/)>) -> AdminResp {
    let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
    let nonce = rand::random::<u64>();
    match auth {
        None => cnc.write_admin_auth(&AdminAuth::ZERO),
        Some((key, ttl, tag_override)) => {
            let meta = cnc.meta();
            let expiry_ns = unix_ns() + ttl.as_nanos() as u64;
            let m = AdminMessage { app_id: &meta.app_id, instance_id: meta.instance_id, seq, nonce, op, id, ip: addr.0, port: addr.1, expiry_ns };
            let tag = tag_override.unwrap_or_else(|| uc2_crypto::admin::sign(key, &m));
            cnc.write_admin_auth(&AdminAuth { tag, expiry_ns, key_name_hash: key.name_hash });
        }
    }
    cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip: addr.0, port: addr.1 });
    // poll like uc2ctl (20 ms, 10 s)
}
```
Tests: `filesystem_policy_ignores_the_auth_line` (Node::start → a signed-with-garbage AND an unsigned add-learner both reach the normal path: status 0 or a normal refusal reason < 20); `hmac_policy_refuses_unsigned` (status 1, reason 20); `hmac_policy_accepts_a_valid_signature` (status 0 — add-learner of a fresh id/addr on a 1-node cluster is accepted: check `reconfig.rs` for the exact op that succeeds trivially); `bad_tag_is_refused` (reason 21 — flip one tag byte); `expired_is_refused` (reason 22 — `ttl = Duration::ZERO` then sleep 1 ms; and a far-future expiry via an override path → 22); `unknown_key_is_refused` (reason 23 — sign with a key whose name is not in the policy); `a_replayed_request_cannot_be_re_presented` (write the SAME (seq, nonce, tag) again after acceptance → `read_admin_req` never returns it: assert the node's config_version does not advance and no second response appears for 500 ms — this documents the no-ring argument); `follower_verifies_before_forwarding` (3-node cluster: send an unsigned request to a FOLLOWER's cnc under Hmac policy on all three → reason 20 from the follower itself, and the leader's config_version unchanged).
- [ ] **Step 2: Run** → FAIL (no `StartOpts`, no `AdminPolicy`). **Step 3: Implement** per the interfaces (keep `start`/`start_with_socket` as thin wrappers — grep shows 36 literal sites keep compiling unchanged). `unix_ns()` exists in the crate (grep; else add to `obs`). **Step 4: Run** — `cargo test -p uc2_node --test admin_auth --test reconfig` PASS; the fast set PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(node): admin requests authenticated first — AdminPolicy via Node::start_with, HMAC verify, reason codes 20-23"`.

---

### Task 4: The audit log (`audit.jsonl`, record-before-respond)

**Files:**
- Create: `uc2_node/src/audit.rs`
- Modify: `uc2_node/src/obs/log.rs` (extract `pub(crate) fn format_line(level, event, fields) -> String`; `emit` calls it), `uc2_node/src/node.rs` (`audit` field + calls at every `write_admin_reply` and in `on_config_proposal`), `uc2_node/src/lib.rs` (`pub mod audit;`)
- Modify: `docs/reference/instance-directory.md` (durability table: `audit.jsonl` = must survive)
- Test: `uc2_node/tests/admin_auth.rs` (extend), `uc2_node/src/audit.rs` unit tests

**Interfaces:**
- Produces:
  ```rust
  pub struct AuditLog { file: std::fs::File, path: PathBuf }
  impl AuditLog {
      pub fn open(instance_dir: &Path) -> io::Result<AuditLog>;   // OpenOptions::new().create(true).append(true)
      pub fn record(&mut self, r: &AuditRecord) -> io::Result<()>; // one JSON line + sync_data()
      pub fn path(&self) -> &Path;
  }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum AuditOutcome { Accepted, Refused(u32 /*reason*/), Retry }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum AuditOrigin { Local, Forwarded }
  pub struct AuditRecord<'a> { pub ts_ns: u64, pub actor: &'a str /* key name | "filesystem" | "peer:<id>" */, pub origin: AuditOrigin,
                               pub op: u32, pub op_name: &'static str, pub id: u32, pub addr: Option<(u32, u16)>, pub seq: u64, pub nonce: u64,
                               pub outcome: AuditOutcome, pub reason: u32, pub config_version: u64 }
  ```
  Line shape (hand-built via `format_line`, no serde_json): `{"ts_ns":…,"event":"admin_op","actor":"ops-alice","origin":"local","op":1,"op_name":"add_learner","id":4,"addr":"10.0.0.4:9100","seq":12,"nonce":…,"outcome":"accepted","reason":0,"config_version":7}`.
  Hook points: in `handle_admin` before EVERY `write_admin_reply` (refused-by-auth, leader accepted/refused/retry, follower no-leader retry, follower superseded-forward retry); in `on_config_reply` (the follower's final answer for a forwarded request: actor = the name captured in `pending_admin_fwd` — extend that tuple with `Option<String>`); in `on_config_proposal` on the LEADER (actor `peer:<from node id>`, origin `Forwarded`) before `send_config_reply`. A failed audit write is **fatal for the request** (respond `status 1` with a new reason `REASON_AUDIT_FAILED = 24` and `obs_event!` at error) — never silently proceed unrecorded. Also emit `obs_event!(Info, "admin_op", …)` with the same fields (stderr sink).

- [ ] **Step 1: Write the failing tests** — `audit.rs` unit: open twice appends (second open doesn't truncate); line shape byte-exact for a known record; `record` returns Err on a read-only dir (skip if root). `admin_auth.rs`: after each of the scenarios from Task 3, read `<instance_dir>/audit.jsonl` and assert one line per request with the right `outcome`/`reason`/`actor` (`"filesystem"` under Filesystem policy; the key name under Hmac; `"refused"`+`20` for unsigned); **ordering proof**: assert the audit line exists at the moment `read_admin_resp(seq)` first returns `Some` (poll the file in the same loop — the line must never be absent when the response is present). 3-node: a forwarded request produces a `local` line on the follower AND a `forwarded` line (actor `peer:<follower id>`) on the leader.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** — `cargo test -p uc2_node --test admin_auth --test reconfig --lib` PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(node): append-only audit.jsonl for every admin request, recorded before the response is published"`.

---

### Task 5: Explicit-choice config + the daemon builds the policy

**Files:**
- Modify: `uc2_node/src/config_file.rs` (`CryptoSection` + `enabled`; new `AdminSection`, `AdminKeyEntry`; `ConfigError::{CryptoChoiceRequired, AdminChoiceRequired, AdminKeys{detail}}`; `StartupOptions.admin: AdminSection`; `load_from_path`), `uc2_node/src/bin/uc2-node.rs` (build `AdminPolicy` from `opts.admin` — load key files via `AdminKey::load`, refusal → exit 2 with `uc2-node: admin key …` — then `Node::start_with(cfg, StartOpts { socket: None, admin })`; print `uc2-node: WARNING: [admin] auth = "none" — anyone who can write the instance directory can change membership` at boot when `none`)
- Modify: `packaging/node.example.toml` (`[crypto] enabled = false` uncommented with the posture comment + the key paths commented below it; new `[admin]` block uncommented: `auth = "hmac"`, one `keys` entry, `request_ttl_ms = 30000`, comments), `bench-infra/scripts/m9_fleet_gate.py` `render_config` (+ `[crypto]\nenabled = false` and `[admin]\nauth = "none"` — m10/m11 reuse it; grep for any other TOML emitter and do the same)
- Test: `uc2_node/src/config_file.rs` tests; `uc2_node/tests/daemon_refusals.rs` (new, or extend the existing bin-smoke test if one exists — grep `CARGO_BIN_EXE_uc2-node`)

**Interfaces:**
- Produces:
  ```toml
  [crypto]
  enabled = false            # REQUIRED. true needs key_path + allowlist_path (as before)
  [admin]
  auth = "hmac"              # REQUIRED: "hmac" | "none"
  keys = [{ name = "ops-alice", key_path = "/etc/uc2/admin/alice.key" }]   # required when auth = "hmac", ≥ 1 entry, names unique
  request_ttl_ms = 30000     # optional, default 30000, must be ≥ 1000
  ```
  ```rust
  pub struct AdminSection { pub auth: AdminAuthMode /* None | Hmac */, pub keys: Vec<AdminKeyEntry { name: String, key_path: PathBuf }>, pub request_ttl_ms: u64 }
  ConfigError::CryptoChoiceRequired  // "[crypto] section is required: set enabled = false for cleartext (the default posture) or enabled = true with key_path/allowlist_path"
  ConfigError::AdminChoiceRequired   // "[admin] section is required: auth = \"hmac\" with keys = [...] or auth = \"none\" (filesystem access is the boundary)"
  ConfigError::Invalid { field: "admin.keys", .. } // hmac with no keys / duplicate names / ttl < 1000 / keys given with auth = "none"
  ```
  `MINIMAL` test fixture gains both sections so every other existing test keeps passing; add tests: absent `[crypto]` → `CryptoChoiceRequired`; `[crypto]` without `enabled` → parse error naming the field; `enabled = true` without paths → refusal naming them; absent `[admin]` → `AdminChoiceRequired`; `auth = "hmac"` with empty keys → `Invalid{admin.keys}`; duplicate key names → refusal; unknown key inside `[admin]` → refusal by name; `auth = "none"` with keys → refusal. Daemon test: run `CARGO_BIN_EXE_uc2-node --config <toml without [admin]>` → exit 2 + stderr contains `[admin] section is required`; with `auth = "hmac"` and a 0644 key file → exit 2 + `admin key`.

- [ ] **Steps 1–5** as the pattern: failing tests → implement → `cargo test -p uc2_node --lib --test daemon_refusals` + clippy → commit `git commit -am "feat(config): [crypto].enabled and [admin] are explicit choices; the daemon builds the admin policy from [admin]"`. Also run `python3 -m py_compile bench-infra/scripts/m9_fleet_gate.py`.

---

### Task 6: `uc2ctl` — `--admin-key`, `gen-admin-key`, `audit`, reason strings; end-to-end bin test

**Files:**
- Modify: `uc2ctl/src/main.rs`, `uc2ctl/Cargo.toml` (+ `uc2_crypto` path dep)
- Create: `uc2ctl/tests/admin_auth_bin.rs`

**Interfaces:**
- `CommonArgs` gains `#[arg(long)] admin_key: Option<PathBuf>`, `#[arg(long)] admin_key_name: Option<String>` (name defaults to the file stem); `run_mutate` → when `admin_key` is set: load the key (`AdminKey::load`), compute `expiry_ns = unix_ns() + 30 s` (`--admin-ttl-secs` optional, default 30), build `AdminMessage` from the cnc `meta()` + the request fields, `write_admin_auth` THEN `write_admin_req`; after the response (or timeout) `write_admin_auth(&AdminAuth::ZERO)`. When not set: `write_admin_auth(&ZERO)` then the request (a node under Hmac answers reason 20; `reason_str` says `auth_missing — pass --admin-key`).
- New verbs: `GenAdminKey { path: PathBuf }` → `generate_key_file` (prints the path and the `[admin]` snippet to paste: `keys = [{ name = "<stem>", key_path = "<abs path>" }]`); `Audit { instance_dir, #[arg(long)] tail: Option<usize>, #[arg(long)] json: bool }` → reads `audit.jsonl` (no cnc attach needed — offline-safe) and prints one line per record as `ts  actor  origin  op_name id addr  outcome(reason)  cfg=version` or raw JSON with `--json`.
- `reason_str`: 20 `auth_missing (pass --admin-key)`, 21 `auth_bad_tag (wrong key or tampered request)`, 22 `auth_expired (clock skew? retry)`, 23 `auth_unknown_key (name not in [admin].keys)`, 24 `audit_failed (node could not record the request)`.

- [ ] **Step 1: Write the failing test** — `uc2ctl/tests/admin_auth_bin.rs`: start a 1-node cluster IN-PROCESS via `uc2_node::Node::start_with(cfg, StartOpts { admin: AdminPolicy::Hmac { keys: [AdminKey "ops-test" from a generated file], ttl: 30 s } })` (uc2ctl already depends on uc2_node); spawn `CARGO_BIN_EXE_uc2ctl`:
  1. `add-learner --instance-dir D --app-id A --id 7 --addr 127.0.0.1:9 ` (no key) → exit ≠ 0, stdout/stderr contains `auth_missing`;
  2. same with `--admin-key <file>` → exit 0, "accepted";
  3. `--admin-key <other generated file>` (unknown name) → `auth_unknown_key`;
  4. `audit --instance-dir D` → three lines, outcomes refused/accepted/refused in order; `--json` lines parse as JSON (just check braces + `"event":"admin_op"`);
  5. `gen-admin-key <path>` → file exists, 32 bytes, 0600; second run fails with a named error.
  Also a `Filesystem` policy node: the no-key add-learner is accepted (legacy unchanged).
- [ ] **Steps 2–5**: → FAIL → implement → `cargo test -p uc2ctl` PASS (+ `-p uc2_node --test reconfig`), clippy → commit `git commit -am "feat(uc2ctl): --admin-key signing, gen-admin-key, audit; reason strings 20-24"`.

---

### Task 7: Docs, gate rows, spec amendment, fixture sweep

**Files:**
- Modify: `docs/reference/configuration.md` (two refusal rows; `## Admin authentication` section: `[admin]` keys/semantics, key-file rule, `app_id` is NOT a credential, `auth = "none"` consequence; `[crypto].enabled`), `docs/how-to/change-cluster-membership.md` (`--admin-key` flow, `gen-admin-key`, the reason strings, `uc2ctl audit`), `docs/how-to/upgrade-a-cluster.md` (new section "Config choices added in v2.6.0: `[crypto].enabled` and `[admin]`" — a `node.toml` from M9–M11 refuses to start until both are written; what to paste; this is NOT a wire flag day but a config one — runs per host before the binary swap), `docs/how-to/encrypt-node-traffic.md` (`enabled = true`), `docs/reference/instance-directory.md` (`audit.jsonl` row: durable, append-only, no rotation, one fsync per admin op), `docs/ops/uc2-runbook.md` (admin auth ops + reading the audit log), `docs/how-to/run-a-cluster.md` (the two sections in the minimal config), `docs/QUICKSTART.md` if it shows a `node.toml`, `docs/benchmarks/uc2-m12-gate-2026-08-22.md` (M12b row → PASS with the CI test names; facts: fsync-per-admin-op on the consensus thread; no replay ring and why), spec §5.2 amendment note (no ring; reason codes 20–24; `AdminPolicy` via `Node::start_with` not a `NodeConfig` field; `audit_failed = 24`), `README.md` (one line under security posture/limits: admin ops need an HMAC key unless `auth = "none"`), `CLAUDE.md` (M12b status line + the two startup refusals), `.github/workflows/ci.yml` (nothing new unless a new test file needs a feature; confirm `uc2ctl` tests run under `cargo test --workspace --exclude uc2_node` — they do).
- Sweep: every in-process fixture keeps working (library default = Filesystem) — run `cargo test -p uc2_node --test reconfig --test lin_v2` and `cargo test -p uc2-crashtest --features hard-crash-tests --test survival` once to prove it; `bench-infra` TOML emitters changed in Task 5 — grep again for `"[crypto]"`/`"instance_dir ="` across `bench-infra/` and `scripts/`.

- [ ] **Steps:** write/modify the docs (every claim traced to code: reason codes, TOML keys, file paths); run the full verification list from Global Constraints + `cargo doc --workspace --no-deps --lib` + the docs.yml link guard; commit `git commit -am "docs(m12b): admin auth + audit + explicit-choice config — reference, how-tos, upgrade note, runbook, gate rows, spec amendment"`.

---

## Self-review against spec §3.3 / §5

- §3.3 explicit choice (`[crypto].enabled`, `[admin]` required, named refusals, example config, upgrade note, fixtures choose) → Task 5 + Task 7. ✔
- §5.1 credential (named keys, key file 0600/32 B, `gen-admin-key`, `AdminChoiceRequired`) → Tasks 2, 5, 6. ✔
- §5.2 signed request (3904 line layout, tag inputs incl. `app_id`/`instance_id`, expiry, key-name hash, seqlock ordering, `hmac` dep, read-only/offline verbs need no key, verify-first on leader AND follower, reason codes) → Tasks 1, 2, 3, 6 — the `(seq, nonce)` ring is dropped with the argument in Task 3 and the spec amended in Task 7 (plan deviation, ruled). ✔
- §5.3 audit (`audit.jsonl`, O_APPEND + fsync, record-before-respond, fields, obs mirror, no rotation, `uc2ctl audit`, leader records forwarded) → Tasks 4, 6, 7. ✔
- §5.4 `[crypto].enabled` → Task 5. ✔
- §5.5 docs + tests (HMAC vector; expiry/replay/unknown-key/permission refusals; absence refusals; CI integration signed/unsigned/replay; `none` unchanged; fixtures) → Tasks 2–7. ✔
- Names consistent across tasks: `AdminAuth`, `CNC_OFF_ADMIN_AUTH`, `AdminKey`, `AdminPolicy`, `AdminMessage`, `sign`/`verify`, `StartOpts`, `Node::start_with`, `REASON_AUTH_*`/`REASON_AUDIT_FAILED`, `AuditLog`/`AuditRecord`/`AuditOutcome`/`AuditOrigin`, `AdminSection`/`AdminKeyEntry`/`AdminAuthMode`, `ConfigError::{CryptoChoiceRequired, AdminChoiceRequired}`. ✔
