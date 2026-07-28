# M8 Wire Crypto Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give UC v2 opt-in authenticated, encrypted node-to-node UDP transport so a cluster can replicate across a network path the operator does not control.

**Architecture:** A new pure-sync `uc2_crypto` crate owns identity keys, a Noise `IK` handshake driver, the group-key schedule, seal/open, and the anti-replay window. `uc_protocol::v2::crypto` holds wire layouts only. `uc2_net` gains exactly two call seams — seal after `assemble()`, open before dispatch. Two key scopes split by datagram kind: pairwise keys for unicast/low-rate kinds, one cluster group key for the identical-to-N fan-out kinds so `fan_out`'s one-seal-N-sends batching survives.

**Tech Stack:** Rust 2024 edition, `snow` (Noise `IK`), `aes-gcm`, `hkdf`, `sha2`, `rand`, `zeroize`.

**Spec:** `docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md`. Read §3 (key scopes), §4 (wire format + nonce hazard) and §5 (handshake/rotation) before Task 1.

## Global Constraints

- **`uc2_crypto` is pure-sync**: no `async`, no `tokio`, no sockets, no clock reads. Time enters as an explicit `now_ns: u64` parameter, exactly like `uc2_consensus::ElectionSm`. I/O is limited to reading key files in constructors.
- **`uc_protocol` stays `core`-friendly**: `v2::crypto` contains byte offsets, constants, and pure codec functions only. No crypto code, no `std` beyond what `v2::datagram` already uses.
- **Never panic on untrusted input.** Every failure on the receive path (bad tag, replay, unknown epoch, malformed handshake) returns an error that the caller turns into a drop + counter bump. A node must not be killable by a datagram.
- **No wall-clock dependency in the handshake.** Freshness comes from random nonces, never timestamps.
- **Wire protocol version: 0.3.0 → 0.4.0** (`uc_protocol::v2::cnc::CNC_V2_VERSION` is the live gate; `version::CURRENT` is documentation-only and must be kept in step by convention).
- **Crypto is OFF by default.** `CryptoConfig::Disabled` is the default in `NodeConfig`, mirroring `PurgePolicy::Disabled`.
- **Nonce rule (non-negotiable):** the sealing key is derived per sender per boot — `HKDF(group_key, sender_id ‖ boot_salt)` — and the 96-bit GCM nonce is `0u32 ‖ counter`. Never derive a nonce from `position`: it repeats under NAK retransmit.
- Every task ends with `cargo clippy --workspace --all-targets -- -D warnings` clean.
- Write test artifacts to real disk (`CARGO_TARGET_TMPDIR`), never `/tmp` — it is RAM-backed tmpfs with no swap on this box.

## File Structure

**Created:**
- `uc2_crypto/Cargo.toml`, `uc2_crypto/src/lib.rs` — crate root, error type, public surface.
- `uc2_crypto/src/identity.rs` — static keypair loading, allowlist parse/lookup/reload.
- `uc2_crypto/src/schedule.rs` — epoch store, per-sender-per-boot derivation, overlap retention.
- `uc2_crypto/src/seal.rs` — `seal_in_place` / `open_in_place`, AAD construction.
- `uc2_crypto/src/replay.rs` — sliding-window anti-replay.
- `uc2_crypto/src/handshake.rs` — Noise `IK` driver as a driven transition function.
- `uc2_crypto/src/rotation.rs` — rotation trigger policy (pure).
- `uc_protocol/src/v2/crypto.rs` — envelope + handshake body layouts, new kind constants.

**Modified:**
- `uc_protocol/src/v2/mod.rs` — add `pub mod crypto;`.
- `uc_protocol/src/v2/datagram.rs` — `DatagramHeader` gains `key_epoch`; reserved slot repurposed.
- `uc_protocol/src/v2/cnc.rs`, `uc_protocol/src/version.rs` — version 0.4.0.
- `uc2_net/src/sender.rs` — seal seam in `assemble`, MTU budget.
- `uc2_net/src/receiver.rs` — open seam in `do_work`, counters.
- `uc2_net/src/fault.rs` — replay/corrupt injection.
- `uc2_node/src/node.rs` — `CryptoConfig`, boot refusal, handshake routing, rotation hook.
- `Cargo.toml` — workspace member + deps.

---

### Task 1: Wire layouts — key epoch in the header, crypto envelope, new kinds

**Files:**
- Create: `uc_protocol/src/v2/crypto.rs`
- Modify: `uc_protocol/src/v2/mod.rs`, `uc_protocol/src/v2/datagram.rs:20-24,353-380`
- Test: inline `#[cfg(test)]` in both files (crate convention)

**Interfaces:**
- Consumes: nothing.
- Produces: `DatagramHeader { position: u64, leadership_term_id: u32, kind: u8, flags: u8, key_epoch: u16 }`; `CRYPTO_OVERHEAD: usize = 24`; `COUNTER_LEN: usize = 8`; `TAG_LEN: usize = 16`; `DGRAM_KIND_HS_INIT: u8 = 18`; `DGRAM_KIND_HS_RESP: u8 = 19`; `DGRAM_KIND_HS_KEY: u8 = 20`; `write_counter(&mut [u8], u64)`; `read_counter(&[u8]) -> u64`.

- [ ] **Step 1: Write the failing test** in `uc_protocol/src/v2/crypto.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_is_counter_plus_tag() {
        assert_eq!(COUNTER_LEN, 8);
        assert_eq!(TAG_LEN, 16);
        assert_eq!(CRYPTO_OVERHEAD, COUNTER_LEN + TAG_LEN);
        assert_eq!(CRYPTO_OVERHEAD, 24);
    }

    #[test]
    fn kinds_do_not_collide_with_m7_admin() {
        // 16/17 are CONFIG_PROPOSAL/CONFIG_REPLY (M7). Crypto starts at 18.
        assert_eq!(DGRAM_KIND_HS_INIT, 18);
        assert_eq!(DGRAM_KIND_HS_RESP, 19);
        assert_eq!(DGRAM_KIND_HS_KEY, 20);
    }

    #[test]
    fn counter_round_trips_little_endian() {
        let mut buf = [0u8; COUNTER_LEN];
        write_counter(&mut buf, 0x0102_0304_0506_0708);
        assert_eq!(buf[0], 0x08, "little-endian, as every other v2 field");
        assert_eq!(read_counter(&buf), 0x0102_0304_0506_0708);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_protocol crypto::`
Expected: FAIL — `unresolved module or unlinked crate 'crypto'`.

- [ ] **Step 3: Write the module**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M8 wire-crypto layouts (spec: docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md §4).
//! Core-only: offsets, constants, and pure codecs. No crypto code lives here —
//! that is `uc2_crypto`'s job. A sealed datagram is:
//!
//! ```text
//! [ 16B header (cleartext, used as AAD; carries key_epoch) ]
//! [  8B nonce counter (u64 LE, per-sender monotonic)       ]
//! [  ciphertext                                            ]
//! [ 16B AES-256-GCM tag                                    ]
//! ```

/// Per-sender monotonic counter; the low 64 bits of the 96-bit GCM nonce.
pub const COUNTER_LEN: usize = 8;
/// AES-256-GCM authentication tag.
pub const TAG_LEN: usize = 16;
/// Bytes a seal adds on top of `DATAGRAM_HEADER_LEN`.
pub const CRYPTO_OVERHEAD: usize = COUNTER_LEN + TAG_LEN;

/// Noise IK message 1 (initiator -> responder). Body is opaque to this layer.
pub const DGRAM_KIND_HS_INIT: u8 = 18;
/// Noise IK message 2 (responder -> initiator).
pub const DGRAM_KIND_HS_RESP: u8 = 19;
/// Group-key delivery/ack over an established pairwise channel.
pub const DGRAM_KIND_HS_KEY: u8 = 20;

/// `buf` must be at least [`COUNTER_LEN`] bytes.
pub fn write_counter(buf: &mut [u8], counter: u64) {
    buf[..COUNTER_LEN].copy_from_slice(&counter.to_le_bytes());
}

/// `buf` must be at least [`COUNTER_LEN`] bytes.
pub fn read_counter(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf[..COUNTER_LEN].try_into().unwrap())
}
```

Add `pub mod crypto;` to `uc_protocol/src/v2/mod.rs` (alphabetical: after `config`, before `datagram`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p uc_protocol crypto::`
Expected: PASS, 3 tests.

- [ ] **Step 5: Write the failing test for the header field**

Add to `uc_protocol/src/v2/datagram.rs`'s test module:

```rust
#[test]
fn key_epoch_occupies_the_reserved_slot_and_round_trips() {
    let mut buf = [0u8; DATAGRAM_HEADER_LEN];
    let h = DatagramHeader {
        position: 4096,
        leadership_term_id: 7,
        kind: DGRAM_KIND_DATA,
        flags: 0,
        key_epoch: 0xBEEF,
    };
    write_datagram_header(&mut buf, &h);
    // Pinned at the old reserved offset — the slot the v2 spec set aside.
    assert_eq!(&buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2], &0xBEEFu16.to_le_bytes());
    assert_eq!(OFF_DGRAM_KEY_EPOCH, 14);
    assert_eq!(read_datagram_header(&buf), h);
}

#[test]
fn cleartext_datagrams_carry_epoch_zero() {
    let mut buf = [0u8; DATAGRAM_HEADER_LEN];
    let h = DatagramHeader {
        position: 0,
        leadership_term_id: 0,
        kind: DGRAM_KIND_HEARTBEAT,
        flags: 0,
        key_epoch: 0,
    };
    write_datagram_header(&mut buf, &h);
    assert_eq!(buf[OFF_DGRAM_KEY_EPOCH], 0);
    assert_eq!(buf[OFF_DGRAM_KEY_EPOCH + 1], 0);
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `cargo test -p uc_protocol datagram::`
Expected: FAIL — no field `key_epoch`, no const `OFF_DGRAM_KEY_EPOCH`.

- [ ] **Step 7: Add the field**

In `uc_protocol/src/v2/datagram.rs`, replace the reserved constant and thread the field:

```rust
/// u16 LE — M8 key epoch (0 = cleartext). Was `OFF_DGRAM_RESERVED`; the v2
/// spec set this slot aside for exactly this purpose.
pub const OFF_DGRAM_KEY_EPOCH: usize = 14;
```

Add `pub key_epoch: u16,` to `DatagramHeader`, derive `PartialEq, Eq, Debug, Clone, Copy` (already present), and update the codecs:

```rust
pub fn write_datagram_header(buf: &mut [u8], h: &DatagramHeader) {
    buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].copy_from_slice(&h.position.to_le_bytes());
    buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4]
        .copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_DGRAM_KIND] = h.kind;
    buf[OFF_DGRAM_FLAGS] = h.flags;
    buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&h.key_epoch.to_le_bytes());
}

pub fn read_datagram_header(buf: &[u8]) -> DatagramHeader {
    DatagramHeader {
        position: u64::from_le_bytes(
            buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].try_into().unwrap(),
        ),
        leadership_term_id: u32::from_le_bytes(
            buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4].try_into().unwrap(),
        ),
        kind: buf[OFF_DGRAM_KIND],
        flags: buf[OFF_DGRAM_FLAGS],
        key_epoch: u16::from_le_bytes(
            buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].try_into().unwrap(),
        ),
    }
}
```

- [ ] **Step 8: Fix every `DatagramHeader` construction site**

Run: `cargo build --workspace 2>&1 | grep -c "missing field"` to get the count, then add `key_epoch: 0` to each. Sites are in `uc2_net/src/sender.rs` (`assemble`, `assemble_snap`, `send_replay_dgram`), `uc2_net/src/receiver.rs`, and their test modules.

- [ ] **Step 9: Bump the wire version**

In `uc_protocol/src/v2/cnc.rs` set `CNC_V2_VERSION` to the 0.4.0 packing, and in `uc_protocol/src/version.rs` set `CURRENT` to 0.4.0. Leave `MIN_COMPATIBLE` at 0.3.0 — a 0.4.0 node still accepts a 0.3.0 peer, and the T6 doc note that these constants are non-enforcing stays accurate.

- [ ] **Step 10: Run the full protocol suite**

Run: `cargo test -p uc_protocol && cargo build --workspace`
Expected: PASS, all offset assertions green.

- [ ] **Step 11: Commit**

```bash
git add uc_protocol/src/v2/crypto.rs uc_protocol/src/v2/mod.rs uc_protocol/src/v2/datagram.rs uc_protocol/src/v2/cnc.rs uc_protocol/src/version.rs uc2_net/src
git commit -m "feat(uc_protocol): M8 crypto wire layouts, key_epoch header field, wire 0.4.0"
```

---

### Task 2: `uc2_crypto` crate skeleton, identity keys, peer allowlist

**Files:**
- Create: `uc2_crypto/Cargo.toml`, `uc2_crypto/src/lib.rs`, `uc2_crypto/src/identity.rs`
- Modify: `Cargo.toml` (workspace members + `[workspace.dependencies]`)
- Test: inline `#[cfg(test)]` in `identity.rs`

**Interfaces:**
- Consumes: Task 1's constants.
- Produces: `CryptoError` (enum, `thiserror`); `NodeId = u32`; `Identity::load(key_path: &Path) -> Result<Identity, CryptoError>`; `Identity::private_bytes(&self) -> &[u8; 32]`; `Identity::public_bytes(&self) -> [u8; 32]`; `Allowlist::load(path: &Path) -> Result<Allowlist, CryptoError>`; `Allowlist::lookup(&self, id: NodeId) -> Option<[u8; 32]>`; `Allowlist::reload_if_stale(&mut self, now_ns: u64) -> Result<bool, CryptoError>`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp() -> std::path::PathBuf {
        let d = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("uc2-crypto-identity");
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn allowlist_parses_id_and_base64_key() {
        let p = tmp().join("allow-ok");
        let mut f = std::fs::File::create(&p).unwrap();
        // node id, base64 X25519 public key, optional trailing comment
        writeln!(f, "1 {} node-one", B64_KEY_A).unwrap();
        writeln!(f, "# a comment line is ignored").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "2 {}", B64_KEY_B).unwrap();
        drop(f);

        let a = Allowlist::load(&p).unwrap();
        assert!(a.lookup(1).is_some());
        assert!(a.lookup(2).is_some());
        assert_ne!(a.lookup(1), a.lookup(2));
        assert_eq!(a.lookup(3), None, "unlisted id is not authorized");
    }

    #[test]
    fn allowlist_rejects_a_malformed_line_rather_than_skipping_it() {
        // A silently-skipped bad line is an authorization hole: the operator
        // thinks a peer is listed when it is not.
        let p = tmp().join("allow-bad");
        std::fs::write(&p, format!("1 {}\nnot-a-line\n", B64_KEY_A)).unwrap();
        assert!(matches!(Allowlist::load(&p), Err(CryptoError::MalformedAllowlist { line: 2 })));
    }

    #[test]
    fn allowlist_rejects_a_duplicate_id() {
        let p = tmp().join("allow-dup");
        std::fs::write(&p, format!("1 {}\n1 {}\n", B64_KEY_A, B64_KEY_B)).unwrap();
        assert!(matches!(Allowlist::load(&p), Err(CryptoError::DuplicateAllowlistId(1))));
    }

    #[test]
    fn identity_refuses_a_world_readable_private_key() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let p = tmp().join("key-loose");
            std::fs::write(&p, PRIV_KEY_A).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(matches!(Identity::load(&p), Err(CryptoError::KeyFilePermissions { .. })));
        }
    }

    #[test]
    fn identity_public_key_is_derived_not_stored() {
        let p = tmp().join("key-ok");
        write_private_key(&p, PRIV_KEY_A);
        let id = Identity::load(&p).unwrap();
        assert_eq!(id.public_bytes().len(), 32);
        assert_ne!(id.public_bytes(), [0u8; 32]);
    }
}
```

Define the fixtures at the top of the test module: `PRIV_KEY_A` as a 32-byte array literal, `B64_KEY_A`/`B64_KEY_B` as the corresponding base64 public keys, and `write_private_key` as a helper that writes the file with mode `0o600`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto identity::`
Expected: FAIL — crate does not exist.

- [ ] **Step 3: Create the crate**

`uc2_crypto/Cargo.toml`:

```toml
[package]
name = "uc2_crypto"
description = "UC v2 wire crypto: Noise IK handshake, group-key schedule, AEAD seal/open (spec 2026-07-28, M8)"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true

[dependencies]
uc_protocol = { path = "../uc_protocol" }
snow = { workspace = true }
aes-gcm = { workspace = true }
hkdf = { workspace = true }
sha2 = { workspace = true }
x25519-dalek = { workspace = true }
rand = { workspace = true }
zeroize = { workspace = true }
thiserror = { workspace = true }
base64 = { workspace = true }
```

Add to the root `Cargo.toml`: `"uc2_crypto"` in `members`, and under `[workspace.dependencies]`:

```toml
# M8 wire crypto. Versions verified against crates.io 2026-07-28. The AEAD/hash
# generation deliberately MATCHES the one snow pins internally (aes-gcm 0.10 /
# sha2 0.10 era) so the binary carries ONE AES-GCM implementation, not two.
snow = { version = "0.10", default-features = false, features = ["default-resolver", "use-aes-gcm", "use-sha2", "use-curve25519", "use-getrandom", "std"] }
aes-gcm = "0.10"
hkdf = "0.12"
sha2 = "0.10"
x25519-dalek = "2"
zeroize = { version = "1", features = ["derive"] }
base64 = "0.22"
```

**If a version does not resolve**, do not silently bump to the next major — the
API changes across RustCrypto generations (`aead` 0.5→0.6, `digest` 0.10→0.11)
and mixing generations reintroduces the duplicate-implementation problem this
block exists to avoid. Report the resolution failure instead.

- [ ] **Step 4: Implement `lib.rs` error type and `identity.rs`**

`lib.rs` declares `pub mod identity;` and the error enum:

```rust
pub type NodeId = u32;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("key file {path} has mode {mode:o}; must not be group- or world-readable")]
    KeyFilePermissions { path: String, mode: u32 },
    #[error("key file {0} is unreadable or not 32 bytes")]
    KeyFileInvalid(String),
    #[error("allowlist line {line} is malformed")]
    MalformedAllowlist { line: usize },
    #[error("allowlist lists node id {0} more than once")]
    DuplicateAllowlistId(NodeId),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
```

`identity.rs` implements `Identity` (32-byte private key, mode check via `std::os::unix::fs::MetadataExt`, public key via `x25519_dalek::PublicKey::from(&StaticSecret)`), and `Allowlist` (a `Vec<(NodeId, [u8; 32])>` plus the source path, mtime, and last-reload timestamp). `reload_if_stale` re-reads when the file mtime changed AND at least 1s has passed since the last attempt, returning whether the contents changed. Wrap the private key in `zeroize::Zeroizing`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p uc2_crypto identity::`
Expected: PASS, 5 tests.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock uc2_crypto
git commit -m "feat(uc2_crypto): crate skeleton, X25519 identity keys, peer allowlist"
```

---

### Task 3: Key schedule — per-sender-per-boot derivation and the epoch store

**Files:**
- Create: `uc2_crypto/src/schedule.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: `CryptoError`, `NodeId`.
- Produces: `GroupKey([u8; 32])`; `BootSalt([u8; 16])`; `derive_send_key(group: &GroupKey, sender: NodeId, salt: &BootSalt) -> [u8; 32]`; `KeySchedule::new()`; `KeySchedule::install(&mut self, epoch: u16, key: GroupKey)`; `KeySchedule::current(&self) -> Option<(u16, &GroupKey)>`; `KeySchedule::get(&self, epoch: u16) -> Option<&GroupKey>`; `KeySchedule::retire_below(&mut self, epoch: u16)`; `epoch_is_newer(a: u16, b: u16) -> bool`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_separates_senders_and_boots() {
        let g = GroupKey([7u8; 32]);
        let s1 = BootSalt([1u8; 16]);
        let s2 = BootSalt([2u8; 16]);
        // Different sender under the same boot salt -> different key.
        assert_ne!(derive_send_key(&g, 1, &s1), derive_send_key(&g, 2, &s1));
        // THE restart hazard: same sender, new boot salt -> different key,
        // so a counter reset to 0 cannot reuse a nonce.
        assert_ne!(derive_send_key(&g, 1, &s1), derive_send_key(&g, 1, &s2));
        // Deterministic: every peer derives the sender's key identically.
        assert_eq!(derive_send_key(&g, 1, &s1), derive_send_key(&g, 1, &s1));
    }

    #[test]
    fn two_epochs_stay_live_and_older_ones_retire() {
        let mut ks = KeySchedule::new();
        ks.install(1, GroupKey([1u8; 32]));
        ks.install(2, GroupKey([2u8; 32]));
        assert_eq!(ks.current().unwrap().0, 2, "newest install is current");
        assert!(ks.get(1).is_some(), "previous epoch retained for the overlap");
        ks.retire_below(2);
        assert!(ks.get(1).is_none(), "retired");
        assert!(ks.get(2).is_some());
    }

    #[test]
    fn epoch_comparison_is_modular_so_wrap_is_a_non_event() {
        assert!(epoch_is_newer(2, 1));
        assert!(!epoch_is_newer(1, 2));
        // Wrap: 0 follows 65535.
        assert!(epoch_is_newer(0, u16::MAX));
        assert!(!epoch_is_newer(u16::MAX, 0));
    }

    #[test]
    fn unknown_epoch_is_a_miss_not_a_panic() {
        let ks = KeySchedule::new();
        assert!(ks.get(9).is_none());
        assert!(ks.current().is_none());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto schedule::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

`derive_send_key` = `Hkdf::<Sha256>::new(Some(salt.0), &group.0)` expanded with info `b"uc2/send" ‖ sender.to_le_bytes()` into 32 bytes. `KeySchedule` holds at most two `(u16, GroupKey)` slots (current + previous); `install` shifts current into previous. `epoch_is_newer(a, b)` is `a.wrapping_sub(b) != 0 && a.wrapping_sub(b) < 0x8000`. Derive `Zeroize` on `GroupKey` and implement `Drop` via `ZeroizeOnDrop`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto schedule::`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/schedule.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): key schedule — per-sender-per-boot derivation, two-epoch overlap"
```

---

### Task 4: Anti-replay sliding window

**Files:**
- Create: `uc2_crypto/src/replay.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: nothing.
- Produces: `ReplayWindow::new()`; `ReplayWindow::check_and_set(&mut self, counter: u64) -> bool` (true = accept, false = replay/too-old); `REPLAY_WINDOW_BITS: u64 = 1024`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_in_order_and_rejects_exact_repeats() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(1));
        assert!(w.check_and_set(2));
        assert!(!w.check_and_set(2), "a captured-and-resent datagram is refused");
        assert!(!w.check_and_set(1));
    }

    #[test]
    fn accepts_reordering_inside_the_window() {
        // UC's transport reorders freely; the window must not turn that into loss.
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(10));
        assert!(w.check_and_set(5), "late arrival within the window is legitimate");
        assert!(!w.check_and_set(5), "but only once");
    }

    #[test]
    fn rejects_anything_older_than_the_window() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(REPLAY_WINDOW_BITS + 100));
        assert!(!w.check_and_set(1), "far-past counter is unverifiable, so refuse");
    }

    #[test]
    fn a_large_forward_jump_resets_cleanly() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(1));
        assert!(w.check_and_set(u64::MAX / 2));
        assert!(!w.check_and_set(u64::MAX / 2), "still tracked after the jump");
        assert!(!w.check_and_set(1), "the old counter fell out of the window");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto replay::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement** the standard IPsec/RFC 6479 bitmap: a `highest: u64` and a `[u64; 16]` bitmap (1024 bits). Counter 0 is never valid (counters start at 1), so a fresh window rejects it.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto replay::`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/replay.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): RFC 6479 anti-replay window (reorder-tolerant)"
```

---

### Task 5: `seal_in_place` / `open_in_place`

**Files:**
- Create: `uc2_crypto/src/seal.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: Task 1 constants, Task 3 `derive_send_key`, Task 4 `ReplayWindow`.
- Produces: `seal_in_place(buf: &mut Vec<u8>, key: &[u8; 32], counter: u64) -> Result<(), CryptoError>`; `open_in_place(buf: &mut Vec<u8>, key: &[u8; 32]) -> Result<u64, CryptoError>` (returns the counter for the caller's replay check); `CryptoError::AuthFailed`; `CryptoError::TooShort`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::datagram::{DATAGRAM_HEADER_LEN, DatagramHeader, write_datagram_header};

    fn dgram(payload: &[u8], epoch: u16) -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut v, &DatagramHeader {
            position: 4096, leadership_term_id: 3, kind: 1, flags: 0, key_epoch: epoch,
        });
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn round_trips_and_leaves_the_header_in_the_clear() {
        let key = [9u8; 32];
        let mut d = dgram(b"frames go here", 1);
        let header_before = d[..DATAGRAM_HEADER_LEN].to_vec();
        seal_in_place(&mut d, &key, 42).unwrap();
        assert_eq!(&d[..DATAGRAM_HEADER_LEN], &header_before[..], "header stays readable");
        assert_ne!(&d[DATAGRAM_HEADER_LEN + 8..], b"frames go here", "payload is sealed");

        assert_eq!(open_in_place(&mut d, &key).unwrap(), 42);
        assert_eq!(&d[DATAGRAM_HEADER_LEN..], b"frames go here");
    }

    #[test]
    fn tampering_with_the_header_fails_the_tag_because_it_is_aad() {
        let key = [9u8; 32];
        let mut d = dgram(b"payload", 1);
        seal_in_place(&mut d, &key, 1).unwrap();
        d[0] ^= 0xFF; // rewrite `position`
        assert!(matches!(open_in_place(&mut d, &key), Err(CryptoError::AuthFailed)));
    }

    #[test]
    fn tampering_with_ciphertext_tag_or_counter_all_fail() {
        let key = [9u8; 32];
        for byte in [DATAGRAM_HEADER_LEN, DATAGRAM_HEADER_LEN + 9, 0] {
            let mut d = dgram(b"payload", 1);
            seal_in_place(&mut d, &key, 1).unwrap();
            let idx = if byte == 0 { d.len() - 1 } else { byte };
            d[idx] ^= 0x01;
            assert!(
                matches!(open_in_place(&mut d, &key), Err(CryptoError::AuthFailed)),
                "flipping byte {idx} must not open"
            );
        }
    }

    #[test]
    fn a_wrong_key_fails_rather_than_producing_garbage() {
        let mut d = dgram(b"payload", 1);
        seal_in_place(&mut d, &[1u8; 32], 1).unwrap();
        assert!(matches!(open_in_place(&mut d, &[2u8; 32]), Err(CryptoError::AuthFailed)));
    }

    #[test]
    fn a_truncated_datagram_is_rejected_not_indexed_out_of_bounds() {
        // The untrusted-input contract: never panic.
        for len in 0..DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD {
            let mut d = vec![0u8; len];
            assert!(matches!(open_in_place(&mut d, &[1u8; 32]), Err(CryptoError::TooShort)));
        }
    }

    #[test]
    fn header_only_datagrams_seal_to_an_empty_payload() {
        // HEARTBEAT and APPEND_POSITION carry no body.
        let key = [9u8; 32];
        let mut d = dgram(b"", 1);
        seal_in_place(&mut d, &key, 7).unwrap();
        assert_eq!(d.len(), DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD);
        assert_eq!(open_in_place(&mut d, &key).unwrap(), 7);
        assert_eq!(d.len(), DATAGRAM_HEADER_LEN);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto seal::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

`seal_in_place`: splice `COUNTER_LEN` zero bytes in at `DATAGRAM_HEADER_LEN`, write the counter there, then AES-256-GCM `encrypt_in_place` over the payload with nonce `0u32 ‖ counter` (12 bytes, big-endian zero prefix) and AAD = `buf[..DATAGRAM_HEADER_LEN]`, appending the tag. `open_in_place`: length-check first (`< DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD` → `TooShort`), read the counter, `decrypt_in_place` with the same AAD, then remove the counter bytes so the caller sees the original layout. Use `aes_gcm::aead::AeadInPlace` with a `Vec` buffer to avoid a second allocation.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto seal::`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/seal.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): AES-256-GCM seal/open in place, header as AAD"
```

---

### Task 6: Noise `IK` handshake driver

**Files:**
- Create: `uc2_crypto/src/handshake.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: `Identity`, `Allowlist`, `BootSalt`, `NodeId`, `CryptoError`.
- Produces: `HandshakeAction` enum (`Send { to: NodeId, kind: u8, body: Vec<u8> }`, `Established { peer: NodeId, boot_salt: BootSalt }`, `Failed { peer: NodeId, reason: &'static str }`); `Peers::new(identity: Identity, allowlist: Allowlist, self_id: NodeId, boot_salt: BootSalt)`; `Peers::initiate(&mut self, peer: NodeId, now_ns: u64) -> Vec<HandshakeAction>`; `Peers::on_message(&mut self, from: NodeId, kind: u8, body: &[u8], now_ns: u64) -> Vec<HandshakeAction>`; `Peers::tick(&mut self, now_ns: u64) -> Vec<HandshakeAction>`; `Peers::seal_pairwise(&mut self, peer: NodeId, buf: &mut Vec<u8>, counter: u64) -> Result<(), CryptoError>`; `Peers::open_pairwise(&mut self, peer: NodeId, buf: &mut Vec<u8>) -> Result<u64, CryptoError>`.

The pattern string is `Noise_IK_25519_AESGCM_SHA256`. The handshake payload carries the sender's `NodeId` and `BootSalt`. Use `snow`'s **stateless** transport mode (`into_stateless_transport_mode()`) so UC supplies its own nonce counter — the stateful mode's internal counter would fight the envelope.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Drive two `Peers` against each other with no sockets — this is the
    /// property that lets uc2_sim adjudicate the handshake deterministically.
    fn pump(a: &mut Peers, b: &mut Peers, mut acts: Vec<HandshakeAction>) -> (bool, bool) {
        let (mut a_up, mut b_up) = (false, false);
        for _ in 0..8 {
            let mut next = Vec::new();
            for act in acts.drain(..) {
                match act {
                    HandshakeAction::Send { to, kind, body } => {
                        let (dst, src) = if to == B_ID { (&mut *b, A_ID) } else { (&mut *a, B_ID) };
                        next.extend(dst.on_message(src, kind, &body, 0));
                    }
                    HandshakeAction::Established { peer, .. } => {
                        if peer == B_ID { a_up = true } else { b_up = true }
                    }
                    HandshakeAction::Failed { reason, .. } => panic!("unexpected failure: {reason}"),
                }
            }
            if next.is_empty() { break }
            acts = next;
        }
        (a_up, b_up)
    }

    #[test]
    fn two_authorized_peers_establish_and_exchange_sealed_traffic() {
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up, "both sides reach transport mode");

        let mut d = sealed_test_datagram();
        let plain = d.clone();
        a.seal_pairwise(B_ID, &mut d, 1).unwrap();
        assert_ne!(d, plain);
        assert_eq!(b.open_pairwise(A_ID, &mut d).unwrap(), 1);
        assert_eq!(d, plain);
    }

    #[test]
    fn a_peer_missing_from_the_allowlist_never_establishes() {
        let (mut a, mut stranger) = pair_with_stranger_not_in_a_allowlist();
        let acts = stranger.initiate(A_ID, 0);
        let mut failed = false;
        for act in acts {
            if let HandshakeAction::Send { kind, body, .. } = act {
                for r in a.on_message(STRANGER_ID, kind, &body, 0) {
                    if matches!(r, HandshakeAction::Failed { .. }) { failed = true }
                    assert!(!matches!(r, HandshakeAction::Established { .. }));
                }
            }
        }
        assert!(failed, "an unlisted id is refused, not silently ignored");
    }

    #[test]
    fn a_wrong_static_key_for_a_listed_id_is_refused() {
        // Impersonation: right id, wrong key. The DH must not produce a
        // matching chaining key, so the handshake payload fails to decrypt.
        let (mut a, mut impostor) = pair_with_impostor_using_a_listed_id();
        let acts = impostor.initiate(A_ID, 0);
        for act in acts {
            if let HandshakeAction::Send { kind, body, .. } = act {
                for r in a.on_message(B_ID, kind, &body, 0) {
                    assert!(!matches!(r, HandshakeAction::Established { .. }));
                }
            }
        }
    }

    #[test]
    fn garbage_and_truncated_handshake_bodies_never_panic() {
        let (mut a, _) = authorized_pair();
        for body in [vec![], vec![0u8; 1], vec![0xAB; 48], vec![0xFF; 1500]] {
            let _ = a.on_message(B_ID, uc_protocol::v2::crypto::DGRAM_KIND_HS_INIT, &body, 0);
        }
    }

    #[test]
    fn simultaneous_initiation_resolves_to_one_session_by_lower_id() {
        let (mut a, mut b) = authorized_pair();
        let a_acts = a.initiate(B_ID, 0);
        let b_acts = b.initiate(A_ID, 0);
        let mut acts = a_acts;
        acts.extend(b_acts);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up, "a race still converges on a working session");
    }

    #[test]
    fn established_peers_carry_the_boot_salt_for_key_derivation() {
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let mut seen = None;
        for act in a.on_message_all(&mut b, acts) {
            if let HandshakeAction::Established { peer, boot_salt } = act {
                if peer == B_ID { seen = Some(boot_salt) }
            }
        }
        assert_eq!(seen, Some(b.boot_salt()), "we learn the PEER's salt, not our own");
    }
}
```

Write the fixtures `authorized_pair()`, `pair_with_stranger_not_in_a_allowlist()`, `pair_with_impostor_using_a_listed_id()`, `sealed_test_datagram()` and the `on_message_all` test helper at the top of the module; `A_ID = 1`, `B_ID = 2`, `STRANGER_ID = 99`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto handshake::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

`Peers` holds `HashMap<NodeId, PeerState>` where `PeerState` is `Idle | Handshaking(Box<snow::HandshakeState>) | Up { transport: Box<snow::StatelessTransportState>, boot_salt: BootSalt, replay: ReplayWindow }`. Simultaneous-open: when a `HS_INIT` arrives while we are `Handshaking` as initiator, keep ours if our id is lower, otherwise discard ours and respond. Retry with backoff in `tick`. Every `snow` error maps to `HandshakeAction::Failed` — never a panic, never an `unwrap`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto handshake::`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/handshake.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): Noise IK handshake driver over snow, sim-drivable"
```

---

### Task 7: Group-key distribution and activation

**Files:**
- Create: `uc2_crypto/src/group.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: Tasks 3, 5, 6 (`HandshakeAction` is re-exported from `lib.rs` and shared with `handshake.rs` — one action type for the whole crate, so the node has a single match site).
- Produces: `GroupPlane::new(self_id: NodeId)`; `GroupPlane::mint(&mut self, peers: &[NodeId], now_ns: u64) -> (u16, Vec<HandshakeAction>)`; `GroupPlane::on_key_message(&mut self, from: NodeId, body: &[u8]) -> Vec<HandshakeAction>`; `GroupPlane::on_ack(&mut self, from: NodeId, epoch: u16)`; `GroupPlane::sealing_epoch(&self, now_ns: u64) -> Option<u16>`; `GroupPlane::schedule(&self) -> &KeySchedule`; `ACTIVATION_TIMEOUT_NS: u64 = 2_000_000_000`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_epoch_only_activates_once_every_peer_acks() {
        let mut g = GroupPlane::new(1);
        let (epoch, actions) = g.mint(&[2, 3], 0);
        assert_eq!(actions.len(), 2, "one HS_KEY per peer");
        assert_eq!(g.sealing_epoch(0), None, "must not seal under an unacked epoch");
        g.on_ack(2, epoch);
        assert_eq!(g.sealing_epoch(0), None, "one ack is not enough");
        g.on_ack(3, epoch);
        assert_eq!(g.sealing_epoch(0), Some(epoch));
    }

    #[test]
    fn a_dead_peer_cannot_block_replication_forever() {
        // The liveness trap: peer 3 never acks. After the activation timeout we
        // seal anyway; peer 3 recovers via the existing NAK path once it gets
        // the key.
        let mut g = GroupPlane::new(1);
        let (epoch, _) = g.mint(&[2, 3], 0);
        g.on_ack(2, epoch);
        assert_eq!(g.sealing_epoch(ACTIVATION_TIMEOUT_NS - 1), None);
        assert_eq!(g.sealing_epoch(ACTIVATION_TIMEOUT_NS + 1), Some(epoch));
    }

    #[test]
    fn the_previous_epoch_stays_openable_during_the_overlap() {
        let mut g = GroupPlane::new(1);
        let (e1, _) = g.mint(&[2], 0);
        g.on_ack(2, e1);
        let (e2, _) = g.mint(&[2], 1_000);
        g.on_ack(2, e2);
        assert!(g.schedule().get(e1).is_some(), "in-flight e1 datagrams still open");
        assert!(g.schedule().get(e2).is_some());
    }

    #[test]
    fn epochs_advance_monotonically_across_mints() {
        let mut g = GroupPlane::new(1);
        let (e1, _) = g.mint(&[2], 0);
        let (e2, _) = g.mint(&[2], 1);
        assert!(epoch_is_newer(e2, e1));
    }

    #[test]
    fn a_malformed_key_message_is_refused_without_installing_anything() {
        let mut g = GroupPlane::new(2);
        for body in [vec![], vec![0u8; 3], vec![0xFF; 200]] {
            let _ = g.on_key_message(1, &body);
        }
        assert!(g.schedule().current().is_none(), "nothing was installed from garbage");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto group::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement.** `mint` generates 32 random bytes via `rand::rngs::OsRng`, bumps the epoch, installs into the `KeySchedule`, and emits one `HS_KEY` action per peer carrying `(epoch, key)` — the caller seals that body over the peer's pairwise channel before sending. `sealing_epoch` returns the pending epoch once all peers acked, or once `now_ns` exceeds `minted_at + ACTIVATION_TIMEOUT_NS`; otherwise the previously active epoch, or `None`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto group::`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/group.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): group-key mint, per-peer delivery, activation with a liveness timeout"
```

---

### Task 8: Rotation policy

**Files:**
- Create: `uc2_crypto/src/rotation.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: nothing (pure).
- Produces: `RotationPolicy { interval_ns: u64, bytes: u64 }` with `Default` = 1 hour / 1 TiB; `RotationState::new(policy)`; `RotationState::on_became_leader(&mut self)`; `RotationState::on_committed_config(&mut self, tombstone_count: usize)`; `RotationState::on_bytes_sealed(&mut self, n: u64)`; `RotationState::take_due(&mut self, now_ns: u64) -> Option<RotationReason>`; `RotationReason { BecameLeader, Periodic, Removal }`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn becoming_leader_always_rotates() {
        let mut r = RotationState::new(RotationPolicy::default());
        r.on_became_leader();
        assert_eq!(r.take_due(0), Some(RotationReason::BecameLeader));
        assert_eq!(r.take_due(0), None, "consumed exactly once");
    }

    #[test]
    fn a_growing_tombstone_set_rotates_but_a_demote_does_not() {
        let mut r = RotationState::new(RotationPolicy::default());
        r.on_committed_config(0);
        assert_eq!(r.take_due(0), None, "baseline observation is not a trigger");
        // A demote leaves the tombstone set unchanged: the node stays in the
        // cluster and must keep replicating.
        r.on_committed_config(0);
        assert_eq!(r.take_due(0), None);
        // A Remove* tombstones an id.
        r.on_committed_config(1);
        assert_eq!(r.take_due(0), Some(RotationReason::Removal));
    }

    #[test]
    fn periodic_fires_on_the_interval_and_on_the_byte_budget() {
        let p = RotationPolicy { interval_ns: 1_000, bytes: 500 };
        let mut r = RotationState::new(p);
        assert_eq!(r.take_due(999), None);
        assert_eq!(r.take_due(1_001), Some(RotationReason::Periodic));

        let mut r2 = RotationState::new(p);
        r2.on_bytes_sealed(499);
        assert_eq!(r2.take_due(0), None);
        r2.on_bytes_sealed(2);
        assert_eq!(r2.take_due(0), Some(RotationReason::Periodic));
    }

    #[test]
    fn a_removal_outranks_a_simultaneously_due_periodic() {
        let mut r = RotationState::new(RotationPolicy { interval_ns: 1, bytes: u64::MAX });
        r.on_committed_config(0);
        r.on_committed_config(1);
        assert_eq!(r.take_due(1_000), Some(RotationReason::Removal), "report the security event");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto rotation::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement.** Latched booleans for `BecameLeader`/`Removal` plus `last_rotate_ns` and `bytes_since`. `take_due` checks in priority order `BecameLeader`, `Removal`, `Periodic`, clearing all counters when it returns `Some`. `on_committed_config` stores the first observation as a baseline and only latches when the count strictly grows.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto rotation::`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/rotation.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): rotation policy — leader/periodic/removal triggers"
```

---

### Task 9: `CryptoConfig` and the transport facade

**Files:**
- Create: `uc2_crypto/src/transport.rs`
- Modify: `uc2_crypto/src/lib.rs`
- Test: inline

**Interfaces:**
- Consumes: Tasks 2–8.
- Produces: `CryptoConfig { Disabled, Enabled { key_path: PathBuf, allowlist_path: PathBuf, rotation: RotationPolicy } }` with `Default = Disabled`; `Transport::new(cfg: &CryptoConfig, self_id: NodeId) -> Result<Option<Transport>, CryptoError>`; `Transport::scope_of(kind: u8) -> Scope`; `Transport::seal(&mut self, kind: u8, peer: Option<NodeId>, buf: &mut Vec<u8>, now_ns: u64) -> Result<(), CryptoError>`; `Transport::open(&mut self, from: NodeId, buf: &mut Vec<u8>) -> Result<(), CryptoError>`; `Transport::rotation_due(&mut self, now_ns: u64) -> Option<RotationReason>`; `Transport::allowlist_reload_if_stale(&mut self, now_ns: u64) -> Result<bool, CryptoError>`; `Scope { Group, Pairwise }`; **adds `CryptoError::NoGroupKey`** to the enum from Task 2.

**`boot_salt` is generated here**, once, in `Transport::new`, from `OsRng` — one salt per process lifetime. This is the value that makes a counter reset after restart safe, so it must never be persisted, reused, or derived from anything stable (hostname, node id, instance dir).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::datagram::*;

    #[test]
    fn fan_out_kinds_take_the_group_key_and_the_rest_are_pairwise() {
        // The rule is BY KIND, never by destination — serve_nak sends DATA to a
        // single peer and still uses the group key.
        for k in [DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_READ_PROBE] {
            assert_eq!(Transport::scope_of(k), Scope::Group, "kind {k}");
        }
        for k in [
            DGRAM_KIND_NAK, DGRAM_KIND_STATUS, DGRAM_KIND_APPEND_POSITION,
            DGRAM_KIND_READ_PROBE_ACK, DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_VOTE,
            DGRAM_KIND_TERM_MAP, DGRAM_KIND_SNAP_BEGIN, DGRAM_KIND_SNAP_CHUNK,
            DGRAM_KIND_SNAP_NAK, DGRAM_KIND_SNAP_DONE, DGRAM_KIND_CONFIG_PROPOSAL,
            DGRAM_KIND_CONFIG_REPLY,
        ] {
            assert_eq!(Transport::scope_of(k), Scope::Pairwise, "kind {k}");
        }
    }

    #[test]
    fn every_wire_kind_has_an_assigned_scope() {
        // Guards against a future kind silently defaulting to the wrong scope.
        for k in 1..=DGRAM_KIND_CONFIG_REPLY {
            let _ = Transport::scope_of(k);
        }
    }

    #[test]
    fn disabled_config_constructs_no_transport() {
        assert!(Transport::new(&CryptoConfig::Disabled, 1).unwrap().is_none());
    }

    #[test]
    fn enabled_config_with_a_missing_key_file_fails_construction() {
        // Boot refusal: a node that cannot authenticate must not run cleartext.
        let cfg = CryptoConfig::Enabled {
            key_path: "/nonexistent/uc2/key".into(),
            allowlist_path: "/nonexistent/uc2/allow".into(),
            rotation: RotationPolicy::default(),
        };
        assert!(Transport::new(&cfg, 1).is_err());
    }

    #[test]
    fn sealing_before_a_group_key_exists_is_an_error_not_a_cleartext_send() {
        let mut t = enabled_transport();
        let mut d = data_datagram();
        assert!(matches!(
            t.seal(DGRAM_KIND_DATA, None, &mut d, 0),
            Err(CryptoError::NoGroupKey)
        ));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_crypto transport::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement.** `Transport` composes `Peers`, `GroupPlane`, `RotationState`, and the counter. `scope_of` is an exhaustive `match` on the kind constants — no `_ =>` arm defaulting to `Group`; unknown kinds map to `Pairwise` explicitly with a comment saying why (a new kind is unicast until proven otherwise). `seal` picks the scope, derives the send key via `derive_send_key`, allocates the next counter, and calls `seal_in_place`. Group sealing with no active epoch returns `CryptoError::NoGroupKey`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_crypto transport::`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_crypto/src/transport.rs uc2_crypto/src/lib.rs
git commit -m "feat(uc2_crypto): transport facade — scope-by-kind, config, boot refusal"
```

---

### Task 10: `uc2_net` send seam

**Files:**
- Modify: `uc2_net/src/sender.rs:560-600` (`assemble`, `fan_out`, `serve_nak`), `uc2_net/src/sender.rs:126-140` (`SenderConfig`), `uc2_net/Cargo.toml`
- Test: inline in `sender.rs`

**Interfaces:**
- Consumes: `uc2_crypto::{Transport, CryptoError}`.
- Produces: `Sender::with_crypto(...)` builder arm; `SenderConfig::crypto_overhead(&self) -> usize`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn sealed_fan_out_seals_once_and_sends_identical_bytes_to_every_follower() {
    // The whole point of the group key: one seal, N sends. If this regresses to
    // per-peer sealing the datagrams would differ.
    let (mut s, f1, f2) = sender_with_crypto_to_two_followers();
    append_and_flush(&mut s, b"hello");
    let d1 = f1.recv_raw().expect("follower 1 got a datagram");
    let d2 = f2.recv_raw().expect("follower 2 got a datagram");
    assert_eq!(d1, d2, "byte-identical: sealed once, fanned out");
    assert_ne!(&d1[DATAGRAM_HEADER_LEN..], b"hello", "payload is not cleartext");
    assert_ne!(read_datagram_header(&d1).key_epoch, 0, "stamped with the epoch");
}

#[test]
fn mtu_budget_shrinks_by_the_crypto_overhead_so_sealed_datagrams_still_fit() {
    let cfg_plain = SenderConfig::new(9);
    let mut cfg_sealed = SenderConfig::new(9);
    cfg_sealed.crypto_enabled = true;
    assert_eq!(
        cfg_sealed.mtu - DATAGRAM_HEADER_LEN - cfg_sealed.crypto_overhead(),
        cfg_plain.mtu - DATAGRAM_HEADER_LEN - CRYPTO_OVERHEAD
    );
    let (mut s, f) = sender_with_crypto_to_one_follower();
    append_and_flush(&mut s, &vec![0xAB; 4096]);
    while let Some(d) = f.recv_raw() {
        assert!(d.len() <= cfg_sealed.mtu, "a sealed datagram must not exceed the MTU");
    }
}

#[test]
fn a_nak_retransmit_reuses_the_position_but_never_the_counter() {
    // The nonce hazard, pinned at the seam: position repeats, counters must not.
    let (mut s, f) = sender_with_crypto_to_one_follower();
    append_and_flush(&mut s, b"payload");
    let first = f.recv_raw().unwrap();
    s.on_nak(f.addr(), read_datagram_header(&first).position, 7);
    s.do_work();
    let retx = f.recv_raw().unwrap();
    assert_eq!(read_datagram_header(&retx).position, read_datagram_header(&first).position);
    assert_ne!(
        read_counter(&retx[DATAGRAM_HEADER_LEN..]),
        read_counter(&first[DATAGRAM_HEADER_LEN..]),
        "a repeated position must not mean a repeated nonce"
    );
}

#[test]
fn cleartext_mode_is_byte_identical_to_pre_m8_output() {
    // Flag-day safety: with crypto off, nothing on the wire changes.
    let (mut s, f) = sender_without_crypto();
    append_and_flush(&mut s, b"hello");
    let d = f.recv_raw().unwrap();
    assert_eq!(&d[DATAGRAM_HEADER_LEN..], b"hello");
    assert_eq!(read_datagram_header(&d).key_epoch, 0);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_net sender::tests::sealed`
Expected: FAIL — no `crypto_enabled` field, no `with_crypto`.

- [ ] **Step 3: Implement.** Add `uc2_crypto` to `uc2_net/Cargo.toml`. `Sender` gains `crypto: Option<Transport>`. In `assemble`, after `extend_from_slice`, call `self.crypto.as_mut()` and seal, stamping the epoch into the header before sealing (the header is AAD, so the epoch must be final first). `fan_out` is unchanged — it seals once inside `assemble` and the loop still sends `&self.scratch` N times. Subtract `crypto_overhead()` in the two places the MTU budget is computed (`serve_nak`'s `budget`, and the run-read budget in `do_work`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_net`
Expected: PASS, all existing sender tests plus 4 new.

- [ ] **Step 5: Commit**

```bash
git add uc2_net/Cargo.toml uc2_net/src/sender.rs
git commit -m "feat(uc2_net): seal seam in assemble; fan-out still seals once for N sends"
```

---

### Task 11: `uc2_net` receive seam

**Files:**
- Modify: `uc2_net/src/receiver.rs:582-615` (`do_work`, `on_datagram`), receiver stats struct
- Test: inline in `receiver.rs`

**Interfaces:**
- Consumes: `uc2_crypto::Transport`.
- Produces: stats counters `dropped_auth_failed`, `dropped_replay`, `dropped_unknown_epoch`, `peer_appears_cleartext`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_sealed_datagram_opens_and_dispatches_exactly_as_cleartext_did() {
    let (mut r, peer) = receiver_with_crypto();
    peer.send_sealed_data(4096, b"frames");
    r.do_work();
    assert_eq!(r.buffer_contents_at(4096), b"frames", "downstream sees plaintext");
}

#[test]
fn a_forged_datagram_under_an_unknown_key_is_dropped_and_counted() {
    let (mut r, peer) = receiver_with_crypto();
    peer.send_sealed_with_wrong_key(4096, b"forged");
    r.do_work();
    assert_eq!(r.stats.dropped_auth_failed.load(Relaxed), 1);
    assert!(r.buffer_is_empty_at(4096), "forged bytes never reach the log buffer");
}

#[test]
fn a_replayed_datagram_is_dropped_and_counted() {
    let (mut r, peer) = receiver_with_crypto();
    let d = peer.send_sealed_data(4096, b"frames");
    r.do_work();
    peer.send_raw(&d); // byte-for-byte capture and resend
    r.do_work();
    assert_eq!(r.stats.dropped_replay.load(Relaxed), 1);
}

#[test]
fn a_cleartext_peer_is_diagnosed_specifically_not_as_a_generic_auth_failure() {
    // The likeliest operator error under flag-day rollout.
    let (mut r, peer) = receiver_with_crypto();
    peer.send_cleartext_data(4096, b"frames"); // key_epoch == 0
    r.do_work();
    assert_eq!(r.stats.peer_appears_cleartext.load(Relaxed), 1);
    assert_eq!(r.stats.dropped_auth_failed.load(Relaxed), 0, "distinguishable");
}

#[test]
fn an_unknown_epoch_is_dropped_without_killing_the_node() {
    let (mut r, peer) = receiver_with_crypto();
    peer.send_sealed_under_epoch(999, 4096, b"frames");
    r.do_work();
    assert_eq!(r.stats.dropped_unknown_epoch.load(Relaxed), 1);
}

#[test]
fn truncated_and_random_datagrams_never_panic() {
    // Anyone who can reach the port must not be able to kill the node.
    let (mut r, peer) = receiver_with_crypto();
    for len in [0usize, 1, 15, 16, 17, 39, 40, 1500] {
        peer.send_raw(&vec![0xAB; len]);
        r.do_work();
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_net receiver::tests::a_sealed`
Expected: FAIL — no crypto on `Receiver`.

- [ ] **Step 3: Implement.** In `do_work`, between `recv_from` and `on_datagram`, if crypto is enabled: read the header, resolve the peer id from `from`, and open in place. `key_epoch == 0` while crypto is enabled → bump `peer_appears_cleartext` and log rate-limited (once per 30s per peer) with the specific diagnostic. Handshake kinds (18/19/20) bypass the open and route to the handshake driver. Every error path drops and counts; none returns `Err` upward.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_net`
Expected: PASS, all existing plus 6 new.

- [ ] **Step 5: Commit**

```bash
git add uc2_net/src/receiver.rs
git commit -m "feat(uc2_net): open seam, drop-and-count failure paths, cleartext-peer diagnostic"
```

---

### Task 12: Node wiring — config, boot refusal, handshake routing, rotation hook

**Files:**
- Modify: `uc2_node/src/node.rs:113-155` (`NodeConfig`), the node construction path, the `Action::ConfigAdopted`/commit-crossing handler, `uc2_node/Cargo.toml`
- Test: inline in `node.rs`

**Interfaces:**
- Consumes: `uc2_crypto::{CryptoConfig, Transport, RotationReason}`.
- Produces: `NodeConfig.crypto: CryptoConfig`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_node_configured_for_crypto_with_unreadable_key_files_refuses_to_start() {
    // Mirrors the M7 self-tombstone boot refusal: a node that cannot
    // authenticate must not silently fall back to cleartext.
    let mut cfg = test_node_config();
    cfg.crypto = CryptoConfig::Enabled {
        key_path: "/nonexistent/key".into(),
        allowlist_path: "/nonexistent/allow".into(),
        rotation: Default::default(),
    };
    assert!(Node::new(cfg).is_err());
}

#[test]
fn default_config_is_disabled_so_existing_deployments_are_untouched() {
    assert!(matches!(test_node_config().crypto, CryptoConfig::Disabled));
}

#[test]
fn winning_an_election_mints_a_fresh_epoch() {
    let mut h = crypto_harness();
    let before = h.node.crypto_epoch();
    h.drive_to_leader();
    assert!(epoch_is_newer(h.node.crypto_epoch().unwrap(), before.unwrap_or(0)));
}

#[test]
fn a_committed_remove_rotates_but_a_committed_demote_does_not() {
    let mut h = crypto_harness();
    h.drive_to_leader();
    let e0 = h.node.crypto_epoch().unwrap();

    h.commit_config_demoting(2);
    assert_eq!(h.node.crypto_epoch().unwrap(), e0, "a demote keeps the node replicating");

    h.commit_config_removing(3);
    assert!(epoch_is_newer(h.node.crypto_epoch().unwrap(), e0), "a removal revokes");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_node node::tests::a_node_configured`
Expected: FAIL — no `crypto` field.

- [ ] **Step 3: Implement.** Add the field with `#[derive(Default)]` giving `Disabled`. Construct the `Transport` in the node's constructor **before** agent spawn (so a failure is a clean early return, as the M7 tombstone refusal does) and hand clones to sender and receiver. Route handshake kinds from the receiver to the handshake driver over the existing `NetEvent` channel. Call `on_became_leader` where `Action::BecomeLeader` is handled and `on_committed_config(config.tombstones.len())` at the same commit-crossing point `rank_leader` already uses for `StepDownRemoved`. Drain `rotation_due` once per duty cycle and mint when it returns `Some`.

Three wiring details that have no other home, so they must land here:

- **`HandshakeAction::Send` for `HS_KEY` bodies is sealed by this layer**, not by `GroupPlane`: the node takes the action's body, calls `Transport::seal(DGRAM_KIND_HS_KEY, Some(peer), &mut body, now)` — which resolves to the pairwise scope — and sends it. `GroupPlane` never touches a socket or a pairwise key, which is what keeps it unit-testable.
- **`allowlist_reload_if_stale` is called from the duty cycle** (cheap: it stats the file and returns early unless both the mtime changed and a second has passed) and eagerly on a handshake from an unknown id. Without a caller, M7's runtime node-add would need a restart to authorize the joiner — the case §5 of the spec exists to serve.
- **Wire `boot_salt` into the peer's derivation**: on `HandshakeAction::Established { peer, boot_salt }`, record the salt so `derive_send_key(group, peer, salt)` can open that peer's group-sealed traffic. A missing salt means every `DATA` datagram from that peer fails to open — the symptom to expect if this step is skipped.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_node`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc2_node/Cargo.toml uc2_node/src/node.rs
git commit -m "feat(uc2_node): CryptoConfig, boot refusal, handshake routing, rotation hook"
```

---

### Task 13: Deterministic sim coverage

**Files:**
- Modify: `uc2_sim/src/world.rs`, `uc2_sim/tests/scenarios.rs`, `uc2_sim/Cargo.toml`
- Test: `uc2_sim/tests/scenarios.rs`

**Interfaces:**
- Consumes: `uc2_crypto::{Peers, GroupPlane, HandshakeAction}`.
- Produces: sim scenarios only.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn handshakes_complete_under_loss_and_reorder() {
    let mut w = World::new(WorldConfig { seed: 7, drop_per_million: 200_000, ..Default::default() });
    w.enable_crypto_plane(3);
    assert!(w.run_until(|w| w.all_peer_sessions_established(), 60_000_000_000).unwrap());
}

#[test]
fn rotation_during_a_partition_converges_once_healed() {
    let mut w = World::new(WorldConfig { seed: 11, ..Default::default() });
    w.enable_crypto_plane(3);
    w.run_until(|w| w.all_peer_sessions_established(), 10_000_000_000).unwrap();
    w.partition(&[2]);
    w.rotate_group_key();
    w.run_for(5_000_000_000);
    assert!(!w.node(2).has_epoch(w.current_epoch()), "isolated node misses the epoch");
    w.heal();
    assert!(w.run_until(|w| w.node(2).has_epoch(w.current_epoch()), 20_000_000_000).unwrap());
}

#[test]
fn a_node_that_missed_an_epoch_recovers_via_the_existing_nak_path() {
    // No new recovery mechanism: the gap heals the way every other gap heals.
    let mut w = World::new(WorldConfig { seed: 13, ..Default::default() });
    w.enable_crypto_plane(3);
    w.run_until(|w| w.all_peer_sessions_established(), 10_000_000_000).unwrap();
    w.drop_next_key_delivery_to(2);
    w.rotate_group_key();
    w.append_and_replicate(64 * 1024);
    assert!(w.run_until(|w| w.node(2).durable() == w.leader_durable(), 30_000_000_000).unwrap());
    assert!(w.nak_count(2) > 0, "recovery went through NAK repair");
}

#[test]
fn every_existing_safety_invariant_still_holds_with_the_crypto_plane_on() {
    let mut w = World::new(WorldConfig { seed: 21, drop_per_million: 50_000, ..Default::default() });
    w.enable_crypto_plane(5);
    w.run_for(120_000_000_000).unwrap();
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_sim handshakes_complete`
Expected: FAIL — no `enable_crypto_plane`.

- [ ] **Step 3: Implement** the sim hooks. The handshake driver is already a pure transition function, so the world feeds it messages on its virtual clock with no sockets. `enable_crypto_plane(n)` gives each simulated node a `Peers` + `GroupPlane` and routes kinds 18/19/20 through the existing lossy link model.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_sim`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc2_sim
git commit -m "test(uc2_sim): crypto plane — handshake under loss, rotation across partition, NAK recovery"
```

---

### Task 14: Adversarial and fault-injection coverage

**Files:**
- Modify: `uc2_net/src/fault.rs`
- Create: `uc2_net/tests/crypto_adversarial.rs`
- Test: the new integration test file

**Interfaces:**
- Consumes: everything above.
- Produces: `FaultConfig.replay_per_million: u32`, `FaultConfig.corrupt_per_million: u32`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_replayed_vote_cannot_be_recounted() {
    // A replayed VOTE is the one that actually costs safety, so pin it directly.
    let mut c = SealedCluster::new(3);
    let vote = c.capture_first(DGRAM_KIND_VOTE);
    let grants_before = c.node(0).vote_grants_seen();
    c.inject_raw(0, &vote);
    c.pump();
    assert_eq!(c.node(0).vote_grants_seen(), grants_before);
}

#[test]
fn a_peer_removed_from_the_allowlist_cannot_re_establish() {
    let mut c = SealedCluster::new(3);
    c.pump_until_established();
    c.remove_from_allowlist(0, 2);
    c.force_rehandshake(2);
    c.pump();
    assert!(!c.node(0).has_session_with(2), "revoked identity stays out");
}

#[test]
fn a_downgrade_to_cleartext_is_refused() {
    let mut c = SealedCluster::new(3);
    c.pump_until_established();
    c.inject_cleartext_data(0, 4096, b"unauthenticated");
    c.pump();
    assert!(c.node(0).buffer_is_empty_at(4096));
}

#[test]
fn heavy_corruption_and_replay_injection_never_panics_and_never_diverges() {
    let mut c = SealedCluster::with_faults(3, FaultConfig {
        seed: 5, corrupt_per_million: 300_000, replay_per_million: 300_000, ..Default::default()
    });
    c.run_load(10_000);
    c.assert_no_divergence();
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_net --test crypto_adversarial`
Expected: FAIL — no such test file / no `replay_per_million`.

- [ ] **Step 3: Implement** the two fault knobs in `FaultSocket::send_to` (corrupt flips one random byte; replay stashes and re-delivers a previous datagram) and the `SealedCluster` harness.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc2_net --test crypto_adversarial`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add uc2_net/src/fault.rs uc2_net/tests/crypto_adversarial.rs
git commit -m "test(uc2_net): adversarial tier — replayed vote, revoked peer, downgrade, corruption storm"
```

---

### Task 15: Capstones with crypto ON

**Files:**
- Modify: `uc2_node/tests/lin_v2.rs`, `uc2_node/tests/lin_partition_v2.rs`, `examples/uc2-crashtest/`, `scripts/elle_check.sh`
- Test: the existing capstones, parameterized

**Interfaces:**
- Consumes: everything above.
- Produces: `UC2_CRYPTO=1` environment switch honored by every harness.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn linearizable_under_failover_with_crypto() {
    // Same capstone, sealed transport. The checker is untouched — if crypto
    // broke ordering or dropped committed bytes, WGL would catch it.
    let mut c = LinClusterV2::new(LinConfig { crypto: true, ..Default::default() });
    c.run_failover_workload();
    assert!(c.check_linearizable());
}
```

Add the same `crypto: true` arm to `linearizable_under_purge_and_snapshot_churn`, `linearizable_under_reconfig_churn`, and `lin_partition_v2`'s three scenarios.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_node --test lin_v2 linearizable_under_failover_with_crypto --release`
Expected: FAIL — `LinConfig` has no `crypto` field.

- [ ] **Step 3: Implement.** Add `crypto: bool` to `LinConfig`; when set, generate a keypair + allowlist per node into the test's `CARGO_TARGET_TMPDIR` instance dir and set `CryptoConfig::Enabled`. Do the same in the crashtest reference bins, gated on `UC2_CRYPTO=1`.

- [ ] **Step 4: Run the full capstone set both ways**

Run: `cargo test -p uc2_node --test lin_v2 --release && cargo test -p uc2_node --test lin_partition_v2 --release && cargo test -p uc2-crashtest --features hard-crash-tests && UC2_CRYPTO=1 ELLE_DIR=/home/claude/elle-out scripts/elle_check.sh`
Expected: PASS. Budget ~6 minutes for the lincheck capstones.

- [ ] **Step 5: Commit**

```bash
git add uc2_node/tests examples/uc2-crashtest scripts/elle_check.sh
git commit -m "test: run the full capstone set with crypto ON"
```

---

### Task 16: Throughput A/B, gate doc, and operator docs

**Files:**
- Create: `docs/benchmarks/uc2-m8-gate-2026-07-28.md`
- Modify: `uc2_node/examples/m5_gate.rs`, `docs/ops/uc2-runbook.md`, `docs/releases.md`, `CLAUDE.md`
- Test: the gate harness itself

**Interfaces:**
- Consumes: everything above.
- Produces: `m5_gate --crypto` flag.

- [ ] **Step 1: Write the decide rule into the gate doc BEFORE running anything**

Create `docs/benchmarks/uc2-m8-gate-2026-07-28.md` with the bar stated up front: **encrypted throughput ≥ 90% of the cleartext control**, both arms run back-to-back on the same box, same seed, same duration, control first. Record the M5 baseline (1.64M responses/s @ p50 0.600ms) as the reference point. Pre-committing the rule is the point — it is what made the read-profile arc trustworthy.

- [ ] **Step 2: Add the `--crypto` flag to the gate harness**

Generate per-node key material into the harness's instance dirs and set `CryptoConfig::Enabled`.

- [ ] **Step 3: Run both arms**

Run: `cargo run -p uc2_node --release --example m5_gate && cargo run -p uc2_node --release --example m5_gate -- --crypto`
Expected: two result blocks. Record both verbatim.

- [ ] **Step 4: Adjudicate against the pre-committed rule and write it up**

If the encrypted arm is within 10%, PASS. If not, the gate doc records the honest number and the failure — do not retune the bar after seeing the data. Investigate with `perf` before changing anything: the likely culprits are a missing AES-NI backend, a per-datagram allocation in the seal path, or the MTU budget forcing extra datagrams.

- [ ] **Step 5: Write the operator documentation**

Add a runbook section covering key generation, allowlist format and distribution, the flag-day rollout procedure, what the four new drop counters mean, and rotation policy tuning. Add the `releases.md` 0.4.0 entry. Update `CLAUDE.md`'s crate list with `uc2_crypto` and the security-posture line (v2.0's "explicit non-goal" is now "opt-in, off by default").

- [ ] **Step 6: Full local proof stack**

Run: `cargo build --workspace && cargo test && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc2_node --test lin_v2 --release && cargo test -p uc2_node --test lin_partition_v2 --release && cargo test -p uc2-crashtest --features hard-crash-tests && cargo test -p uc2_service --features ultima_db && cargo run -p uc2_node --release --example m6_gate -- all --secs 6 --cycles 3 && cargo run -p uc2_node --release --example m7_gate -- all --secs 6`
Expected: all green. Budget ~10 minutes.

- [ ] **Step 7: Commit**

```bash
git add docs uc2_node/examples/m5_gate.rs CLAUDE.md
git commit -m "docs(m8): gate doc with pre-committed bar, runbook ops section, releases 0.4.0"
```

---

## After the plan

The **cross-host fleet gate is a separate, user-approved step** — it costs real AWS money and the M7 precedent is that it runs only on explicit approval. Do not launch it as part of executing this plan. When approved, it extends `bench-infra/scripts/m6_fleet_gate.py` with a `--crypto` arm.
