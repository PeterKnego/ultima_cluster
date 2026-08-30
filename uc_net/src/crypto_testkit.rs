// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Test-only fixtures for standing up REAL `uc_crypto` state — key files,
//! allowlists, completed Noise-IK handshakes, delivered group keys — using
//! only `uc_crypto`'s public API.
//!
//! M8 Task 17. T11 grew these inside `receiver.rs`'s test module; T17 needs
//! the same fixtures in `sender.rs` (the snapshot path's pairwise seals need
//! an ESTABLISHED session, which no unit test could fake before). Hoisted
//! here rather than copied a second time — `receiver.rs`'s existing helpers
//! now forward to these, so there is one definition of "a real established
//! pair" in this crate and the receiver's 20-odd shipped crypto tests are
//! themselves the regression suite for it.
//!
//! `#[cfg(test)]` only: nothing here is compiled into the shipped library,
//! and it is deliberately unreachable from `tests/*.rs` integration tests
//! (which link the crate as an external consumer) — those build their own.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use uc_crypto::{HandshakeAction, NodeId, SharedTransport};

/// A fresh scratch directory under `CARGO_TARGET_TMPDIR` (real ext4), never
/// `/tmp` — see CLAUDE.md: `/tmp` on the dev box is RAM-backed tmpfs with no
/// swap, and test artifacts there race the busy-spin agents for the RAM pool.
/// The `SEQ` suffix keeps parallel tests from colliding on a shared tag.
pub fn scratch_dir(area: &str, tag: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::var("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_net_tests"))
        .join(area)
        .join(format!("{tag}-{seq}"));
    std::fs::create_dir_all(&dir).unwrap();
    assert!(!dir.starts_with("/tmp"), "test scratch must not live on tmpfs: {dir:?}");
    dir
}

/// Writes a raw 32-byte X25519 private key at the 0600 mode
/// `uc_crypto::identity::Identity::load` insists on.
pub fn write_key_file(path: &Path, private: [u8; 32]) {
    std::fs::write(path, private).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

/// Derives a node's public key from its raw private key bytes, via a
/// throwaway `Identity` — `uc_net` has no X25519 dependency of its own (nor
/// should it gain one for fixture plumbing); `Identity::public_bytes` is
/// already the crate's own public accessor for exactly this.
pub fn identity_public(tag: &str, private: [u8; 32]) -> [u8; 32] {
    let dir = scratch_dir("uc2-net-crypto-pub", tag);
    let key_path = dir.join("node.key");
    write_key_file(&key_path, private);
    uc_crypto::identity::Identity::load(&key_path).unwrap().public_bytes()
}

/// Minimal standard-alphabet base64 WITH padding, matching
/// `uc_crypto::identity`'s allowlist parser (which uses the `base64` crate's
/// `STANDARD` engine) — hand-rolled rather than adding a `base64`
/// dev-dependency to `uc_net` for one fixture's allowlist text.
pub fn b64_32(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 { ALPHABET[((n >> 6) & 0x3F) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { ALPHABET[(n & 0x3F) as usize] as char } else { '=' });
    }
    out
}

/// A real `CryptoConfig::Enabled` `SharedTransport` for `self_id`, with
/// `allow` written out as a genuine allowlist file.
pub fn shared_transport(
    area: &str,
    tag: &str,
    self_id: NodeId,
    private: [u8; 32],
    allow: &[(NodeId, [u8; 32])],
) -> SharedTransport {
    let dir = scratch_dir(area, tag);
    let key_path = dir.join("node.key");
    write_key_file(&key_path, private);
    let allow_path = dir.join("allowlist");
    let mut text = String::new();
    for (id, public) in allow {
        text.push_str(&format!("{id} {}\n", b64_32(public)));
    }
    std::fs::write(&allow_path, text).unwrap();
    let cfg = uc_crypto::CryptoConfig::Enabled {
        key_path,
        allowlist_path: allow_path,
        rotation: uc_crypto::rotation::RotationPolicy::default(),
    };
    SharedTransport::new(&cfg, self_id).unwrap().unwrap()
}

/// Drives a genuine Noise-IK handshake between `a` and `b` to completion,
/// through the public `initiate`/`on_handshake_message` forwarders only.
pub fn establish(a: &SharedTransport, a_id: NodeId, b: &SharedTransport, b_id: NodeId) {
    let mut acts = a.initiate(b_id, 0);
    for _ in 0..8 {
        let mut next = Vec::new();
        for act in acts.drain(..) {
            if let HandshakeAction::Send { to, kind, body } = act {
                if to == b_id {
                    next.extend(b.on_handshake_message(a_id, kind, &body, 0));
                } else if to == a_id {
                    next.extend(a.on_handshake_message(b_id, kind, &body, 0));
                }
            }
        }
        if next.is_empty() {
            break;
        }
        acts = next;
    }
    assert!(a.is_established(b_id), "{a_id} failed to establish with {b_id}");
    assert!(b.is_established(a_id), "{b_id} failed to establish with {a_id}");
}

/// Mints a group epoch on `leader` and delivers+acks it to `follower` over
/// the already-established pairwise channel. Returns the real epoch (never
/// 0 — `GroupPlane` reserves 0 as the wire's cleartext sentinel).
pub fn deliver_group_key(
    leader: &SharedTransport,
    leader_id: NodeId,
    follower: &SharedTransport,
    follower_id: NodeId,
) -> u16 {
    let (epoch, acts) = leader.mint_group_key(&[follower_id], 0);
    assert_ne!(epoch, 0, "epoch 0 is reserved and must never be minted");
    for act in acts {
        let HandshakeAction::Send { to, body, .. } = act else {
            panic!("mint must emit a Send action")
        };
        assert_eq!(to, follower_id);
        for r in follower.on_group_key_message(leader_id, &body) {
            let HandshakeAction::Send { body: rbody, .. } = r else {
                panic!("a well-formed delivery must ack back")
            };
            leader.on_group_key_message(follower_id, &rbody);
        }
    }
    epoch
}
