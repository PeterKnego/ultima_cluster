// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared helpers for the uc2 fuzz targets. Nothing here is shipped.
//!
//! This crate lives OUTSIDE the root workspace on purpose — see
//! `fuzz/README.md` and the `exclude = ["fuzz"]` line in the root
//! `Cargo.toml`. It builds on nightly only.

pub mod seeds;

/// Split `data` into `n` slices at lengths taken from its own leading bytes
/// (so the fuzzer controls the split points). Always returns exactly `n`
/// slices; trailing ones may be empty.
pub fn split(data: &[u8], n: usize) -> Vec<&[u8]> {
    let mut out = Vec::with_capacity(n);
    let (lens, mut rest) = data.split_at(data.len().min(n.saturating_sub(1)));
    for &l in lens {
        let take = (l as usize).min(rest.len());
        let (a, b) = rest.split_at(take);
        out.push(a);
        rest = b;
    }
    while out.len() < n {
        out.push(rest);
        rest = &[];
    }
    out
}

/// A state machine that ignores its input — for fuzzing adapters that wrap
/// one (`Sessioned<S>`) without caring what `S` does.
pub struct NoopSm;

impl uc2_service::RawStateMachine for NoopSm {
    fn apply(&mut self, _position: u64, _cmd: &[u8], out: &mut Vec<u8>) {
        out.clear();
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.clear();
    }
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

/// No-op snapshot capability, so `Sessioned<NoopSm>` (which forwards
/// `SnapshotStateMachine` from its inner SM) can itself be fuzzed through the
/// snapshot seam.
impl uc2_service::SnapshotStateMachine for NoopSm {
    type SnapshotHandle = ();

    fn freeze(&self) -> Result<((), u64), uc2_service::SnapshotError> {
        Ok(((), 0))
    }

    fn stream_snapshot(
        _handle: (),
        _dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        let mut sink = std::io::sink();
        let _ = std::io::copy(src, &mut sink);
        Ok(position)
    }
}

// ---------------------------------------------------------------------------
// Crypto-plane constructors
//
// `uc2_crypto`'s `Identity` and `Allowlist` have NO from-bytes constructor by
// design (identity.rs) — both go through the real on-disk loaders, including
// the 0600 permission check. These helpers are copied from `uc2_crypto`'s own
// `handshake.rs` test module (`node`, `authorized_pair`, `public_of`,
// `scratch`) because those are `#[cfg(test)]` and invisible from here. They
// are the same construction, with fixed key material so everything derived
// from them is deterministic.
// ---------------------------------------------------------------------------

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use uc2_crypto::NodeId;
use uc2_crypto::handshake::Peers;
use uc2_crypto::identity::{Allowlist, Identity};
use uc2_crypto::schedule::BootSalt;

/// Fixed X25519 private scalars — the same values `uc2_crypto`'s tests use.
pub const PRIV_A: [u8; 32] = [0x11; 32];
pub const PRIV_B: [u8; 32] = [0x22; 32];
pub const A_ID: NodeId = 1;
pub const B_ID: NodeId = 2;

pub fn public_of(private: [u8; 32]) -> [u8; 32] {
    let secret = x25519_dalek::StaticSecret::from(private);
    x25519_dalek::PublicKey::from(&secret).to_bytes()
}

/// Scratch root on real disk — NEVER `/tmp`, which is RAM-backed tmpfs with no
/// swap on the dev box (CLAUDE.md). Lands beside the fuzz build artifacts.
fn scratch(name: &str) -> std::path::PathBuf {
    let root = std::env::var("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".scratch"));
    let d = root.join("uc2-fuzz-scratch").join(name);
    assert!(!d.starts_with("/tmp"), "fuzz scratch must not live on tmpfs: {d:?}");
    std::fs::create_dir_all(&d).expect("create fuzz scratch dir");
    d
}

/// Builds a `Peers` from a private key, the id it claims, and the peers it
/// authorizes — key and allowlist through the real on-disk loaders, exactly as
/// `uc2_crypto`'s `handshake.rs::node` does.
pub fn build_peers(
    tag: &str,
    private: [u8; 32],
    self_id: NodeId,
    allow: &[(NodeId, [u8; 32])],
    salt: u8,
) -> Peers {
    let dir = scratch(tag);

    let key_path = dir.join("node.key");
    std::fs::write(&key_path, private).expect("write fuzz node key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 fuzz node key");
    }

    let allow_path = dir.join("allowlist");
    let mut text = String::new();
    for (id, public) in allow {
        text.push_str(&format!("{id} {}\n", BASE64.encode(public)));
    }
    std::fs::write(&allow_path, text).expect("write fuzz allowlist");

    Peers::new(
        Identity::load(&key_path).expect("load fuzz identity"),
        Allowlist::load(&allow_path).expect("load fuzz allowlist"),
        self_id,
        BootSalt([salt; 16]),
    )
}

/// Node A (id 1), authorizing A and B. This is the RESPONDER the handshake
/// target hammers: it has never seen a message, which is the state a node is
/// in when an unauthenticated packet from anywhere on the network arrives.
pub fn responder_peers() -> Peers {
    let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
    build_peers("responder", PRIV_A, A_ID, &allow, 0xA1)
}

/// Node B (id 2), authorizing A and B — the initiator side, used only to
/// produce a genuine first handshake message for the seed corpus.
pub fn initiator_peers() -> Peers {
    let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
    build_peers("initiator", PRIV_B, B_ID, &allow, 0xB2)
}

/// A `GroupPlane` that has already MINTED an epoch and is waiting for acks —
/// so `on_key_message`'s `MSG_ACK` arm actually has a pending epoch to fold
/// an ack into. With a virgin plane every ack is a no-op and that branch is
/// vacuous. `mint` draws its key from the OS RNG, which is fine here: this
/// builds fuzz-time state, not a committed seed.
pub fn group_plane_with_pending() -> uc2_crypto::group::GroupPlane {
    let mut plane = uc2_crypto::group::GroupPlane::new(A_ID);
    let _ = plane.mint(&[B_ID, 3, 4], 1_000_000);
    plane
}
