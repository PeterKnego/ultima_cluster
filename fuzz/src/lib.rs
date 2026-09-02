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

impl uc_service::RawStateMachine for NoopSm {
    const NAME: &'static str = "noop";

    fn apply(&mut self, _ctx: &mut uc_service::ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) {
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
impl uc_service::SnapshotStateMachine for NoopSm {
    type SnapshotHandle = ();

    fn freeze(&self) -> Result<((), u64), uc_service::SnapshotError> {
        Ok(((), 0))
    }

    fn stream_snapshot(
        _handle: (),
        _dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut sink = std::io::sink();
        let _ = std::io::copy(src, &mut sink);
        Ok(position)
    }
}

/// A state machine that ECHOES its command body back as the response.
///
/// Exists because [`NoopSm`] cannot exercise `Sessioned`'s byte budget:
/// `Sessioned` accounts `total_bytes` from the length of each cached FRESH
/// response, and `NoopSm` produces zero-length responses, so `total_bytes`
/// stays 0 forever and `evict_bytes_over_budget` never fires no matter how
/// small `SessionConfig::max_bytes` is set. Echoing the body makes the
/// response length attacker-controlled, which is what puts the budget path in
/// reach (M12d Task 3 review).
///
/// Deliberately APPENDS rather than clearing `out` — the opposite discipline
/// to `NoopSm`, so the two together cover both readings of
/// [`uc_service::RawStateMachine::apply`]'s "cleared by the caller" contract
/// (the reading that clears is what found M12d finding #1).
#[derive(Default)]
pub struct EchoSm {
    applied: Option<u64>,
}

impl uc_service::RawStateMachine for EchoSm {
    const NAME: &'static str = "echo";

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.applied = Some(ctx.position);
        out.extend_from_slice(cmd);
    }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(q);
    }
    fn last_applied(&self) -> Option<u64> {
        self.applied
    }
}

/// Snapshot capability for [`EchoSm`], so `Sessioned<EchoSm>` can be driven
/// through the snapshot seam like `Sessioned<NoopSm>`. The state is a single
/// `Option<u64>`, encoded as 8 bytes plus a presence byte.
impl uc_service::SnapshotStateMachine for EchoSm {
    type SnapshotHandle = Option<u64>;

    fn freeze(&self) -> Result<(Option<u64>, u64), uc_service::SnapshotError> {
        Ok((self.applied, self.applied.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: Option<u64>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        dst.write_all(&[u8::from(handle.is_some())])?;
        dst.write_all(&handle.unwrap_or(0).to_le_bytes())?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut buf = [0u8; 9];
        src.read_exact(&mut buf)?;
        self.applied = if buf[0] == 0 {
            None
        } else {
            Some(u64::from_le_bytes(buf[1..9].try_into().expect("9-byte buffer")))
        };
        Ok(position)
    }
}

// ---------------------------------------------------------------------------
// Crypto-plane constructors
//
// `uc_crypto`'s `Identity` and `Allowlist` have NO from-bytes constructor by
// design (identity.rs) — both go through the real on-disk loaders, including
// the 0600 permission check. These helpers are copied from `uc_crypto`'s own
// `handshake.rs` test module (`node`, `authorized_pair`, `public_of`,
// `scratch`) because those are `#[cfg(test)]` and invisible from here. They
// are the same construction, with fixed key material so everything derived
// from them is deterministic.
// ---------------------------------------------------------------------------

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use uc_crypto::NodeId;
use uc_crypto::handshake::Peers;
use uc_crypto::identity::{Allowlist, Identity};
use uc_crypto::schedule::BootSalt;

/// Fixed X25519 private scalars — the same values `uc_crypto`'s tests use.
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
    let d = root.join("uc_fuzz-scratch").join(name);
    assert!(!d.starts_with("/tmp"), "fuzz scratch must not live on tmpfs: {d:?}");
    std::fs::create_dir_all(&d).expect("create fuzz scratch dir");
    d
}

/// Builds a `Peers` from a private key, the id it claims, and the peers it
/// authorizes — key and allowlist through the real on-disk loaders, exactly as
/// `uc_crypto`'s `handshake.rs::node` does.
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
pub fn group_plane_with_pending() -> uc_crypto::group::GroupPlane {
    let mut plane = uc_crypto::group::GroupPlane::new(A_ID);
    let _ = plane.mint(&[B_ID, 3, 4], 1_000_000);
    plane
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_service::{RawStateMachine, SessionConfig, Sessioned, SnapshotStateMachine};

    fn envelope(client_id: u64, seq: u64, body: &[u8]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&client_id.to_le_bytes());
        v.extend_from_slice(&seq.to_le_bytes());
        v.extend_from_slice(body);
        v
    }

    /// Drive eight clients with 24-byte bodies through a `Sessioned<S>` at the
    /// given `max_bytes`, and report how big the frozen dedup table is.
    fn table_len<S>(inner: S, max_bytes: usize) -> usize
    where
        S: RawStateMachine + SnapshotStateMachine,
    {
        let cfg = SessionConfig { window: 4, max_clients: 4, max_bytes };
        let mut sm = Sessioned::new(inner, cfg);
        let mut out = Vec::new();
        for c in 1..=8u64 {
            out.clear();
            sm.apply(c, &envelope(c, 1, &[0xEEu8; 24]), &mut out);
        }
        let (handle, _) = sm.freeze().expect("freeze");
        let mut buf = Vec::new();
        <Sessioned<S> as SnapshotStateMachine>::stream_snapshot(handle, &mut buf).expect("stream");
        buf.len()
    }

    /// M12d Task 3 review: the session fuzz target claimed to exercise all
    /// three of `Sessioned`'s eviction paths and actually reached two.
    /// `Sessioned` accounts `total_bytes` from the length of each cached FRESH
    /// response, so an inner SM that returns nothing pins it at 0 and
    /// `evict_bytes_over_budget` is unreachable at ANY `max_bytes`.
    ///
    /// This is the reviewer's own experiment, kept as a guard: with `NoopSm`
    /// the frozen table is identical whatever the budget; with `EchoSm` it is
    /// not. If someone swaps the fuzz target back to a silent SM, this fails.
    #[test]
    fn only_a_response_producing_sm_can_reach_the_byte_budget() {
        let noop_tight = table_len(NoopSm, 16);
        let noop_loose = table_len(NoopSm, 64);
        assert_eq!(
            noop_tight, noop_loose,
            "NoopSm produces zero-length responses, so total_bytes never moves \
             and max_bytes cannot change the table — that is exactly why it \
             cannot fuzz the byte budget"
        );

        let echo_tight = table_len(EchoSm::default(), 16);
        let echo_loose = table_len(EchoSm::default(), 64);
        assert_ne!(
            echo_tight, echo_loose,
            "EchoSm caches 24-byte responses, so a 16-byte budget must evict \
             strictly more than a 64-byte one — if these match, the byte-budget \
             path is not being exercised and the fuzz target is back to 2/3"
        );
        assert!(echo_tight < echo_loose, "a tighter budget must evict more");
    }
}
