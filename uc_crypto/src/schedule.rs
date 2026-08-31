//! The group-key epoch schedule: per-sender-per-boot key derivation, and a
//! two-epoch-deep store for the cluster-wide group key.
//!
//! # Why derivation, not the group key directly
//!
//! The transport (T5, AES-256-GCM) treats a repeated `(key, nonce)` pair as
//! catastrophic: reusing a nonce under one key doesn't just leak one message,
//! it leaks the authentication subkey and breaks every message ever sealed
//! under that key. Nonces here are per-sender counters, and this system has
//! two structural ways to repeat one:
//!
//! - a NAK retransmit re-sends the same log position (and therefore the same
//!   counter) under the same sealing key;
//! - a process restart resets a sender's counter to 0 while the cluster-wide
//!   group key is still live (the group key rotates on its own, much slower,
//!   schedule — see `rotation.rs`, T8).
//!
//! [`derive_send_key`] closes this by never sealing under the group key
//! directly: the actual sealing key for a sender is
//! `HKDF(group_key, salt = sender_id ‖ … )`-style separated on BOTH the
//! sender id AND a fresh random boot salt chosen once per process start. A
//! restarted node gets a brand-new key space for the same counter sequence,
//! so a counter reset can never reuse a nonce under a key another peer still
//! has live. Derivation must be deterministic across peers (every node
//! derives a given sender's key identically from the same group key +
//! sender id + boot salt) or fan-out traffic couldn't be decrypted at all —
//! see `derivation_separates_senders_and_boots` below for both properties
//! nailed down together.
//!
//! # Why two epochs, not one
//!
//! [`KeySchedule`] keeps at most two group keys live (current + previous) so
//! that a group-key rotation (T8) has an overlap window: a follower that
//! hasn't yet observed the new epoch can still be served under the outgoing
//! one until [`KeySchedule::retire_below`] drops it. `epoch_is_newer` treats
//! the epoch counter as a wrapping `u16` — only two epochs are ever live at
//! once, so a wraparound (65535 -> 0) is a non-event as long as comparisons
//! stay modular rather than a plain `>`.

use crate::NodeId;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// The cluster-wide group key for one epoch. Zeroized on drop — this is
/// long-lived key material shared by the whole cluster, held in the
/// `KeySchedule` for as long as an epoch stays live.
///
/// The inner array is deliberately private: `derive(Zeroize, ZeroizeOnDrop)`
/// only scrubs the wrapper's own storage, so a `pub` field on a `Copy` array
/// would let any caller write `let raw = group_key.0;` and hold an
/// un-zeroized copy of live cluster key material forever — the same hazard
/// `identity.rs` documents (and was fixed for, commit 818eecb) for
/// `Identity::private`. Construct via [`GroupKey::new`]; read via
/// [`GroupKey::as_bytes`].
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct GroupKey([u8; 32]);

impl GroupKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        GroupKey(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A random value chosen once per process start and mixed into
/// [`derive_send_key`]. Not secret on its own (it is not key material by
/// itself — it is a public separator, exchanged alongside the handshake in
/// T6), so it is not zeroized.
/// `Hash` added (T9 review round 1, F1): `Transport`'s group-scope replay
/// window is keyed by `(sender, epoch, salt)` — the salt has to be part of
/// the key so a recurring epoch number (`GroupPlane::next_epoch` restarts at
/// 0 on every fresh process) under a NEW boot salt starts a fresh window
/// instead of colliding with the old boot's high-water mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BootSalt(pub [u8; 16]);

/// Derives the per-sender-per-boot sealing key: `HKDF-SHA256` with `salt`
/// as the HKDF salt, `group`'s bytes as the input keying material, and
/// `b"uc2/send" ‖ sender.to_le_bytes()` as the info string, expanded to 32
/// bytes.
///
/// Separates on BOTH `sender` and `salt`: two different senders under the
/// same boot salt get different keys (so peers can't be confused for each
/// other), and the same sender across two different boot salts (i.e. across
/// a restart) also gets different keys (so a counter reset after a restart
/// can never repeat a nonce under a key still live for the previous boot).
/// Deterministic in all three inputs, so every peer that holds the same
/// group key derives the same sender's key identically.
///
/// # Callers must
///
/// This returns bare, un-wrapped key material — the signature is pinned by
/// the design for T5/T9 to consume directly. The raw `[u8; 32]` must be
/// wrapped into a zeroizing type (e.g. `Zeroizing<[u8; 32]>`, or moved
/// straight into an AEAD key type) in the SAME statement that calls this
/// function, never bound to a bare intermediate (`let raw = derive_send_key(...)`)
/// that outlives the wrap — that intermediate is exactly the un-scrubbed
/// copy this crate's zeroize discipline exists to prevent.
pub fn derive_send_key(group: &GroupKey, sender: NodeId, salt: &BootSalt) -> [u8; 32] {
    let mut info = [0u8; 8 + 4];
    info[..8].copy_from_slice(b"uc2/send");
    info[8..].copy_from_slice(&sender.to_le_bytes());

    let hkdf = Hkdf::<Sha256>::new(Some(&salt.0), group.as_bytes());
    let mut out = [0u8; 32];
    hkdf.expand(&info, &mut out)
        .expect("32-byte output is within HKDF-SHA256's valid expand length");
    out
}

/// Compares two `u16` epoch numbers modularly: `a` is "newer" than `b` if
/// stepping forward from `b` reaches `a` within half the counter's range.
/// The epoch counter wraps (`u16`); since at most two epochs are ever live
/// at once (see [`KeySchedule`]), a wraparound can never be ambiguous under
/// this comparison.
pub fn epoch_is_newer(a: u16, b: u16) -> bool {
    let diff = a.wrapping_sub(b);
    diff != 0 && diff < 0x8000
}

/// Holds at most two live group-key epochs: the current one and the
/// previous one, kept around for the overlap window during a rotation (T8).
/// [`KeySchedule::install`] shifts the current epoch into the previous slot
/// before installing the new one, so the two most recently installed epochs
/// are always the ones retained — never more.
pub struct KeySchedule {
    current: Option<(u16, GroupKey)>,
    previous: Option<(u16, GroupKey)>,
}

impl KeySchedule {
    pub fn new() -> Self {
        KeySchedule {
            current: None,
            previous: None,
        }
    }

    /// Installs `key` as the new current epoch, demoting the prior current
    /// epoch (if any) to previous. A previous-previous epoch (if one was
    /// already sitting in the previous slot) is dropped and zeroized here.
    pub fn install(&mut self, epoch: u16, key: GroupKey) {
        self.previous = self.current.take();
        self.current = Some((epoch, key));
    }

    /// The current epoch number and its key, or `None` before the first
    /// `install`.
    pub fn current(&self) -> Option<(u16, &GroupKey)> {
        self.current.as_ref().map(|(epoch, key)| (*epoch, key))
    }

    /// Looks up the key for `epoch` if it is one of the (at most two) live
    /// epochs. An unrecognized epoch — a peer using a rotated-out key, or
    /// attacker-supplied garbage — is a plain miss, never a panic: callers
    /// on the wire-facing path (T5/T9) treat this as "cannot open,"  not as
    /// a program error.
    pub fn get(&self, epoch: u16) -> Option<&GroupKey> {
        self.current
            .as_ref()
            .filter(|(e, _)| *e == epoch)
            .or_else(|| self.previous.as_ref().filter(|(e, _)| *e == epoch))
            .map(|(_, key)| key)
    }

    /// Drops (and zeroizes) any live epoch strictly older than `epoch`,
    /// under the same modular comparison as [`epoch_is_newer`]. Used once a
    /// rotation's overlap window has closed and every peer is confirmed on
    /// the new epoch.
    pub fn retire_below(&mut self, epoch: u16) {
        if matches!(&self.current, Some((e, _)) if *e != epoch && epoch_is_newer(epoch, *e)) {
            self.current = None;
        }
        if matches!(&self.previous, Some((e, _)) if *e != epoch && epoch_is_newer(epoch, *e)) {
            self.previous = None;
        }
    }
}

impl Default for KeySchedule {
    fn default() -> Self {
        KeySchedule::new()
    }
}

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
        assert!(
            ks.get(1).is_some(),
            "previous epoch retained for the overlap"
        );
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
