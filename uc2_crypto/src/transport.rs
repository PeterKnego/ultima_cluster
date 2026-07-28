// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The transport facade (spec §3, §6, §9 task list T9): the crate's single
//! entry point for `uc2_net` (T10/T11). [`Transport`] composes [`Peers`]
//! (T6, handshake + pairwise sessions), [`GroupPlane`] (T7, group-key
//! mint/distribute/activate), and [`RotationState`] (T8, when a new epoch is
//! due) behind two calls: [`Transport::seal`] before a send,
//! [`Transport::open`] after a receive. Everything else in this crate stays
//! reachable only through those two calls plus the small maintenance surface
//! ([`Transport::rotation_due`], [`Transport::allowlist_reload_if_stale`]).
//!
//! # The scope split, decided once, here
//!
//! [`Scope`] is chosen **by kind, never by destination** (spec §3): the four
//! fanned-out kinds (`DATA`, `HEARTBEAT`, `COMMIT_POSITION`, `READ_PROBE`)
//! take the group key so a leader seals once and sends N times; everything
//! else — including a `DATA` retransmit or deep-NAK replay that happens to
//! target exactly one peer — still takes the group key precisely because it
//! is still `DATA`. [`Transport::scope_of`] is the one place this rule is
//! encoded; every other module and every future call site defers to it
//! rather than re-deriving "is this fanned out" per call.
//!
//! # The counter
//!
//! `Transport` owns exactly one `u64` counter, shared across BOTH scopes and
//! every epoch. This is deliberately more conservative than the minimum the
//! safety argument needs (group and pairwise seal under structurally
//! different keys — a per-sender-per-boot HKDF derivation for group traffic,
//! a fresh per-handshake ephemeral DH for pairwise — so nothing would
//! actually collide if each scope kept its own counter). A single
//! ever-increasing counter is simpler to reason about and to test than two
//! interleaved ones, and "the counter never repeats, full stop" is a
//! strictly stronger invariant than "the counter never repeats within a key
//! space" — it makes the nonce-reuse argument true independent of which
//! scope or epoch a given call turns out to take, rather than relying on the
//! two scopes' key derivations staying disjoint forever.
//!
//! # Carried requirement #2 — never read `KeySchedule::current()` directly
//!
//! [`Transport::seal`]'s group branch asks [`GroupPlane::sealing_epoch`]
//! which epoch to seal new traffic under (never `group.schedule().current()`
//! — see `group.rs`'s docs on why a reordered late `HS_KEY` duplicate makes
//! `current()`/`previous()` an unsafe thing to branch on). [`Transport::open`]'s
//! group branch, by contrast, looks up a SPECIFIC epoch NAMED BY THE
//! INCOMING DATAGRAM's header via `group.schedule().get(epoch)` — that is
//! the safe accessor `group.rs` documents (`get`/`retire_below` key off the
//! epoch NUMBER, never the current/previous label), and it is a different
//! operation from reading "the" current epoch.
//!
//! # Carried requirement #3 — `peer_boot_salt`, not a cached `Established` salt
//!
//! Opening group traffic from `from` requires deriving `from`'s sealing key,
//! which needs `from`'s boot salt. [`Transport::open`]'s group branch calls
//! [`Peers::peer_boot_salt`] FRESH on every call rather than caching a salt
//! observed from a past `HandshakeAction::Established` action — a session can
//! be established-but-unpromoted (`confirmed: false`) for up to 30s after a
//! peer restart (`handshake.rs`'s `PENDING_TTL_NS`), during which the peer's
//! GROUP-scoped fan-out already uses its new salt even though the PAIRWISE
//! path has not switched yet. A cached salt from the wrong side of that
//! window derives the wrong key and every group datagram from a just-restarted
//! peer fails to open until the cache is somehow invalidated. `peer_boot_salt`
//! always reports the salt of the session in force right now, so this class
//! of staleness cannot occur no matter how the caller's own bookkeeping is
//! shaped.
//!
//! # Carried requirement #4 — cipher caching on the fan-out hot path
//!
//! Measured: `Aes256Gcm::new` is ~9% of a seal's cost, and it is avoidable —
//! the group-scope sealing key depends only on (epoch, our own id, our own
//! boot salt), all fixed for as long as an epoch is current. [`Transport`]
//! caches ONE constructed cipher for the currently-sealing epoch
//! (`seal_cache`) and rebuilds it only when [`GroupPlane::sealing_epoch`]
//! reports a different epoch than last time — which is exactly the
//! seal-once-send-to-N-peers case the group key exists for: one
//! `Aes256Gcm::new` amortized over an entire fan-out, not one per
//! destination. This targets the measured hot path directly (`DATA`,
//! `HEARTBEAT`, `COMMIT_POSITION`, `READ_PROBE` — the exact four group-scope
//! kinds); the pairwise send path is deliberately left calling
//! [`Peers::seal_pairwise`] (which still builds a cipher per call) because
//! those kinds are unicast, low-rate control traffic (spec §3: "N seals is
//! irrelevant at these rates") and caching there would mean reaching into
//! `handshake.rs`'s private `Session` state from outside its own module for
//! a case the measured bar does not require.
//!
//! [`Transport::open`]'s group branch deliberately does NOT cache a cipher
//! (unlike the seal side): caching it correctly would need a cache key wide
//! enough to invalidate on a peer's boot-salt change independent of any
//! epoch rotation (see carried requirement #3 above) — solvable, but the
//! measured perf bar this task answers to is about the SEND path (finding
//! (f) in the plan ledger), and getting the receive-side invalidation wrong
//! would silently reintroduce the exact staleness class #3 exists to close.
//! Left as a documented, deliberately deferred optimization rather than a
//! rushed one.
//!
//! # Carried requirement #5 — `open_detached`
//!
//! [`Transport::open`] takes `&mut Vec<u8>`, per this task's pinned
//! interface, and therefore still goes through [`crate::seal::open_in_place`]
//! (which shrinks the `Vec`). The zero-copy option this requirement asks for
//! — `open_detached`, for a caller reading into a persistent oversized
//! buffer — is added to `seal.rs` as a sibling primitive for `uc2_net`'s
//! receiver (T11) to call directly on its own scratch buffer, bypassing
//! `Transport::open`'s `Vec`-shrinking path entirely. It is not used inside
//! this module; it exists for the lower layer this facade sits on top of.

use crate::group::GroupPlane;
use crate::handshake::Peers;
use crate::identity::{Allowlist, Identity};
use crate::replay::ReplayWindow;
use crate::rotation::{RotationPolicy, RotationReason, RotationState};
use crate::schedule::derive_send_key;
use crate::seal::{open_in_place, seal_with};
use crate::{CryptoError, NodeId};
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::{Aes256Gcm, KeyInit};
use rand::TryRngCore;
use rand::rngs::OsRng;
use std::collections::HashMap;
use std::path::PathBuf;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_COMMIT_POSITION,
    DGRAM_KIND_CONFIG_PROPOSAL, DGRAM_KIND_CONFIG_REPLY, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT,
    DGRAM_KIND_NAK, DGRAM_KIND_READ_PROBE, DGRAM_KIND_READ_PROBE_ACK, DGRAM_KIND_REQUEST_VOTE,
    DGRAM_KIND_SNAP_BEGIN, DGRAM_KIND_SNAP_CHUNK, DGRAM_KIND_SNAP_DONE, DGRAM_KIND_SNAP_NAK,
    DGRAM_KIND_STATUS, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE, OFF_DGRAM_KEY_EPOCH,
    read_datagram_header,
};
use zeroize::Zeroizing;

/// How this node's node-to-node UDP transport is configured. `Disabled` (the
/// default) is plain cleartext, exactly today's M1-M7 behavior. `Enabled`
/// turns on this whole crate; a bad or missing key/allowlist file there is a
/// boot refusal (see [`Transport::new`]), never a silent fallback to
/// cleartext.
#[derive(Debug, Clone, Default)]
pub enum CryptoConfig {
    #[default]
    Disabled,
    Enabled {
        key_path: PathBuf,
        allowlist_path: PathBuf,
        rotation: RotationPolicy,
    },
}

/// Which key a datagram kind takes. See the module docs — this is decided
/// purely by [`Transport::scope_of`]'s match on `kind`, never by who a given
/// call happens to be sending to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Group,
    Pairwise,
}

/// The crate's facade: one `seal`/`open` pair, the counter, and the key
/// schedule/session/rotation state they are built on. See the module docs.
pub struct Transport {
    self_id: NodeId,
    peers: Peers,
    group: GroupPlane,
    rotation: RotationState,
    /// The one counter this crate ever allocates a nonce from. See the
    /// module docs' "The counter" section for why this is a single field
    /// shared by both scopes rather than one per scope/epoch.
    counter: u64,
    /// One cached cipher for whichever epoch [`GroupPlane::sealing_epoch`]
    /// most recently reported — see the module docs' carried requirement #4.
    /// Rebuilt only when the epoch actually changes.
    seal_cache: Option<(u16, Aes256Gcm)>,
    /// Anti-replay state for RECEIVED group-scope traffic, one window per
    /// (sender, epoch) — `GroupPlane` itself tracks no receive-side replay
    /// state (that is a `Peers`/`Session` concept on the pairwise side; the
    /// group side has no `Session` to hang a window off, so `Transport` is
    /// where it has to live).
    group_replay: HashMap<(NodeId, u16), ReplayWindow>,
}

impl Transport {
    /// Builds a `Transport` from `cfg`. `Disabled` yields `Ok(None)` — the
    /// caller wires nothing further and every datagram stays cleartext,
    /// exactly today. `Enabled` loads the identity and allowlist from disk
    /// (propagating [`CryptoError`] on a missing/malformed/unreadable file —
    /// boot refusal, never a silent cleartext fallback: see the crate's
    /// `KeyFilePermissions`/`KeyFileInvalid`/`MalformedAllowlist` variants)
    /// and mints a fresh [`crate::schedule::BootSalt`] from the OS RNG.
    ///
    /// `boot_salt` is generated **here**, once, per process lifetime — never
    /// persisted, reused, or derived from anything stable (hostname, node
    /// id, instance dir). It is what makes a post-restart counter reset safe
    /// (`schedule.rs`'s `derive_send_key` docs): a fresh salt puts a
    /// restarted node's counter sequence in a brand-new key space no matter
    /// what value the counter itself resumes at.
    pub fn new(cfg: &CryptoConfig, self_id: NodeId) -> Result<Option<Transport>, CryptoError> {
        let (key_path, allowlist_path, rotation) = match cfg {
            CryptoConfig::Disabled => return Ok(None),
            CryptoConfig::Enabled {
                key_path,
                allowlist_path,
                rotation,
            } => (key_path, allowlist_path, *rotation),
        };

        let identity = Identity::load(key_path)?;
        let allowlist = Allowlist::load(allowlist_path)?;

        // Not key material (see `schedule::BootSalt`'s doc: it is a public
        // separator, not a secret), so no `Zeroizing` wrap is needed here —
        // unlike every raw KEY this crate mints (`group.rs`'s `mint`), which
        // does wrap at the point of creation per carried requirement #1.
        let mut salt_bytes = [0u8; 16];
        OsRng
            .try_fill_bytes(&mut salt_bytes)
            .expect("the OS RNG is unavailable");
        let boot_salt = crate::schedule::BootSalt(salt_bytes);

        let peers = Peers::new(identity, allowlist, self_id, boot_salt);

        Ok(Some(Transport {
            self_id,
            peers,
            group: GroupPlane::new(self_id),
            rotation: RotationState::new(rotation),
            counter: 0,
            seal_cache: None,
            group_replay: HashMap::new(),
        }))
    }

    /// Which key scope datagram `kind` takes — see the module docs and spec
    /// §3. Exhaustive `match`, deliberately with NO catch-all defaulting to
    /// `Group`: an unrecognized kind (one that does not exist yet, or one
    /// this crate's list has fallen out of date with) falls through to the
    /// LAST arm below, which maps it to `Pairwise` — explicitly, with the
    /// reasoning inline, never silently. A brand-new kind is unicast/control
    /// traffic until someone deliberately adds it to the fan-out list; the
    /// failure mode of guessing `Group` wrong (seal-once-send-to-N when the
    /// kind was really meant for one peer) is a correctness/security defect,
    /// while guessing `Pairwise` wrong is merely a missed optimization.
    pub fn scope_of(kind: u8) -> Scope {
        match kind {
            DGRAM_KIND_DATA
            | DGRAM_KIND_HEARTBEAT
            | DGRAM_KIND_COMMIT_POSITION
            | DGRAM_KIND_READ_PROBE => Scope::Group,

            DGRAM_KIND_NAK
            | DGRAM_KIND_STATUS
            | DGRAM_KIND_APPEND_POSITION
            | DGRAM_KIND_READ_PROBE_ACK
            | DGRAM_KIND_REQUEST_VOTE
            | DGRAM_KIND_VOTE
            | DGRAM_KIND_TERM_MAP
            | DGRAM_KIND_SNAP_BEGIN
            | DGRAM_KIND_SNAP_CHUNK
            | DGRAM_KIND_SNAP_NAK
            | DGRAM_KIND_SNAP_DONE
            | DGRAM_KIND_CONFIG_PROPOSAL
            | DGRAM_KIND_CONFIG_REPLY => Scope::Pairwise,

            // Every kind this crate knows about is named above. Anything
            // else — a future kind not yet added to this match, or garbage
            // read off the wire before the kind is even validated — is
            // unicast/control until someone deliberately classifies it
            // otherwise: see this function's doc comment. NOT a silent
            // default to `Group`, which is the unsafe direction to guess.
            _ => Scope::Pairwise,
        }
    }

    /// Seals a staged outgoing datagram (cleartext header already written by
    /// the caller, payload appended) under the scope [`Transport::scope_of`]
    /// picks for `kind`.
    ///
    /// For `Scope::Group`: stamps the sealing epoch into the datagram's
    /// `key_epoch` header field (spec §6: "`seal_in_place` ... stamps the
    /// epoch into the header"), derives that epoch's per-sender-per-boot
    /// send key, and seals — via the per-epoch cached cipher (carried
    /// requirement #4). `peer` is ignored: the whole point of the group key
    /// is that the same sealed bytes go to every destination, so which one
    /// `peer` names (if any) has no bearing on what gets sealed. Fails
    /// closed with [`CryptoError::NoGroupKey`] if no epoch has ever
    /// activated yet — NEVER falls back to sending `buf` unsealed.
    ///
    /// For `Scope::Pairwise`: `peer` MUST be `Some` — see the panic note
    /// below — and this seals under that peer's established session via
    /// [`Peers::seal_pairwise`].
    ///
    /// Either branch allocates the next value from [`Transport`]'s single
    /// counter before attempting the seal; a failed attempt (e.g.
    /// `NoSession`) simply burns that counter value rather than reusing it —
    /// harmless (counters need only never repeat, not be dense) and simpler
    /// than threading the allocation back out of a failed call.
    ///
    /// # Panics
    ///
    /// If `Transport::scope_of(kind) == Scope::Pairwise` and `peer` is
    /// `None`. This is a caller-contract violation, not attacker-controlled
    /// input (`kind` and `peer` are the CALLER's own routing decision, never
    /// bytes off the wire — the crate's "never panic on untrusted input"
    /// rule is about `open`, which is where adversarial bytes actually
    /// enter), so it panics loudly rather than inventing a
    /// `CryptoError::NoSession(0)` that would misrepresent an internal bug
    /// as "no session with node 0" in an operator's logs.
    pub fn seal(
        &mut self,
        kind: u8,
        peer: Option<NodeId>,
        buf: &mut Vec<u8>,
        now_ns: u64,
    ) -> Result<(), CryptoError> {
        match Self::scope_of(kind) {
            Scope::Group => self.seal_group(buf, now_ns),
            Scope::Pairwise => {
                let peer = peer.expect(
                    "Transport::seal: scope_of(kind) == Pairwise requires Some(peer) — \
                     caller-contract violation, not attacker input",
                );
                let counter = self.next_counter();
                self.peers.seal_pairwise(peer, buf, counter)
            }
        }
    }

    fn seal_group(&mut self, buf: &mut Vec<u8>, now_ns: u64) -> Result<(), CryptoError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        let epoch = self
            .group
            .sealing_epoch(now_ns)
            .ok_or(CryptoError::NoGroupKey)?;

        buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&epoch.to_le_bytes());

        let counter = self.next_counter();
        {
            let cipher = self.group_seal_cipher(epoch)?;
            seal_with(buf, cipher, counter)?;
        }
        self.rotation.on_bytes_sealed(buf.len() as u64);
        Ok(())
    }

    /// Returns the cached cipher for `epoch`, rebuilding it if the last
    /// cached epoch does not match. See carried requirement #4 in the module
    /// docs.
    fn group_seal_cipher(&mut self, epoch: u16) -> Result<&Aes256Gcm, CryptoError> {
        let stale = !matches!(&self.seal_cache, Some((e, _)) if *e == epoch);
        if stale {
            let group_key = self
                .group
                .schedule()
                .get(epoch)
                .ok_or(CryptoError::NoGroupKey)?;
            // Bound into a zeroizing wrapper in the SAME statement that
            // derives it (carried requirement #1) — the cipher built from it
            // below outlives this function; the raw key bytes never do.
            let key: Zeroizing<[u8; 32]> = Zeroizing::new(derive_send_key(
                group_key,
                self.self_id,
                &self.peers.boot_salt(),
            ));
            let cipher = Aes256Gcm::new(GenericArray::from_slice(&*key));
            self.seal_cache = Some((epoch, cipher));
        }
        Ok(&self.seal_cache.as_ref().unwrap().1)
    }

    fn next_counter(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }

    /// Opens a received datagram in place: reads the cleartext header (never
    /// indexing before checking `buf` is at least long enough to hold one —
    /// this sees bytes off the wire from anyone who can reach the UDP port),
    /// picks the scope from `kind`, and authenticates+decrypts under the
    /// matching key.
    ///
    /// `Scope::Group`: looks up the key for the epoch NAMED IN THE DATAGRAM's
    /// header via `group.schedule().get(epoch)` — never `.current()`, see
    /// carried requirement #2 — derives `from`'s send key using
    /// [`Peers::peer_boot_salt`] fetched FRESH on this call (carried
    /// requirement #3, never a cached salt), opens, and checks+records the
    /// counter in this sender+epoch's replay window. An epoch this node has
    /// never seen (rotated out, or a peer on a newer epoch we have not yet
    /// received `HS_KEY` for) is [`CryptoError::NoGroupKey`] — never a panic,
    /// always self-heals once `HS_KEY` lands (spec §5/§6).
    ///
    /// `Scope::Pairwise`: delegates entirely to [`Peers::open_pairwise`],
    /// which owns that peer's session lookup, current/pending trial, and
    /// replay window.
    pub fn open(&mut self, from: NodeId, buf: &mut Vec<u8>) -> Result<(), CryptoError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        let header = read_datagram_header(buf);
        match Self::scope_of(header.kind) {
            Scope::Group => self.open_group(from, header.key_epoch, buf),
            Scope::Pairwise => self.peers.open_pairwise(from, buf).map(|_counter| ()),
        }
    }

    fn open_group(
        &mut self,
        from: NodeId,
        epoch: u16,
        buf: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        let group_key = self
            .group
            .schedule()
            .get(epoch)
            .ok_or(CryptoError::NoGroupKey)?;
        // Fresh every call — carried requirement #3. Never cache this value
        // across calls; see the module docs for why a cached salt can be
        // stale for up to 30s after a peer restart.
        let salt = self
            .peers
            .peer_boot_salt(from)
            .ok_or(CryptoError::NoSession(from))?;
        let key: Zeroizing<[u8; 32]> = Zeroizing::new(derive_send_key(group_key, from, &salt));

        let counter = open_in_place(buf, &key)?;

        let window = self.group_replay.entry((from, epoch)).or_default();
        if window.check_and_set(counter) {
            Ok(())
        } else {
            Err(CryptoError::Replayed(counter))
        }
    }

    /// Forwards to [`RotationState::take_due`]. See `rotation.rs` — this is
    /// a pure decision, driven by whatever the node layer has fed into the
    /// underlying [`RotationState`] via its own event methods (a later
    /// task's wiring: this crate does not yet expose
    /// `on_became_leader`/`on_committed_config` through `Transport`, only
    /// `on_bytes_sealed`, which [`Transport::seal`]'s group branch drives
    /// automatically on every successful group seal).
    pub fn rotation_due(&mut self, now_ns: u64) -> Option<RotationReason> {
        self.rotation.take_due(now_ns)
    }

    /// Forwards to [`Peers::allowlist_reload_if_stale`] — see that method's
    /// doc for why this is a distinct entry point from whatever
    /// [`Peers::tick`] does internally.
    pub fn allowlist_reload_if_stale(&mut self, now_ns: u64) -> Result<bool, CryptoError> {
        self.peers.allowlist_reload_if_stale(now_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handshake::HandshakeAction;
    use uc_protocol::v2::datagram::*;

    #[test]
    fn fan_out_kinds_take_the_group_key_and_the_rest_are_pairwise() {
        // The rule is BY KIND, never by destination — serve_nak sends DATA to a
        // single peer and still uses the group key.
        for k in [
            DGRAM_KIND_DATA,
            DGRAM_KIND_HEARTBEAT,
            DGRAM_KIND_COMMIT_POSITION,
            DGRAM_KIND_READ_PROBE,
        ] {
            assert_eq!(Transport::scope_of(k), Scope::Group, "kind {k}");
        }
        for k in [
            DGRAM_KIND_NAK,
            DGRAM_KIND_STATUS,
            DGRAM_KIND_APPEND_POSITION,
            DGRAM_KIND_READ_PROBE_ACK,
            DGRAM_KIND_REQUEST_VOTE,
            DGRAM_KIND_VOTE,
            DGRAM_KIND_TERM_MAP,
            DGRAM_KIND_SNAP_BEGIN,
            DGRAM_KIND_SNAP_CHUNK,
            DGRAM_KIND_SNAP_NAK,
            DGRAM_KIND_SNAP_DONE,
            DGRAM_KIND_CONFIG_PROPOSAL,
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
        assert!(
            Transport::new(&CryptoConfig::Disabled, 1)
                .unwrap()
                .is_none()
        );
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

    // ---- Test scaffolding shared by the tests below --------------------

    /// Scratch root on real ext4 (`target/`), never `/tmp` — see
    /// `identity.rs`'s `tmp()`, same rationale (CLAUDE.md: `/tmp` here is
    /// RAM-backed tmpfs with no swap).
    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../target/uc2_crypto_tests")
            })
            .join("uc2-crypto-transport")
            .join(tag);
        std::fs::create_dir_all(&d).unwrap();
        assert!(
            !d.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {d:?}"
        );
        d
    }

    /// A well-formed `Enabled` `Transport` with a fresh key file and an empty
    /// (but well-formed) allowlist — matches the brief's mandated test
    /// exactly (`enabled_transport()`, no arguments). Used by exactly one
    /// mandated test; every other test needing an `Enabled` transport uses
    /// [`node_transport`] below (parameterized, so parallel tests do not
    /// race on the same key/allowlist file).
    fn enabled_transport() -> Transport {
        node_transport("mandated-shared", 1, PRIV_SOLO, &[])
    }

    // Fixed X25519 private-key fixtures, same discipline as `identity.rs`'s
    // and `handshake.rs`'s tests: arbitrary-looking but real key material,
    // with public halves DERIVED (never pasted as opaque base64), so the
    // relationship between the private fixture and any allowlist entry built
    // from it is visible here rather than asserted by fiat.
    const PRIV_SOLO: [u8; 32] = [0x55; 32];
    const PRIV_A: [u8; 32] = [0x11; 32];
    const PRIV_B: [u8; 32] = [0x22; 32];

    fn public_of(private: [u8; 32]) -> [u8; 32] {
        let secret = x25519_dalek::StaticSecret::from(private);
        x25519_dalek::PublicKey::from(&secret).to_bytes()
    }

    /// Builds an `Enabled` `Transport` from a private key, the id it claims,
    /// and the peers it authorizes — `tag` gives each caller its own
    /// directory so parallel `cargo test` runs never race on the same file.
    fn node_transport(
        tag: &str,
        self_id: NodeId,
        private: [u8; 32],
        allow: &[(NodeId, [u8; 32])],
    ) -> Transport {
        let dir = scratch_dir(tag);
        let key_path = dir.join("node.key");
        std::fs::write(&key_path, private).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }

        let allow_path = dir.join("allowlist");
        let mut text = String::new();
        for (id, public) in allow {
            use base64::Engine;
            text.push_str(&format!(
                "{id} {}\n",
                base64::engine::general_purpose::STANDARD.encode(public)
            ));
        }
        std::fs::write(&allow_path, text).unwrap();

        let cfg = CryptoConfig::Enabled {
            key_path,
            allowlist_path: allow_path,
            rotation: RotationPolicy::default(),
        };
        Transport::new(&cfg, self_id).unwrap().unwrap()
    }

    fn data_datagram() -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut v,
            &DatagramHeader {
                position: 4096,
                leadership_term_id: 3,
                kind: DGRAM_KIND_DATA,
                flags: 0,
                key_epoch: 0,
            },
        );
        v.extend_from_slice(b"log bytes here");
        v
    }

    fn vote_datagram() -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut v,
            &DatagramHeader {
                position: 0,
                leadership_term_id: 7,
                kind: DGRAM_KIND_VOTE,
                flags: 0,
                key_epoch: 0,
            },
        );
        v.extend_from_slice(b"vote body");
        v
    }

    /// Drives two `Transport`s' internal `Peers` against each other with no
    /// sockets, exactly `handshake.rs`'s own `pump` helper (T9 cannot reuse
    /// that one directly — it lives in a different file's private test
    /// module — but the shape is identical, and it is exercising the exact
    /// same public `Peers` API this crate's own reviewed handshake tests do,
    /// not anything transport.rs invents). Runs until both sides report
    /// `Established { confirmed: true }` or the exchange quiesces.
    fn establish(a: &mut Transport, b: &mut Transport, mut acts: Vec<HandshakeAction>) {
        let (a_id, b_id) = (a.self_id, b.self_id);
        for _ in 0..8 {
            let mut next = Vec::new();
            for act in acts.drain(..) {
                if let HandshakeAction::Send { to, kind, body } = act {
                    if to == b_id {
                        next.extend(b.peers.on_message(a_id, kind, &body, 0));
                    } else if to == a_id {
                        next.extend(a.peers.on_message(b_id, kind, &body, 0));
                    }
                }
                // `Established`/`Failed` carry no further action to replay.
            }
            if next.is_empty() {
                break;
            }
            acts = next;
        }
        assert!(a.peers.is_established(b_id), "a failed to establish with b");
        assert!(b.peers.is_established(a_id), "b failed to establish with a");
    }

    // ---- Beyond the mandated five. Every prior task in this plan shipped
    // mandated tests a wrong implementation passed anyway (T4/T5/T6/T7/T8 —
    // see the plan ledger). The tests below target the three areas the task
    // brief names explicitly: `scope_of`'s exhaustiveness beyond the pinned
    // 17 kinds, the counter's monotonicity across scopes and epochs, and the
    // no-group-key error path leaving no trace in the buffer. Each is paired
    // with the wrong implementation it was written to kill (see the task
    // report for the real red-then-green transcripts).

    #[test]
    fn an_unrecognized_kind_defaults_to_pairwise_never_group() {
        // `every_wire_kind_has_an_assigned_scope` only sweeps 1..=17 and
        // discards the result — it cannot catch a `_ => Scope::Group`
        // catch-all, which would still pass every mandated test (every
        // KNOWN kind is still asserted correctly by
        // `fan_out_kinds_take_the_group_key_and_the_rest_are_pairwise`).
        // This is exactly the design rule the brief calls out by name: "no
        // catch-all that silently defaults to Group".
        for k in [0u8, 21, 22, 100, 200, 255] {
            assert_eq!(
                Transport::scope_of(k),
                Scope::Pairwise,
                "unrecognized kind {k} must default to Pairwise, never Group"
            );
        }
    }

    #[test]
    fn sealing_with_no_group_key_leaves_the_buffer_completely_untouched() {
        // The mandated test only checks the Err variant. A wrong
        // implementation could still write the epoch/counter/tag scaffolding
        // into `buf` before discovering there is no key and returning
        // `Err` anyway — corrupting the caller's staged datagram even though
        // the call "failed". The brief's own framing is "never a cleartext
        // send"; this pins the stronger property "never ANY mutation" on the
        // failure path.
        let mut t = node_transport("no-group-key-untouched", 1, PRIV_SOLO, &[]);
        let mut d = data_datagram();
        let before = d.clone();
        assert!(matches!(
            t.seal(DGRAM_KIND_DATA, None, &mut d, 0),
            Err(CryptoError::NoGroupKey)
        ));
        assert_eq!(
            d, before,
            "a failed group seal must not mutate the staged buffer at all"
        );
    }

    #[test]
    fn seal_group_stamps_the_chosen_epoch_into_the_header() {
        // Spec §6: "seal_in_place(&mut scratch, kind) selects the scope by
        // kind, stamps the epoch into the header". A wrong implementation
        // could seal successfully under the right key while leaving the
        // header's key_epoch field at whatever the caller happened to write
        // (typically 0) — the receiver would then look up the WRONG epoch
        // and fail to open traffic that was actually sealed correctly.
        let mut t = node_transport("seal-stamps-epoch", 1, PRIV_SOLO, &[]);
        t.group.mint(&[], 0); // no peers named -> activates immediately (vacuous all())
        t.group.mint(&[], 0); // mint a second time so the epoch under test is NOT the field's zero-init default
        let epoch = t.group.sealing_epoch(0).expect("just minted");
        assert_ne!(
            epoch, 0,
            "confirms this test observes the real epoch, not a zero-initialized field by coincidence"
        );
        let mut d = data_datagram();
        t.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        let got = u16::from_le_bytes([d[OFF_DGRAM_KEY_EPOCH], d[OFF_DGRAM_KEY_EPOCH + 1]]);
        assert_eq!(
            got, epoch,
            "the sealed header must carry the epoch that was actually used to seal it"
        );
    }

    #[test]
    fn counter_is_monotonic_across_repeated_group_seals_and_an_epoch_rotation() {
        // Targets: a wrong `next_counter` that resets per call (always
        // returns 1), or a design that keys the counter off the epoch (so
        // rotating restarts it at 1) — either would still pass every
        // mandated test, which never seals more than once.
        let mut t = node_transport("counter-monotonic", 1, PRIV_SOLO, &[]);
        t.group.mint(&[], 0);
        for _ in 0..3 {
            let mut d = data_datagram();
            t.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        }
        assert_eq!(
            t.counter, 3,
            "three successful seals must allocate three distinct counters"
        );

        // Rotate to a second epoch and keep sealing — the counter must NOT
        // reset just because the underlying key changed.
        t.group.mint(&[], 0);
        for _ in 0..3 {
            let mut d = data_datagram();
            t.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        }
        assert_eq!(
            t.counter, 6,
            "the counter must keep advancing across an epoch rotation, not reset"
        );
    }

    #[test]
    fn counter_never_repeats_across_group_and_pairwise_scopes() {
        // Targets a design that (wrongly) keeps a SEPARATE counter per scope
        // instead of the one field the module docs describe — two
        // independent counters would each start at 1, so the wire bytes of
        // the FIRST group seal and the FIRST pairwise seal would carry the
        // identical counter value 1. Establishes a real pairwise session (no
        // shortcuts through private `Session` state, which is not visible
        // from this module) so `seal_pairwise` actually succeeds.
        let mut a = node_transport(
            "counter-cross-scope-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let mut b = node_transport(
            "counter-cross-scope-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let acts = a.peers.initiate(2, 0);
        establish(&mut a, &mut b, acts);

        a.group.mint(&[], 0);
        let mut group_d = data_datagram();
        a.seal(DGRAM_KIND_DATA, None, &mut group_d, 0).unwrap();
        assert_eq!(a.counter, 1);

        let mut pairwise_d = vote_datagram();
        a.seal(DGRAM_KIND_VOTE, Some(2), &mut pairwise_d, 0)
            .unwrap();
        assert_eq!(
            a.counter, 2,
            "the SAME counter must keep advancing across scopes, not restart at 1"
        );

        let group_counter = u64::from_le_bytes(
            group_d[DATAGRAM_HEADER_LEN..DATAGRAM_HEADER_LEN + 8]
                .try_into()
                .unwrap(),
        );
        let pairwise_counter = u64::from_le_bytes(
            pairwise_d[DATAGRAM_HEADER_LEN..DATAGRAM_HEADER_LEN + 8]
                .try_into()
                .unwrap(),
        );
        assert_ne!(
            group_counter, pairwise_counter,
            "the two scopes' wire counters must not collide"
        );
    }

    #[test]
    fn group_scope_round_trips_end_to_end_through_seal_and_open() {
        // A full integration proof, not just "compiles": two real
        // Transports, a real handshake, a real HS_KEY delivery, then
        // seal-on-a / open-on-b through the public Transport facade only.
        let mut a = node_transport(
            "group-e2e-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let mut b = node_transport(
            "group-e2e-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let acts = a.peers.initiate(2, 0);
        establish(&mut a, &mut b, acts);

        // a mints and delivers the group key to b over the now-established
        // pairwise channel (exactly what the node layer will do in a later
        // task — GroupPlane never touches sockets itself).
        let (epoch, key_actions) = a.group.mint(&[2], 0);
        for act in key_actions {
            let HandshakeAction::Send { to, kind, body } = act else {
                panic!("mint emits Send")
            };
            assert_eq!(to, 2);
            let reply = b.group.on_key_message(1, &body);
            for r in reply {
                let HandshakeAction::Send {
                    kind: rkind,
                    body: rbody,
                    ..
                } = r
                else {
                    panic!("ack is Send")
                };
                let _ = kind; // (HS_KEY, unused beyond the assert above)
                a.group.on_key_message(2, &rbody);
                let _ = rkind;
            }
        }
        assert_eq!(
            a.group.sealing_epoch(0),
            Some(epoch),
            "b's ack must have activated the epoch"
        );

        let mut d = data_datagram();
        let plain = d.clone();
        a.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        assert_ne!(d, plain, "sealing must actually change the buffer");
        b.open(1, &mut d)
            .expect("b must be able to open what a sealed");
        assert_eq!(
            d, plain,
            "round trip through the public facade is byte-exact"
        );
    }

    #[test]
    fn pairwise_scope_round_trips_end_to_end_through_seal_and_open() {
        let mut a = node_transport(
            "pairwise-e2e-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let mut b = node_transport(
            "pairwise-e2e-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let acts = a.peers.initiate(2, 0);
        establish(&mut a, &mut b, acts);

        let mut d = vote_datagram();
        let plain = d.clone();
        a.seal(DGRAM_KIND_VOTE, Some(2), &mut d, 0).unwrap();
        assert_ne!(d, plain);
        b.open(1, &mut d)
            .expect("b must be able to open what a sealed");
        assert_eq!(d, plain);
    }

    #[test]
    fn opening_group_traffic_under_an_unknown_epoch_is_no_group_key_not_a_panic() {
        // A peer on a newer epoch we have not received HS_KEY for yet, or a
        // rotated-out epoch, must self-heal via the existing NAK path (spec
        // §5/§6) — never panic, never silently accept.
        let mut b = node_transport(
            "unknown-epoch-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let mut d = data_datagram();
        d[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&7u16.to_le_bytes());
        assert!(matches!(b.open(1, &mut d), Err(CryptoError::NoGroupKey)));
    }

    #[test]
    fn a_replayed_group_datagram_is_rejected_on_the_second_open() {
        let mut a = node_transport(
            "replay-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let mut b = node_transport(
            "replay-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let acts = a.peers.initiate(2, 0);
        establish(&mut a, &mut b, acts);
        let (_epoch, key_actions) = a.group.mint(&[2], 0);
        for act in key_actions {
            let HandshakeAction::Send { body, .. } = act else {
                unreachable!()
            };
            let reply = b.group.on_key_message(1, &body);
            for r in reply {
                let HandshakeAction::Send { body: rbody, .. } = r else {
                    unreachable!()
                };
                a.group.on_key_message(2, &rbody);
            }
        }

        let mut d = data_datagram();
        a.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        let mut replay = d.clone();
        b.open(1, &mut d).expect("first open succeeds");
        assert!(
            matches!(b.open(1, &mut replay), Err(CryptoError::Replayed(_))),
            "a captured-and-resent group datagram must be refused on replay"
        );
    }

    #[test]
    fn opening_a_short_buffer_returns_too_short_and_never_panics() {
        // Untrusted-input contract: Transport::open reads the cleartext
        // header before it knows anything about the sender, so it must never
        // index into a buffer shorter than the header itself.
        let mut b = node_transport(
            "short-buf-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        for len in 0..DATAGRAM_HEADER_LEN {
            let mut d = vec![0u8; len];
            assert!(
                matches!(b.open(1, &mut d), Err(CryptoError::TooShort)),
                "len {len} must reject, not panic"
            );
        }
    }

    #[test]
    fn rotation_due_forwards_to_the_underlying_rotation_state() {
        let mut t = node_transport("rotation-due", 1, PRIV_SOLO, &[]);
        assert_eq!(
            t.rotation_due(0),
            None,
            "nothing due yet on a fresh transport"
        );
    }

    #[test]
    fn allowlist_reload_if_stale_forwards_and_rate_limits() {
        let mut t = node_transport("allowlist-reload", 1, PRIV_SOLO, &[]);
        // Immediately after construction the rate limit has not elapsed
        // (last_reload_attempt_ns starts at 0, and this call also happens at
        // now_ns=0), so this must be a false/no-op, not an error.
        assert!(matches!(t.allowlist_reload_if_stale(0), Ok(false)));
    }
}
