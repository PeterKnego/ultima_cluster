// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The transport facade (spec §3, §6, §9 task list T9): the crate's single
//! entry point for `uc_net` (T10/T11). [`Transport`] composes [`Peers`]
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
//! (revised after review round 1 — F1/F2 were a real bug in the original version)
//!
//! Opening group traffic from `from` requires deriving `from`'s sealing key,
//! which needs `from`'s boot salt. [`Transport::open_group`] never caches a
//! salt across calls — no `Transport` field holds one, and nothing observed
//! from a past `HandshakeAction::Established` action is stored. Every call
//! re-derives from whatever [`Peers`] reports RIGHT NOW.
//!
//! **The first version of this section claimed that calling
//! [`Peers::peer_boot_salt`] fresh on every call was BY ITSELF enough to
//! close this staleness class "no matter how the caller's own bookkeeping is
//! shaped." That was wrong, and review round 1 (F1, F2) demonstrated it with
//! a probe: a restarted peer's group traffic came back `Err(AuthFailed)`,**
//! because `peer_boot_salt` reports only the PAIRWISE session's `current`
//! salt — and a restarted peer's session lands in `pending` (WireGuard-style,
//! `handshake.rs`) until something PROVES the peer adopted it. Nothing but a
//! successful PAIRWISE open drives that promotion on `open_pairwise`'s own,
//! and a restarted LEADER's steady-state traffic to a given follower is
//! almost entirely GROUP-scope — so the pairwise proof needed to make
//! `peer_boot_salt` start reporting the new salt may not arrive for a long
//! time, not just the 30s `PENDING_TTL_NS` bound this doc originally (wrongly)
//! implied.
//!
//! The fix, in [`Transport::open_group`]: try the current salt via
//! `peer_boot_salt` first (the common case), and on failure ALSO try the
//! `pending` session's salt via [`Peers::peer_pending_boot_salt`] — mirroring
//! `open_pairwise`'s own current-then-pending trial — promoting `pending` via
//! [`Peers::promote_pending`] on a group-scope success, since that is exactly
//! as strong a proof the peer adopted its new session as a pairwise success
//! would be. See [`Transport::open_group`]'s own doc for the full account,
//! including F1 (the replay-window key had to widen to include the salt too
//! — see the next paragraph).
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
//! [`Transport::open_group`] deliberately does NOT cache a cipher (unlike the
//! seal side): it still pays one `Aes256Gcm::new` per received group-scope
//! datagram — that cost is real, not free, just not the one the measured
//! finding (f) was about (finding (f), in the plan ledger, is specifically
//! the SEND-side fan-out cost). Caching the receive side correctly would
//! need a cache key wide enough to invalidate on a peer's boot-salt change
//! independent of any epoch rotation — solvable (review round 1's F1 fix
//! shows the shape: key on `(sender, epoch, salt)`), but getting a CACHE's
//! invalidation wrong is a materially worse failure mode than the
//! uncached version paying an avoidable ~133 ns: review round 1's F2 was
//! exactly this class of bug in the uncached code (using the wrong salt,
//! full stop, not merely a stale cache entry), and a wrong invalidation rule
//! would have been the same bug wearing a cache. Left as a documented,
//! deliberately deferred optimization, not a claim that the receive path is
//! free.
//!
//! # Carried requirement #5 — `open_detached`
//!
//! [`Transport::open`] takes `&mut Vec<u8>`, per this task's pinned
//! interface, and therefore still goes through [`crate::seal::open_in_place`]
//! (which shrinks the `Vec`). The zero-copy option this requirement asks for
//! — `open_detached`, for a caller reading into a persistent oversized
//! buffer — is added to `seal.rs` as a sibling primitive for `uc_net`'s
//! receiver (T11) to call directly on its own scratch buffer, bypassing
//! `Transport::open`'s `Vec`-shrinking path entirely. It is not used inside
//! this module; it exists for the lower layer this facade sits on top of.

use crate::group::GroupPlane;
use crate::handshake::{HandshakeAction, Peers};
use crate::identity::{Allowlist, Identity};
use crate::replay::ReplayWindow;
use crate::rotation::{RotationPolicy, RotationReason, RotationState};
use crate::schedule::{BootSalt, derive_send_key};
use crate::seal::{open_detached, open_in_place, seal_with};
use crate::{CryptoError, NodeId};
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::{Aes256Gcm, KeyInit};
use rand::TryRngCore;
use rand::rngs::OsRng;
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uc_protocol::v2::crypto::{DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_KEY, DGRAM_KIND_HS_RESP};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_COMMIT_POSITION,
    DGRAM_KIND_CONFIG_PROPOSAL, DGRAM_KIND_CONFIG_REPLY, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT,
    DGRAM_KIND_NAK, DGRAM_KIND_READ_PROBE, DGRAM_KIND_READ_PROBE_ACK, DGRAM_KIND_REQUEST_VOTE,
    DGRAM_KIND_SNAP_BEGIN, DGRAM_KIND_SNAP_CHUNK, DGRAM_KIND_SNAP_DONE, DGRAM_KIND_SNAP_NAK,
    DGRAM_KIND_SNAP_TABLE, DGRAM_KIND_STATUS, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE,
    OFF_DGRAM_KEY_EPOCH, read_datagram_header,
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
    /// The bootstrap handshake kinds (`HS_INIT`/`HS_RESP`, spec §5) — no
    /// session and no key exist yet for these; they are what CREATES a
    /// session, driven directly via [`Peers::initiate`]/[`Peers::on_message`],
    /// never through [`Transport::seal`]/[`Transport::open`]. Named
    /// explicitly (review F4) rather than left to the catch-all: the
    /// catch-all's `Pairwise` default is safe for a genuinely unknown FUTURE
    /// kind (worst case, a missed fan-out optimization), but would be
    /// actively wrong here — attempting to AEAD-seal/open a handshake
    /// message under a nonexistent pairwise session, rather than refusing.
    Unsealed,
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
    /// (sender, epoch, salt) — `GroupPlane` itself tracks no receive-side
    /// replay state (that is a `Peers`/`Session` concept on the pairwise
    /// side; the group side has no `Session` to hang a window off, so
    /// `Transport` is where it has to live). The key is 3-wide, not
    /// `(sender, epoch)` — review round 1, F1: `GroupPlane::next_epoch`
    /// restarts at 0 on every fresh process, so a restarted sender's epoch
    /// numbers recur; the salt (fresh per process start) is what keeps a
    /// recurring epoch number from colliding with a stale high-water mark
    /// left over from the sender's previous boot. See
    /// [`Transport::open_group`]'s doc for the full account.
    ///
    /// F6 (review round 1, minor, deferred): entries are never pruned — no
    /// counterpart to `KeySchedule::retire_below`, no eviction when a peer
    /// leaves the cluster. Widening the key (the F1 fix above) makes this
    /// LESS harmful than it was: a stale entry for a peer's old boot is
    /// permanently dead weight, never a false positive, since it can only
    /// ever be looked up again under the salt that produced it. Still
    /// unbounded growth over a very long-lived cluster with many
    /// restarts/removals; left as a known gap rather than fixed here — a
    /// real fix wants to hang off the SAME membership-change signal
    /// `rotation.rs`'s `Removal` trigger already observes
    /// (`on_committed_config`), which is a small enough addition to belong
    /// in whichever task next touches this map, not squeezed into a review
    /// fix round.
    group_replay: HashMap<(NodeId, u16, BootSalt), ReplayWindow>,
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
        let boot_salt = BootSalt(salt_bytes);

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
            | DGRAM_KIND_SNAP_TABLE
            | DGRAM_KIND_CONFIG_PROPOSAL
            | DGRAM_KIND_CONFIG_REPLY => Scope::Pairwise,

            // Bootstrap: no session exists yet, so neither AEAD scope
            // applies — see [`Scope::Unsealed`]'s doc. Named explicitly
            // (review F4), not left to the catch-all below.
            DGRAM_KIND_HS_INIT | DGRAM_KIND_HS_RESP => Scope::Unsealed,
            // Rides the ALREADY-established pairwise channel (group.rs:
            // "the node layer seals the HS_KEY body over the peer's
            // already-established pairwise channel and sends it") — this
            // one DOES go through `Transport::seal`/`open` like any other
            // pairwise kind, unlike its 18/19 bootstrap siblings above.
            DGRAM_KIND_HS_KEY => Scope::Pairwise,

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
    /// For `Scope::Pairwise`: `peer` MUST be `Some` — a `None` here returns
    /// [`CryptoError::MissingPeer`] rather than sealing anything (see that
    /// variant's doc for why this is a `Result`, not a panic) — and this
    /// seals under that peer's established session via [`Peers::seal_pairwise`].
    ///
    /// For `Scope::Unsealed` (the `HS_INIT`/`HS_RESP` handshake bootstrap
    /// kinds): always [`CryptoError::UnsealedKind`]. These never have a
    /// session to seal under BY DESIGN — they are driven directly via
    /// [`Peers::initiate`]/[`Peers::on_message`], never through this facade.
    ///
    /// Either non-`Unsealed` branch allocates the next value from
    /// [`Transport`]'s single counter before attempting the seal; a failed
    /// attempt (e.g. `NoSession`) simply burns that counter value rather
    /// than reusing it — harmless (counters need only never repeat, not be
    /// dense) and simpler than threading the allocation back out of a
    /// failed call.
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
                let Some(peer) = peer else {
                    return Err(CryptoError::MissingPeer(kind));
                };
                let counter = self.next_counter();
                self.peers.seal_pairwise(peer, buf, counter)
            }
            Scope::Unsealed => Err(CryptoError::UnsealedKind(kind)),
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

        // F3 (review round 1): `sealing_epoch` and `schedule().get(epoch)`
        // CAN disagree — `sealing_epoch` can name an epoch `KeySchedule`'s
        // 2-deep window has since evicted (reachable: two mints in a row
        // that never activate before being superseded; see
        // `seal_group_fails_closed_when_sealing_epoch_names_an_epoch_evicted_from_the_schedule`).
        // The cipher/key lookup — the only fallible step left — MUST resolve
        // before `buf` is mutated at all, or a failure here leaves the
        // caller's staged datagram corrupted even though the call "failed".
        // This is byte-for-byte mutant 3's failure mode from the original
        // task report, reachable for real via this path.
        let counter = self.next_counter();
        let cipher = self.group_seal_cipher(epoch)?;

        buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&epoch.to_le_bytes());
        seal_with(buf, cipher, counter)?;
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
    /// carried requirement #2. An epoch this node has never seen (rotated
    /// out, or a peer on a newer epoch we have not yet received `HS_KEY`
    /// for) is [`CryptoError::NoGroupKey`] — never a panic, always self-heals
    /// once `HS_KEY` lands (spec §5/§6). See [`Transport::open_group`] for
    /// the salt-trial and replay-window details (review F1/F2 fixed a real
    /// liveness bug here — read that method's doc before touching it).
    ///
    /// `Scope::Pairwise`: delegates entirely to [`Peers::open_pairwise`],
    /// which owns that peer's session lookup, current/pending trial, and
    /// replay window.
    ///
    /// `Scope::Unsealed`: always [`CryptoError::UnsealedKind`] — see
    /// [`Scope::Unsealed`]'s doc.
    pub fn open(&mut self, from: NodeId, buf: &mut Vec<u8>) -> Result<(), CryptoError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        // The pre-guard above already refused a short buffer; the reader is
        // total on `&[u8]` (M12d), so the `else` arm is belt and braces.
        let Some(header) = read_datagram_header(buf) else {
            return Err(CryptoError::TooShort);
        };
        match Self::scope_of(header.kind) {
            Scope::Group => self.open_group(from, header.key_epoch, buf),
            Scope::Pairwise => self.peers.open_pairwise(from, buf).map(|_counter| ()),
            Scope::Unsealed => Err(CryptoError::UnsealedKind(header.kind)),
        }
    }

    /// Opens a group-scope datagram from `from` under the epoch named in its
    /// header.
    ///
    /// # Review F1/F2 (round 1) — a real liveness bug, fixed here
    ///
    /// The FIRST version of this method derived `from`'s key using only
    /// [`Peers::peer_boot_salt`] (the PAIRWISE session's salt — `entry.current`
    /// only) and recorded replay state keyed by `(from, epoch)` alone. Both
    /// were wrong, and compounded each other:
    ///
    /// - **F1**: [`crate::group::GroupPlane::next_epoch`] starts at 0 on
    ///   EVERY fresh process, so a restarted peer's first post-restart mint
    ///   recurs the SAME epoch number it used pre-restart. A `(from, epoch)`
    ///   replay window survives the restart (it lives in THIS node's memory,
    ///   not the restarted peer's), so the restarted peer's fresh counter
    ///   (starting back at 1) lands under the OLD boot's high-water mark and
    ///   is rejected as a replay — permanently, since the mark only grows.
    /// - **F2**: `Peers::peer_boot_salt` reports the salt of the PAIRWISE
    ///   session in force — but a restarted peer's session lands in
    ///   `pending` (WireGuard-style, `handshake.rs`) until something PROVES
    ///   the peer adopted it, and nothing but a successful PAIRWISE open
    ///   drives that promotion on its own. A restarted LEADER's steady-state
    ///   traffic to a given follower is almost entirely GROUP-scope, so that
    ///   proof may not arrive for a long time — `peer_boot_salt` alone keeps
    ///   reporting the peer's OLD, pre-restart salt, and every group
    ///   datagram from the restarted peer fails AEAD authentication.
    ///
    /// The fix mirrors [`Peers::open_pairwise`]'s own current-then-pending
    /// trial: try the CURRENT session's salt first (the common case, and the
    /// only one most datagrams ever need); if that fails, try the PENDING
    /// session's salt (via [`Peers::peer_pending_boot_salt`]), and — a group-
    /// scope success under `pending` is exactly as strong a proof the peer
    /// adopted its new session as a pairwise success would be — promote it
    /// via [`Peers::promote_pending`]. The replay window is now keyed by
    /// `(from, epoch, salt)`, not `(from, epoch)`: the salt that actually
    /// opened the datagram is part of the key, so a recurring epoch number
    /// under a NEW salt starts a brand-new window rather than colliding with
    /// the old boot's.
    fn open_group(
        &mut self,
        from: NodeId,
        epoch: u16,
        buf: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        // Epoch validity is salt-independent — check once, up front, so an
        // unknown epoch is always `NoGroupKey` regardless of whether any
        // session (current or pending) even exists yet.
        if self.group.schedule().get(epoch).is_none() {
            return Err(CryptoError::NoGroupKey);
        }

        let mut last_err = CryptoError::NoSession(from);

        if let Some(salt) = self.peers.peer_boot_salt(from) {
            match self.open_group_under_salt(from, epoch, &salt, buf) {
                Ok(counter) => return self.finish_group_open(from, epoch, salt, counter),
                Err(e) => last_err = e,
            }
        }

        // Trial 2 — F2's fix. `open_pairwise` tries `pending` on the receive
        // path already; group traffic gets the same trial here instead of
        // waiting on a pairwise datagram that may not come.
        if let Some(salt) = self.peers.peer_pending_boot_salt(from) {
            match self.open_group_under_salt(from, epoch, &salt, buf) {
                Ok(counter) => {
                    self.peers.promote_pending(from);
                    return self.finish_group_open(from, epoch, salt, counter);
                }
                Err(e) => last_err = e,
            }
        }

        Err(last_err)
    }

    /// One salt trial: derive `from`'s key under `epoch` and `salt`, and
    /// attempt to open `buf` with it. `&self` only — no session-table
    /// mutation happens here, so a caller can try multiple salts against the
    /// same `buf` without any intermediate `&mut self` step (`open_in_place`
    /// leaves `buf` byte-for-byte unchanged on failure, same discipline
    /// `open_pairwise` relies on for its own current/pending trial).
    fn open_group_under_salt(
        &self,
        from: NodeId,
        epoch: u16,
        salt: &BootSalt,
        buf: &mut Vec<u8>,
    ) -> Result<u64, CryptoError> {
        let group_key = self
            .group
            .schedule()
            .get(epoch)
            .ok_or(CryptoError::NoGroupKey)?;
        let key: Zeroizing<[u8; 32]> = Zeroizing::new(derive_send_key(group_key, from, salt));
        open_in_place(buf, &key)
    }

    /// Records `counter` in the `(from, epoch, salt)` replay window — see
    /// this key's rationale in [`Transport::open_group`]'s F1 discussion.
    ///
    /// F7 (review round 1, minor, documented not fixed): by the time this
    /// runs, `open_in_place` has already decrypted `buf` in place and
    /// shrunk it (the `Vec`-based `open_*` contract). If `check_and_set`
    /// rejects the counter as a replay, `buf` is left decrypted+shrunk
    /// anyway — asymmetric with [`Transport::seal_group`]'s untouched-
    /// on-failure property. This is not a new inconsistency introduced
    /// here: it is the SAME order `Peers::open_pairwise` already uses
    /// (open, then a separate `replay_check` step) — AEAD-open-then-replay-
    /// check is this crate's established order (`seal.rs`'s module docs:
    /// "AEAD stops forgery, not replay"), and restoring the pre-open bytes
    /// on a replay-rejection would need a full buffer copy on every group
    /// open to cover a rejection path that is rare by construction (a
    /// replay is, definitionally, not routine traffic). Left matching the
    /// pairwise path's existing behavior rather than fixed to something
    /// inconsistent with it.
    fn finish_group_open(
        &mut self,
        from: NodeId,
        epoch: u16,
        salt: BootSalt,
        counter: u64,
    ) -> Result<(), CryptoError> {
        let window = self.group_replay.entry((from, epoch, salt)).or_default();
        if window.check_and_set(counter) {
            Ok(())
        } else {
            Err(CryptoError::Replayed(counter))
        }
    }

    /// Forwards to [`RotationState::on_became_leader`] — call this whenever
    /// the node layer observes it has just become leader. Added T9 review
    /// round 1 (F5): without this, [`Transport::rotation_due`] could never
    /// report [`RotationReason::BecameLeader`], and there was no compile
    /// error to catch a future task forgetting to wire it — `RotationState`
    /// was reachable in theory but not through anything `Transport` exposed.
    pub fn on_became_leader(&mut self) {
        self.rotation.on_became_leader();
    }

    /// Forwards to [`RotationState::on_committed_config`] — call this on
    /// EVERY committed config change, promotes and demotes included, not
    /// only removals (`rotation.rs`'s own doc: the pure function needs to
    /// see demotes to correctly NOT rotate on them). Added T9 review round 1
    /// (F5): this is the ONLY path that can latch
    /// [`RotationReason::Removal`] — `rotation.rs` names it "the
    /// security-relevant trigger" because a removed node keeps decrypting
    /// captured group traffic until the key actually changes — and it was
    /// unreachable through `Transport` before this fix.
    pub fn on_committed_config(&mut self, tombstone_count: usize) {
        self.rotation.on_committed_config(tombstone_count);
    }

    /// Forwards to [`RotationState::take_due`]. See `rotation.rs` — this is
    /// a pure decision, driven by whatever the node layer has fed into the
    /// underlying [`RotationState`] via [`Transport::on_became_leader`],
    /// [`Transport::on_committed_config`], and `on_bytes_sealed` (which
    /// [`Transport::seal`]'s group branch drives automatically on every
    /// successful group seal — the only one of the three with no separate
    /// public entry point, since it is purely a function of what `Transport`
    /// itself just did).
    pub fn rotation_due(&mut self, now_ns: u64) -> Option<RotationReason> {
        self.rotation.take_due(now_ns)
    }

    /// Forwards to [`Peers::allowlist_reload_if_stale`] — see that method's
    /// doc for why this is a distinct entry point from whatever
    /// [`Peers::tick`] does internally.
    pub fn allowlist_reload_if_stale(&mut self, now_ns: u64) -> Result<bool, CryptoError> {
        self.peers.allowlist_reload_if_stale(now_ns)
    }

    /// Forwards to [`GroupPlane::mint`] — call this when [`Transport::rotation_due`]
    /// reports a reason (T12's node-layer job: "drain `rotation_due` ... and mint
    /// when it returns `Some`"). Returns the fresh epoch plus one `HS_KEY` delivery
    /// action per named peer, for the caller to seal (`Scope::Pairwise`, via this
    /// same `Transport::seal`) and send over that peer's already-established
    /// pairwise channel.
    ///
    /// Added ahead of T12 (found while implementing T10): before this method,
    /// `GroupPlane` was reachable ONLY through this struct's private `group`
    /// field, so nothing outside this crate could EVER mint a group epoch —
    /// every `Scope::Group` seal was permanently `Err(NoGroupKey)` from any
    /// external caller, T10's own send-seam included (its tests need an
    /// activated epoch to exercise the seal path at all; see
    /// `sealing_before_a_group_key_exists_is_an_error_not_a_cleartext_send`
    /// for the state every fresh `Transport` starts in). A pure forwarder,
    /// same shape as [`Transport::on_became_leader`]/[`Transport::on_committed_config`]/
    /// [`Transport::rotation_due`] above — `GroupPlane` itself is unchanged
    /// and stays unit-testable in isolation.
    pub fn mint_group_key(&mut self, peers: &[NodeId], now_ns: u64) -> (u16, Vec<HandshakeAction>) {
        self.group.mint(peers, now_ns)
    }
}

// =============================================================================
// M8 ownership correction (ruling 2026-07-29, Task 10 review round 1) — the
// Arc-shareable split `uc_net` actually consumes.
// =============================================================================
//
// [`Transport`] above is this crate's original single-threaded facade —
// every one of its tests (unchanged by this section) exercises it directly,
// and it stays the primary correctness harness for the handshake/AEAD/
// rotation logic. **It is not what `uc_net` uses.**
//
// `uc_net`'s `Sender` and (T11) `FollowerReceiver` are separate agents
// spawned on separate threads, but ONE process has exactly one set of
// handshake sessions, one group-key plane, and one boot salt. Task 10's
// original plan text said to move `Transport` by value into `Sender` — that
// makes the sessions, group plane, and receive replay state permanently
// unreachable by the receiver (T11) and the node layer (T12: minting,
// rotation, handshake routing). The two naive fixes are both wrong: wrapping
// the WHOLE `Transport` in a mutex lands a lock on the send agent's
// busy-spin per-datagram path; giving each agent its OWN `Transport` gives
// each its OWN boot salt, so a follower would derive the leader's group key
// from the WRONG salt and group-scope `open` would fail cluster-wide the
// instant a real second agent existed (see `Peers`/`GroupPlane`'s own docs:
// `boot_salt` is one-per-PROCESS, never one-per-`Transport`).
//
// The corrected shape, per the 2026-07-29 ruling:
// - [`KeyState`] — handshake sessions (`Peers`), the group-key plane
//   (`GroupPlane`), and rotation triggering (`RotationState`), behind
//   [`SharedTransport`]'s `Arc<Mutex<_>>`. Mutated only on handshake
//   completion and rotation events (mint/ack/became-leader/committed-config)
//   — all rare — so the lock costs nothing at the throughput this crate is
//   built for. `RotationState::on_bytes_sealed` is the one call on this path
//   that fires on every successful group seal, not just on a rare event —
//   but it runs under the SAME lock acquisition [`SendHalf::seal_group`]
//   already needs for `GroupPlane::sealing_epoch`/the cipher-cache miss
//   path, so it is not an ADDITIONAL lock.
// - [`SendHalf`] — the nonce counter and the seal-cipher cache. Exclusive to
//   whichever ONE agent calls `seal` (the sender agent). Constructing a
//   SECOND `SendHalf` from the same [`SharedTransport`] would give it an
//   independent counter starting back at 0 under the SAME key — a repeated
//   `(key, nonce)` pair, catastrophic under AES-GCM — so
//   [`SharedTransport::send_half`] must be called exactly once per process.
// - [`ReceiveHalf`] — the group-scope receive replay windows (the pairwise
//   ones already live inside `Peers`/`Session`, which is shared). Exclusive
//   to whichever ONE agent calls `open` (the receiver agent, T11), for the
//   same single-instance reason.
//
// `self_id` and the process `boot_salt` are invariant for the process's
// lifetime (set once in [`SharedTransport::new`], never mutated after), so
// both halves carry their own plain `Copy` values rather than reaching
// through the lock to read them on every call.
//
// **One clock source.** [`GroupPlane::sealing_epoch`]`(now_ns)` compares
// `now_ns` against the mint timestamp to decide whether the activation grace
// period elapsed. If the sender agent and a future minting/rotating agent
// each computed `now_ns` from their OWN independently-started `Instant`,
// that comparison would be meaningless — the grace period would either never
// elapse or elapse instantly, purely depending on which agent happened to
// start first and by how much (an `Instant` has no relation to any other
// process's or thread's `Instant` except by comparing elapsed durations from
// a COMMON origin). [`SharedTransport`] records ONE `base: Instant` at
// construction and copies it (a `Copy` type, no lock needed) into every half
// it hands out, so `now_ns()` means the same thing everywhere it is called.
// This does NOT reintroduce a clock read inside the crate's core logic —
// `seal`/`open`/`mint` still take `now_ns: u64` as an explicit parameter,
// exactly like `Transport`, so a test or the deterministic sim can still
// drive them with a hand-picked value; `now_ns()` only fixes what a REAL
// caller (`uc_net`) should compute that parameter from, and does so by
// construction rather than by convention.

/// The handshake/rotation state actually shared across agents — see the
/// module section docs above. Not `pub`: reachable only through
/// [`SharedTransport`]'s methods and the two halves it hands out.
struct KeyState {
    peers: Peers,
    group: GroupPlane,
    rotation: RotationState,
}

/// The Arc-shareable production facade `uc_net`/`uc_node` construct once
/// per process and hand [`SendHalf`]/[`ReceiveHalf`] out from. See the
/// module section docs above.
///
/// **Round-2 review fix (2026-07-29):** `send_half`/`receive_half` singleness
/// was originally documentation-only ("call exactly once per process"). That
/// is not enforceable: both methods take `&self`, `SharedTransport` is
/// `Clone`, and every half starts its own nonce counter at `0` — so ANY
/// holder of ANY clone calling `send_half` a second time silently mints a
/// second counter that starts back at `0` under the SAME group key. That is
/// a repeated `(key, nonce)` pair under AES-256-GCM: not a wrong answer, a
/// full authentication-subkey compromise for every message ever sealed
/// under that key. A doc comment cannot bind a call site it will never see.
/// `send_half_taken`/`receive_half_taken` make the SECOND call impossible
/// instead of merely discouraged — see both methods below.
#[derive(Clone)]
pub struct SharedTransport {
    self_id: NodeId,
    boot_salt: BootSalt,
    base: Instant,
    key: Arc<Mutex<KeyState>>,
    /// **This `SharedTransport` family's one nonce counter**, shared by every
    /// seal path reachable from it — [`SendHalf::seal`] (the sender agent's
    /// hot path) and [`SharedTransport::seal_pairwise_control`] (the node
    /// layer's rare control sends) alike, across every clone.
    ///
    /// Scoped to this family, NOT to the OS process, and the distinction is
    /// load-bearing for whoever reads this next: the legacy [`Transport`]
    /// facade above still owns a private `counter: u64` of its own. That is
    /// safe today because `Transport` is never constructed in production
    /// (only `SharedTransport` is — see the M8 ownership-correction section)
    /// and a `Transport` built in a test holds its own independently minted
    /// group key and its own handshake sessions, so its key material is
    /// disjoint from any `SharedTransport`'s. It is NOT safe to generalize
    /// this doc into "any sealer in the process draws from here": a future
    /// task that wires a `Transport` alongside a `SharedTransport` over
    /// SHARED key material would reintroduce exactly the two-counters-one-key
    /// hazard this field exists to close.
    ///
    /// Added T12. Before it, the counter lived by value inside `SendHalf`,
    /// which was sound only while `SendHalf` was the sole sealer in the
    /// process. T12 gives the node's consensus agent its own seal path (it
    /// must seal `HS_KEY` deliveries pairwise, and it cannot hold a second
    /// `SendHalf` — see [`SharedTransport::send_half`]'s single-call
    /// enforcement), and T17 extends that path to the node's own consensus
    /// datagrams and to `uc_net`'s pairwise `SNAP`/`NAK`/`STATUS` sends. The
    /// moment two paths seal under the SAME key — and they do: a pairwise
    /// `Session`'s `seal_key` is per-peer, not per-agent — two independent
    /// counters both starting at 0 would repeat a `(key, nonce)` pair under
    /// AES-256-GCM, the exact catastrophe `send_half`'s single-call guard
    /// exists to prevent. One shared `AtomicU64` makes the module docs'
    /// stated invariant ("the counter never repeats, full stop") true
    /// process-wide instead of per-half.
    ///
    /// Cost on the hot path: one uncontended `fetch_add` per seal in place
    /// of a plain increment — a rounding error against the AEAD itself, and
    /// far cheaper than the alternative of taking the `KeyState` lock to
    /// reach a counter stored there.
    counter: Arc<AtomicU64>,
    send_half_taken: Arc<AtomicBool>,
    receive_half_taken: Arc<AtomicBool>,
}

impl SharedTransport {
    /// Same contract as [`Transport::new`]: `Disabled` yields `Ok(None)`;
    /// `Enabled` loads identity/allowlist from disk (boot refusal on
    /// failure) and mints a fresh per-process `boot_salt` from the OS RNG.
    pub fn new(
        cfg: &CryptoConfig,
        self_id: NodeId,
    ) -> Result<Option<SharedTransport>, CryptoError> {
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

        let mut salt_bytes = [0u8; 16];
        OsRng
            .try_fill_bytes(&mut salt_bytes)
            .expect("the OS RNG is unavailable");
        let boot_salt = BootSalt(salt_bytes);

        let peers = Peers::new(identity, allowlist, self_id, boot_salt);

        Ok(Some(SharedTransport {
            self_id,
            boot_salt,
            base: Instant::now(),
            key: Arc::new(Mutex::new(KeyState {
                peers,
                group: GroupPlane::new(self_id),
                rotation: RotationState::new(rotation),
            })),
            counter: Arc::new(AtomicU64::new(0)),
            send_half_taken: Arc::new(AtomicBool::new(false)),
            receive_half_taken: Arc::new(AtomicBool::new(false)),
        }))
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// This process's boot salt — see the module docs and `schedule.rs`:
    /// documented as "a public separator, not secret material" (unlike
    /// `self_id`, it changes every process lifetime, which is the whole
    /// point — see `Transport::new`'s own doc for why `boot_salt` is never
    /// wrapped in a zeroizing type). Exposed for the same reason `self_id`
    /// is: a caller outside this crate that needs to derive the exact key a
    /// real seal would use (e.g. a test constructing a scenario `mint`
    /// itself can no longer reach, like epoch 0 — see `group.rs`'s
    /// reservation of epoch 0 as the wire's cleartext sentinel) has no
    /// other way to reach it.
    pub fn boot_salt(&self) -> BootSalt {
        self.boot_salt
    }

    /// The canonical crypto clock — see the module section docs' "One clock
    /// source" paragraph. Every [`SendHalf`]/[`ReceiveHalf`] handed out by
    /// this [`SharedTransport`] computes `now_ns` from the SAME origin.
    pub fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// How many seals this family has performed — the current value of the
    /// shared nonce counter (see the `counter` field doc). Every successful
    /// AEAD seal on every path reachable from this `SharedTransport` draws
    /// exactly one counter value, so this is a direct count of AEAD work done,
    /// not an estimate.
    ///
    /// Added M8 Task 16 for the throughput gate, which has to show that the
    /// measured load actually exercised the seal path CONCURRENTLY from more
    /// than one agent (T10 review's standing requirement) rather than assume
    /// it. Observability only — nothing branches on this value.
    ///
    /// Slight over-count by design: a seal that FAILS (no usable group key,
    /// no session with the peer) still burns its counter value, because
    /// counter allocation precedes the fallible key lookup so that a failure
    /// cannot leave the caller's staged datagram half-mutated. Failures are
    /// separately visible as `seal_failures` in the sender/receiver stats.
    pub fn seal_count(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    /// Hands out the send half. Enforced (round-2 review fix), not merely
    /// documented, to be callable exactly once per process — across EVERY
    /// clone of this `SharedTransport`, since `send_half_taken` is an `Arc`
    /// shared by all of them. A second call panics rather than silently
    /// minting a second nonce counter over the same key — see
    /// [`SharedTransport`]'s doc above and [`SendHalf`]'s doc for why that
    /// would be catastrophic, not just wrong.
    ///
    /// # Panics
    /// If called more than once (on this `SharedTransport` or any clone of
    /// it).
    pub fn send_half(&self) -> SendHalf {
        self.send_half_taken
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .expect(
                "SharedTransport::send_half called more than once for the same process (directly \
                 or via a clone) — a second SendHalf would start its nonce counter back at 0 \
                 under the SAME group key: a repeated (key, nonce) pair under AES-256-GCM, which \
                 leaks the authentication subkey and breaks every message ever sealed under that \
                 key, not just the repeat. Call this exactly once per process and hand the result \
                 to the sender agent.",
            );
        SendHalf {
            self_id: self.self_id,
            boot_salt: self.boot_salt,
            base: self.base,
            key: Arc::clone(&self.key),
            counter: Arc::clone(&self.counter),
            seal_cache: None,
        }
    }

    /// Hands out the receive half. Same enforced-once discipline as
    /// [`SharedTransport::send_half`] — see that method's doc.
    ///
    /// # Panics
    /// If called more than once (on this `SharedTransport` or any clone of
    /// it).
    pub fn receive_half(&self) -> ReceiveHalf {
        self.receive_half_taken
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .expect(
                "SharedTransport::receive_half called more than once for the same process \
                 (directly or via a clone) — two receive halves would keep two independent \
                 group-replay windows, each seeing only part of the traffic, which can accept a \
                 counter one of them should have rejected as a replay. Call this exactly once \
                 per process and hand the result to the receiver agent.",
            );
        ReceiveHalf {
            base: self.base,
            key: Arc::clone(&self.key),
            group_replay: HashMap::new(),
            pairwise_scratch: Vec::new(),
        }
    }

    /// Forwards to [`GroupPlane::mint`] — see [`Transport::mint_group_key`]'s
    /// doc (identical rationale and forwarding shape; this is the
    /// `SharedTransport`-side entry point T12's node layer calls).
    pub fn mint_group_key(&self, peers: &[NodeId], now_ns: u64) -> (u16, Vec<HandshakeAction>) {
        let mut ks = self.key.lock().unwrap();
        // Gate activation on the peers we can actually DELIVER to. An `HS_KEY`
        // is sealed pairwise, so a peer with no established session cannot
        // receive it at all and its ack can never come — waiting the full
        // activation timeout for it is waiting for the impossible, and with
        // `DATA`/`HEARTBEAT` both group-scope that wait mutes a fresh leader
        // completely (see `GroupPlane::mint_gated` and
        // `docs/notes/uc2-the-mute-leader.md`). Everyone still gets a delivery;
        // an excluded peer is picked up by `peers_missing_key` once its session
        // exists. Computed under the SAME lock as the mint, so the gate cannot
        // be decided against a session set that changes underneath it.
        let gate_on: Vec<NodeId> = peers
            .iter()
            .copied()
            .filter(|&p| ks.peers.is_established(p))
            .collect();
        ks.group.mint_gated(peers, &gate_on, now_ns)
    }

    /// Forwards to [`RotationState::on_became_leader`] — see
    /// [`Transport::on_became_leader`]'s doc.
    pub fn on_became_leader(&self) {
        self.key.lock().unwrap().rotation.on_became_leader();
    }

    /// Forwards to [`RotationState::on_committed_config`] — see
    /// [`Transport::on_committed_config`]'s doc.
    pub fn on_committed_config(&self, tombstone_count: usize) {
        self.key
            .lock()
            .unwrap()
            .rotation
            .on_committed_config(tombstone_count);
    }

    /// Forwards to [`RotationState::take_due`] — see
    /// [`Transport::rotation_due`]'s doc.
    pub fn rotation_due(&self, now_ns: u64) -> Option<RotationReason> {
        self.key.lock().unwrap().rotation.take_due(now_ns)
    }

    /// Forwards to [`Peers::allowlist_reload_if_stale`] — see
    /// [`Transport::allowlist_reload_if_stale`]'s doc.
    pub fn allowlist_reload_if_stale(&self, now_ns: u64) -> Result<bool, CryptoError> {
        self.key
            .lock()
            .unwrap()
            .peers
            .allowlist_reload_if_stale(now_ns)
    }

    // ---- T11 plan gap: handshake driving was unreachable from outside this
    // crate ---------------------------------------------------------------
    //
    // Neither `Transport` nor `SharedTransport` exposed ANY way for another
    // crate to drive `Peers::initiate`/`Peers::on_message` or
    // `GroupPlane::on_key_message` — every existing forwarder (above) covers
    // the rare admin/rotation events, not the handshake bootstrap itself.
    // `CryptoError::UnsealedKind`'s own doc already says HS_INIT/HS_RESP
    // "must be driven directly via `Peers::initiate`/`Peers::on_message`,
    // never through this facade" — but nothing let a caller outside
    // `uc_crypto` reach those methods to do so. T11 (the receive seam)
    // needs this to route HS_INIT/HS_RESP/HS_KEY (kinds 18-20) anywhere
    // useful, and its own test suite needs it to build two nodes with a
    // real established session + a shared group key (`open_group` requires
    // `Peers::peer_boot_salt`, which only a completed handshake populates —
    // there is no way to fake that from outside this crate either).
    //
    // Same class of gap, and the same "flag prominently and add the pure
    // forwarder" resolution, as T10's `mint_group_key` addition (see that
    // method's doc and the T10 report's "Plan gap found and fixed" section):
    // each of these three methods is a one-line lock-and-forward with zero
    // design surface of its own (the forwarded function, its signature, and
    // its semantics are already reviewed and shipped in `handshake.rs`/
    // `group.rs`), and Task 12's node-level handshake driver will need
    // exactly these calls verbatim regardless of who adds them first.

    /// Forwards to [`Peers::initiate`] — starts (or restarts) a handshake
    /// with `peer`, returning the `HS_INIT` [`HandshakeAction::Send`] (or a
    /// `Failed` action if `peer` is not allowlisted).
    pub fn initiate(&self, peer: NodeId, now_ns: u64) -> Vec<HandshakeAction> {
        self.key.lock().unwrap().peers.initiate(peer, now_ns)
    }

    /// Forwards to [`Peers::on_message`] — feeds a received `HS_INIT`/
    /// `HS_RESP` datagram's body (kinds 18/19, `Scope::Unsealed` — never
    /// opened first) into the handshake state machine.
    pub fn on_handshake_message(
        &self,
        from: NodeId,
        kind: u8,
        body: &[u8],
        now_ns: u64,
    ) -> Vec<HandshakeAction> {
        self.key
            .lock()
            .unwrap()
            .peers
            .on_message(from, kind, body, now_ns)
    }

    /// Forwards to [`Peers::tick`] — the monotonic maintenance tick that
    /// retransmits unanswered `HS_INIT`s with backoff, restarts handshakes
    /// for links the caller asked for that are not up, expires unproven
    /// `pending` sessions, and announces a promoted session as
    /// `Established { confirmed: true }`.
    ///
    /// Added T12 (same pure-forwarder shape and rationale as T11's three
    /// handshake forwarders above): `Peers::tick` was unreachable from
    /// outside this crate, so nothing could ever retransmit a lost `HS_INIT`
    /// — a single dropped handshake datagram would have left that link
    /// permanently down, since `initiate` is idempotent and produces no
    /// traffic once a handshake is in flight.
    pub fn tick(&self, now_ns: u64) -> Vec<HandshakeAction> {
        self.key.lock().unwrap().peers.tick(now_ns)
    }

    /// Whether a pairwise session with `peer` is usable for sealing right
    /// now — forwards to [`Peers::is_established`]. Diagnostics/observability
    /// for the node layer (T12); never a gate on whether to send in the
    /// clear.
    pub fn is_established(&self, peer: NodeId) -> bool {
        self.key.lock().unwrap().peers.is_established(peer)
    }

    /// Seals one **pairwise-scope control datagram** under `peer`'s
    /// established session — the node layer's own seal path, distinct from
    /// [`SendHalf::seal`] (which belongs exclusively to the sender agent).
    ///
    /// Added T12, and it is the reason [`SharedTransport`]'s `counter` field
    /// exists. The node's consensus agent must seal `HS_KEY` deliveries
    /// (`GroupPlane` emits the body and deliberately never touches a socket
    /// or a pairwise key), and T17 extends this path to the node's own
    /// `VOTE`/`REQUEST_VOTE`/`TERM_MAP`/`CONFIG_*` sends. It cannot take a
    /// second [`SendHalf`] to do that — [`SharedTransport::send_half`]
    /// panics on a second call, by design. Locking here is free at these
    /// rates: every kind that routes through this method is rare, unicast
    /// control traffic, exactly the traffic class `Transport`'s carried
    /// requirement #4 already declined to optimize.
    ///
    /// `kind` MUST classify as [`Scope::Pairwise`]; a `Group` kind returns
    /// [`CryptoError::NotPairwiseKind`] rather than being sealed the wrong
    /// way (group sealing belongs on the fan-out path, which owns the
    /// epoch-stamping and the cipher cache), and an `Unsealed` handshake
    /// bootstrap kind returns [`CryptoError::UnsealedKind`] exactly as
    /// [`Transport::seal`] does.
    pub fn seal_pairwise_control(
        &self,
        kind: u8,
        peer: NodeId,
        buf: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        match Transport::scope_of(kind) {
            // The guard this entry point exists for: a fan-out kind reaching
            // the PAIRWISE control path is a routing bug, and refusing it is
            // what keeps `seal_control`'s group branch (below) the only way a
            // group kind can ever be sealed off the sender agent's path.
            Scope::Group => Err(CryptoError::NotPairwiseKind(kind)),
            // T17: one implementation, two entry points — `seal_control`
            // owns the actual seal so the two cannot drift (in particular so
            // they cannot end up drawing from two different counters).
            _ => self.seal_control(kind, Some(peer), buf, 0),
        }
    }

    /// Seals one **control datagram** of ANY scope under this
    /// `SharedTransport` — the node layer's own seal path, and the superset
    /// of [`SharedTransport::seal_pairwise_control`].
    ///
    /// Added T17, by the 2026-07-29 ruling. T12 shipped only the pairwise
    /// branch, which was enough for `HS_KEY`. It is not enough for the node's
    /// OWN consensus sends: `READ_PROBE` and `COMMIT_POSITION` are
    /// [`Scope::Group`] ([`Transport::scope_of`] — they fan out to every
    /// peer), they are emitted by the consensus agent on its own socket, and
    /// that agent **cannot hold a [`SendHalf`]** —
    /// [`SharedTransport::send_half`] is single-call by design and the one
    /// half went to the sender agent. A second half would mean a second nonce
    /// counter starting back at 0 under the same key: a repeated
    /// `(key, nonce)` under AES-256-GCM, which leaks the authentication
    /// subkey for every message ever sealed under that key. So the group seal
    /// is reachable HERE instead, drawing from the same `Arc<AtomicU64>`
    /// every other path draws from — nonce-safety by construction rather than
    /// by a cross-agent protocol.
    ///
    /// Scope dispatch is identical to [`Transport::seal`]'s: `Group` ignores
    /// `peer` (the same sealed bytes go to every destination — that is what
    /// the group key is for) and stamps the sealing epoch into the header;
    /// `Pairwise` requires `Some(peer)` and returns
    /// [`CryptoError::MissingPeer`] otherwise; `Unsealed` is always
    /// [`CryptoError::UnsealedKind`]. `now_ns` is read only by the group
    /// branch (`GroupPlane::sealing_epoch`'s activation grace) and must come
    /// from [`SharedTransport::now_ns`] — see the module section docs' "One
    /// clock source" paragraph.
    pub fn seal_control(
        &self,
        kind: u8,
        peer: Option<NodeId>,
        buf: &mut Vec<u8>,
        now_ns: u64,
    ) -> Result<(), CryptoError> {
        match Transport::scope_of(kind) {
            Scope::Group => self.seal_group_control(buf, now_ns),
            Scope::Pairwise => {
                let Some(peer) = peer else {
                    return Err(CryptoError::MissingPeer(kind));
                };
                let counter = next_counter(&self.counter);
                self.key
                    .lock()
                    .unwrap()
                    .peers
                    .seal_pairwise(peer, buf, counter)
            }
            Scope::Unsealed => Err(CryptoError::UnsealedKind(kind)),
        }
    }

    /// The group branch of [`SharedTransport::seal_control`]. Same three
    /// steps as [`SendHalf::seal_group`] — resolve the sealing epoch, stamp
    /// it into the header, seal under this node's per-epoch-per-boot send key
    /// — and the same T9-review-F3 ordering discipline: **every fallible
    /// lookup resolves before `buf` is mutated at all**, so a failure leaves
    /// the caller's staged datagram byte-for-byte intact rather than
    /// half-built.
    ///
    /// Deliberately does NOT carry [`SendHalf`]'s per-epoch cipher cache. The
    /// cache exists for the measured fan-out hot path — one `Aes256Gcm::new`
    /// (~133 ns) amortized over a whole `DATA` fan-out at M5-gate rates. The
    /// callers here are the consensus agent's `READ_PROBE` (one seal per read
    /// round, ~one per RTT) and `COMMIT_POSITION` gossip (one seal per commit
    /// advance) — each of which seals ONCE and then sends the identical bytes
    /// to every peer, so the per-fan-out construction cost is already
    /// amortized exactly as the cache would amortize it. Caching behind
    /// `&self` would mean putting mutable state in `KeyState` for a saving
    /// the measured bar does not ask for; `transport.rs`'s carried
    /// requirement #4 makes the same call for the pairwise path.
    fn seal_group_control(&self, buf: &mut Vec<u8>, now_ns: u64) -> Result<(), CryptoError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        let mut key = self.key.lock().unwrap();
        let epoch = key
            .group
            .sealing_epoch(now_ns)
            .ok_or(CryptoError::NoGroupKey)?;
        // `sealing_epoch` and `schedule().get(epoch)` CAN disagree (T9 review
        // F3): resolve the key BEFORE touching `buf`.
        let group_key = key
            .group
            .schedule()
            .get(epoch)
            .ok_or(CryptoError::NoGroupKey)?;
        let send_key: Zeroizing<[u8; 32]> =
            Zeroizing::new(derive_send_key(group_key, self.self_id, &self.boot_salt));
        let cipher = Aes256Gcm::new(GenericArray::from_slice(&*send_key));

        let counter = next_counter(&self.counter);
        buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&epoch.to_le_bytes());
        seal_with(buf, &cipher, counter)?;
        key.rotation.on_bytes_sealed(buf.len() as u64);
        Ok(())
    }

    /// Forwards to [`GroupPlane::unacked_peers`] — the peers of the newest
    /// minted epoch that have not acked it yet. Drives the node layer's
    /// `HS_KEY` re-delivery timer (T12).
    pub fn unacked_group_key_peers(&self) -> Vec<NodeId> {
        self.key.lock().unwrap().group.unacked_peers()
    }

    /// Forwards to [`GroupPlane::peers_missing_key`] — which of `targets`
    /// still lack the newest minted epoch, INCLUDING peers that joined after
    /// the mint and so never appear in `unacked_group_key_peers`.
    pub fn group_key_missing_peers(&self, targets: &[NodeId]) -> Vec<NodeId> {
        self.key.lock().unwrap().group.peers_missing_key(targets)
    }

    /// Forwards to [`GroupPlane::redeliver_to`] — re-emits the newest minted
    /// epoch's `HS_KEY` delivery to `peers`, for the node layer to seal and
    /// send again (T12).
    pub fn redeliver_group_key_to(&self, peers: &[NodeId]) -> Vec<HandshakeAction> {
        self.key.lock().unwrap().group.redeliver_to(peers)
    }

    /// Forwards to [`GroupPlane::on_key_message`] — feeds an ALREADY-OPENED
    /// `HS_KEY` body (kind 20, `Scope::Pairwise` — opened via
    /// [`ReceiveHalf::open_slice`]/[`Transport::open`] like any other
    /// pairwise datagram before reaching here) into the group-key plane.
    /// `body` carries either a key delivery or an ack — `GroupPlane` tells
    /// the two apart internally (see its module docs); the returned actions
    /// are the ack to seal and send back, if `body` was a delivery.
    pub fn on_group_key_message(&self, from: NodeId, body: &[u8]) -> Vec<HandshakeAction> {
        self.key.lock().unwrap().group.on_key_message(from, body)
    }
}

/// The send-side half of the [`SharedTransport`] split — see the module
/// section docs above. Exclusive to the ONE agent that calls `seal`.
pub struct SendHalf {
    self_id: NodeId,
    boot_salt: BootSalt,
    base: Instant,
    key: Arc<Mutex<KeyState>>,
    /// The PROCESS's one counter, shared with every other seal path — see
    /// [`SharedTransport`]'s `counter` field doc (T12) for why this is an
    /// `Arc<AtomicU64>` rather than a `u64` owned by this half.
    counter: Arc<AtomicU64>,
    /// One cached cipher for whichever epoch was last sealed under — see
    /// [`Transport::group_seal_cipher`]'s doc (carried requirement #4);
    /// identical caching, just living on this half instead of on `Transport`.
    seal_cache: Option<(u16, Aes256Gcm)>,
}

impl SendHalf {
    /// The canonical crypto clock — see [`SharedTransport::now_ns`]'s doc.
    pub fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Same contract as [`Transport::seal`] — see that method's doc for the
    /// full per-scope account. `Scope::Group` locks the shared [`KeyState`]
    /// once (for `GroupPlane::sealing_epoch` and, on a cache miss, the key
    /// lookup) — see the module section docs' `RotationState::on_bytes_sealed`
    /// paragraph for why this is not an ADDITIONAL lock beyond that one.
    /// `Scope::Pairwise` locks it once for `Peers::seal_pairwise` (unicast,
    /// low-rate control traffic — a lock here was already the design before
    /// this split; see `Transport`'s own carried requirement #4 doc).
    pub fn seal(
        &mut self,
        kind: u8,
        peer: Option<NodeId>,
        buf: &mut Vec<u8>,
        now_ns: u64,
    ) -> Result<(), CryptoError> {
        match Transport::scope_of(kind) {
            Scope::Group => self.seal_group(buf, now_ns),
            Scope::Pairwise => {
                let Some(peer) = peer else {
                    return Err(CryptoError::MissingPeer(kind));
                };
                let counter = next_counter(&self.counter);
                self.key
                    .lock()
                    .unwrap()
                    .peers
                    .seal_pairwise(peer, buf, counter)
            }
            Scope::Unsealed => Err(CryptoError::UnsealedKind(kind)),
        }
    }

    fn seal_group(&mut self, buf: &mut Vec<u8>, now_ns: u64) -> Result<(), CryptoError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        let mut key = self.key.lock().unwrap();
        let epoch = key
            .group
            .sealing_epoch(now_ns)
            .ok_or(CryptoError::NoGroupKey)?;

        // Same ordering discipline as Transport::seal_group (T9 review F3):
        // the fallible cipher/key lookup MUST resolve before `buf` is
        // mutated at all, or a failure here leaves the caller's staged
        // datagram corrupted even though the call "failed".
        let counter = next_counter(&self.counter);
        let cipher = Self::group_seal_cipher(
            &mut self.seal_cache,
            &key.group,
            epoch,
            self.self_id,
            &self.boot_salt,
        )?;

        buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&epoch.to_le_bytes());
        seal_with(buf, cipher, counter)?;
        key.rotation.on_bytes_sealed(buf.len() as u64);
        Ok(())
    }

    /// Returns the cached cipher for `epoch`, rebuilding it (and touching
    /// `group`/the shared lock, already held by the caller) only if the
    /// last cached epoch does not match. See [`Transport::group_seal_cipher`]'s
    /// doc (carried requirement #4) — identical logic, adapted to take its
    /// inputs as parameters instead of `&mut self` fields, since this half's
    /// `self_id`/`boot_salt` are local `Copy` fields and `group` lives behind
    /// the shared lock.
    fn group_seal_cipher<'a>(
        cache: &'a mut Option<(u16, Aes256Gcm)>,
        group: &GroupPlane,
        epoch: u16,
        self_id: NodeId,
        boot_salt: &BootSalt,
    ) -> Result<&'a Aes256Gcm, CryptoError> {
        let stale = !matches!(cache, Some((e, _)) if *e == epoch);
        if stale {
            let group_key = group.schedule().get(epoch).ok_or(CryptoError::NoGroupKey)?;
            let key: Zeroizing<[u8; 32]> =
                Zeroizing::new(derive_send_key(group_key, self_id, boot_salt));
            let cipher = Aes256Gcm::new(GenericArray::from_slice(&*key));
            *cache = Some((epoch, cipher));
        }
        Ok(&cache.as_ref().unwrap().1)
    }
}

/// Allocates the next value from a [`SharedTransport`] family's shared nonce
/// counter (T12) — see that struct's `counter` field doc, including why the
/// legacy [`Transport`]'s own private counter is NOT this one. Starts at 1 and
/// never repeats within the family;
/// `Relaxed` suffices because the only requirement is uniqueness, not any
/// ordering relative to other memory (every `fetch_add` returns a distinct
/// value regardless of ordering).
///
/// A failed seal simply burns its value — see [`Transport::seal`]'s doc;
/// counters need only never repeat, not be dense.
fn next_counter(counter: &AtomicU64) -> u64 {
    let prev = counter.fetch_add(1, Ordering::Relaxed);
    // `fetch_add` wraps silently where the pre-T12 `self.counter += 1` would
    // have panicked in a debug build. Named rather than left implicit: a wrap
    // WOULD repeat every nonce under any key still in use, which is the one
    // thing this counter exists to prevent. It is unreachable in practice —
    // 2^64 seals at the M5 gate's 1.64M/s is ~350,000 years, and a process
    // restart mints a fresh `boot_salt` (a new key space) long before that —
    // so this is a debug-build tripwire for a logic error that reset or
    // corrupted the counter, not a runtime guard against honest exhaustion.
    debug_assert!(
        prev != u64::MAX,
        "the nonce counter wrapped: every nonce under the current keys repeats from here"
    );
    prev.wrapping_add(1)
}

/// The receive-side half of the [`SharedTransport`] split — see the module
/// section docs above. Exclusive to the ONE agent that calls `open`.
pub struct ReceiveHalf {
    base: Instant,
    key: Arc<Mutex<KeyState>>,
    /// Anti-replay state for RECEIVED group-scope traffic — see
    /// [`Transport`]'s `group_replay` field doc for the full rationale
    /// (3-wide key, F1/F6/F7 from T9 review round 1); identical logic, just
    /// living on this half instead of on `Transport`.
    group_replay: HashMap<(NodeId, u16, BootSalt), ReplayWindow>,
    /// Reused scratch buffer for [`ReceiveHalf::open_slice`]'s `Pairwise`
    /// branch — see that method's doc for why this exists at all
    /// (`Peers::open_pairwise` only takes `&mut Vec<u8>`, and this half's
    /// contract with its caller is a persistent, reused receive buffer, not
    /// a fresh allocation per datagram). Cleared and refilled every call,
    /// never shrunk in a way that gives up its allocation — `Vec::clear`
    /// keeps capacity, so after the first few calls settle at the largest
    /// pairwise datagram seen, this steady-states to zero further
    /// allocation.
    pairwise_scratch: Vec<u8>,
}

impl ReceiveHalf {
    /// The canonical crypto clock — see [`SharedTransport::now_ns`]'s doc.
    pub fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Same contract as [`Transport::open`].
    pub fn open(&mut self, from: NodeId, buf: &mut Vec<u8>) -> Result<(), CryptoError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        let Some(header) = read_datagram_header(buf) else {
            return Err(CryptoError::TooShort);
        };
        match Transport::scope_of(header.kind) {
            Scope::Group => self.open_group(from, header.key_epoch, buf),
            Scope::Pairwise => self
                .key
                .lock()
                .unwrap()
                .peers
                .open_pairwise(from, buf)
                .map(|_counter| ()),
            Scope::Unsealed => Err(CryptoError::UnsealedKind(header.kind)),
        }
    }

    /// Same contract and salt-trial behavior as [`Transport::open_group`] —
    /// see that method's doc for the full F1/F2 (T9 review round 1) account.
    fn open_group(
        &mut self,
        from: NodeId,
        epoch: u16,
        buf: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        let mut key = self.key.lock().unwrap();
        if key.group.schedule().get(epoch).is_none() {
            return Err(CryptoError::NoGroupKey);
        }

        let mut last_err = CryptoError::NoSession(from);

        if let Some(salt) = key.peers.peer_boot_salt(from) {
            match Self::open_group_under_salt(&key.group, from, epoch, &salt, buf) {
                Ok(counter) => {
                    drop(key);
                    return Self::finish_group_open(
                        &mut self.group_replay,
                        from,
                        epoch,
                        salt,
                        counter,
                    );
                }
                Err(e) => last_err = e,
            }
        }

        if let Some(salt) = key.peers.peer_pending_boot_salt(from) {
            match Self::open_group_under_salt(&key.group, from, epoch, &salt, buf) {
                Ok(counter) => {
                    key.peers.promote_pending(from);
                    drop(key);
                    return Self::finish_group_open(
                        &mut self.group_replay,
                        from,
                        epoch,
                        salt,
                        counter,
                    );
                }
                Err(e) => last_err = e,
            }
        }

        Err(last_err)
    }

    /// One salt trial — see [`Transport::open_group_under_salt`]'s doc.
    fn open_group_under_salt(
        group: &GroupPlane,
        from: NodeId,
        epoch: u16,
        salt: &BootSalt,
        buf: &mut Vec<u8>,
    ) -> Result<u64, CryptoError> {
        let group_key = group.schedule().get(epoch).ok_or(CryptoError::NoGroupKey)?;
        let key: Zeroizing<[u8; 32]> = Zeroizing::new(derive_send_key(group_key, from, salt));
        open_in_place(buf, &key)
    }

    /// Records `counter` in the `(from, epoch, salt)` replay window — see
    /// [`Transport::finish_group_open`]'s doc (including F7, T9 review round 1).
    fn finish_group_open(
        group_replay: &mut HashMap<(NodeId, u16, BootSalt), ReplayWindow>,
        from: NodeId,
        epoch: u16,
        salt: BootSalt,
        counter: u64,
    ) -> Result<(), CryptoError> {
        let window = group_replay.entry((from, epoch, salt)).or_default();
        if window.check_and_set(counter) {
            Ok(())
        } else {
            Err(CryptoError::Replayed(counter))
        }
    }

    /// T11 (`uc_net`'s receive seam): the zero-copy-on-the-hot-path sibling
    /// of [`ReceiveHalf::open`], for a caller holding a persistent, reused,
    /// oversized receive buffer instead of a right-sized `Vec` — the shape
    /// `uc_net`'s `FollowerReceiver::do_work` already uses (a 64 KiB
    /// `recv_buf`, `recv_from`'d into fresh each duty cycle). [`open`]
    /// requires `buf.len()` to already equal the received datagram's exact
    /// length (see [`crate::seal::open_in_place`]'s invariant); satisfying
    /// that from a fixed 64 KiB buffer means `truncate(n) -> open ->
    /// resize(65536, 0)` — that `resize` memsets up to 64 KiB PER DATAGRAM
    /// (T5 review carry (d); `crate::seal::open_detached`'s doc), the same
    /// order of cost as the AEAD open itself. `open_detached` exists
    /// precisely so a caller never has to pay that: it decrypts in place at
    /// the SAME offsets, no truncation, no resize.
    ///
    /// Takes `buf` (the receive buffer's full backing storage) and `n` (the
    /// exact received length — `recv_from`'s return, NOT `buf.len()`).
    /// Reads and writes only `buf[..n]`; nothing past `n` is ever touched.
    /// On success, returns the length of the now-plaintext datagram, laid
    /// out identically to what `open`/`open_in_place` would have left in a
    /// Vec (`header ++ plaintext`, starting at `buf[0]`) — the caller passes
    /// `&buf[..len]` on to whatever parses a cleartext datagram today.
    ///
    /// `Scope::Group` (the hot path — DATA/HEARTBEAT/COMMIT_POSITION/
    /// READ_PROBE) is genuinely zero-copy: [`open_group_detached`] decrypts
    /// in place via [`open_detached`], then this function does one
    /// `copy_within` to close the 8-byte spent-counter gap `open_detached`
    /// leaves between the header and the plaintext (see that function's
    /// doc) — a move bounded by the PAYLOAD's length, not the buffer's
    /// capacity, and nothing beyond that single move.
    ///
    /// `Scope::Pairwise` (control traffic — NAK/STATUS/VOTE/etc., low rate)
    /// goes through [`ReceiveHalf::pairwise_scratch`] instead: `Peers::
    /// open_pairwise` only takes `&mut Vec<u8>` (no slice-based sibling
    /// exists — unlike the group path, nothing on the pairwise side has
    /// needed one before this), so this copies `buf[..n]` into a reused
    /// scratch `Vec`, opens that, and copies the result back. A real copy,
    /// not zero-copy, but bounded by the datagram's size (never the 64 KiB
    /// buffer) and never a fresh allocation after the scratch vec's
    /// capacity settles — see that field's doc for why this tradeoff is
    /// fine for control-plane traffic specifically (the same reasoning
    /// `Transport`'s own module docs give for not extending cipher caching
    /// to the pairwise send path: unicast, low-rate).
    ///
    /// `Scope::Unsealed` (`HS_INIT`/`HS_RESP`) is refused with
    /// [`CryptoError::UnsealedKind`] — same contract as [`open`]; the caller
    /// must route these to the handshake driver without calling this at
    /// all (see [`SharedTransport::on_handshake_message`]).
    pub fn open_slice(
        &mut self,
        from: NodeId,
        buf: &mut [u8],
        n: usize,
    ) -> Result<usize, CryptoError> {
        if n < DATAGRAM_HEADER_LEN {
            return Err(CryptoError::TooShort);
        }
        let Some(header) = read_datagram_header(&buf[..n]) else {
            return Err(CryptoError::TooShort);
        };
        match Transport::scope_of(header.kind) {
            Scope::Group => {
                let range = self.open_group_detached(from, header.key_epoch, &mut buf[..n])?;
                let total = DATAGRAM_HEADER_LEN + (range.end - range.start);
                buf.copy_within(range, DATAGRAM_HEADER_LEN);
                Ok(total)
            }
            Scope::Pairwise => self.open_pairwise_via_scratch(from, &mut buf[..n]),
            Scope::Unsealed => Err(CryptoError::UnsealedKind(header.kind)),
        }
    }

    /// Slice-based sibling of [`ReceiveHalf::open_group`] — same salt-trial
    /// contract (current, then pending; see that method's doc for the full
    /// F1/F2 account), just calling [`open_detached`] instead of
    /// [`crate::seal::open_in_place`]/[`crate::seal::open_with`] so the
    /// caller's buffer is decrypted in place rather than shrunk. Kept as a
    /// deliberate near-duplicate of `open_group` rather than a generic
    /// refactor of both: `open_group` is already reviewed and shipped
    /// (T9/T10), and entangling its Vec-shrinking contract with this
    /// slice-based one over a shared-code refactor is a bigger, riskier
    /// change than the ~30 lines of duplication costs.
    fn open_group_detached(
        &mut self,
        from: NodeId,
        epoch: u16,
        buf: &mut [u8],
    ) -> Result<Range<usize>, CryptoError> {
        let mut key = self.key.lock().unwrap();
        if key.group.schedule().get(epoch).is_none() {
            return Err(CryptoError::NoGroupKey);
        }

        let mut last_err = CryptoError::NoSession(from);

        if let Some(salt) = key.peers.peer_boot_salt(from) {
            match Self::open_group_detached_under_salt(&key.group, from, epoch, &salt, buf) {
                Ok((counter, range)) => {
                    drop(key);
                    Self::finish_group_open(&mut self.group_replay, from, epoch, salt, counter)?;
                    return Ok(range);
                }
                Err(e) => last_err = e,
            }
        }

        if let Some(salt) = key.peers.peer_pending_boot_salt(from) {
            match Self::open_group_detached_under_salt(&key.group, from, epoch, &salt, buf) {
                Ok((counter, range)) => {
                    key.peers.promote_pending(from);
                    drop(key);
                    Self::finish_group_open(&mut self.group_replay, from, epoch, salt, counter)?;
                    return Ok(range);
                }
                Err(e) => last_err = e,
            }
        }

        Err(last_err)
    }

    /// One salt trial, slice-based — see [`Transport::open_group_under_salt`]'s
    /// doc for the shared rationale.
    fn open_group_detached_under_salt(
        group: &GroupPlane,
        from: NodeId,
        epoch: u16,
        salt: &BootSalt,
        buf: &mut [u8],
    ) -> Result<(u64, Range<usize>), CryptoError> {
        let group_key = group.schedule().get(epoch).ok_or(CryptoError::NoGroupKey)?;
        let key: Zeroizing<[u8; 32]> = Zeroizing::new(derive_send_key(group_key, from, salt));
        open_detached(buf, &key)
    }

    /// `Scope::Pairwise` branch of [`ReceiveHalf::open_slice`] — see that
    /// method's doc and [`ReceiveHalf::pairwise_scratch`]'s field doc for
    /// why this copies rather than decrypting truly in place.
    fn open_pairwise_via_scratch(
        &mut self,
        from: NodeId,
        buf: &mut [u8],
    ) -> Result<usize, CryptoError> {
        self.pairwise_scratch.clear();
        self.pairwise_scratch.extend_from_slice(buf);
        {
            let mut key = self.key.lock().unwrap();
            key.peers.open_pairwise(from, &mut self.pairwise_scratch)?;
        }
        let len = self.pairwise_scratch.len();
        buf[..len].copy_from_slice(&self.pairwise_scratch);
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handshake::HandshakeAction;
    use uc_protocol::v2::crypto::read_counter;
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
            DGRAM_KIND_SNAP_TABLE,
            DGRAM_KIND_CONFIG_PROPOSAL,
            DGRAM_KIND_CONFIG_REPLY,
        ] {
            assert_eq!(Transport::scope_of(k), Scope::Pairwise, "kind {k}");
        }
    }

    #[test]
    fn every_wire_kind_has_an_assigned_scope() {
        // Guards against a future kind silently defaulting to the wrong scope.
        //
        // Review round 1 (F4): the brief-mandated form of this test —
        // `for k in 1..=17 { let _ = scope_of(k); }` — discards its return
        // value and can only ever catch a PANIC, never a wrong
        // classification (every kind here is ALSO covered, correctly, by
        // `fan_out_kinds_take_the_group_key_and_the_rest_are_pairwise`, so
        // this test was never adding coverage beyond "does not panic").
        // Strengthened in place to actually assert, rather than adding a
        // second near-duplicate test.
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
            DGRAM_KIND_SNAP_TABLE,
            DGRAM_KIND_CONFIG_PROPOSAL,
            DGRAM_KIND_CONFIG_REPLY,
        ] {
            assert_eq!(Transport::scope_of(k), Scope::Pairwise, "kind {k}");
        }
        // The full 1..=17 sweep still runs too, now checked against the same
        // partition instead of merely not-panicking.
        for k in 1..=DGRAM_KIND_CONFIG_REPLY {
            let expect_group = matches!(
                k,
                DGRAM_KIND_DATA
                    | DGRAM_KIND_HEARTBEAT
                    | DGRAM_KIND_COMMIT_POSITION
                    | DGRAM_KIND_READ_PROBE
            );
            let want = if expect_group {
                Scope::Group
            } else {
                Scope::Pairwise
            };
            assert_eq!(Transport::scope_of(k), want, "kind {k}");
        }
    }

    #[test]
    fn handshake_bootstrap_kinds_are_unsealed_hs_key_is_pairwise() {
        // F4: HS_INIT(18)/HS_RESP(19) must never be classified Pairwise (or
        // Group) — no session exists yet for them, they ARE the bootstrap.
        // HS_KEY(20) rides an ALREADY-established pairwise channel and stays
        // Pairwise. Before this fix, all three fell through `scope_of`'s
        // catch-all to `Pairwise` — HS_KEY correctly by luck, HS_INIT/HS_RESP
        // for the wrong reason (failed closed only because no session
        // exists yet, not because the design says so).
        assert_eq!(Transport::scope_of(DGRAM_KIND_HS_INIT), Scope::Unsealed);
        assert_eq!(Transport::scope_of(DGRAM_KIND_HS_RESP), Scope::Unsealed);
        assert_eq!(Transport::scope_of(DGRAM_KIND_HS_KEY), Scope::Pairwise);
    }

    #[test]
    fn seal_and_open_refuse_unsealed_kinds_rather_than_attempting_anything() {
        let mut t = node_transport("unsealed-kind-seal", 1, PRIV_SOLO, &[]);
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: 0,
                kind: DGRAM_KIND_HS_INIT,
                flags: 0,
                key_epoch: 0,
            },
        );
        assert!(matches!(
            t.seal(DGRAM_KIND_HS_INIT, None, &mut d, 0),
            Err(CryptoError::UnsealedKind(k)) if k == DGRAM_KIND_HS_INIT
        ));
        assert!(matches!(
            t.open(2, &mut d),
            Err(CryptoError::UnsealedKind(k)) if k == DGRAM_KIND_HS_INIT
        ));
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
                    .join("../target/uc_crypto_tests")
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
    /// not anything transport.rs invents). Runs until the exchange quiesces.
    /// Does NOT assert anything about the resulting session state — a
    /// second handshake against a peer that already has a `current` session
    /// (the restart scenario) legitimately lands in `pending`, not
    /// `current`; see [`establish`] for the common "first-ever handshake"
    /// case that DOES assert.
    fn pump_handshake(a: &mut Transport, b: &mut Transport, mut acts: Vec<HandshakeAction>) {
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
    }

    /// [`pump_handshake`] plus the assertion that both sides landed on
    /// `current` — the right check for a peer's FIRST-EVER handshake, wrong
    /// for a second one against an already-established peer (see
    /// [`pump_handshake`]'s doc).
    fn establish(a: &mut Transport, b: &mut Transport, acts: Vec<HandshakeAction>) {
        let (a_id, b_id) = (a.self_id, b.self_id);
        pump_handshake(a, b, acts);
        assert!(a.peers.is_established(b_id), "a failed to establish with b");
        assert!(b.peers.is_established(a_id), "b failed to establish with a");
    }

    /// Drives a `GroupPlane::mint`'s `HS_KEY` delivery actions to
    /// completion: feeds each delivery to `follower`, and each resulting ack
    /// back to `leader`, activating the epoch once the mint's peer set is
    /// fully acked. Bypasses the pairwise AEAD layer entirely (feeds
    /// `GroupPlane::on_key_message` directly rather than sealing/opening
    /// over the peers' pairwise session) — `group.rs`'s own tests establish
    /// that this module's job (`GroupPlane`'s mint/ack/activate state
    /// machine) is correct in isolation; this helper only needs it to KEY
    /// synchronization, not to re-prove that layer.
    fn deliver_group_key(
        leader: &mut Transport,
        follower: &mut Transport,
        actions: Vec<HandshakeAction>,
    ) {
        let (leader_id, follower_id) = (leader.self_id, follower.self_id);
        for act in actions {
            let HandshakeAction::Send { to, body, .. } = act else {
                panic!("mint must emit a Send action")
            };
            assert_eq!(to, follower_id);
            let reply = follower.group.on_key_message(leader_id, &body);
            for r in reply {
                let HandshakeAction::Send { body: rbody, .. } = r else {
                    panic!("a well-formed delivery must ack back")
                };
                leader.group.on_key_message(follower_id, &rbody);
            }
        }
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
    fn seal_group_fails_closed_when_sealing_epoch_names_an_epoch_evicted_from_the_schedule() {
        // F3 (review round 1): sealing_epoch() and schedule().get(epoch) can
        // disagree. Reproduction: mint e0 with no peers (activates
        // immediately once folded); mint e1 with a peer that never acks
        // (folds e0 into active_epoch, since e0 HAD activated); mint e2 with
        // a peer that never acks EITHER, before e1 ever activates — e1 is
        // dropped unactivated, and KeySchedule's 2-deep window evicts e0 (the
        // schedule now only holds {e1, e2}). active_epoch is untouched by an
        // unactivated fold, so it STILL names e0 — which sealing_epoch falls
        // back to (e2 hasn't activated yet either), and schedule().get(e0)
        // is now None.
        //
        // `e0`/`e1`/`e2` are LABELS for "1st/2nd/3rd mint", not literal
        // epoch numbers: `GroupPlane::new` now starts `next_epoch` at 1 (0
        // is reserved as the wire's cleartext sentinel — see its doc), so
        // e0/e1/e2 are epochs 1/2/3, not 0/1/2.
        let mut t = node_transport("evicted-epoch-seal", 1, PRIV_SOLO, &[]);
        t.group.mint(&[], 0); // e0 = epoch 1: vacuous peers
        t.group.mint(&[9], 1); // e1 = epoch 2: folds e0 (activated) into active_epoch; peer 9 never acks
        t.group.mint(&[9], 2); // e2 = epoch 3: e1 never activated -> dropped; schedule evicts e0; active_epoch still names e0
        assert_eq!(
            t.group.sealing_epoch(2),
            Some(1),
            "fixture must reproduce sealing_epoch naming a stale active_epoch (e0 = epoch 1)"
        );
        assert!(
            t.group.schedule().get(1).is_none(),
            "e0 (epoch 1) must actually be evicted from the 2-deep schedule for this test to mean anything"
        );

        // A non-zero sentinel key_epoch, so ANY write to the header field —
        // even one that coincidentally writes the "correct" value 0 — is
        // observable as a buffer change. data_datagram()'s default of 0
        // would make a stamp-then-fail bug invisible here (the exact
        // near-miss disclosed in the original task report for mutant 3).
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: 0,
                kind: DGRAM_KIND_DATA,
                flags: 0,
                key_epoch: 0xBEEF,
            },
        );
        d.extend_from_slice(b"payload");
        let before = d.clone();

        assert!(matches!(
            t.seal(DGRAM_KIND_DATA, None, &mut d, 2),
            Err(CryptoError::NoGroupKey)
        ));
        assert_eq!(
            d, before,
            "an evicted-epoch seal failure must leave the buffer completely untouched, \
             not just the sealing_epoch()==None path the earlier test covers"
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
        deliver_group_key(&mut a, &mut b, key_actions);
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
        // `open` does NOT restore the header's `key_epoch` to whatever the
        // caller staged before sealing -- `seal_group` permanently stamps
        // the REAL epoch it sealed under (the receiver needs to see which
        // epoch to trust), so a byte-exact whole-buffer comparison against
        // `plain` (whose `key_epoch` is the zero-init default) is only ever
        // true if the epoch under test happens to BE 0 -- exactly the
        // fixture trap this file's own tests name repeatedly elsewhere
        // (`seal_group_stamps_the_chosen_epoch_into_the_header`,
        // `shared_transport_group_scope_round_trips_through_send_and_receive_halves`).
        // `GroupPlane`'s first-ever mint is now epoch 1, not 0 (0 is
        // reserved as the wire's cleartext sentinel — see `GroupPlane::new`'s
        // doc), so this single-mint fixture no longer coincides with that
        // trap by accident; assert the epoch explicitly rather than lean on
        // it staying that way.
        assert_eq!(
            read_datagram_header(&d).unwrap().key_epoch,
            epoch,
            "the header keeps the REAL epoch after open, not whatever was staged before seal"
        );
        assert_eq!(
            &d[DATAGRAM_HEADER_LEN..],
            &plain[DATAGRAM_HEADER_LEN..],
            "payload is byte-exact after the round trip"
        );
        assert_eq!(
            &d[..OFF_DGRAM_KEY_EPOCH],
            &plain[..OFF_DGRAM_KEY_EPOCH],
            "every header field OTHER than key_epoch is unchanged"
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
        deliver_group_key(&mut a, &mut b, key_actions);

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
    fn on_became_leader_and_on_committed_config_are_reachable_and_drive_rotation_due() {
        // F5: before this fix, RotationState::on_became_leader and
        // on_committed_config had no Transport method to call them through —
        // rotation_due could only ever return Periodic. This proves both
        // event methods are wired all the way through to the pure decision.
        let mut leader = node_transport("f5-became-leader", 1, PRIV_SOLO, &[]);
        leader.on_became_leader();
        assert_eq!(leader.rotation_due(0), Some(RotationReason::BecameLeader));
        assert_eq!(leader.rotation_due(0), None, "consumed exactly once");

        let mut removal = node_transport("f5-committed-config", 1, PRIV_SOLO, &[]);
        removal.on_committed_config(0); // baseline observation, not a trigger
        assert_eq!(removal.rotation_due(0), None);
        removal.on_committed_config(1); // tombstone count grew -> Removal
        assert_eq!(
            removal.rotation_due(0),
            Some(RotationReason::Removal),
            "on_committed_config must be reachable — it is the ONLY path to \
             the security-relevant Removal trigger (rotation.rs)"
        );
    }

    #[test]
    fn sealing_a_pairwise_kind_with_no_peer_returns_missingpeer_not_a_panic() {
        // Adjudicated in review round 1: the original `.expect(...)` panic
        // was reachability-safe (no attacker-controlled path reaches this),
        // but it defeats scope_of's OWN stated design rationale — a future
        // fan-out kind added to uc_net without updating scope_of would
        // degrade from "missed optimization" (the catch-all's whole point)
        // to "node crash". A Result closes that gap structurally.
        let mut t = node_transport("missing-peer", 1, PRIV_SOLO, &[]);
        let mut d = vote_datagram();
        assert!(matches!(
            t.seal(DGRAM_KIND_VOTE, None, &mut d, 0),
            Err(CryptoError::MissingPeer(k)) if k == DGRAM_KIND_VOTE
        ));
    }

    #[test]
    fn group_traffic_from_a_restarted_leader_opens_correctly_not_replayed_not_authfailed() {
        // Reproduces F1 + F2 together, with a REAL restart: two entirely
        // separate `Transport` instances for the SAME node id (`l1`, `l2`,
        // matching a real process crash+restart — same on-disk identity key,
        // fresh `OsRng` boot salt, fresh `GroupPlane` per `Transport::new`),
        // driven against ONE persistent follower `f` whose in-memory state
        // (session table, group_replay) survives across both.
        //
        // F1: `GroupPlane::next_epoch` always starts at 0 on a fresh
        // process, so `l2`'s first mint recurs `l1`'s first epoch number.
        // F2: `f` already has a `current` pairwise session with node 1 (from
        // `l1`); `l2`'s fresh handshake lands in `f`'s `pending` (WireGuard-
        // style) until proven adopted — and `l2`, playing the leader, sends
        // ONLY group-scope traffic here, never anything pairwise.
        let mut f = node_transport(
            "restart-f",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );

        // --- l's first lifetime ---
        let mut l1 = node_transport(
            "restart-l1",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let acts = l1.peers.initiate(2, 0);
        establish(&mut l1, &mut f, acts);
        let (epoch1, key_actions) = l1.group.mint(&[2], 0);
        deliver_group_key(&mut l1, &mut f, key_actions);
        // `sealing_epoch` is only meaningful on the MINTING side (l1) — f
        // only installs the key into its schedule on delivery, it never
        // tracks pending/activation for a key it merely received.
        assert_eq!(l1.group.sealing_epoch(0), Some(epoch1));
        assert!(
            f.group.schedule().get(epoch1).is_some(),
            "f must have installed the delivered key"
        );

        for _ in 0..3 {
            let mut d = data_datagram();
            l1.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
            f.open(1, &mut d)
                .expect("l1's traffic opens fine before the restart");
        }

        // --- l "restarts": brand-new Transport, same identity (same
        // PRIV_A), fresh boot salt (Transport::new mints one from OsRng
        // every call, exactly like a real restart), fresh GroupPlane
        // (next_epoch back at 0). ---
        let mut l2 = node_transport(
            "restart-l2",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let acts = l2.peers.initiate(2, 0);
        // Deliberately NOT `establish`: f's `is_established(1)` is ALREADY
        // true (against the STALE l1 session) before this pump even runs,
        // so asserting it here would pass for the wrong reason. l2's fresh
        // handshake lands in f's `pending`, not `current`.
        pump_handshake(&mut l2, &mut f, acts);
        assert!(
            f.peers.peer_pending_boot_salt(1).is_some(),
            "fixture must land l2's session in f's pending slot, not replace current"
        );
        assert_ne!(
            f.peers.peer_boot_salt(1),
            f.peers.peer_pending_boot_salt(1),
            "l1's (current) and l2's (pending) boot salts must actually differ for this test to mean anything"
        );

        let (epoch2, key_actions) = l2.group.mint(&[2], 0);
        assert_eq!(
            epoch2, epoch1,
            "GroupPlane::next_epoch restarts at 0 on every fresh process — the epoch number recurs"
        );
        deliver_group_key(&mut l2, &mut f, key_actions);

        let mut d = data_datagram();
        l2.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap(); // l2's own counter starts at 1 again too
        assert!(
            f.open(1, &mut d).is_ok(),
            "group traffic from a restarted peer must open — neither AuthFailed \
             (F2: stale current-session salt) nor Replayed (F1: replay window \
             keyed only by (sender, epoch), colliding with the recurring epoch number)"
        );

        // The promotion side-effect: a group-scope success under the
        // pending salt must promote it exactly as a pairwise success would —
        // `current` now reports l2's salt, and `pending` is empty again.
        assert_eq!(
            f.peers.peer_boot_salt(1),
            Some(l2.peers.boot_salt()),
            "current must now be l2's session, not l1's stale one"
        );
        assert!(
            f.peers.peer_pending_boot_salt(1).is_none(),
            "pending must be cleared once promoted"
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

    // ------------------------------------------------- M8 ownership split
    // (SharedTransport / SendHalf / ReceiveHalf, T10 review round 1 fix,
    // 2026-07-29). These mirror Transport's own end-to-end tests above —
    // the point is to prove the split is BEHAVIORALLY IDENTICAL to the
    // monolithic facade it was carved out of, not merely that it compiles.

    /// [`SharedTransport`] sibling of [`node_transport`] — same fixture
    /// discipline (real key material under `CARGO_TARGET_TMPDIR`, per-tag
    /// scratch dir so parallel tests never race on the same file).
    fn shared_node_transport(
        tag: &str,
        self_id: NodeId,
        private: [u8; 32],
        allow: &[(NodeId, [u8; 32])],
    ) -> SharedTransport {
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
        SharedTransport::new(&cfg, self_id).unwrap().unwrap()
    }

    /// Drives a real Noise IK handshake between two [`SharedTransport`]s'
    /// shared `Peers` directly (same shape as [`pump_handshake`]/[`establish`]
    /// above, adapted for the `Arc<Mutex<KeyState>>` each locks into
    /// independently — this is legitimate here even though `SharedTransport`
    /// is `Clone`+`Arc`-backed: `a` and `b` are two DIFFERENT nodes' state in
    /// this test, not two handles to the SAME node).
    fn shared_establish(a: &SharedTransport, b: &SharedTransport) {
        let (a_id, b_id) = (a.self_id(), b.self_id());
        let mut acts = {
            let mut ak = a.key.lock().unwrap();
            ak.peers.initiate(b_id, 0)
        };
        for _ in 0..8 {
            let mut next = Vec::new();
            for act in acts.drain(..) {
                if let HandshakeAction::Send { to, kind, body } = act {
                    if to == b_id {
                        next.extend(b.key.lock().unwrap().peers.on_message(a_id, kind, &body, 0));
                    } else if to == a_id {
                        next.extend(a.key.lock().unwrap().peers.on_message(b_id, kind, &body, 0));
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            acts = next;
        }
        assert!(
            a.key.lock().unwrap().peers.is_established(b_id),
            "a failed to establish with b"
        );
        assert!(
            b.key.lock().unwrap().peers.is_established(a_id),
            "b failed to establish with a"
        );
    }

    /// Mints a group key on `leader` and delivers it to `follower` — same
    /// shape as [`deliver_group_key`] above, bypassing the pairwise AEAD
    /// layer (this only needs to key-synchronize `GroupPlane`, not re-prove
    /// the handshake layer).
    fn shared_deliver_group_key(
        leader: &SharedTransport,
        follower: &SharedTransport,
        peers: &[NodeId],
    ) -> u16 {
        let (leader_id, follower_id) = (leader.self_id(), follower.self_id());
        let (epoch, actions) = leader.mint_group_key(peers, 0);
        for act in actions {
            let HandshakeAction::Send { to, body, .. } = act else {
                panic!("mint must emit a Send action")
            };
            assert_eq!(to, follower_id);
            let reply = follower
                .key
                .lock()
                .unwrap()
                .group
                .on_key_message(leader_id, &body);
            for r in reply {
                let HandshakeAction::Send { body: rbody, .. } = r else {
                    panic!("a well-formed delivery must ack back")
                };
                leader
                    .key
                    .lock()
                    .unwrap()
                    .group
                    .on_key_message(follower_id, &rbody);
            }
        }
        epoch
    }

    #[test]
    fn shared_transport_group_scope_round_trips_through_send_and_receive_halves() {
        // The core proof this correction actually works: a SendHalf derived
        // from the LEADER's SharedTransport seals, a ReceiveHalf derived
        // from the FOLLOWER's SharedTransport opens — through the split
        // public API only (send_half/receive_half), not by reaching into
        // KeyState. Mirrors Transport's own
        // group_scope_round_trips_end_to_end_through_seal_and_open.
        let leader = shared_node_transport(
            "split-group-e2e-leader",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let follower = shared_node_transport(
            "split-group-e2e-follower",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        shared_establish(&leader, &follower);
        // A vacuous throwaway mint first: GroupPlane::next_epoch starts at 0
        // on a fresh process, so the FIRST-EVER mint's epoch is 0 —
        // indistinguishable from a header's zero-initialized key_epoch field
        // (same trap `seal_group_stamps_the_chosen_epoch_into_the_header`
        // names on Transport directly; hit for real here on first run).
        let _ = leader.mint_group_key(&[], 0);
        let epoch = shared_deliver_group_key(&leader, &follower, &[2]);
        assert_ne!(
            epoch, 0,
            "fixture should not accidentally observe the zero-init epoch"
        );

        let mut send = leader.send_half();
        let mut recv = follower.receive_half();

        let mut d = data_datagram();
        let plain = d.clone();
        send.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        assert_ne!(d, plain, "sealing must actually change the buffer");
        recv.open(1, &mut d)
            .expect("follower's ReceiveHalf must open what leader's SendHalf sealed");
        // `open` does NOT restore the header's `key_epoch` to whatever the
        // caller staged before sealing -- `seal_group` permanently stamps
        // the REAL epoch it sealed under (by design: the receiver needs to
        // see which epoch to trust), and open never rewrites it. Comparing
        // the WHOLE buffer against `plain` (which used the zero-init
        // default) would only pass by the SAME "mint #1 is epoch 0"
        // coincidence `seal_group_stamps_the_chosen_epoch_into_the_header`
        // guards against directly -- caught here for real by minting a
        // second (non-zero) epoch first. Check the epoch explicitly, then
        // the rest of the buffer.
        assert_eq!(
            read_datagram_header(&d).unwrap().key_epoch,
            epoch,
            "the header keeps the REAL epoch after open, not whatever was staged before seal"
        );
        assert_eq!(
            &d[DATAGRAM_HEADER_LEN..],
            &plain[DATAGRAM_HEADER_LEN..],
            "payload is byte-exact after the round trip"
        );
        assert_eq!(
            &d[..OFF_DGRAM_KEY_EPOCH],
            &plain[..OFF_DGRAM_KEY_EPOCH],
            "every header field OTHER than key_epoch is unchanged"
        );
    }

    #[test]
    fn shared_transport_pairwise_scope_round_trips_through_send_and_receive_halves() {
        let a = shared_node_transport(
            "split-pairwise-e2e-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let b = shared_node_transport(
            "split-pairwise-e2e-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        shared_establish(&a, &b);

        let mut send = a.send_half();
        let mut recv = b.receive_half();

        let mut d = vote_datagram();
        let plain = d.clone();
        send.seal(DGRAM_KIND_VOTE, Some(2), &mut d, 0).unwrap();
        assert_ne!(d, plain);
        recv.open(1, &mut d)
            .expect("b's ReceiveHalf must open what a's SendHalf sealed");
        assert_eq!(d, plain);
    }

    #[test]
    fn send_half_and_receive_half_agree_on_now_ns_because_they_share_one_clock() {
        // The "one clock source" requirement, pinned structurally: both
        // halves derive `now_ns()` from the SAME `SharedTransport::base`, so
        // calling both back-to-back must yield near-identical values — NOT
        // two independently-started `Instant`s (which this fixture cannot
        // literally simulate without sleeping, but the shared-origin
        // property IS directly checkable: both must be small, close, and
        // monotonically consistent with a SINGLE elapsed-since-construction
        // clock rather than each starting back near zero independently).
        let t = shared_node_transport("shared-clock", 1, PRIV_SOLO, &[]);
        let send = t.send_half();
        let recv = t.receive_half();
        let (n1, n2) = (send.now_ns(), recv.now_ns());
        // Both computed from the identical `base`, microseconds apart at
        // most (this line of code, not a network hop) -- an independent
        // per-half Instant::now() origin would instead read close to ZERO
        // on EACH call (since each would have JUST started), making this
        // assertion vacuously true either way; the discriminating check is
        // that `t.now_ns()` (the canonical source) also agrees, monotonically.
        let t_ns = t.now_ns();
        assert!(
            n2 >= n1,
            "receive half's clock must not run behind the send half's"
        );
        assert!(
            t_ns >= n2,
            "SharedTransport's own now_ns must not run behind either half's"
        );
        assert!(
            t_ns - n1 < 50_000_000,
            "all three readings must be close together (same origin), not independently-started clocks: {n1} vs {t_ns}"
        );
    }

    // ---- Round-2 review fix (2026-07-29): send_half/receive_half must be
    // enforced single-call, not merely documented — a second SendHalf would
    // start its nonce counter back at 0 under the SAME group key.

    #[test]
    #[should_panic(expected = "send_half called more than once")]
    fn a_second_send_half_from_the_same_shared_transport_panics() {
        let t = shared_node_transport("second-send-half-same", 1, PRIV_SOLO, &[]);
        let _first = t.send_half();
        let _second = t.send_half(); // must panic: would nonce-collide with `_first`
    }

    #[test]
    #[should_panic(expected = "send_half called more than once")]
    fn a_second_send_half_via_a_clone_of_shared_transport_also_panics() {
        // The whole point: SharedTransport is Clone, so the enforcement
        // cannot be a plain (non-Arc) field on the struct — it must be
        // shared across every clone, exactly like `key` is. A holder of a
        // CLONE, not the original, is the realistic way a second SendHalf
        // would ever get minted (e.g. two components of the node layer each
        // holding their own clone, one of them wrongly assuming it owns
        // "the" send half).
        let t = shared_node_transport("second-send-half-clone", 1, PRIV_SOLO, &[]);
        let clone = t.clone();
        let _first = t.send_half();
        let _second = clone.send_half(); // must panic: same underlying counter state
    }

    #[test]
    #[should_panic(expected = "receive_half called more than once")]
    fn a_second_receive_half_from_the_same_shared_transport_panics() {
        let t = shared_node_transport("second-receive-half-same", 1, PRIV_SOLO, &[]);
        let _first = t.receive_half();
        let _second = t.receive_half(); // must panic: independent replay windows would diverge
    }

    #[test]
    fn send_half_and_receive_half_are_independent_single_call_budgets() {
        // Calling send_half() must not consume receive_half()'s allowance,
        // or vice versa -- they are two SEPARATE flags, not one shared "any
        // half taken" bit. Both succeed once each on the SAME SharedTransport.
        let t = shared_node_transport("independent-budgets", 1, PRIV_SOLO, &[]);
        let _send = t.send_half();
        let _recv = t.receive_half(); // must NOT panic
    }

    // ---- T11: `open_slice` (the zero-copy-on-the-hot-path receive entry
    // point `uc_net`'s `FollowerReceiver` calls) and the three handshake-
    // driving forwarders (`initiate`/`on_handshake_message`/
    // `on_group_key_message`) T11's own receiver test suite needs, since
    // nothing outside this crate could reach `Peers`/`GroupPlane` before
    // this task — see the forwarders' doc comments for the full account.

    #[test]
    fn open_slice_group_scope_matches_open_and_touches_nothing_past_n() {
        let leader = shared_node_transport(
            "open-slice-group-leader",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let follower = shared_node_transport(
            "open-slice-group-follower",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        shared_establish(&leader, &follower);
        let _ = leader.mint_group_key(&[], 0); // vacuous mint 0 -- see epoch-0 trap note above
        let epoch = shared_deliver_group_key(&leader, &follower, &[2]);
        assert_ne!(epoch, 0);

        let mut send = leader.send_half();
        // ONE receive half for both opens below -- two DIFFERENT sealed
        // datagrams (the send half's counter is monotonic per call, so no
        // replay collision), letting `open` and `open_slice` be compared
        // against the SAME key/session/replay state instead of building a
        // second independent (and therefore differently-keyed) node.
        let mut recv = follower.receive_half();

        // Reference: a sealed datagram opened the already-reviewed way.
        let mut d1 = data_datagram();
        send.seal(DGRAM_KIND_DATA, None, &mut d1, 0).unwrap();
        recv.open(1, &mut d1).expect("Vec-based open must succeed");
        let want = d1; // header ++ plaintext, per `open`'s own contract

        // Same content, a fresh counter, opened via `open_slice` instead --
        // into an oversized buffer, sentinel-filled PAST `n` so touching
        // anything past the real datagram length is directly observable.
        let mut d2 = data_datagram();
        send.seal(DGRAM_KIND_DATA, None, &mut d2, 0).unwrap();
        let n = d2.len();
        let mut oversized = vec![0xEEu8; 256];
        oversized[..n].copy_from_slice(&d2);
        let sentinel_tail = oversized[n..].to_vec();

        let len = recv
            .open_slice(1, &mut oversized, n)
            .expect("slice-based open_slice must succeed identically");
        assert_eq!(
            &oversized[..len],
            &want[..],
            "open_slice output matches open's Vec output exactly"
        );
        assert_eq!(
            oversized[n..],
            sentinel_tail[..],
            "nothing past n was ever touched"
        );
        assert_eq!(
            oversized.len(),
            256,
            "the buffer's own length is never resized -- it is a slice call"
        );
    }

    #[test]
    fn open_slice_pairwise_scope_round_trips() {
        let a = shared_node_transport(
            "open-slice-pairwise-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let b = shared_node_transport(
            "open-slice-pairwise-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        shared_establish(&a, &b);

        let mut send = a.send_half();
        let mut recv = b.receive_half();

        let mut d = vote_datagram();
        let plain = d.clone();
        send.seal(DGRAM_KIND_VOTE, Some(2), &mut d, 0).unwrap();
        let n = d.len();

        let mut buf = vec![0x33u8; 128];
        buf[..n].copy_from_slice(&d);
        let len = recv
            .open_slice(1, &mut buf, n)
            .expect("pairwise open_slice must succeed");
        assert_eq!(
            &buf[..len],
            &plain[..],
            "pairwise open_slice round-trips byte-exact, like open"
        );
    }

    #[test]
    fn open_slice_refuses_unsealed_kinds_rather_than_attempting_anything() {
        let b = shared_node_transport("open-slice-unsealed", 2, PRIV_B, &[]);
        let mut recv = b.receive_half();
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: 0,
                kind: DGRAM_KIND_HS_INIT,
                flags: 0,
                key_epoch: 0,
            },
        );
        let n = d.len();
        assert!(matches!(
            recv.open_slice(1, &mut d, n),
            Err(CryptoError::UnsealedKind(k)) if k == DGRAM_KIND_HS_INIT
        ));
    }

    #[test]
    fn open_slice_never_panics_on_truncated_or_random_input() {
        // The untrusted-input contract, at the seam T11 owns: anyone who can
        // reach the UDP port controls `n` and every byte in `buf`.
        let b = shared_node_transport("open-slice-truncated", 2, PRIV_B, &[]);
        let mut recv = b.receive_half();
        for n in [0usize, 1, 15, 16, 17, 39, 40, 1500] {
            let mut buf = vec![0xABu8; n.max(1500)];
            let _ = recv.open_slice(1, &mut buf, n); // must not panic, whatever it returns
        }
    }

    #[test]
    fn shared_transport_handshake_and_group_key_forwarders_drive_a_real_session() {
        // Unlike `shared_establish`/`shared_deliver_group_key` above (which
        // reach into the private `key` field -- legitimate for THIS crate's
        // own tests), this test uses ONLY the public forwarders T11 needs
        // from OUTSIDE the crate: `initiate`, `on_handshake_message`,
        // `on_group_key_message`. Proves those three are sufficient, on
        // their own, to stand up a real session + shared group key and then
        // round-trip a sealed datagram through `send_half`/`receive_half`.
        let a = shared_node_transport(
            "fwd-handshake-a",
            1,
            PRIV_A,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );
        let b = shared_node_transport(
            "fwd-handshake-b",
            2,
            PRIV_B,
            &[(1, public_of(PRIV_A)), (2, public_of(PRIV_B))],
        );

        let mut acts = a.initiate(2, 0);
        for _ in 0..8 {
            let mut next = Vec::new();
            for act in acts.drain(..) {
                if let HandshakeAction::Send { to, kind, body } = act {
                    if to == 2 {
                        next.extend(b.on_handshake_message(1, kind, &body, 0));
                    } else if to == 1 {
                        next.extend(a.on_handshake_message(2, kind, &body, 0));
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            acts = next;
        }

        let _ = a.mint_group_key(&[], 0); // vacuous first mint -- epoch-0 trap
        let (epoch, mint_acts) = a.mint_group_key(&[2], 0);
        assert_ne!(epoch, 0);
        for act in mint_acts {
            let HandshakeAction::Send { to, body, .. } = act else {
                panic!("mint must emit a Send action")
            };
            assert_eq!(to, 2);
            let reply = b.on_group_key_message(1, &body);
            for r in reply {
                let HandshakeAction::Send { body: rbody, .. } = r else {
                    panic!("a well-formed delivery must ack back")
                };
                a.on_group_key_message(2, &rbody);
            }
        }

        let mut send = a.send_half();
        let mut recv = b.receive_half();
        let mut d = data_datagram();
        let plain = d.clone();
        send.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
        assert_ne!(d, plain);
        recv.open(1, &mut d)
            .expect("a session + group key built ENTIRELY through the pub forwarders must open");
        assert_eq!(&d[DATAGRAM_HEADER_LEN..], &plain[DATAGRAM_HEADER_LEN..]);
    }

    // ---- M8 Task 12: the node layer's own seal path -----------------------
    /// A staged (cleartext header + payload) datagram of an arbitrary kind —
    /// `data_datagram`'s generalization, for the T12 control-path tests.
    fn staged(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut v,
            &DatagramHeader {
                position: 4096,
                leadership_term_id: 3,
                kind,
                flags: 0,
                key_epoch: 0,
            },
        );
        v.extend_from_slice(payload);
        v
    }

    /// The counter is per-PROCESS, not per-`SendHalf`. Before T12 it lived by
    /// value inside `SendHalf`, which was sound only while that half was the
    /// process's sole sealer. The node layer now seals `HS_KEY` (and, from
    /// T17, its own consensus datagrams) through
    /// `seal_pairwise_control` — under the SAME per-peer session key the
    /// sender's `SendHalf` uses for pairwise kinds. Two independent counters
    /// both starting at 0 would repeat a `(key, nonce)` pair under
    /// AES-256-GCM: not a wrong answer, an authentication-subkey compromise
    /// for every message ever sealed under that key.
    ///
    /// Discriminating: the two paths are interleaved deliberately, and the
    /// assertion is on the counters actually stamped on the wire. Give
    /// `SendHalf` its own `u64` again and the two sequences both start at 1.
    #[test]
    fn the_nonce_counter_is_shared_across_every_seal_path_in_the_process() {
        let a_pub = public_of(PRIV_A);
        let b_pub = public_of(PRIV_B);
        let a = shared_node_transport("t12-counter-a-t", 1, PRIV_A, &[(2, b_pub)]);
        let b = shared_node_transport("t12-counter-b-t", 2, PRIV_B, &[(1, a_pub)]);
        shared_establish(&a, &b);
        shared_deliver_group_key(&a, &b, &[2]);

        let mut send = a.send_half();
        let mut seen = Vec::new();
        for _ in 0..4 {
            // Sender agent's path (group scope, the fan-out hot path).
            let mut d = staged(DGRAM_KIND_DATA, b"xx");
            send.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
            seen.push(read_counter(&d[DATAGRAM_HEADER_LEN..]));
            // Node layer's path (pairwise scope, control traffic).
            let mut c = staged(DGRAM_KIND_HS_KEY, b"yy");
            a.seal_pairwise_control(DGRAM_KIND_HS_KEY, 2, &mut c)
                .unwrap();
            seen.push(read_counter(&c[DATAGRAM_HEADER_LEN..]));
        }
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            seen.len(),
            "a counter repeated across the two paths: {seen:?}"
        );
        assert_eq!(
            seen,
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            "one interleaved sequence, no gaps"
        );
    }

    /// A control-path seal is a REAL seal: the peer's `ReceiveHalf` opens it,
    /// and the plaintext is not on the wire.
    #[test]
    fn a_control_path_seal_opens_on_the_peers_receive_half() {
        let a_pub = public_of(PRIV_A);
        let b_pub = public_of(PRIV_B);
        let a = shared_node_transport("t12-ctrl-a-t", 1, PRIV_A, &[(2, b_pub)]);
        let b = shared_node_transport("t12-ctrl-b-t", 2, PRIV_B, &[(1, a_pub)]);
        shared_establish(&a, &b);

        let mut d = staged(DGRAM_KIND_HS_KEY, b"group-key-body");
        a.seal_pairwise_control(DGRAM_KIND_HS_KEY, 2, &mut d)
            .unwrap();
        assert!(
            !d.windows(14).any(|w| w == b"group-key-body"),
            "the body must not be readable on the wire"
        );
        let mut recv = b.receive_half();
        recv.open(1, &mut d)
            .expect("the peer opens it under the same session");
        assert_eq!(&d[DATAGRAM_HEADER_LEN..], b"group-key-body");
    }

    /// The scope guard: a fan-out kind must not be sealed through the control
    /// path (it would bypass the epoch stamping and the cipher cache), and a
    /// bootstrap kind has nothing to seal under.
    #[test]
    fn the_control_seal_path_refuses_group_and_bootstrap_kinds() {
        let a = shared_node_transport("t12-scope-guard", 1, PRIV_A, &[]);
        let mut d = staged(DGRAM_KIND_DATA, b"x");
        assert!(matches!(
            a.seal_pairwise_control(DGRAM_KIND_DATA, 2, &mut d),
            Err(CryptoError::NotPairwiseKind(DGRAM_KIND_DATA))
        ));
        let mut d = staged(DGRAM_KIND_HS_INIT, b"x");
        assert!(matches!(
            a.seal_pairwise_control(DGRAM_KIND_HS_INIT, 2, &mut d),
            Err(CryptoError::UnsealedKind(DGRAM_KIND_HS_INIT))
        ));
    }

    // ---- M8 Task 17: the GROUP branch of the control seal path ------------
    //
    // `READ_PROBE` and `COMMIT_POSITION` are `Scope::Group` and are emitted by
    // the node's consensus agent on its own socket — not by the sender agent.
    // The consensus agent cannot hold a `SendHalf` (`send_half` is single-call
    // by design, and the one half went to the sender agent), so the group seal
    // has to be reachable from `SharedTransport` itself. Ruling 2026-07-29.

    /// A group-scope control seal is a REAL group seal: it stamps the sealing
    /// epoch into the header, hides the payload, and the peer's `ReceiveHalf`
    /// opens it through the ordinary group path (same key derivation, same
    /// replay window) — no separate wire dialect for the node layer.
    #[test]
    fn a_group_control_seal_opens_on_the_peers_receive_half() {
        let a_pub = public_of(PRIV_A);
        let b_pub = public_of(PRIV_B);
        let a = shared_node_transport("t17-group-ctrl-a", 1, PRIV_A, &[(2, b_pub)]);
        let b = shared_node_transport("t17-group-ctrl-b", 2, PRIV_B, &[(1, a_pub)]);
        shared_establish(&a, &b);
        let epoch = shared_deliver_group_key(&a, &b, &[2]);
        assert_ne!(epoch, 0, "epoch 0 is the cleartext sentinel");

        let mut d = staged(DGRAM_KIND_READ_PROBE, b"read-probe-body");
        let now = a.now_ns();
        a.seal_control(DGRAM_KIND_READ_PROBE, None, &mut d, now)
            .expect("a group kind seals on the control path");
        assert!(
            !d.windows(15).any(|w| w == b"read-probe-body"),
            "the body must not be readable on the wire"
        );
        assert_eq!(
            read_datagram_header(&d).unwrap().key_epoch,
            epoch,
            "the group branch must stamp the sealing epoch into the header"
        );
        let mut recv = b.receive_half();
        recv.open(1, &mut d)
            .expect("the peer opens it on the group path");
        assert_eq!(&d[DATAGRAM_HEADER_LEN..], b"read-probe-body");
    }

    /// The whole reason this lives on `SharedTransport` instead of a second
    /// `SendHalf`: every seal path in the process must draw from ONE counter.
    /// Interleaves all three — the sender agent's group fan-out, the node
    /// layer's group control seal, and the node layer's pairwise control seal
    /// — and asserts one gapless sequence on the wire.
    #[test]
    fn the_group_control_path_draws_from_the_same_process_counter() {
        let a_pub = public_of(PRIV_A);
        let b_pub = public_of(PRIV_B);
        let a = shared_node_transport("t17-counter-a", 1, PRIV_A, &[(2, b_pub)]);
        let b = shared_node_transport("t17-counter-b", 2, PRIV_B, &[(1, a_pub)]);
        shared_establish(&a, &b);
        shared_deliver_group_key(&a, &b, &[2]);

        let mut send = a.send_half();
        let mut seen = Vec::new();
        for _ in 0..3 {
            let mut d = staged(DGRAM_KIND_DATA, b"xx");
            send.seal(DGRAM_KIND_DATA, None, &mut d, 0).unwrap();
            seen.push(read_counter(&d[DATAGRAM_HEADER_LEN..]));

            let mut g = staged(DGRAM_KIND_COMMIT_POSITION, b"");
            a.seal_control(DGRAM_KIND_COMMIT_POSITION, None, &mut g, a.now_ns())
                .unwrap();
            seen.push(read_counter(&g[DATAGRAM_HEADER_LEN..]));

            let mut c = staged(DGRAM_KIND_VOTE, b"yy");
            a.seal_pairwise_control(DGRAM_KIND_VOTE, 2, &mut c).unwrap();
            seen.push(read_counter(&c[DATAGRAM_HEADER_LEN..]));
        }
        assert_eq!(
            seen,
            (1..=9).collect::<Vec<u64>>(),
            "one interleaved sequence across all three seal paths, no repeats: {seen:?}"
        );
    }

    /// `seal_control` is scope-dispatched, exactly like `Transport::seal`: a
    /// pairwise kind still needs a peer, a bootstrap kind is still refused,
    /// and a group kind ignores whatever peer it is handed (the same sealed
    /// bytes go to every destination — that is what the group key is FOR).
    #[test]
    fn seal_control_dispatches_by_scope_like_transport_seal() {
        let a_pub = public_of(PRIV_A);
        let b_pub = public_of(PRIV_B);
        let a = shared_node_transport("t17-dispatch-a", 1, PRIV_A, &[(2, b_pub)]);
        let b = shared_node_transport("t17-dispatch-b", 2, PRIV_B, &[(1, a_pub)]);
        shared_establish(&a, &b);
        shared_deliver_group_key(&a, &b, &[2]);

        // Pairwise with no peer: refused, not sealed under some default.
        let mut d = staged(DGRAM_KIND_VOTE, b"x");
        assert!(matches!(
            a.seal_control(DGRAM_KIND_VOTE, None, &mut d, 0),
            Err(CryptoError::MissingPeer(DGRAM_KIND_VOTE))
        ));
        // Bootstrap: nothing to seal under.
        let mut d = staged(DGRAM_KIND_HS_INIT, b"x");
        assert!(matches!(
            a.seal_control(DGRAM_KIND_HS_INIT, None, &mut d, 0),
            Err(CryptoError::UnsealedKind(DGRAM_KIND_HS_INIT))
        ));
        // Group with a peer named: the peer is ignored, and the bytes are the
        // same bytes any other destination would get.
        let mut with_peer = staged(DGRAM_KIND_READ_PROBE, b"probe");
        let mut without = staged(DGRAM_KIND_READ_PROBE, b"probe");
        a.seal_control(DGRAM_KIND_READ_PROBE, Some(2), &mut with_peer, a.now_ns())
            .unwrap();
        a.seal_control(DGRAM_KIND_READ_PROBE, Some(9), &mut without, a.now_ns())
            .unwrap();
        assert_eq!(
            read_datagram_header(&with_peer).unwrap().key_epoch,
            read_datagram_header(&without).unwrap().key_epoch,
            "a group seal must not vary with the destination"
        );
        // Node 9 does not exist; a pairwise seal would have failed `NoSession`.
    }

    /// Fails CLOSED before any group epoch has activated — never a cleartext
    /// send, and `buf` is left untouched so a caller that ignores the error
    /// cannot ship a half-mutated datagram (the same ordering discipline
    /// `SendHalf::seal_group` carries from T9 review F3).
    #[test]
    fn group_control_seal_before_any_epoch_fails_closed_and_leaves_buf_untouched() {
        let a = shared_node_transport("t17-no-group-key", 1, PRIV_A, &[]);
        let mut d = staged(DGRAM_KIND_COMMIT_POSITION, b"payload");
        let before = d.clone();
        assert!(matches!(
            a.seal_control(DGRAM_KIND_COMMIT_POSITION, None, &mut d, a.now_ns()),
            Err(CryptoError::NoGroupKey)
        ));
        assert_eq!(
            d, before,
            "a failed seal must not mutate the staged datagram"
        );
    }
}
