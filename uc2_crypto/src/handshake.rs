// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The Noise `IK` handshake driver (spec §5) and the per-peer session state
//! everything else in this crate hangs off.
//!
//! [`Peers`] is a **driven transition function**, the `ElectionSm` shape: feed
//! it a message ([`Peers::on_message`]), an intent ([`Peers::initiate`]), or a
//! monotonic tick ([`Peers::tick`]), and it hands back [`HandshakeAction`]s for
//! the caller to execute. No `async`, no sockets, no clock reads — time enters
//! only as `now_ns`. That is what lets `uc2_sim` (T13) adjudicate the handshake
//! deterministically under loss, reorder, and partition, and it is a hard
//! requirement rather than a stylistic one.
//!
//! # Why `IK`
//!
//! `IK`'s precondition is that the initiator already knows the responder's
//! static public key — which is exactly what the allowlist is. That buys 1-RTT
//! establishment. Authentication is implicit in the pattern's DH operations;
//! there is no signature layer here and none should be added.
//!
//! Note the asymmetry the responder side has to respect: the `IK` **responder**
//! does *not* know the initiator's static key in advance. It learns it from
//! message 1 (`e, es, s, ss`), decrypts it, and only then computes `ss`. So an
//! impostor holding *any* valid X25519 key pair produces a message 1 that
//! decrypts perfectly — nothing in the pattern fails. Binding the peer to the
//! allowlist is therefore an explicit application check
//! (`get_remote_static()` vs [`Allowlist::lookup`]) that this module performs
//! on both sides; without it the allowlist would be decorative on the
//! responder side and the whole identity model hollow.
//!
//! # Noise supplies the key exchange, not the record layer
//!
//! At the end of the handshake UC takes the two 32-byte transport keys with
//! `dangerously_get_raw_split()` and seals pairwise datagrams with
//! [`crate::seal`]'s envelope — the same one the group-key path uses. This is
//! deliberate (ruling 2026-07-28, spec §5), not a shortcut:
//!
//! **snow's transport modes hard-code empty associated data.**
//! `StatelessTransportState::write_message` calls `StatelessCipherState::encrypt`,
//! which is `encrypt_ad(nonce, &[], plaintext, out)` (snow-0.10.0
//! `cipherstate.rs:170`) — the Noise spec gives transport messages no AD. Using
//! them would have left the 16-byte v2 header **unauthenticated on the pairwise
//! path** while the group path authenticates it. That is not cosmetic:
//! `DGRAM_KIND_APPEND_POSITION` is a pairwise, *header-only* kind whose entire
//! semantic content is the header, and `uc2_net`'s receiver reads a follower's
//! durable position straight out of `h.position` into the leader's commit
//! ranking. An on-path attacker could inflate it on an otherwise-valid
//! datagram and drive the leader to commit over a range no quorum holds — the
//! Finding #6b acked-write-loss class, reintroduced through the transport.
//!
//! Taking the raw split keeps every property the "use snow's transport mode"
//! instruction was protecting — UC owns the nonce counter, forward secrecy is
//! retained (the keys still come from the ephemeral DH) — and adds uniformity:
//! one envelope and one reviewed AEAD path for both key scopes. The
//! `dangerously_` in the name is snow warning that the caller then owns nonce
//! management. UC owns it structurally, not by discipline: keys are
//! per-sender-per-boot ([`crate::schedule::derive_send_key`] for the group
//! scope; a fresh ephemeral DH per handshake for this one), so a counter reset
//! after a restart lands in a brand-new key space and can never repeat a
//! `(key, nonce)` pair.
//!
//! # Simultaneous open, and the trap after it
//!
//! Both peers may initiate at once. The race is resolved by **lower node id**:
//! a `HS_INIT` arriving while we are mid-handshake as initiator is ignored if
//! our id is lower (the peer, being higher, will drop its own attempt and
//! answer ours), and if our id is higher we drop ours and answer theirs. One
//! session survives, both sides on it.
//!
//! The subtler case is a `HS_INIT` arriving when a session is **already up**,
//! and it is where a naive "discard mine and respond" silently breaks the
//! link. `IK` is 1-RTT, so the two roles have different knowledge at
//! completion:
//!
//! - the **initiator** completes on reading message 2, and *knows* the
//!   responder completed (the responder only emits message 2 after finishing);
//! - the **responder** completes on writing message 2, and knows *nothing*
//!   about whether it arrived.
//!
//! So a responder that replaces a live session on every `HS_INIT` can be made
//! to throw away a working link by a delayed duplicate — or by an attacker
//! replaying a captured message 1, which needs no key material at all. It
//! would then seal under a session the peer never adopted: a link that looks
//! established from both ends and silently drops everything. Ignoring
//! `HS_INIT` while up avoids that but black-holes the link forever after a
//! legitimate peer restart, which is worse.
//!
//! [`PeerEntry`] therefore holds up to two sessions, WireGuard-style:
//!
//! - completing as **initiator** installs `current` directly (confirmed);
//! - completing as **responder** while `current` is live installs `pending`
//!   instead. [`Peers::seal_pairwise`] keeps using `current`;
//!   [`Peers::open_pairwise`] tries `current` then `pending`, and a successful
//!   open under `pending` — the peer *proving* it is using the new session —
//!   promotes it.
//!
//! A replayed `HS_INIT` then costs one DH and a discarded `pending`, never a
//! working link, and a restarted peer converges as soon as it sends anything
//! (in UC, `APPEND_POSITION` continuously). `pending` carries a TTL so stale
//! state cannot be pinned indefinitely.

use crate::identity::{Allowlist, Identity};
use crate::replay::ReplayWindow;
use crate::schedule::BootSalt;
use crate::seal::{open_in_place, seal_in_place};
use crate::{CryptoError, NodeId};
use std::collections::HashMap;
use uc_protocol::v2::crypto::{DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_RESP};
use zeroize::{Zeroize, Zeroizing};

/// The one pattern this crate speaks. `25519` matches [`Identity`]'s X25519
/// static keys; `AESGCM` matches [`crate::seal`], so the binary carries one
/// AEAD implementation rather than two.
const NOISE_PATTERN: &str = "Noise_IK_25519_AESGCM_SHA256";

/// Handshake payload: `node_id` (u32 LE, every other v2 field's byte order)
/// followed by the sender's 16-byte boot salt. Fixed length — a payload of any
/// other size is refused rather than parsed leniently.
const HS_PAYLOAD_LEN: usize = 4 + 16;

/// Scratch for a handshake message or its decrypted payload. `IK` message 1 is
/// 32 (`e`) + 48 (`s` + tag) + 36 (payload + tag) = 116 bytes and message 2 is
/// 32 + 36 = 68, so this is ~4x headroom; it also bounds how large a payload an
/// attacker can make us decrypt (snow returns `Error::Decrypt` rather than
/// panicking when the output buffer is too small — `cipherstate.rs:50`).
const HS_BUF_LEN: usize = 512;

/// Retransmit backoff for an unanswered message 1: 200 ms doubling to a 2 s
/// ceiling. Never gives up — a peer that is down must re-establish the moment
/// it returns, and a cluster link has no "stop trying" state.
const HS_RETRY_BASE_NS: u64 = 200_000_000;
const HS_RETRY_MAX_NS: u64 = 2_000_000_000;
const HS_RETRY_MAX_SHIFT: u32 = 4;

/// How long an unproven `pending` session is retained before [`Peers::tick`]
/// drops it. Long enough for a restarted peer to produce its first datagram,
/// short enough that stale state cannot be pinned indefinitely.
const PENDING_TTL_NS: u64 = 30_000_000_000;

/// What the caller must do next. The driver never touches a socket itself.
#[derive(Debug)]
pub enum HandshakeAction {
    /// Send `body` to `to` as datagram kind `kind` (18 or 19).
    Send { to: NodeId, kind: u8, body: Vec<u8> },
    /// A handshake with `peer` completed, and `boot_salt` is **the peer's**
    /// salt — the input to that peer's group-key derivation
    /// ([`crate::schedule::derive_send_key`]). Learning the peer's salt is the
    /// point of the payload exchange; our own is already known.
    ///
    /// `confirmed` says whether this session is **in force for sealing**:
    ///
    /// - `true` — it is `current`. [`Peers::seal_pairwise`] uses it now, and
    ///   [`Peers::peer_boot_salt`] returns this salt.
    /// - `false` — it is parked as `pending` because a live session already
    ///   exists (we completed as *responder*, which in a 1-RTT pattern proves
    ///   nothing about whether our message 2 arrived — see the module docs).
    ///   The peer's *group*-sealed fan-out already uses this salt, so the
    ///   caller needs it; the *pairwise* path does not switch until the peer
    ///   proves it is using the new session, at which point this action is
    ///   emitted again with `confirmed: true`.
    ///
    /// The flag is a field rather than a doc note deliberately: a caller that
    /// caches the salt for the pairwise path without noticing the distinction
    /// gets a bug that only surfaces after a peer restart. Destructuring is
    /// forced to acknowledge it.
    Established {
        peer: NodeId,
        boot_salt: BootSalt,
        confirmed: bool,
    },
    /// The handshake with `peer` was refused. `reason` is for operator logs; it
    /// is never sent on the wire, so it may name the failed check.
    Failed { peer: NodeId, reason: &'static str },
}

/// An in-flight handshake we started.
struct Initiating {
    state: Box<snow::HandshakeState>,
    /// Message 1, cached verbatim so [`Peers::tick`] can retransmit it without
    /// a fresh DH. Re-sending the identical bytes also keeps the peer on one
    /// responder session instead of manufacturing a new one per retry.
    msg1: Vec<u8>,
}

/// One established pairwise session: the two directional keys, the peer's boot
/// salt, and the replay window that belongs to this key (a window is only
/// meaningful per key — a new session starts a new counter space).
struct Session {
    seal_key: Zeroizing<[u8; 32]>,
    open_key: Zeroizing<[u8; 32]>,
    boot_salt: BootSalt,
    replay: ReplayWindow,
    installed_ns: u64,
}

impl Session {
    fn from_finished_handshake(
        state: &mut snow::HandshakeState,
        initiator: bool,
        boot_salt: BootSalt,
        now_ns: u64,
    ) -> Session {
        // snow's split yields (initiator->responder, responder->initiator) —
        // the same order its own transport state indexes by role
        // (`stateless_transportstate.rs:69` picks `cipherstates.0` when
        // initiator). Each side seals with its egress key and opens with the
        // other.
        let (i_to_r, r_to_i) = split_keys(state);
        let (seal_key, open_key) = if initiator {
            (i_to_r, r_to_i)
        } else {
            (r_to_i, i_to_r)
        };
        Session {
            seal_key,
            open_key,
            boot_salt,
            replay: ReplayWindow::new(),
            installed_ns: now_ns,
        }
    }
}

/// Takes the finished handshake's two transport keys.
///
/// `dangerously_get_raw_split` hands back bare `[u8; 32]`s by value, so one
/// un-wrapped copy is unavoidable; it is bound to a `mut` local and scrubbed
/// explicitly before this function returns, so nothing un-zeroized outlives the
/// call. Same hazard `identity.rs` documents for the private key and
/// `schedule.rs` for `derive_send_key`'s return: arrays are `Copy`, so wrapping
/// a value does not scrub the slot it was copied from.
///
/// **The scrub covers OUR copy only.** snow keeps its own copy of the same two
/// keys in `HandshakeState::cipherstates` (the split runs there first) and
/// snow 0.10 implements `Zeroize` nowhere, so dropping the `HandshakeState`
/// leaves that copy in freed memory regardless of what this function does.
/// Upstream's to fix; recorded here so nobody reads this scrub as a guarantee
/// that no unwrapped copy of the transport keys exists in the process.
fn split_keys(state: &mut snow::HandshakeState) -> (Zeroizing<[u8; 32]>, Zeroizing<[u8; 32]>) {
    let mut raw = state.dangerously_get_raw_split();
    let keys = (Zeroizing::new(raw.0), Zeroizing::new(raw.1));
    raw.0.zeroize();
    raw.1.zeroize();
    keys
}

/// Everything known about one peer link.
#[derive(Default)]
struct PeerEntry {
    /// The caller asked for this link ([`Peers::initiate`]). Kept set for the
    /// lifetime of the process so [`Peers::tick`] re-attempts after any failure
    /// — including "not in the allowlist yet", which is exactly the M7
    /// add-a-node-at-runtime case once the operator drops the key in.
    desired: bool,
    hs: Option<Initiating>,
    /// Attempt counter and clock for the backoff, held on the ENTRY rather than
    /// on `hs`: a `start_initiator` that fails (the peer's key has not landed
    /// in the allowlist yet) leaves no `hs` behind, and gating only the
    /// retransmit would leave that failure path ungated. UC's agents busy-spin,
    /// so "ungated" means a `Failed` action and a heap allocation per poll
    /// iteration, not per second.
    attempts: u32,
    last_attempt_ns: u64,
    /// Set by [`Peers::open_pairwise`] when a `pending` session is promoted, so
    /// the next [`Peers::tick`] can announce it. The promotion happens on the
    /// receive path, which returns a `Result`, not actions.
    promoted: bool,
    /// The session used for sealing, and tried first for opening.
    current: Option<Session>,
    /// A session we completed as responder while `current` was already live.
    /// Not used for sealing until the peer proves it is using it — see the
    /// module docs.
    pending: Option<Session>,
}

/// The handshake driver and per-peer session store.
pub struct Peers {
    identity: Identity,
    allowlist: Allowlist,
    self_id: NodeId,
    boot_salt: BootSalt,
    peers: HashMap<NodeId, PeerEntry>,
}

impl Peers {
    pub fn new(
        identity: Identity,
        allowlist: Allowlist,
        self_id: NodeId,
        boot_salt: BootSalt,
    ) -> Peers {
        Peers {
            identity,
            allowlist,
            self_id,
            boot_salt,
            peers: HashMap::new(),
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// **Our** boot salt — the one we advertise. A peer's salt arrives via
    /// [`HandshakeAction::Established`].
    pub fn boot_salt(&self) -> BootSalt {
        self.boot_salt
    }

    /// Whether a pairwise session with `peer` is usable for sealing right now.
    pub fn is_established(&self, peer: NodeId) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|entry| entry.current.is_some())
    }

    /// The boot salt of the session currently in use with `peer` — the input to
    /// *that peer's* group-key derivation
    /// ([`crate::schedule::derive_send_key`]). The same value arrives in
    /// [`HandshakeAction::Established`]; this is the pull-side view for a
    /// caller (T7/T9) that would otherwise have to mirror the driver's session
    /// bookkeeping to know which salt is live after a peer restart or a
    /// `pending` promotion.
    ///
    /// This reports the salt of the **pairwise session in force**. A restarted
    /// peer's *group*-sealed fan-out switches salt the moment it restarts,
    /// which is what [`HandshakeAction::Established`] announces immediately —
    /// so T7/T9 must not treat this accessor as the only salt they need during
    /// a changeover.
    pub fn peer_boot_salt(&self, peer: NodeId) -> Option<BootSalt> {
        self.peers
            .get(&peer)
            .and_then(|entry| entry.current.as_ref())
            .map(|session| session.boot_salt)
    }

    /// Asks for a link to `peer`. Idempotent: a second call while a handshake
    /// is in flight, or while a session is up, produces no traffic. The intent
    /// is remembered, so [`Peers::tick`] keeps retrying if this attempt cannot
    /// be made now.
    pub fn initiate(&mut self, peer: NodeId, now_ns: u64) -> Vec<HandshakeAction> {
        if peer == self.self_id {
            return vec![fail(peer, "cannot handshake with ourselves")];
        }
        let entry = self.peers.entry(peer).or_default();
        entry.desired = true;
        if entry.current.is_some() || entry.hs.is_some() {
            return Vec::new();
        }
        self.start_initiator(peer, now_ns)
    }

    /// Feeds in a received handshake datagram body. `from` is the sender id the
    /// transport demultiplexed; it is treated as a *claim* until the pattern
    /// and the allowlist agree with it.
    ///
    /// Every failure path here returns [`HandshakeAction::Failed`]. Nothing in
    /// this function may panic on `body`: it is the first thing in the process
    /// to see bytes from anyone who can reach the UDP port.
    pub fn on_message(
        &mut self,
        from: NodeId,
        kind: u8,
        body: &[u8],
        now_ns: u64,
    ) -> Vec<HandshakeAction> {
        if from == self.self_id {
            return vec![fail(from, "datagram claims our own node id")];
        }
        match kind {
            DGRAM_KIND_HS_INIT => self.on_init(from, body, now_ns),
            DGRAM_KIND_HS_RESP => self.on_resp(from, body, now_ns),
            // `DGRAM_KIND_HS_KEY` (20) rides this same pairwise channel and is
            // T7's (group-key distribution); anything else is not ours. Both
            // are dropped silently rather than reported as a handshake failure.
            _ => Vec::new(),
        }
    }

    /// Monotonic tick: retransmits unanswered message 1s with backoff, restarts
    /// handshakes for links the caller asked for but that are not up, expires
    /// unproven `pending` sessions, and gives the allowlist its chance to
    /// notice an operator edit (that call is self-rate-limited to once a second
    /// and touches no disk before the gate passes).
    pub fn tick(&mut self, now_ns: u64) -> Vec<HandshakeAction> {
        // A read error (file briefly absent mid-edit, permissions) leaves the
        // in-memory entries in place; refusing every peer because a file read
        // blipped would be a far worse failure mode than running a second
        // stale.
        let _ = self.allowlist.reload_if_stale(now_ns);

        let mut actions = Vec::new();
        let mut restart: Vec<NodeId> = Vec::new();

        for (peer, entry) in self.peers.iter_mut() {
            if entry
                .pending
                .as_ref()
                .is_some_and(|s| now_ns.saturating_sub(s.installed_ns) >= PENDING_TTL_NS)
            {
                entry.pending = None;
            }

            if entry.promoted {
                entry.promoted = false;
                if let Some(session) = entry.current.as_ref() {
                    actions.push(HandshakeAction::Established {
                        peer: *peer,
                        boot_salt: session.boot_salt,
                        confirmed: true,
                    });
                }
            }

            // ONE clock for both the retransmit and the failure path. See
            // `PeerEntry::attempts`.
            if now_ns.saturating_sub(entry.last_attempt_ns) < retry_delay_ns(entry.attempts) {
                continue;
            }
            match entry.hs.as_ref() {
                Some(hs) => {
                    entry.attempts = entry.attempts.saturating_add(1);
                    entry.last_attempt_ns = now_ns;
                    actions.push(HandshakeAction::Send {
                        to: *peer,
                        kind: DGRAM_KIND_HS_INIT,
                        body: hs.msg1.clone(),
                    });
                }
                None => {
                    if entry.desired && entry.current.is_none() {
                        restart.push(*peer);
                    }
                }
            }
        }

        for peer in restart {
            actions.extend(self.start_initiator(peer, now_ns));
        }
        actions
    }

    /// Seals a staged outgoing datagram (cleartext v2 header followed by the
    /// payload) under the pairwise session with `peer`, via
    /// [`crate::seal::seal_in_place`] — so the header is authenticated as
    /// associated data, exactly as on the group-key path.
    ///
    /// `counter` is the caller's per-sender monotonic nonce counter. It must
    /// not repeat under one session; sessions never share a key, so a counter
    /// reset across a re-handshake is safe.
    pub fn seal_pairwise(
        &mut self,
        peer: NodeId,
        buf: &mut Vec<u8>,
        counter: u64,
    ) -> Result<(), CryptoError> {
        let session = self
            .peers
            .get(&peer)
            .and_then(|entry| entry.current.as_ref())
            .ok_or(CryptoError::NoSession(peer))?;
        seal_in_place(buf, &session.seal_key, counter)
    }

    /// Opens a received datagram sealed by `peer`, returning its counter.
    ///
    /// Tries `current` first, then any `pending` session; a successful open
    /// under `pending` is the peer proving it has adopted the new session, and
    /// promotes it (see the module docs). `open_in_place` leaves `buf`
    /// byte-for-byte unchanged when it fails — aes-gcm verifies the tag
    /// *before* applying the keystream (`aes-gcm-0.10 lib.rs:305`) — so the
    /// second attempt sees the same bytes and no defensive copy is needed on
    /// the receive path.
    ///
    /// The session's replay window is consulted here: a counter already seen
    /// under this key is [`CryptoError::Replayed`], not a successful open. AEAD
    /// stops forgery but not replay, and a replayed `VOTE` or admin datagram is
    /// not harmless.
    pub fn open_pairwise(&mut self, peer: NodeId, buf: &mut Vec<u8>) -> Result<u64, CryptoError> {
        let entry = self
            .peers
            .get_mut(&peer)
            .ok_or(CryptoError::NoSession(peer))?;
        let mut last_err = CryptoError::NoSession(peer);

        if let Some(session) = entry.current.as_mut() {
            match open_in_place(buf, &session.open_key) {
                Ok(counter) => return replay_check(session, counter),
                Err(err) => last_err = err,
            }
        }

        if let Some(session) = entry.pending.as_mut() {
            match open_in_place(buf, &session.open_key) {
                Ok(counter) => {
                    let result = replay_check(session, counter);
                    if result.is_ok() {
                        entry.current = entry.pending.take();
                        // The next `tick` announces this as
                        // `Established { confirmed: true }` — the seal path and
                        // `peer_boot_salt` have just switched, and a caller that
                        // only saw the `confirmed: false` action needs to know.
                        entry.promoted = true;
                    }
                    return result;
                }
                Err(err) => last_err = err,
            }
        }

        Err(last_err)
    }

    /// Builds and sends message 1. Also the retry entry point, so it always
    /// replaces any previous in-flight handshake state for the peer.
    fn start_initiator(&mut self, peer: NodeId, now_ns: u64) -> Vec<HandshakeAction> {
        let Peers {
            identity,
            allowlist,
            self_id,
            boot_salt,
            peers,
        } = self;

        // Stamp the attempt BEFORE anything that can fail, so every outcome —
        // sent, refused by the allowlist, refused by snow — lands on the same
        // backoff clock. `tick` gates on this.
        {
            let entry = peers.entry(peer).or_default();
            entry.attempts = entry.attempts.saturating_add(1);
            entry.last_attempt_ns = now_ns;
        }

        let Some(peer_public) = authorized_key(allowlist, peer, now_ns) else {
            return vec![fail(peer, "peer is not in the allowlist")];
        };

        let mut state = match build_initiator(identity, &peer_public) {
            Ok(state) => state,
            Err(reason) => return vec![fail(peer, reason)],
        };

        let mut buf = [0u8; HS_BUF_LEN];
        let Ok(len) = state.write_message(&encode_payload(*self_id, boot_salt), &mut buf) else {
            return vec![fail(peer, "could not write handshake message 1")];
        };
        let msg1 = buf[..len].to_vec();

        let entry = peers.entry(peer).or_default();
        entry.hs = Some(Initiating {
            state: Box::new(state),
            msg1: msg1.clone(),
        });

        vec![HandshakeAction::Send {
            to: peer,
            kind: DGRAM_KIND_HS_INIT,
            body: msg1,
        }]
    }

    fn on_init(&mut self, from: NodeId, body: &[u8], now_ns: u64) -> Vec<HandshakeAction> {
        let Peers {
            identity,
            allowlist,
            self_id,
            boot_salt,
            peers,
        } = self;

        let Some(expected_public) = authorized_key(allowlist, from, now_ns) else {
            return vec![fail(from, "peer is not in the allowlist")];
        };

        // Simultaneous open. Lower id wins: it keeps its own attempt and drops
        // the peer's message 1 on the floor, because the peer (being higher) is
        // about to drop its own attempt and answer ours. Deterministic, so the
        // race converges on exactly one session rather than two.
        //
        // NOTHING IS MUTATED HERE. At this point `body` is unauthenticated —
        // `from` is a claim the transport made, and the pattern has not run.
        // Tearing our own in-flight handshake down on the strength of these
        // bytes would hand anyone who can reach the port a free
        // handshake-cancel: 116 bytes of noise with the source id of an
        // allowlisted lower-id peer, sustained, keeps a well-defined half of
        // every peer pair permanently down — a quorum problem, not a nuisance.
        // The losing side's teardown is therefore DEFERRED to the install block
        // below, after `read_message`, the payload decode, the id bind, and the
        // static-key check have all passed. Same rule `on_resp` follows for
        // message 2; a later refactor must not hoist it back up here.
        let losing_the_race = match peers.get(&from) {
            Some(entry) if entry.hs.is_some() => {
                if *self_id < from {
                    return Vec::new();
                }
                true
            }
            _ => false,
        };

        let mut state = match build_responder(identity) {
            Ok(state) => state,
            Err(reason) => return vec![fail(from, reason)],
        };

        let mut payload = [0u8; HS_BUF_LEN];
        let Ok(len) = state.read_message(body, &mut payload) else {
            return vec![fail(from, "handshake message 1 rejected")];
        };
        let Some((claimed_id, peer_salt)) = decode_payload(&payload[..len]) else {
            return vec![fail(from, "malformed handshake payload")];
        };
        if claimed_id != from {
            return vec![fail(from, "handshake payload claims a different node id")];
        }
        // THE responder-side authentication step. `IK` gives the responder no
        // advance knowledge of the initiator's static key — it learns it from
        // message 1 — so an impostor with any valid key pair gets this far with
        // a perfectly decryptable message. Binding the learned static key to
        // the allowlist entry for the claimed id is what makes the id mean
        // anything at all. Public key, so a plain comparison is fine.
        if state
            .get_remote_static()
            .is_none_or(|key| key != expected_public.as_slice())
        {
            return vec![fail(from, "static key does not match the allowlist")];
        }

        let mut out = [0u8; HS_BUF_LEN];
        let Ok(out_len) = state.write_message(&encode_payload(*self_id, boot_salt), &mut out) else {
            return vec![fail(from, "could not write handshake message 2")];
        };
        if !state.is_handshake_finished() {
            return vec![fail(from, "handshake did not complete")];
        }

        let session = Session::from_finished_handshake(&mut state, false, peer_salt, now_ns);
        let entry = peers.entry(from).or_default();
        // Authenticated at last — so now, and only now, the loser of a
        // simultaneous open gives up its own attempt.
        if losing_the_race {
            entry.hs = None;
        }
        // Responder completion is UNCONFIRMED: we have no idea whether message
        // 2 arrives. If a session is already live, park this one in `pending`
        // and keep sealing under the proven one.
        let confirmed = entry.current.is_none();
        if confirmed {
            entry.current = Some(session);
        } else {
            entry.pending = Some(session);
        }

        vec![
            HandshakeAction::Send {
                to: from,
                kind: DGRAM_KIND_HS_RESP,
                body: out[..out_len].to_vec(),
            },
            HandshakeAction::Established {
                peer: from,
                boot_salt: peer_salt,
                confirmed,
            },
        ]
    }

    fn on_resp(&mut self, from: NodeId, body: &[u8], now_ns: u64) -> Vec<HandshakeAction> {
        let Peers {
            allowlist, peers, ..
        } = self;

        let Some(entry) = peers.get_mut(&from) else {
            return Vec::new();
        };
        // An unsolicited message 2 is benign and expected: it is what the
        // *winner* of a simultaneous open sends to a peer that has already
        // moved on, and what we send to a peer whose replayed message 1 we
        // answered. Ignore it — reporting a failure here would turn a normal
        // race into log noise the operator would chase.
        let Some(mut hs) = entry.hs.take() else {
            return Vec::new();
        };

        let mut payload = [0u8; HS_BUF_LEN];
        let Ok(len) = hs.state.read_message(body, &mut payload) else {
            // KEEP the in-flight handshake on a failed read. snow checkpoints
            // the symmetric state before `read_message` and restores it on
            // error (`handshakestate.rs:336`), so a forged or corrupted message
            // 2 does NOT poison the handshake — the real message 2, or tick's
            // retransmit, still completes. Dropping `hs` here instead would
            // hand any off-path attacker a trivial handshake-cancel DoS: spray
            // garbage kind-19 datagrams and no link ever comes up. A later
            // refactor MUST NOT "simplify" this by discarding `hs`.
            entry.hs = Some(hs);
            return vec![fail(from, "handshake message 2 rejected")];
        };

        // Past `read_message` the sender is AUTHENTICATED — only the holder of
        // the static key we pinned from the allowlist can produce a message 2
        // this handshake accepts. So the checks below, unlike the read itself,
        // are not attacker-triggerable, and the attempt is deliberately NOT
        // restored on their failure: a peer that is misconfigured (or was just
        // revoked) must not leave a dead, already-finished handshake behind for
        // `tick` to retransmit forever. Dropping it makes `tick` build a fresh
        // one on the backoff clock instead of wedging the link.
        let Some((claimed_id, peer_salt)) = decode_payload(&payload[..len]) else {
            return vec![fail(from, "malformed handshake payload")];
        };
        if claimed_id != from {
            return vec![fail(from, "handshake payload claims a different node id")];
        }
        // We pinned the responder's static key from the allowlist when we built
        // message 1, so a wrong key already fails the DH above. Re-checking
        // against the *current* allowlist closes the case where the operator
        // revoked the peer while this handshake was in flight.
        let authorized = authorized_key(allowlist, from, now_ns)
            .is_some_and(|expected| hs.state.get_remote_static() == Some(expected.as_slice()));
        if !authorized {
            return vec![fail(from, "static key does not match the allowlist")];
        }
        // Unreachable in `IK` (reading message 2 completes the pattern), kept
        // as a belt: never split a handshake that did not finish.
        if !hs.state.is_handshake_finished() {
            return vec![fail(from, "handshake did not complete")];
        }

        // Initiator completion is CONFIRMED: the responder emits message 2 only
        // after finishing, so it certainly holds this session. Adopt it as
        // current outright and drop anything older.
        let session = Session::from_finished_handshake(&mut hs.state, true, peer_salt, now_ns);
        entry.current = Some(session);
        entry.pending = None;

        vec![HandshakeAction::Established {
            peer: from,
            boot_salt: peer_salt,
            confirmed: true,
        }]
    }
}

fn fail(peer: NodeId, reason: &'static str) -> HandshakeAction {
    HandshakeAction::Failed { peer, reason }
}

fn replay_check(session: &mut Session, counter: u64) -> Result<u64, CryptoError> {
    if session.replay.check_and_set(counter) {
        Ok(counter)
    } else {
        Err(CryptoError::Replayed(counter))
    }
}

fn retry_delay_ns(attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(HS_RETRY_MAX_SHIFT);
    (HS_RETRY_BASE_NS << shift).min(HS_RETRY_MAX_NS)
}

/// Looks up `id`'s authorized static key, giving the allowlist one chance to
/// pick up an operator edit if the id is unknown. M7 adds nodes at runtime, so
/// an unknown id is a routine "the key was just dropped in" rather than an
/// error; `reload_if_stale` self-rate-limits to once a second, so a flood of
/// unknown ids cannot turn this into a `stat(2)` storm.
fn authorized_key(allowlist: &mut Allowlist, id: NodeId, now_ns: u64) -> Option<[u8; 32]> {
    if let Some(key) = allowlist.lookup(id) {
        return Some(key);
    }
    let _ = allowlist.reload_if_stale(now_ns);
    allowlist.lookup(id)
}

fn build_initiator(
    identity: &Identity,
    peer_public: &[u8; 32],
) -> Result<snow::HandshakeState, &'static str> {
    let params: snow::params::NoiseParams =
        NOISE_PATTERN.parse().map_err(|_| "invalid noise pattern")?;
    snow::Builder::new(params)
        .local_private_key(identity.private_bytes())
        .map_err(|_| "invalid local private key")?
        .remote_public_key(peer_public)
        .map_err(|_| "invalid peer public key")?
        .build_initiator()
        .map_err(|_| "could not build the initiator")
}

fn build_responder(identity: &Identity) -> Result<snow::HandshakeState, &'static str> {
    let params: snow::params::NoiseParams =
        NOISE_PATTERN.parse().map_err(|_| "invalid noise pattern")?;
    snow::Builder::new(params)
        .local_private_key(identity.private_bytes())
        .map_err(|_| "invalid local private key")?
        .build_responder()
        .map_err(|_| "could not build the responder")
}

fn encode_payload(id: NodeId, salt: &BootSalt) -> [u8; HS_PAYLOAD_LEN] {
    let mut payload = [0u8; HS_PAYLOAD_LEN];
    payload[..4].copy_from_slice(&id.to_le_bytes());
    payload[4..].copy_from_slice(&salt.0);
    payload
}

/// Parses a handshake payload. Length-checked before any indexing; a payload of
/// the wrong size is `None`, never a partial parse.
fn decode_payload(bytes: &[u8]) -> Option<(NodeId, BootSalt)> {
    if bytes.len() != HS_PAYLOAD_LEN {
        return None;
    }
    let id = NodeId::from_le_bytes(bytes[..4].try_into().ok()?);
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&bytes[4..]);
    Some((id, BootSalt(salt)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uc_protocol::v2::datagram::{DATAGRAM_HEADER_LEN, DatagramHeader, write_datagram_header};

    const A_ID: NodeId = 1;
    const B_ID: NodeId = 2;
    const STRANGER_ID: NodeId = 99;

    // Fixed X25519 private scalars, as in `identity.rs`'s tests: arbitrary but
    // visibly real key material, with the public halves DERIVED here rather
    // than pasted as opaque base64.
    const PRIV_A: [u8; 32] = [0x11; 32];
    const PRIV_B: [u8; 32] = [0x22; 32];
    const PRIV_STRANGER: [u8; 32] = [0x33; 32];
    const PRIV_IMPOSTOR: [u8; 32] = [0x44; 32];

    fn public_of(private: [u8; 32]) -> [u8; 32] {
        let secret = x25519_dalek::StaticSecret::from(private);
        x25519_dalek::PublicKey::from(&secret).to_bytes()
    }

    /// Scratch root on real ext4 (`target/`), never `/tmp` (RAM-backed tmpfs,
    /// no swap on the dev box — CLAUDE.md). Same shape as `identity.rs`'s
    /// `tmp()`; `CARGO_TARGET_TMPDIR` is not set for inline unit tests in a lib
    /// target, so this falls back to a package-relative `target/` directory.
    fn scratch() -> PathBuf {
        // A fresh subdirectory per node keeps parallel tests from racing on
        // truncate-then-write of the same key file (a concurrent
        // `Identity::load` would see a zero-length file).
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let d = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc2_crypto_tests")
            })
            .join("uc2-crypto-handshake")
            .join(format!("node-{n}"));
        std::fs::create_dir_all(&d).unwrap();
        assert!(
            !d.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {d:?}"
        );
        d
    }

    /// Builds a `Peers` from a private key, the id it claims, and the peers it
    /// authorizes. Key and allowlist go through the real on-disk loaders —
    /// `Identity`/`Allowlist` have no from-bytes constructor by design.
    fn node(private: [u8; 32], self_id: NodeId, allow: &[(NodeId, [u8; 32])], salt: u8) -> Peers {
        let dir = scratch();

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
            text.push_str(&format!("{id} {}\n", BASE64.encode(public)));
        }
        std::fs::write(&allow_path, text).unwrap();

        Peers::new(
            Identity::load(&key_path).unwrap(),
            Allowlist::load(&allow_path).unwrap(),
            self_id,
            BootSalt([salt; 16]),
        )
    }

    /// A and B, each authorizing the other.
    fn authorized_pair() -> (Peers, Peers) {
        let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        (
            node(PRIV_A, A_ID, &allow, 0xA1),
            node(PRIV_B, B_ID, &allow, 0xB2),
        )
    }

    /// A (allowlist: A and B only) and a stranger claiming id 99. The stranger
    /// knows A's public key — an allowlist is public information — so it can
    /// produce a cryptographically well-formed `IK` message 1. What it cannot
    /// do is appear in A's allowlist.
    fn pair_with_stranger_not_in_a_allowlist() -> (Peers, Peers) {
        let a_allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        let stranger_allow = [(A_ID, public_of(PRIV_A))];
        (
            node(PRIV_A, A_ID, &a_allow, 0xA1),
            node(PRIV_STRANGER, STRANGER_ID, &stranger_allow, 0x99),
        )
    }

    /// A, and an impostor that claims id 2 (which IS in A's allowlist) while
    /// holding a private key whose public half is NOT the one A lists for id 2.
    fn pair_with_impostor_using_a_listed_id() -> (Peers, Peers) {
        let a_allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        let impostor_allow = [(A_ID, public_of(PRIV_A))];
        (
            node(PRIV_A, A_ID, &a_allow, 0xA1),
            node(PRIV_IMPOSTOR, B_ID, &impostor_allow, 0x44),
        )
    }

    /// A staged outgoing datagram in the shape `seal_pairwise` expects: the
    /// cleartext v2 header followed by the payload. `VOTE` because it is one of
    /// the pairwise kinds (spec §3), and one whose header carries load-bearing
    /// semantics.
    fn sealed_test_datagram() -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut v,
            &DatagramHeader {
                position: 4096,
                leadership_term_id: 3,
                kind: uc_protocol::v2::datagram::DGRAM_KIND_VOTE,
                flags: 0,
                key_epoch: 0,
            },
        );
        v.extend_from_slice(b"pairwise payload");
        v
    }

    impl Peers {
        /// Test-only pump that also RETURNS everything both sides emitted, so a
        /// test can assert on an action's payload and not merely on whether the
        /// link came up.
        fn on_message_all(
            &mut self,
            other: &mut Peers,
            mut acts: Vec<HandshakeAction>,
        ) -> Vec<HandshakeAction> {
            let mut seen = Vec::new();
            for _ in 0..8 {
                let mut next = Vec::new();
                for act in acts.drain(..) {
                    if let HandshakeAction::Send { to, kind, body } = &act {
                        let (to, kind, body) = (*to, *kind, body.clone());
                        if to == other.self_id() {
                            next.extend(other.on_message(self.self_id(), kind, &body, 0));
                        } else {
                            next.extend(self.on_message(other.self_id(), kind, &body, 0));
                        }
                    }
                    seen.push(act);
                }
                if next.is_empty() {
                    break;
                }
                acts = next;
            }
            seen
        }
    }

    /// Drive two `Peers` against each other with no sockets — this is the
    /// property that lets uc2_sim adjudicate the handshake deterministically.
    fn pump(a: &mut Peers, b: &mut Peers, mut acts: Vec<HandshakeAction>) -> (bool, bool) {
        let (mut a_up, mut b_up) = (false, false);
        for _ in 0..8 {
            let mut next = Vec::new();
            for act in acts.drain(..) {
                match act {
                    HandshakeAction::Send { to, kind, body } => {
                        let (dst, src) = if to == B_ID {
                            (&mut *b, A_ID)
                        } else {
                            (&mut *a, B_ID)
                        };
                        next.extend(dst.on_message(src, kind, &body, 0));
                    }
                    HandshakeAction::Established { peer, .. } => {
                        if peer == B_ID {
                            a_up = true
                        } else {
                            b_up = true
                        }
                    }
                    HandshakeAction::Failed { reason, .. } => panic!("unexpected failure: {reason}"),
                }
            }
            if next.is_empty() {
                break;
            }
            acts = next;
        }
        (a_up, b_up)
    }

    /// Seals a datagram on `from` and opens it on `to`, asserting the round trip
    /// is byte-exact. The real liveness question for a link — `Established` on
    /// both sides proves much less than this does.
    fn assert_traffic_flows(from: &mut Peers, to: &mut Peers, counter: u64) {
        let from_id = from.self_id();
        let to_id = to.self_id();
        let mut d = sealed_test_datagram();
        let plain = d.clone();
        from.seal_pairwise(to_id, &mut d, counter)
            .expect("seal under the established session");
        assert_ne!(d, plain, "sealing must actually change the buffer");
        assert_eq!(
            to.open_pairwise(from_id, &mut d).expect("peer can open it"),
            counter
        );
        assert_eq!(d, plain, "round trip is byte-exact");
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
                    if matches!(r, HandshakeAction::Failed { .. }) {
                        failed = true
                    }
                    assert!(!matches!(r, HandshakeAction::Established { .. }));
                }
            }
        }
        assert!(failed, "an unlisted id is refused, not silently ignored");
    }

    #[test]
    fn a_wrong_static_key_for_a_listed_id_is_refused() {
        // Impersonation: right id, wrong key.
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

    // The body below is quoted from the task brief, nested `if`s included;
    // collapsing it would silently diverge the suite from the specified test.
    // The single divergence is `, ..` in the `Established` pattern, forced by
    // the `confirmed` field the review asked for — the compile error IS the
    // "make the discrimination visible" fix doing its job.
    #[allow(clippy::collapsible_if)]
    #[test]
    fn established_peers_carry_the_boot_salt_for_key_derivation() {
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let mut seen = None;
        for act in a.on_message_all(&mut b, acts) {
            if let HandshakeAction::Established {
                peer, boot_salt, ..
            } = act
            {
                if peer == B_ID {
                    seen = Some(boot_salt)
                }
            }
        }
        assert_eq!(
            seen,
            Some(b.boot_salt()),
            "we learn the PEER's salt, not our own"
        );
    }

    // -- Beyond the mandated six. Each of these covers a property the six above
    // can pass without: they observe `Established` and one one-directional
    // seal, which several wrong implementations also satisfy.

    #[test]
    fn simultaneous_initiation_leaves_both_sides_able_to_seal_and_open() {
        // `simultaneous_initiation_resolves_to_one_session_by_lower_id` asserts
        // only that both sides REPORT `Established`. That is exactly what the
        // dangerous implementation does too: if the race leaves the two sides
        // holding different sessions, both still report success and the link
        // then silently drops everything. Sealing in BOTH directions is the
        // assertion that distinguishes them.
        let (mut a, mut b) = authorized_pair();
        let a_acts = a.initiate(B_ID, 0);
        let b_acts = b.initiate(A_ID, 0);
        let mut acts = a_acts;
        acts.extend(b_acts);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up);

        assert_traffic_flows(&mut a, &mut b, 1);
        assert_traffic_flows(&mut b, &mut a, 1);
    }

    #[test]
    fn a_replayed_message_one_cannot_tear_down_a_live_link() {
        // The trap the `current`/`pending` split exists for. Message 1 is
        // replayable by anyone who saw it — no key material needed. A responder
        // that adopts every fresh handshake would here throw away the session
        // its peer is still using, and the link would look established from
        // both ends while dropping every datagram.
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let msg1 = acts
            .iter()
            .find_map(|act| match act {
                HandshakeAction::Send { kind, body, .. } if *kind == DGRAM_KIND_HS_INIT => {
                    Some(body.clone())
                }
                _ => None,
            })
            .expect("initiate emits message 1");
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up);
        assert_traffic_flows(&mut a, &mut b, 1);

        // The attacker replays the captured message 1 at B, repeatedly.
        for _ in 0..3 {
            let replay = b.on_message(A_ID, DGRAM_KIND_HS_INIT, &msg1, 1_000_000_000);
            assert!(
                !replay
                    .iter()
                    .any(|act| matches!(act, HandshakeAction::Failed { .. })),
                "a replayed message 1 is well-formed; it is answered, not reported as an error"
            );
        }

        // ... and the live link is untouched, in both directions.
        assert_traffic_flows(&mut a, &mut b, 2);
        assert_traffic_flows(&mut b, &mut a, 2);
    }

    #[test]
    fn a_restarted_peer_reestablishes_and_takes_over() {
        // The other half of the same design: refusing new handshakes while up
        // would be safe but would black-hole the link forever after a restart.
        // B restarts (same identity, NEW boot salt, no session state) and
        // re-initiates; A must converge onto the new session.
        let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        let mut a = node(PRIV_A, A_ID, &allow, 0xA1);
        let mut b = node(PRIV_B, B_ID, &allow, 0xB2);
        let acts = a.initiate(B_ID, 0);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up);
        assert_traffic_flows(&mut a, &mut b, 1);

        let mut b2 = node(PRIV_B, B_ID, &allow, 0xCC);
        let acts = b2.initiate(A_ID, 5_000_000_000);
        let mut established_salt = None;
        for act in b2.on_message_all(&mut a, acts) {
            if let HandshakeAction::Established {
                peer, boot_salt, ..
            } = act
                && peer == B_ID
            {
                established_salt = Some(boot_salt);
            }
        }
        assert_eq!(
            established_salt,
            Some(b2.boot_salt()),
            "A learns the restarted peer's NEW boot salt"
        );

        // A parked the new session as `pending` and is still sealing under the
        // old one, which the restarted B cannot open — correct: A has no proof
        // yet that B holds the new session.
        assert_eq!(
            a.peer_boot_salt(B_ID),
            Some(b.boot_salt()),
            "the pairwise session in force is still the pre-restart one"
        );
        let mut stale = sealed_test_datagram();
        a.seal_pairwise(B_ID, &mut stale, 9).unwrap();
        assert!(
            b2.open_pairwise(A_ID, &mut stale).is_err(),
            "the restarted peer cannot open traffic sealed under the dead session"
        );

        // B sends first — as a real follower does continuously with
        // APPEND_POSITION — and that is A's proof. It promotes, and the link is
        // live in both directions again.
        assert_traffic_flows(&mut b2, &mut a, 1);
        assert_traffic_flows(&mut a, &mut b2, 3);
        assert_eq!(
            a.peer_boot_salt(B_ID),
            Some(b2.boot_salt()),
            "promotion carries the restarted peer's salt with it"
        );
    }

    #[test]
    fn an_impostor_is_refused_by_name_not_merely_left_unestablished() {
        // The mandated impostor test asserts only the absence of `Established`,
        // which an implementation that silently ignores everything also
        // satisfies. It also mis-states the mechanism: the `IK` responder does
        // not know the initiator's static key in advance, so the impostor's
        // message 1 decrypts perfectly and NOTHING in the pattern fails. The
        // refusal comes from the explicit allowlist check in `on_init`. Assert
        // that check by name, so deleting it fails the suite.
        let (mut a, mut impostor) = pair_with_impostor_using_a_listed_id();
        let acts = impostor.initiate(A_ID, 0);
        let mut reasons = Vec::new();
        for act in acts {
            if let HandshakeAction::Send { kind, body, .. } = act {
                for r in a.on_message(B_ID, kind, &body, 0) {
                    match r {
                        HandshakeAction::Failed { reason, .. } => reasons.push(reason),
                        HandshakeAction::Established { .. } => panic!("impostor established"),
                        HandshakeAction::Send { .. } => panic!("impostor got a reply"),
                    }
                }
            }
        }
        assert_eq!(reasons, vec!["static key does not match the allowlist"]);
        assert!(!a.is_established(B_ID));
    }

    /// A node holding `private` but claiming an id different from the one the
    /// allowlist binds that key to — the payload-vs-`from` mismatch case.
    fn node_claiming_the_wrong_id() -> Peers {
        let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        node(PRIV_B, 42, &allow, 0xB2)
    }

    #[test]
    fn a_payload_claiming_a_different_node_id_is_refused_on_message_one() {
        // The ruling-mandated bind: the id in the authenticated payload must
        // match the id the transport demultiplexed. Without it the two
        // identities the rest of the system keys on — the session's `NodeId`
        // and the static key the allowlist authorized — are only accidentally
        // the same thing.
        let (mut a, mut liar) = (authorized_pair().0, node_claiming_the_wrong_id());
        let acts = liar.initiate(A_ID, 0);
        let mut reasons = Vec::new();
        for act in acts {
            if let HandshakeAction::Send { kind, body, .. } = act {
                // The key IS the one A lists for id 2, so the static-key check
                // passes; only the payload's claimed id (42) is wrong.
                for r in a.on_message(B_ID, kind, &body, 0) {
                    match r {
                        HandshakeAction::Failed { reason, .. } => reasons.push(reason),
                        other => panic!("expected refusal, got {other:?}"),
                    }
                }
            }
        }
        assert_eq!(
            reasons,
            vec!["handshake payload claims a different node id"]
        );
        assert!(!a.is_established(B_ID));
    }

    #[test]
    fn a_payload_claiming_a_different_node_id_is_refused_on_message_two() {
        // Same bind on the initiator side, where the responder is the liar.
        let mut a = authorized_pair().0;
        let mut liar = node_claiming_the_wrong_id();

        let msg1 = a
            .initiate(B_ID, 0)
            .into_iter()
            .find_map(|act| match act {
                HandshakeAction::Send { body, .. } => Some(body),
                _ => None,
            })
            .expect("message 1");
        let msg2 = liar
            .on_message(A_ID, DGRAM_KIND_HS_INIT, &msg1, 0)
            .into_iter()
            .find_map(|act| match act {
                HandshakeAction::Send { body, .. } => Some(body),
                _ => None,
            })
            .expect("message 2");

        let out = a.on_message(B_ID, DGRAM_KIND_HS_RESP, &msg2, 0);
        assert!(
            matches!(
                out.as_slice(),
                [HandshakeAction::Failed {
                    reason: "handshake payload claims a different node id",
                    ..
                }]
            ),
            "got {out:?}"
        );
        assert!(!a.is_established(B_ID));

        // NOTE: whether the link is left WEDGED is not observable here — see
        // `a_mismatched_id_leaves_no_dead_handshake_behind`, which is the test
        // for that property. `tick` emits an `HS_INIT` under either
        // implementation, so asserting on it proves nothing.
    }

    #[test]
    fn a_mismatched_id_leaves_no_dead_handshake_behind() {
        // The second round trip, which is the only thing that distinguishes
        // "drop the attempt" from "retain it" after a check that follows a
        // successful `read_message`.
        //
        // `tick`'s retransmit branch resends the CACHED message 1 bytes without
        // inspecting whether `hs.state` is still usable, so both
        // implementations emit an `HS_INIT` after the mismatch — which is why
        // the assertion that used to live in the test above was worthless. What
        // separates them is what happens when the peer ANSWERS that retransmit:
        // a retained, already-finished `HandshakeState` fails every subsequent
        // `read_message` with `HandshakeAlreadyFinished`, re-enters the
        // retain-on-read-failure branch, and the link never comes up again.
        let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        let mut a = node(PRIV_A, A_ID, &allow, 0xA1);
        let mut liar = node_claiming_the_wrong_id();

        let msg1 = a
            .initiate(B_ID, 0)
            .into_iter()
            .find_map(|act| match act {
                HandshakeAction::Send { body, .. } => Some(body),
                _ => None,
            })
            .expect("message 1");
        let msg2 = liar
            .on_message(A_ID, DGRAM_KIND_HS_INIT, &msg1, 0)
            .into_iter()
            .find_map(|act| match act {
                HandshakeAction::Send { body, .. } => Some(body),
                _ => None,
            })
            .expect("message 2");
        assert!(matches!(
            a.on_message(B_ID, DGRAM_KIND_HS_RESP, &msg2, 0).as_slice(),
            [HandshakeAction::Failed { .. }]
        ));

        // The peer's misconfiguration is corrected — or, equivalently, the
        // honest holder of that key answers the retry.
        let mut b = node(PRIV_B, B_ID, &allow, 0xB2);
        let retry_msg1 = a
            .tick(HS_RETRY_MAX_NS * 2)
            .into_iter()
            .find_map(|act| match act {
                HandshakeAction::Send { to, kind, body }
                    if to == B_ID && kind == DGRAM_KIND_HS_INIT =>
                {
                    Some(body)
                }
                _ => None,
            })
            .expect("tick retries the handshake");
        let reply = b
            .on_message(A_ID, DGRAM_KIND_HS_INIT, &retry_msg1, 0)
            .into_iter()
            .find_map(|act| match act {
                HandshakeAction::Send { body, .. } => Some(body),
                _ => None,
            })
            .expect("the peer answers the retry");

        let out = a.on_message(B_ID, DGRAM_KIND_HS_RESP, &reply, 0);
        assert!(
            matches!(
                out.as_slice(),
                [HandshakeAction::Established {
                    peer: B_ID,
                    confirmed: true,
                    ..
                }]
            ),
            "a dead handshake was retained and now rejects every reply: {out:?}"
        );
        assert_traffic_flows(&mut a, &mut b, 1);
    }

    #[test]
    fn an_unproven_pending_session_expires_after_the_ttl() {
        // The TTL is load-bearing for the whole current/pending design: it is
        // what stops an attacker (or a dead peer) from pinning a session slot
        // indefinitely. Tested through the front door — a peer whose pending
        // session expired can no longer be opened — and at the boundary, so an
        // inverted or missing comparison fails.
        for (elapsed, opens) in [(PENDING_TTL_NS - 1, true), (PENDING_TTL_NS, false)] {
            let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
            let mut a = node(PRIV_A, A_ID, &allow, 0xA1);
            let mut b = node(PRIV_B, B_ID, &allow, 0xB2);
            let acts = a.initiate(B_ID, 0);
            assert_eq!(pump(&mut a, &mut b, acts), (true, true));

            // A restarts and re-handshakes; B parks the new session as pending
            // (installed at now_ns = 0, which is what `on_message_all` feeds).
            let mut a2 = node(PRIV_A, A_ID, &allow, 0xDD);
            let acts = a2.initiate(B_ID, 0);
            let _ = a2.on_message_all(&mut b, acts);

            b.tick(elapsed);

            let mut d = sealed_test_datagram();
            a2.seal_pairwise(B_ID, &mut d, 1).unwrap();
            assert_eq!(
                b.open_pairwise(A_ID, &mut d).is_ok(),
                opens,
                "pending session at {elapsed} ns of a {PENDING_TTL_NS} ns TTL"
            );
        }
    }

    #[test]
    fn an_unconfirmed_session_is_flagged_and_its_promotion_announced() {
        // `Established` alone cannot tell a caller whether the seal path
        // actually switched, and the two answers can differ for as long as the
        // pending TTL. The flag makes that visible at the destructuring site;
        // the re-announcement on promotion gives the caller the later edge.
        let allow = [(A_ID, public_of(PRIV_A)), (B_ID, public_of(PRIV_B))];
        let mut a = node(PRIV_A, A_ID, &allow, 0xA1);
        let mut b = node(PRIV_B, B_ID, &allow, 0xB2);

        let acts = a.initiate(B_ID, 0);
        for act in a.on_message_all(&mut b, acts) {
            if let HandshakeAction::Established { confirmed, .. } = act {
                assert!(confirmed, "a first session is in force immediately");
            }
        }

        let mut a2 = node(PRIV_A, A_ID, &allow, 0xDD);
        let acts = a2.initiate(B_ID, 0);
        let mut flags = Vec::new();
        for act in a2.on_message_all(&mut b, acts) {
            if let HandshakeAction::Established {
                peer, confirmed, ..
            } = act
                && peer == A_ID
            {
                flags.push(confirmed);
            }
        }
        assert_eq!(
            flags,
            vec![false],
            "B parked the restarted peer's session: NOT in force"
        );
        assert_eq!(
            b.peer_boot_salt(A_ID),
            Some(a.boot_salt()),
            "and the salt in force is still the old one"
        );

        // A2 proves it is using the new session.
        assert_traffic_flows(&mut a2, &mut b, 1);
        let announced: Vec<_> = b
            .tick(1_000_000_000)
            .into_iter()
            .filter_map(|act| match act {
                HandshakeAction::Established {
                    peer,
                    boot_salt,
                    confirmed,
                } if peer == A_ID => Some((boot_salt, confirmed)),
                _ => None,
            })
            .collect();
        assert_eq!(announced, vec![(a2.boot_salt(), true)], "promotion announced");
        assert_eq!(b.peer_boot_salt(A_ID), Some(a2.boot_salt()));
        // Announced exactly once, not on every subsequent tick.
        assert!(b.tick(2_000_000_000).is_empty());
    }

    #[test]
    fn a_replayed_datagram_is_rejected_by_the_session_replay_window() {
        // AEAD stops forgery but not replay, and a replayed VOTE or admin
        // datagram is not harmless (spec §4).
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up);

        let mut d = sealed_test_datagram();
        a.seal_pairwise(B_ID, &mut d, 7).unwrap();
        let captured = d.clone();
        assert_eq!(b.open_pairwise(A_ID, &mut d).unwrap(), 7);

        let mut replayed = captured;
        assert!(matches!(
            b.open_pairwise(A_ID, &mut replayed),
            Err(CryptoError::Replayed(7))
        ));
    }

    #[test]
    fn the_sealed_pairwise_envelope_authenticates_the_header() {
        // The whole reason this module takes the raw split instead of using
        // snow's transport modes (which hard-code empty AAD). APPEND_POSITION
        // is a pairwise, header-ONLY kind: if the header were not
        // authenticated, an on-path attacker could rewrite a follower's
        // reported durable position on an otherwise-valid datagram.
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up);

        for i in 0..DATAGRAM_HEADER_LEN {
            let mut d = sealed_test_datagram();
            a.seal_pairwise(B_ID, &mut d, 100 + i as u64).unwrap();
            d[i] ^= 0xFF;
            assert!(
                matches!(b.open_pairwise(A_ID, &mut d), Err(CryptoError::AuthFailed)),
                "header byte {i} is not authenticated on the pairwise path"
            );
        }
    }

    #[test]
    fn sealing_or_opening_without_a_session_is_an_error_not_a_panic() {
        let (mut a, mut b) = authorized_pair();
        let mut d = sealed_test_datagram();
        assert!(matches!(
            a.seal_pairwise(B_ID, &mut d, 1),
            Err(CryptoError::NoSession(B_ID))
        ));
        assert!(matches!(
            a.open_pairwise(B_ID, &mut d),
            Err(CryptoError::NoSession(B_ID))
        ));

        // Established, but handed a buffer too short to be a sealed datagram.
        let acts = a.initiate(B_ID, 0);
        let (a_up, _) = pump(&mut a, &mut b, acts);
        assert!(a_up);
        let mut runt = vec![0u8; 4];
        assert!(matches!(
            a.open_pairwise(B_ID, &mut runt),
            Err(CryptoError::TooShort)
        ));
    }

    #[test]
    fn an_unsolicited_or_unknown_handshake_datagram_is_ignored() {
        let (mut a, _) = authorized_pair();
        // Message 2 with no handshake in flight: benign (it is what the winner
        // of a simultaneous open sends a peer that moved on), so no action.
        assert!(
            a.on_message(B_ID, DGRAM_KIND_HS_RESP, &[0u8; 68], 0)
                .is_empty()
        );
        // HS_KEY is T7's; unknown kinds are not ours.
        assert!(
            a.on_message(B_ID, uc_protocol::v2::crypto::DGRAM_KIND_HS_KEY, &[1, 2, 3], 0)
                .is_empty()
        );
        assert!(a.on_message(B_ID, 250, &[1, 2, 3], 0).is_empty());
        // A datagram claiming to be from us is refused, not processed.
        assert!(matches!(
            a.on_message(A_ID, DGRAM_KIND_HS_INIT, &[0u8; 116], 0)
                .as_slice(),
            [HandshakeAction::Failed { .. }]
        ));
    }

    #[test]
    fn a_forged_message_two_cannot_cancel_an_in_flight_handshake() {
        // snow checkpoints and restores its symmetric state on a failed
        // `read_message`, so the in-flight handshake survives garbage. If
        // `on_resp` dropped it instead, any off-path attacker could stop a link
        // from ever coming up by spraying kind-19 datagrams.
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        for junk in [vec![0u8; 68], vec![0xFF; 68], vec![0u8; 3]] {
            let out = a.on_message(B_ID, DGRAM_KIND_HS_RESP, &junk, 0);
            assert!(matches!(out.as_slice(), [HandshakeAction::Failed { .. }]));
        }
        // The real handshake still completes.
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up, "a forged message 2 must not cancel the link");
        assert_traffic_flows(&mut a, &mut b, 1);
    }

    #[test]
    fn a_spoofed_message_one_cannot_cancel_an_in_flight_handshake() {
        // The mirror of `a_forged_message_two_cannot_cancel_an_in_flight_handshake`,
        // on the `on_init` side. The simultaneous-open tiebreak must not act on
        // UNAUTHENTICATED bytes: B is the higher id, so the rule says "drop ours
        // and respond" — and if that runs before the message is authenticated,
        // anyone who can put 116 bytes on the port with the source id of an
        // allowlisted lower-id peer destroys B's in-flight handshake for free,
        // no key material required. Sustained, that keeps a well-defined half
        // of every peer pair permanently down, which in a Raft cluster is a
        // quorum problem.
        let (mut a, mut b) = authorized_pair();
        let acts = b.initiate(A_ID, 0);

        // Sustained, in the shape of the reviewer's probe: 20 rounds of spray.
        for round in 0..20u64 {
            for junk in [
                vec![0u8; 116],
                vec![0xFF; 116],
                vec![],
                vec![0xAB; 48],
                vec![0x5A; 1500],
            ] {
                let out = b.on_message(A_ID, DGRAM_KIND_HS_INIT, &junk, round);
                assert!(
                    !out.iter()
                        .any(|act| matches!(act, HandshakeAction::Send { .. })),
                    "garbage must not be answered"
                );
                assert!(
                    !out.iter()
                        .any(|act| matches!(act, HandshakeAction::Established { .. })),
                    "garbage must not establish"
                );
            }
        }

        // The attempt is still there to retransmit — the observable the probe
        // watched: under the defect, `tick` had nothing left to resend.
        assert!(
            b.tick(HS_RETRY_MAX_NS * 2).iter().any(|act| matches!(
                act,
                HandshakeAction::Send { to, kind, .. }
                    if *to == A_ID && *kind == DGRAM_KIND_HS_INIT
            )),
            "the in-flight handshake survived the spray"
        );

        // B's handshake is still in flight, so the REAL peer still completes it.
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(
            a_up && b_up,
            "a spoofed message 1 must not cancel our in-flight handshake"
        );
        assert_traffic_flows(&mut b, &mut a, 1);
    }

    #[test]
    fn a_peer_not_yet_in_the_allowlist_is_retried_on_the_backoff_clock() {
        // UC's agents busy-spin — that is the core architectural choice of the
        // whole system — so an ungated failure path costs a `Failed` action and
        // a heap allocation PER POLL ITERATION, not per second. The retry
        // clock must gate the failure path exactly as it gates the retransmit.
        let mut a = node(PRIV_A, A_ID, &[(A_ID, public_of(PRIV_A))], 0xA1);
        assert!(matches!(
            a.initiate(B_ID, 0).as_slice(),
            [HandshakeAction::Failed { .. }]
        ));

        for now in [1, 1_000, 1_000_000, HS_RETRY_BASE_NS - 1] {
            assert!(
                a.tick(now).is_empty(),
                "no work at all before the backoff elapses (poll at {now} ns)"
            );
        }
        assert_eq!(
            a.tick(HS_RETRY_BASE_NS).len(),
            1,
            "exactly one retry at the deadline"
        );
        assert!(a.tick(HS_RETRY_BASE_NS + 1).is_empty(), "and then quiet again");
    }

    #[test]
    fn tick_retransmits_message_one_with_backoff_and_never_gives_up() {
        let (mut a, _) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        assert_eq!(acts.len(), 1);

        // Too soon: no retransmit.
        assert!(a.tick(HS_RETRY_BASE_NS - 1).is_empty());

        let mut sends = 0;
        let mut now = HS_RETRY_BASE_NS;
        for _ in 0..12 {
            sends += a
                .tick(now)
                .iter()
                .filter(|act| {
                    matches!(act, HandshakeAction::Send { kind, .. } if *kind == DGRAM_KIND_HS_INIT)
                })
                .count();
            now += HS_RETRY_MAX_NS;
        }
        assert_eq!(sends, 12, "retransmits continue indefinitely while down");
    }

    #[test]
    fn tick_retries_a_peer_whose_key_was_not_in_the_allowlist_yet() {
        // M7 adds nodes at runtime: `initiate` before the operator drops the
        // key in must refuse, then succeed on its own once the file changes.
        let dir = scratch();
        let key_path = dir.join("node.key");
        std::fs::write(&key_path, PRIV_A).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let allow_path = dir.join("allowlist");
        std::fs::write(
            &allow_path,
            format!("{A_ID} {}\n", BASE64.encode(public_of(PRIV_A))),
        )
        .unwrap();
        let mut a = Peers::new(
            Identity::load(&key_path).unwrap(),
            Allowlist::load(&allow_path).unwrap(),
            A_ID,
            BootSalt([0xA1; 16]),
        );

        assert!(matches!(
            a.initiate(B_ID, 0).as_slice(),
            [HandshakeAction::Failed {
                reason: "peer is not in the allowlist",
                ..
            }]
        ));

        // The operator adds B. mtime must actually move.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(
            &allow_path,
            format!(
                "{A_ID} {}\n{B_ID} {}\n",
                BASE64.encode(public_of(PRIV_A)),
                BASE64.encode(public_of(PRIV_B))
            ),
        )
        .unwrap();

        let acts = a.tick(5_000_000_000);
        assert!(
            acts.iter().any(|act| matches!(
                act,
                HandshakeAction::Send { to, kind, .. }
                    if *to == B_ID && *kind == DGRAM_KIND_HS_INIT
            )),
            "tick picks up the added key and starts the handshake: {acts:?}"
        );
    }

    #[test]
    fn handshake_payload_layout_is_pinned() {
        // A second-language peer has nothing but this to implement against.
        let salt = BootSalt([0xEE; 16]);
        let encoded = encode_payload(0x0102_0304, &salt);
        assert_eq!(encoded.len(), 20);
        assert_eq!(&encoded[..4], &[0x04, 0x03, 0x02, 0x01], "node id is LE");
        assert_eq!(&encoded[4..], &salt.0);
        assert_eq!(decode_payload(&encoded), Some((0x0102_0304, salt)));
        // Wrong length is refused outright, never partially parsed.
        assert_eq!(decode_payload(&encoded[..19]), None);
        assert_eq!(decode_payload(&[]), None);
    }

    #[test]
    fn the_two_directional_keys_differ_and_match_across_the_link() {
        // A tx/rx mix-up would make each side seal under the key it also opens
        // with — which still round-trips in a one-directional test.
        let (mut a, mut b) = authorized_pair();
        let acts = a.initiate(B_ID, 0);
        let (a_up, b_up) = pump(&mut a, &mut b, acts);
        assert!(a_up && b_up);

        let a_session = a.peers[&B_ID].current.as_ref().unwrap();
        let b_session = b.peers[&A_ID].current.as_ref().unwrap();
        assert_ne!(
            a_session.seal_key.as_slice(),
            a_session.open_key.as_slice(),
            "the two directions must not share a key"
        );
        assert_eq!(a_session.seal_key.as_slice(), b_session.open_key.as_slice());
        assert_eq!(a_session.open_key.as_slice(), b_session.seal_key.as_slice());
        assert_eq!(a_session.boot_salt, b.boot_salt());
        assert_eq!(b_session.boot_salt, a.boot_salt());
    }
}
