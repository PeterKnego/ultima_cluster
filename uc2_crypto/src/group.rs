//! Group-key distribution and activation (spec §5 "Group key lifecycle").
//!
//! [`GroupPlane`] is the **leader's** minting/delivery/ack bookkeeping AND
//! the **follower's** receive/install/ack-back logic — one type plays both
//! roles depending on which methods the node layer calls, exactly like
//! [`crate::handshake::Peers`] plays both initiator and responder. It never
//! touches a socket or a pairwise key: it emits [`HandshakeAction`]s
//! describing what to send, and the node layer (T12) seals the `HS_KEY`
//! body over the peer's already-established pairwise channel and sends it.
//! That split — no sockets, no clock reads (`now_ns` is always explicit) —
//! is what makes this unit-testable and, later, `uc2_sim`-drivable.
//!
//! # The liveness trap
//!
//! The leader may only seal fan-out under a new epoch once peers can
//! actually open it, but it must never let a dead peer block replication
//! forever. [`GroupPlane::sealing_epoch`] resolves this: an epoch is safe to
//! seal under once every peer named in the [`GroupPlane::mint`] call has
//! acked it, **or** [`ACTIVATION_TIMEOUT_NS`] has elapsed since minting,
//! whichever comes first. A peer that missed the key simply fails to open
//! group-sealed datagrams and recovers through the system's existing NAK
//! repair path once `HS_KEY` lands on it (a later `tick`/retry, not
//! anything this module invents) — no new recovery mechanism here, by
//! design.
//!
//! # Epoch bookkeeping: mint-time reconciliation, not stored derived state
//!
//! Exactly one epoch is "pending" (minted, not yet folded into history) at a
//! time. [`GroupPlane::sealing_epoch`] is a pure function of the CURRENT
//! pending epoch's ack/timeout state plus `active_epoch` — the last epoch
//! that was ever seen to satisfy the activation rule. When [`GroupPlane::
//! mint`] is called again, it first reconciles: if the outgoing pending
//! epoch had already activated (by the SAME rule, evaluated at the new
//! mint's `now_ns`), it is folded into `active_epoch` before being replaced.
//! This is what makes `sealing_epoch` correctly return the OLD epoch while a
//! freshly-minted one is still waiting on acks, rather than momentarily
//! going back to `None` — a rotation must never make the leader stop
//! sealing group traffic altogether.
//!
//! An outgoing pending epoch that had NOT yet activated when superseded is
//! dropped as an EPOCH — it never got to be "the" active epoch, and
//! superseding it (a second mint before the first settled) is not a case this
//! design needs to preserve history for; `active_epoch` still holds whatever
//! the last real activation was. **Its activation CLOCK, however, is
//! inherited by the superseding mint** (T17): the grace measures "time since
//! this node first tried to distribute a key to this peer set", not "time
//! since the latest mint". Restarting the clock per mint was a real cold-start
//! livelock — the node layer mints on every `BecomeLeader` and elections
//! retry every 150-300 ms, an order of magnitude faster than the 2 s grace, so
//! a cluster booting with one member down could never seal a single `DATA`
//! datagram. See [`GroupPlane::mint`] and its two regression tests.
//!
//! # Wire format for the `HS_KEY` body (opaque to `uc_protocol`, like
//! `handshake.rs`'s handshake payload)
//!
//! `DGRAM_KIND_HS_KEY` (20) carries two different message shapes over the
//! SAME kind — delivery (leader -> peer) and ack (peer -> leader) — because
//! both ride the pairwise channel the handshake already authenticated, and
//! giving them their own kind would cost a second demux constant for no
//! safety gain:
//!
//! ```text
//! delivery: [ 1B type=0 ][ 2B epoch LE ][ 32B group key ]   (35 bytes)
//! ack:      [ 1B type=1 ][ 2B epoch LE ]                    ( 3 bytes)
//! ```
//!
//! [`GroupPlane::on_key_message`] is fed bytes that arrived over the
//! network (already opened by the pairwise session, but their CONTENT is
//! still an unauthenticated claim as far as this module is concerned) — it
//! never panics and never installs anything from a message that fails the
//! type tag or exact-length check.
//!
//! # Key material handled here
//!
//! [`GroupPlane::mint`] is the one place in this crate where fresh entropy
//! is legitimate. The 32 random bytes are written straight into a
//! `Zeroizing`-wrapped buffer (never a bare array) before being handed to
//! [`GroupKey::new`] for the schedule and copied into the delivery body for
//! the wire — the same "no unwrapped copy of live key material outlives a
//! statement" discipline `identity.rs`/`handshake.rs` document. The body
//! `Vec<u8>` DOES carry the plaintext key — that is unavoidable: it is the
//! very thing being delivered, sealed by the caller over the pairwise
//! channel before it ever reaches a socket.
//!
//! [`GroupPlane::on_key_message`]'s receive path (a peer installing a
//! `MSG_KEY` delivery) copies the wire bytes into the SAME kind of
//! `Zeroizing`-wrapped buffer before constructing the `GroupKey` — a receive
//! path is not exempt just because the bytes came from the network instead
//! of an RNG; it is handling the identical live, cluster-wide secret.
//!
//! **The crate-wide rule, stated positively so it doesn't have to be
//! rediscovered per call site:** every place that touches raw key-material
//! bytes — minted, derived, split off a finished handshake, or received off
//! the wire — binds them into a zeroizing type in the SAME statement that
//! produces them, with no bare `[u8; 32]`/`Vec<u8>` intermediate at any
//! point, receive paths included. This exact class of gap has been caught
//! four times in this crate under different shapes — `Identity::load`'s
//! read buffer (T2), `GroupKey`'s inner field almost staying `pub` (T3),
//! the `derive_send_key` call-site contract (T5), and `on_key_message`'s
//! receive-side copy here (T7, fixed post-review) — never as a correctness
//! bug, always as key material sitting in freed memory a moment longer than
//! it needed to. Follow the pattern at an existing call site rather than
//! writing a fresh one from scratch.

use crate::handshake::HandshakeAction;
#[cfg(test)]
use crate::schedule::epoch_is_newer;
use crate::schedule::{GroupKey, KeySchedule};
use crate::NodeId;
use rand::rngs::OsRng;
use rand::TryRngCore;
use std::collections::HashSet;
use uc_protocol::v2::crypto::DGRAM_KIND_HS_KEY;

/// How long the leader waits for every reachable peer to ack a freshly
/// minted epoch before sealing under it anyway. The liveness half of the
/// activation rule — see the module docs.
pub const ACTIVATION_TIMEOUT_NS: u64 = 2_000_000_000;

const MSG_KEY: u8 = 0;
const MSG_ACK: u8 = 1;
/// `type(1) + epoch(2) + key(32)`.
const KEY_MSG_LEN: usize = 1 + 2 + 32;
/// `type(1) + epoch(2)`.
const ACK_MSG_LEN: usize = 1 + 2;

/// One in-flight mint: the epoch, when it was minted, the peer set it must
/// clear to activate by consensus (rather than by timeout), and who has
/// acked so far.
struct PendingEpoch {
    epoch: u16,
    minted_at: u64,
    peers: Vec<NodeId>,
    acked: HashSet<NodeId>,
}

/// The group-key mint/delivery/ack/activation state machine. See the module
/// docs for the split between this and the node layer.
pub struct GroupPlane {
    self_id: NodeId,
    schedule: KeySchedule,
    /// Monotonically bumped on every [`GroupPlane::mint`] call, independent
    /// of whether the PREVIOUS mint ever activated — epoch numbers must
    /// advance across mints regardless of ack/timeout outcome, so a caller
    /// can always tell "later minted" from "earlier minted" via
    /// [`crate::schedule::epoch_is_newer`].
    next_epoch: u16,
    /// The last epoch this plane has ever seen satisfy the activation rule.
    /// `None` until the very first mint activates.
    active_epoch: Option<u16>,
    pending: Option<PendingEpoch>,
}

impl GroupPlane {
    pub fn new(self_id: NodeId) -> GroupPlane {
        GroupPlane {
            self_id,
            schedule: KeySchedule::new(),
            // 0 is reserved: `uc_protocol::v2::datagram::OFF_DGRAM_KEY_EPOCH`'s
            // own doc says "0 = cleartext", and `uc2_net`'s receive seam
            // (T11) relies on that being true to diagnose a genuinely
            // cleartext peer distinctly from a generic auth failure. Start
            // minting at 1 so the first epoch a fresh process ever mints is
            // never the wire's cleartext sentinel — see `mint`'s wrap guard
            // for the corresponding "never mint 0 again" rule on overflow.
            next_epoch: 1,
            active_epoch: None,
            pending: None,
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    pub fn schedule(&self) -> &KeySchedule {
        &self.schedule
    }

    /// Mints a fresh group-key epoch, installs it into the schedule (so it
    /// is immediately OPENABLE — installation is not gated on activation;
    /// only SEALING is, via [`GroupPlane::sealing_epoch`]), and emits one
    /// `HS_KEY` delivery per peer in `peers` for the caller to seal over
    /// that peer's pairwise channel and send.
    ///
    /// Before minting, reconciles the outgoing pending epoch (if any) into
    /// `active_epoch` if it had already satisfied the activation rule as of
    /// `now_ns` — see the module docs. This is what lets `sealing_epoch`
    /// keep returning a still-good older epoch while the new one is
    /// outstanding, instead of momentarily going back to `None`.
    ///
    /// If the outgoing epoch had NOT activated, the new epoch INHERITS its
    /// activation clock rather than restarting the grace from `now_ns` (T17).
    /// Without that, a caller that re-mints faster than
    /// [`ACTIVATION_TIMEOUT_NS`] — which the node layer does, minting on every
    /// `BecomeLeader` while elections retry every 150-300 ms — can never let
    /// the grace elapse, and a cluster that cold-starts with one member down
    /// never seals a single `DATA` datagram. See
    /// `a_superseding_mint_inherits_an_unactivated_epochs_activation_clock`.
    ///
    /// **Say plainly what inheriting the clock costs** (T17 review, M1): an
    /// inherited clock can report an epoch ACTIVATED within milliseconds of
    /// the mint that created it — once the inherited grace has already
    /// elapsed, the very next mint is sealable immediately, with none of its
    /// own `HS_KEY` deliveries acked. The un-inherited timeout never did
    /// that; it always gave each epoch its own full 2 s. The consequence is
    /// bounded and benign — a peer that has not yet installed epoch N simply
    /// cannot open traffic sealed under it, exactly as if the datagram had
    /// been lost, and it self-heals the moment the `HS_KEY` (or the node
    /// layer's re-delivery sweep) lands. Nothing is sealed under a key no
    /// peer will ever hold, and no peer accepts anything it could not have
    /// accepted before. But the cost is real and is paid to buy back the
    /// liveness the reset destroyed: without it there is no epoch to be
    /// late for, because the cluster never forms at all.
    /// As [`GroupPlane::mint`], but with the ACTIVATION SET stated separately
    /// from the delivery set.
    ///
    /// Every peer in `peers` still receives an `HS_KEY` delivery — a peer that
    /// is merely slow, or whose session comes up a moment later, must still get
    /// the key. Only `gate_on` is required to ack before
    /// [`GroupPlane::sealing_epoch`] will use the epoch.
    ///
    /// The caller passes as `gate_on` the peers it can actually deliver to. A
    /// peer with no established pairwise session cannot be delivered to at all
    /// — the `HS_KEY` is sealed pairwise, so the send fails outright — and so
    /// its ack can never arrive. Waiting [`ACTIVATION_TIMEOUT_NS`] for it is
    /// waiting for something the caller already knows is impossible.
    ///
    /// This narrows the WAIT, not the SECRECY. Confidentiality comes from the
    /// delivery being sealed pairwise to an allowlisted peer; the activation
    /// set only decides how long the leader holds off using an epoch. A peer
    /// excluded here is picked up by [`GroupPlane::peers_missing_key`] once its
    /// session exists, receives the key by redelivery, and acks then.
    ///
    /// Why it matters (2026-08-05): `DATA` and `HEARTBEAT` are both group-scope,
    /// so a leader with no usable epoch can neither replicate nor heartbeat. A
    /// FRESH leader has no activated epoch to fall back on, so one unreachable
    /// peer in `gossip_targets()` — an M7 learner added but never started, or a
    /// crashed voter — muted it for the full 2 s, against a 150-300 ms follower
    /// election timeout. It could not survive its own mute window, so the
    /// cluster could not hold a leader at all. See
    /// `docs/notes/uc2-the-mute-leader.md`.
    pub fn mint_gated(
        &mut self,
        peers: &[NodeId],
        gate_on: &[NodeId],
        now_ns: u64,
    ) -> (u16, Vec<HandshakeAction>) {
        let (epoch, acts) = self.mint(peers, now_ns);
        if let Some(p) = self.pending.as_mut() {
            p.peers.retain(|id| gate_on.contains(id));
        }
        (epoch, acts)
    }

    pub fn mint(&mut self, peers: &[NodeId], now_ns: u64) -> (u16, Vec<HandshakeAction>) {
        // T17 (2026-07-29) — the activation clock is INHERITED, not restarted,
        // when this mint supersedes a pending epoch that never activated. See
        // `a_superseding_mint_inherits_an_unactivated_epochs_activation_clock`
        // for the full account: stamping `now_ns` unconditionally livelocks
        // any cluster that cold-starts with a member down, because the node
        // layer mints on EVERY `BecomeLeader` and the election timeout
        // (150-300 ms) is an order of magnitude shorter than the 2 s grace, so
        // the grace never elapses and the leader can never seal `DATA`.
        //
        // Read BEFORE `fold_pending_if_activated`, which `take()`s `pending`
        // unconditionally (an un-activated pending is simply dropped, so its
        // timestamp would be gone by the time we needed it).
        let inherited_clock = self
            .pending
            .as_ref()
            .filter(|p| !Self::is_activated(p, now_ns))
            .map(|p| p.minted_at);
        self.fold_pending_if_activated(now_ns);

        let epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.wrapping_add(1);
        if self.next_epoch == 0 {
            // 0 is reserved (see `new`'s doc) -- `wrapping_add` would
            // otherwise recur to it every 65,536 mints. Years-to-decades
            // away under any real rotation cadence, but the reservation is
            // only actually complete if BOTH the start and the wrap skip it.
            self.next_epoch = 1;
        }

        // Written straight into the zeroizing wrapper: no bare `[u8; 32]`
        // ever holds the fresh key, matching `identity.rs`'s
        // read-straight-into-Zeroizing discipline for the private key file.
        let mut key_bytes = zeroize::Zeroizing::new([0u8; 32]);
        OsRng
            .try_fill_bytes(&mut *key_bytes)
            .expect("the OS RNG is unavailable");

        self.schedule.install(epoch, GroupKey::new(*key_bytes));

        let actions = peers
            .iter()
            .map(|&peer| HandshakeAction::Send {
                to: peer,
                kind: DGRAM_KIND_HS_KEY,
                body: encode_key_delivery(epoch, &key_bytes),
            })
            .collect();

        self.pending = Some(PendingEpoch {
            epoch,
            // T17: the inherited clock, if this mint supersedes a pending
            // epoch that never activated — see the block at the top of this
            // function. `now_ns` for the ordinary case (nothing pending, or
            // the outgoing epoch just folded into `active_epoch`).
            minted_at: inherited_clock.unwrap_or(now_ns),
            peers: peers.to_vec(),
            acked: HashSet::new(),
        });

        (epoch, actions)
    }

    /// Records that `from` acked `epoch`. A no-op if `epoch` is not the
    /// current pending epoch (a late ack for a superseded mint, or a
    /// forged/garbled epoch number) or if `from` was not one of the peers
    /// [`GroupPlane::mint`] was told to deliver to — an ack from an
    /// unexpected peer must not let a wrong count-based activation check
    /// slip past the actual peer-set requirement.
    pub fn on_ack(&mut self, from: NodeId, epoch: u16) {
        if let Some(pending) = &mut self.pending
            && pending.epoch == epoch
            && pending.peers.contains(&from)
        {
            pending.acked.insert(from);
        }
    }

    /// The epoch it is safe to SEAL new group traffic under, if any: the
    /// pending epoch once it has activated (every named peer acked, or the
    /// activation timeout elapsed), else the last epoch that ever
    /// activated, else `None` before anything ever has.
    ///
    /// Pure: does not mutate `pending`/`active_epoch` itself (that
    /// reconciliation happens in [`GroupPlane::mint`]) — calling this
    /// repeatedly with different `now_ns` values, including ones the leader
    /// never explicitly re-mints against, always yields the answer that
    /// `now_ns` implies.
    pub fn sealing_epoch(&self, now_ns: u64) -> Option<u16> {
        if let Some(pending) = &self.pending
            && Self::is_activated(pending, now_ns)
        {
            return Some(pending.epoch);
        }
        self.active_epoch
    }

    /// The peers of the newest minted epoch that have not acked it yet —
    /// empty if nothing has ever been minted, or once everyone has acked.
    ///
    /// Added T12, with [`GroupPlane::redeliver_to`], to close a liveness gap
    /// this module shipped with: [`GroupPlane::mint`] emits each peer's
    /// `HS_KEY` delivery EXACTLY ONCE and nothing ever re-emits it. The
    /// datagram rides UDP, so a single drop leaves that peer unable to open
    /// ANY group-scope traffic — and it cannot recover on its own: the spec's
    /// "recovers through the existing NAK repair path once `HS_KEY` lands"
    /// is only true if something makes `HS_KEY` land again, and a NAK'd
    /// retransmit is itself `DATA`, sealed under the very epoch the peer is
    /// missing. Without re-delivery the peer stays dark until the NEXT
    /// rotation, which by default is an hour away (`RotationPolicy`'s 1h /
    /// 1 TiB) — i.e. a lost handshake datagram silently costs a replica.
    /// The node layer polls this on its maintenance tick and re-delivers.
    pub fn unacked_peers(&self) -> Vec<NodeId> {
        match &self.pending {
            Some(p) => p.peers.iter().copied().filter(|id| !p.acked.contains(id)).collect(),
            None => Vec::new(),
        }
    }

    /// Which of `targets` still lack the newest minted epoch — the peers a
    /// caller must (re)deliver to.
    ///
    /// This is [`GroupPlane::unacked_peers`] widened in the one direction that
    /// matters: a peer is "missing" if it has not ACKED, INCLUDING a peer that
    /// was never in the mint's delivery list at all. `unacked_peers` can only
    /// ever name peers the mint knew about, so a node that joins the peer set
    /// AFTER a mint is invisible to it — never unacked, never redelivered to,
    /// and therefore holding no group key for as long as this leader reigns.
    /// Its fan-out traffic is then dropped "no usable group key" indefinitely.
    ///
    /// Reached in the field via the ordinary boot sequence rather than any
    /// exotic race: a node elects itself under a solo genesis config (peer set
    /// empty, mint correct and delivered to nobody), then adopts the real
    /// multi-voter config. Measured 2026-08-05 in `sigkill_mid_config_window`
    /// with crypto ON — ~25 %/run failure, 34-80 mints per 15 s run against 6
    /// for the same test without reconfiguration.
    ///
    /// With nothing minted every target is missing: there is no epoch to hold.
    pub fn peers_missing_key(&self, targets: &[NodeId]) -> Vec<NodeId> {
        match &self.pending {
            Some(p) => targets.iter().copied().filter(|id| !p.acked.contains(id)).collect(),
            None => targets.to_vec(),
        }
    }

    /// Re-emits the newest minted epoch's `HS_KEY` delivery to each of
    /// `peers`, for the caller to seal pairwise and send again — see
    /// [`GroupPlane::unacked_peers`] for why this exists.
    ///
    /// Deliberately does NOT consult the ack set: the caller also uses this
    /// for a peer that has ALREADY acked but has since RESTARTED (a fresh
    /// `HandshakeAction::Established` for a peer we thought was done), whose
    /// new process holds no keys at all. Empty if nothing has been minted,
    /// or if the pending epoch's key is somehow absent from the schedule
    /// (unreachable — `mint` installs before it returns — but expressed as a
    /// filter rather than an `unwrap`, per this crate's no-panic rule).
    ///
    /// Delivers the PENDING (newest minted) epoch, not
    /// [`GroupPlane::sealing_epoch`]'s answer. Those differ only in the
    /// bounded window between a mint and its activation, during which a
    /// restarted peer may be unable to open the still-current older epoch —
    /// self-healing within [`ACTIVATION_TIMEOUT_NS`], and the alternative
    /// (shipping the older key too) would widen key exposure to buy back at
    /// most two seconds.
    pub fn redeliver_to(&self, peers: &[NodeId]) -> Vec<HandshakeAction> {
        let Some(pending) = &self.pending else {
            return Vec::new();
        };
        let Some(key) = self.schedule.get(pending.epoch) else {
            return Vec::new();
        };
        peers
            .iter()
            .map(|&peer| HandshakeAction::Send {
                to: peer,
                kind: DGRAM_KIND_HS_KEY,
                body: encode_key_delivery(pending.epoch, key.as_bytes()),
            })
            .collect()
    }

    /// Feeds in a received `HS_KEY` body (kind 20), already opened by the
    /// pairwise session — `from` is therefore an authenticated sender, but
    /// the BODY's content is still parsed as untrusted bytes: a length or
    /// type-tag mismatch is refused without installing or acking anything,
    /// never a panic.
    ///
    /// A well-formed delivery (`MSG_KEY`) installs the epoch into the
    /// schedule (making it immediately openable) and returns an action to
    /// send an ack back to `from`. A well-formed ack (`MSG_ACK`) is folded
    /// straight into [`GroupPlane::on_ack`] and returns no actions — acks
    /// never need a reply.
    pub fn on_key_message(&mut self, from: NodeId, body: &[u8]) -> Vec<HandshakeAction> {
        match body.first().copied() {
            Some(MSG_KEY) if body.len() == KEY_MSG_LEN => {
                let epoch = u16::from_le_bytes([body[1], body[2]]);
                // Written straight into a zeroizing buffer, same discipline
                // as `mint`'s send path (see the module docs' "Key material
                // handled here" section) — this is received, live,
                // cluster-wide key material, not a throwaway local.
                let mut key = zeroize::Zeroizing::new([0u8; 32]);
                key.copy_from_slice(&body[3..KEY_MSG_LEN]);
                // NO `epoch_is_newer` freshness check here: under UDP
                // reordering a late duplicate of an OLDER `HS_KEY` can
                // install after a newer one, which flips which epoch
                // `KeySchedule::current()`/`previous()` LABEL. Confirmed
                // harmless in this task's scope — `KeySchedule::get()` and
                // `retire_below()` both key off the epoch NUMBER, not the
                // current/previous label, and `GroupPlane::sealing_epoch`
                // (the only sealing-side epoch source) never reads
                // `schedule.current()` directly. A future caller (T9/T12)
                // MUST go through `sealing_epoch`, not `schedule.current()`,
                // or this reordering hazard becomes live.
                self.schedule.install(epoch, GroupKey::new(*key));
                vec![HandshakeAction::Send {
                    to: from,
                    kind: DGRAM_KIND_HS_KEY,
                    body: encode_ack(epoch),
                }]
            }
            Some(MSG_ACK) if body.len() == ACK_MSG_LEN => {
                let epoch = u16::from_le_bytes([body[1], body[2]]);
                self.on_ack(from, epoch);
                Vec::new()
            }
            _ => vec![HandshakeAction::Failed {
                peer: from,
                reason: "malformed group-key message",
            }],
        }
    }

    fn fold_pending_if_activated(&mut self, now_ns: u64) {
        if let Some(pending) = self.pending.take()
            && Self::is_activated(&pending, now_ns)
        {
            self.active_epoch = Some(pending.epoch);
        }
        // Else: superseded before it ever activated. Simply dropped —
        // `active_epoch` still holds whatever the last real activation
        // was (or `None`, if there never was one).
    }

    /// The activation rule itself: every peer named at mint time has acked,
    /// OR the bounded timeout has elapsed since minting. `all()` over an
    /// empty peer set is vacuously `true`, so a mint with no peers (e.g. a
    /// single-node cluster) activates immediately without a special case.
    fn is_activated(pending: &PendingEpoch, now_ns: u64) -> bool {
        let all_acked = pending.peers.iter().all(|p| pending.acked.contains(p));
        let timed_out = now_ns.saturating_sub(pending.minted_at) > ACTIVATION_TIMEOUT_NS;
        all_acked || timed_out
    }
}

fn encode_key_delivery(epoch: u16, key: &[u8; 32]) -> Vec<u8> {
    let mut body = Vec::with_capacity(KEY_MSG_LEN);
    body.push(MSG_KEY);
    body.extend_from_slice(&epoch.to_le_bytes());
    body.extend_from_slice(key);
    body
}

fn encode_ack(epoch: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(ACK_MSG_LEN);
    body.push(MSG_ACK);
    body.extend_from_slice(&epoch.to_le_bytes());
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A never-acking peer costs a fresh leader [`ACTIVATION_TIMEOUT_NS`] of
    /// MUTE FAN-OUT — every group-scope datagram dropped "no usable group
    /// key" — because activation is `all_acked || timed_out` and an
    /// unreachable peer can only ever satisfy the second disjunct.
    ///
    /// An M7 learner that is added to the config but is not running (or is not
    /// in the crypto allowlist) is exactly such a peer, and it sits in
    /// `gossip_targets()`, so it is named in every mint. Rotation fires on
    /// `BecameLeader`, so each election buys another 2 s of silence — and a
    /// FRESH leader has no previously-activated epoch to fall back on.
    ///
    /// Measured NOT to be permanent, which is what a first draft of this test
    /// wrongly asserted: a later mint folds the earlier pending epoch in once
    /// its own 2 s has rolled past, so the leader does recover. The cost is
    /// bounded per mint, not a closed loop.
    ///
    /// Field shape (2026-08-05, `sigkill_mid_config_window` + crypto, ~25 % of
    /// runs): 34-80 mints per 15 s run against 6 for the same test without a
    /// config change, 8-13 "no usable group key" drops, ~30x fewer client ops
    /// than the same test with crypto off.
    #[test]
    fn a_never_acking_peer_mutes_a_fresh_leader_for_the_activation_timeout() {
        let mut g = GroupPlane::new(1);
        // Peer 0 is live; peer 100 is the unreachable learner.
        let (e1, _) = g.mint(&[0, 100], 0);
        g.on_ack(0, e1);
        assert_eq!(
            g.sealing_epoch(ACTIVATION_TIMEOUT_NS - 1),
            None,
            "the whole activation window: peer 100 cannot ack, and this leader \
             has no earlier activated epoch — every fan-out is dropped"
        );
        assert_eq!(
            g.sealing_epoch(ACTIVATION_TIMEOUT_NS + 1),
            Some(e1),
            "the liveness half of the rule releases it"
        );
    }

    /// Option A (2026-08-05): a peer we cannot deliver to must not gate
    /// activation. Contrast with
    /// `a_never_acking_peer_mutes_a_fresh_leader_for_the_activation_timeout`,
    /// which documents the ungated behaviour this replaces at the call site.
    #[test]
    fn a_peer_we_cannot_deliver_to_does_not_gate_activation() {
        let mut g = GroupPlane::new(1);
        // 0 has a session; 100 does not, so only 0 can gate.
        let (e1, _) = g.mint_gated(&[0, 100], &[0], 0);
        g.on_ack(0, e1);
        assert_eq!(
            g.sealing_epoch(1_000),
            Some(e1),
            "the deliverable peer acked — the leader must be able to seal at once, \
             not sit mute for the activation timeout waiting on a peer that \
             provably never received the key"
        );
        assert_eq!(
            g.peers_missing_key(&[0, 100]),
            vec![100],
            "and the excluded peer is still owed the key: redelivery must keep \
             targeting it so it can open group traffic once its session exists"
        );
    }

    /// A peer that joins AFTER the mint is invisible to `unacked_peers` — it
    /// was never in the delivery list, so it is never "unacked" and never
    /// redelivered to, and it holds no group key for as long as this leader
    /// reigns. `peers_missing_key` is the widened question the caller needs.
    #[test]
    fn a_peer_that_joined_after_the_mint_is_missing_the_key_not_merely_unacked() {
        let mut g = GroupPlane::new(1);
        let (epoch, _acts) = g.mint(&[0], 1_000);
        // Node 0 was delivered to and acks; node 2 joined the peer set later.
        g.on_ack(0, epoch);
        assert!(g.unacked_peers().is_empty(), "0 acked, and 2 was never in the mint");
        assert_eq!(
            g.peers_missing_key(&[0, 2]),
            vec![2],
            "the late joiner must be reported as needing the key"
        );
        assert!(
            !g.redeliver_to(&[2]).is_empty(),
            "and redelivery to it must actually emit a key delivery"
        );
    }

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

    // -- Beyond the mandated five. Every prior task in this plan shipped
    // mandated tests that a wrong implementation passed anyway (T4's
    // word-boundary shift, T5's no-encryption/AAD-truncation mutants, T6's
    // simultaneous-open-onto-two-sessions mutant). The five above observe
    // only the two leader-side entry points (`mint`/`on_ack`) and the
    // follower-side refusal path; they never exercise `on_key_message`'s
    // success path, never check WHO an ack must come from, never check that
    // a stale-epoch ack is ignored, and never pin the exact activation-rule
    // reconciliation across a second mint. Each test below is paired with
    // the specific wrong implementation it was written to kill (see the
    // task report for the actual red-then-green transcripts).

    #[test]
    fn a_well_formed_delivery_installs_the_key_and_acks_back_to_the_sender() {
        // Exercises on_key_message's SUCCESS path end to end: this is the
        // one the five mandated tests never touch (they only feed it
        // garbage). A leader mints; a follower processes the delivery
        // action; the follower's schedule must hold the SAME key bytes
        // (not merely "some" key — an off-by-one in the 1/2/32 byte layout,
        // e.g. swapping the epoch and type-tag bytes, or reading the key
        // from the wrong offset, would still "install something" and pass
        // the mandated garbage test while shipping a peer that can never
        // open real traffic).
        let mut leader = GroupPlane::new(1);
        let (epoch, actions) = leader.mint(&[2], 0);
        let HandshakeAction::Send { to, kind, body } = &actions[0] else {
            panic!("mint must emit a Send action");
        };
        assert_eq!(*to, 2);
        assert_eq!(*kind, DGRAM_KIND_HS_KEY);

        let mut follower = GroupPlane::new(2);
        let reply = follower.on_key_message(1, body);
        assert_eq!(
            follower.schedule().get(epoch).map(|k| *k.as_bytes()),
            leader.schedule().get(epoch).map(|k| *k.as_bytes()),
            "the follower must install the EXACT bytes the leader minted"
        );

        // The reply must be the ack, addressed back to the leader, and
        // feeding it to the leader must be what actually activates the
        // epoch — not merely "on_key_message returned something".
        assert_eq!(reply.len(), 1);
        let HandshakeAction::Send {
            to: ack_to,
            kind: ack_kind,
            body: ack_body,
        } = &reply[0]
        else {
            panic!("a well-formed delivery must ack back")
        };
        assert_eq!(*ack_to, 1);
        assert_eq!(*ack_kind, DGRAM_KIND_HS_KEY);

        assert_eq!(leader.sealing_epoch(0), None, "not yet fed the ack");
        let ack_actions = leader.on_key_message(2, ack_body);
        assert!(ack_actions.is_empty(), "an ack needs no reply");
        assert_eq!(
            leader.sealing_epoch(0),
            Some(epoch),
            "on_key_message's ack path must actually drive on_ack, not merely parse"
        );
    }

    #[test]
    fn an_ack_from_a_peer_outside_the_mint_set_does_not_count() {
        // A wrong implementation might activate once it has seen
        // `peers.len()` acks from ANYONE, rather than acks from every
        // NAMED peer specifically. Peer 9 (never in the mint list, e.g. a
        // stale/forged sender id) acking twice must not substitute for
        // peer 3's real ack.
        let mut g = GroupPlane::new(1);
        let (epoch, _) = g.mint(&[2, 3], 0);
        g.on_ack(2, epoch);
        g.on_ack(9, epoch);
        g.on_ack(9, epoch);
        assert_eq!(
            g.sealing_epoch(0),
            None,
            "an ack from a peer outside the mint set must not count toward activation"
        );
        g.on_ack(3, epoch);
        assert_eq!(g.sealing_epoch(0), Some(epoch));
    }

    #[test]
    fn an_ack_for_a_stale_epoch_does_not_activate_the_current_one() {
        // A wrong implementation might key acks only by peer id (dropping
        // the epoch check, or comparing against the WRONG epoch), so a
        // leftover/delayed ack for the epoch that was just superseded
        // wrongly counts toward the new one. Real scenario: peer 3's ack
        // for e1 is delayed in flight; by the time it arrives the leader
        // has already minted e2 (e.g. a fresh election). It must not
        // silently activate e2.
        let mut g = GroupPlane::new(1);
        let (e1, _) = g.mint(&[2, 3], 0);
        g.on_ack(3, e1); // peer 3 acks e1 promptly, well before it is superseded
        let (e2, _) = g.mint(&[2, 3], 1_000);
        assert_ne!(e1, e2);
        g.on_ack(2, e2); // peer 2 acks the NEW epoch
        // A duplicate/delayed ack for the OLD epoch arrives from peer 3 after
        // e2 was minted. If it were wrongly counted against the current
        // pending epoch, this alone would complete e2's ack set (2 and 3)
        // and prematurely activate it.
        g.on_ack(3, e1);
        assert_eq!(
            g.sealing_epoch(1_000),
            None,
            "a stale ack tagged with the OLD epoch must not complete the NEW epoch's ack set"
        );
        g.on_ack(3, e2);
        assert_eq!(g.sealing_epoch(1_000), Some(e2));
    }

    #[test]
    fn sealing_epoch_falls_back_to_the_last_activated_epoch_while_a_new_one_is_pending() {
        // The property the module doc calls out as the whole reason
        // `mint` reconciles before replacing `pending`: a rotation must
        // never make the leader go back to sealing NOTHING while the new
        // epoch's acks are still outstanding. None of the five mandated
        // tests mint twice with the first one having already fully
        // activated, so a naive "sealing_epoch only ever looks at
        // `pending`" implementation (returning `None` the instant a new
        // mint starts) would still pass all five.
        let mut g = GroupPlane::new(1);
        let (e1, _) = g.mint(&[2, 3], 0);
        g.on_ack(2, e1);
        g.on_ack(3, e1);
        assert_eq!(g.sealing_epoch(0), Some(e1));

        // Re-mint with peer 3 unreachable; e2 has not activated yet.
        let (e2, _) = g.mint(&[2, 3], 1_000);
        g.on_ack(2, e2);
        assert_eq!(
            g.sealing_epoch(1_500),
            Some(e1),
            "still sealing under the last GOOD epoch, not None, while e2 awaits peer 3"
        );

        // Once e2 activates (by ack or timeout), sealing moves onto it.
        g.on_ack(3, e2);
        assert_eq!(g.sealing_epoch(1_500), Some(e2));
    }

    #[test]
    fn epoch_boundary_exactly_at_the_timeout_has_not_yet_elapsed() {
        // Pins the boundary the mandated timeout test straddles but never
        // lands on exactly: the brief's wording is "once now_ns EXCEEDS
        // minted_at + ACTIVATION_TIMEOUT_NS", i.e. strictly greater. At
        // exactly the deadline the peer has not yet had the full bounded
        // window; a `>=` implementation would activate one instant early
        // and both readings pass the mandated test (which only checks
        // TIMEOUT-1 and TIMEOUT+1).
        let mut g = GroupPlane::new(1);
        let (_, _) = g.mint(&[2], 0);
        assert_eq!(
            g.sealing_epoch(ACTIVATION_TIMEOUT_NS),
            None,
            "exactly at the deadline the bounded window has not yet been exceeded"
        );
    }

    #[test]
    fn a_second_mint_before_the_first_activates_still_advances_and_the_dropped_one_never_activates_later(
    ) {
        // If `fold_pending_if_activated` mistakenly evaluated the
        // OUTGOING pending epoch's timeout against a stale `minted_at`
        // read after replacement (or leaked the old PendingEpoch into the
        // new one), a late ack for the abandoned e1 could resurrect it.
        // Also checks on_key_message's ack path can't resurrect a fully
        // superseded epoch either.
        let mut g = GroupPlane::new(1);
        let (e1, _) = g.mint(&[2, 3], 0);
        // Neither peer acks e1 before it is superseded well within the
        // activation timeout.
        let (e2, _) = g.mint(&[2, 3], 100);
        assert!(epoch_is_newer(e2, e1));
        assert_eq!(
            g.sealing_epoch(100),
            None,
            "e1 never activated and is gone; e2 has no acks yet"
        );
        let ack_body = encode_ack(e1);
        let actions = g.on_key_message(2, &ack_body);
        assert!(actions.is_empty());
        assert_eq!(
            g.sealing_epoch(100),
            None,
            "a late ack for the abandoned e1 must not resurrect anything"
        );
    }

    // ---- M8 Task 12: `HS_KEY` re-delivery -------------------------------

    #[test]
    fn unacked_peers_names_exactly_who_still_owes_an_ack() {
        let mut g = GroupPlane::new(1);
        assert!(g.unacked_peers().is_empty(), "nothing minted, nobody owes anything");
        let (epoch, _) = g.mint(&[2, 3], 0);
        assert_eq!(g.unacked_peers(), vec![2, 3]);
        g.on_ack(2, epoch);
        assert_eq!(g.unacked_peers(), vec![3]);
        g.on_ack(3, epoch);
        assert!(g.unacked_peers().is_empty(), "fully acked: the sweep goes quiet");
    }

    /// The liveness gap this pair closes: `mint` emits each delivery ONCE,
    /// over UDP. A peer that loses it can open no group-scope traffic and
    /// cannot self-heal — a NAK'd retransmit is itself `DATA` sealed under
    /// the epoch it is missing — so without re-delivery it stays dark until
    /// the next rotation, an hour away by default.
    #[test]
    fn a_lost_key_delivery_can_be_re_sent_byte_identically() {
        let mut g = GroupPlane::new(1);
        let (epoch, first) = g.mint(&[2, 3], 0);
        let again = g.redeliver_to(&g.unacked_peers());
        assert_eq!(again.len(), 2);
        for (a, b) in first.iter().zip(again.iter()) {
            let (HandshakeAction::Send { to: t1, kind: k1, body: b1 }, HandshakeAction::Send { to: t2, kind: k2, body: b2 }) = (a, b) else {
                panic!("mint and redeliver must both emit Send actions");
            };
            assert_eq!((t1, k1, b1), (t2, k2, b2), "the SAME epoch's key, verbatim");
        }
        // And it really is the minted epoch, not a fresh one.
        let HandshakeAction::Send { body, .. } = &again[0] else { unreachable!() };
        assert_eq!(u16::from_le_bytes([body[1], body[2]]), epoch);
    }

    /// A peer that ALREADY acked but has since restarted holds no keys at
    /// all, and `unacked_peers` will never name it — so `redeliver_to` must
    /// not consult the ack set. (The node layer drives this off a fresh
    /// `HandshakeAction::Established`.)
    #[test]
    fn redelivery_to_an_already_acked_peer_still_ships_the_key() {
        let mut g = GroupPlane::new(1);
        let (epoch, _) = g.mint(&[2], 0);
        g.on_ack(2, epoch);
        assert!(g.unacked_peers().is_empty());
        let acts = g.redeliver_to(&[2]);
        assert_eq!(acts.len(), 1, "a restarted peer is re-keyed on demand");
        let HandshakeAction::Send { to, kind, body } = &acts[0] else { unreachable!() };
        assert_eq!((*to, *kind), (2, DGRAM_KIND_HS_KEY));
        assert_eq!(u16::from_le_bytes([body[1], body[2]]), epoch);
    }

    #[test]
    fn redelivery_before_any_mint_is_a_no_op_not_a_panic() {
        let g = GroupPlane::new(1);
        assert!(g.redeliver_to(&[2, 3]).is_empty());
    }

    /// **T17 (2026-07-29), a real cold-start livelock — found by T17's own
    /// capstone, fixed here.**
    ///
    /// `mint` used to stamp `minted_at = now_ns` unconditionally, so a mint
    /// that SUPERSEDED a still-unactivated pending epoch restarted the
    /// activation grace from zero. That is a livelock whenever a leader
    /// re-mints faster than [`ACTIVATION_TIMEOUT_NS`], and the node layer
    /// does exactly that: rotation trigger 1 mints on EVERY `BecomeLeader`,
    /// and the election timeout (150-300 ms in production config) is an order
    /// of magnitude shorter than the 2 s grace.
    ///
    /// The reachable, ordinary scenario: a 3-member cluster cold-starts with
    /// one member down (a rolling restart, or one host slow to boot). The
    /// absent member can never ack, so activation depends entirely on the
    /// timeout. No epoch has ever activated, so `active_epoch` is `None` and
    /// `sealing_epoch` returns `None` — the leader can seal no `DATA`, no
    /// `HEARTBEAT` and no `COMMIT_POSITION`. The live follower therefore sees
    /// no leader activity, times out, and starts a fresh election; the winner
    /// mints again, resetting the clock. The grace never elapses and the
    /// cluster never forms. Reproduced end-to-end before this fix by
    /// `uc2_node/tests/crypto_cluster.rs`'s
    /// `a_cluster_forms_even_when_one_member_never_comes_up`, which hung for
    /// its full 60 s deadline.
    ///
    /// The fix inherits the superseded epoch's clock, so the grace measures
    /// "time since this node FIRST tried to distribute a key to this peer
    /// set". No security property moves: sealing under an epoch some peer
    /// never acked is exactly what the timeout already permits, and a peer
    /// that has not acked could not open the superseded epoch either.
    ///
    /// Discriminating: the loop re-mints every 300 ms, which under the old
    /// rule keeps `now - minted_at` pinned at 0 forever.
    #[test]
    fn a_superseding_mint_inherits_an_unactivated_epochs_activation_clock() {
        let mut g = GroupPlane::new(1);
        g.mint(&[2, 3], 0);
        g.on_ack(2, 1); // peer 3 is down and never acks
        let mut t = 0u64;
        for _ in 0..20 {
            t += 300_000_000; // one election timeout apart
            assert_eq!(g.mint(&[2, 3], t).1.len(), 2, "each mint re-delivers to both peers");
            g.on_ack(2, g.sealing_epoch(t).unwrap_or(0)); // peer 2 keeps acking
        }
        assert!(
            t > ACTIVATION_TIMEOUT_NS,
            "the loop must actually run past the grace for this to mean anything"
        );
        assert!(
            g.sealing_epoch(t).is_some(),
            "a leader re-minting faster than the activation grace must still eventually seal — \
             otherwise a cluster that cold-starts with one member down never forms"
        );
    }

    /// The other direction: inheriting the clock must NOT make an epoch
    /// activate EARLY. A single mint with an unacked peer is unsealable until
    /// the grace genuinely elapses.
    #[test]
    fn inheriting_the_clock_does_not_activate_an_epoch_before_the_grace_elapses() {
        let mut g = GroupPlane::new(1);
        g.mint(&[2, 3], 1_000_000_000);
        assert!(g.sealing_epoch(1_500_000_000).is_none(), "half a grace in: not yet");
        g.mint(&[2, 3], 1_500_000_000);
        assert!(
            g.sealing_epoch(2_000_000_000).is_none(),
            "the inherited clock starts at the FIRST mint (1.0s), so 2.0s is still inside it"
        );
        assert!(g.sealing_epoch(3_100_000_000).is_some(), "past the grace from the first mint");
    }
}
