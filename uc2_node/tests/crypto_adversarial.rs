// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M8 Task 14: the **adversarial tier** — attacks against a real, running
//! crypto-enabled cluster, not against a function.
//!
//! Threat model (task brief): an adversary on the network path who can read,
//! inject, replay, reorder and corrupt datagrams, but does not hold a node's
//! private key. Explicitly NOT modeled: a compromised host, or a malicious
//! cluster member (the group key is symmetric — any holder can forge
//! fan-out traffic as any node; accepted, documented residual).
//!
//! Four attacks, each proving something specific:
//! 1. A replayed `VOTE` cannot be recounted (safety: split-brain risk).
//! 2. A peer removed from the allowlist cannot re-establish, and an
//!    impostor cannot borrow a peer's identity slot (T6's finding: an IK
//!    responder learns the initiator's static key from message 1, so the
//!    allowlist is decorative without an explicit key-vs-claimed-id check
//!    AND a claimed-id-vs-transport-source binding).
//! 3. A downgrade to cleartext never reaches the log buffer or the
//!    consensus event route.
//! 4. Heavy corruption + replay never panics and never diverges — "a node
//!    must not be killable by a datagram."
//!
//! ## What already existed, and why this file does not duplicate it
//!
//! - `uc2_node/tests/crypto_cluster.rs` (T17): the payoff capstone — a real
//!   3-node crypto cluster elects/replicates/serves, plus a negative control
//!   proving a cleartext node cannot join. This file borrows its fixture
//!   *shape* (real `Identity` key files, a real on-disk allowlist, real
//!   `Node::start_with_socket` over real loopback UDP) but not its code —
//!   every prior M8 task in this plan has duplicated these fixtures per
//!   file rather than exporting a shared test-only crate (`uc2_node/src/
//!   node.rs`'s own T12 module doc says as much), so this follows the same
//!   convention.
//! - `uc2_crypto`'s unit tests (esp. `handshake.rs`) cover forged/truncated/
//!   replayed inputs at the PRIMITIVE level (`Peers::on_message` fed
//!   fabricated bytes directly). This file never calls into `uc2_crypto`
//!   directly for the attacks themselves — every attack datagram is
//!   delivered over a REAL `UdpSocket` to a REAL, running [`Node`], because
//!   the property under test is "the wired system refuses this," not "the
//!   primitive rejects malformed input."
//! - `uc2_net`'s receiver tests (esp. `receiver.rs`'s crypto module) cover
//!   drop-and-count paths against a bare `Receiver` (no consensus, no
//!   election, no leader). This file is the level above: the SAFETY
//!   property (a replayed `VOTE` must not move an election) can only be
//!   observed against a real `Consensus`/`ElectionSm`, which only a real
//!   [`Node`] wires up.
//! - `uc2_net/src/fault.rs`'s new `corrupt_per_million`/`replay_per_million`
//!   knobs are unit-pinned in that module directly (byte-exact: flips
//!   exactly one bit, re-delivers a stashed datagram, both inert at 0
//!   without touching the RNG). This file's storm test (#4) exercises them
//!   at the cluster level, which is a different property (liveness/safety
//!   under load, not the fault mechanism's own correctness).
//!
//! Scratch (key files, journals) lives under `CARGO_TARGET_TMPDIR` — real
//! ext4, never `/tmp` (RAM-backed tmpfs with no swap on the dev box; see
//! CLAUDE.md).

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_client::Client;
use uc2_crypto::identity::Identity;
use uc2_crypto::{CryptoConfig, HandshakeAction, NodeId, ReceiveHalf, SharedTransport};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};
use uc_protocol::v2::crypto::{CRYPTO_OVERHEAD, DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_RESP};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_VOTE, DatagramHeader,
    RequestVoteBody, VoteBody, read_datagram_header, read_request_vote_body, write_datagram_header,
    write_vote_body,
};

const APP: &str = "crypto-adversarial";

/// Same reasoning as `crypto_cluster.rs`'s `TEST_LOCK`: several real `Node`s
/// x four busy-spin agents apiece, well past the core count, with
/// timing-sensitive elections. Serialize the whole file's tests.
static TEST_LOCK: Mutex<()> = Mutex::new(());
fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ------------------------------------------------------------ fixtures

/// Returns the `TempDir` itself (T14 review, M-1) — NOT `.keep()`'d. The
/// first version of this file leaked a whole tempdir per test run
/// (~200 MB), which drove the shared dev box to 91% disk before it was
/// caught; every call site now holds the returned value as `dir_handle` for
/// the test's whole lifetime (`let dir = dir_handle.path();` for the `&Path`
/// every existing call site already expects) so the directory is reclaimed
/// on drop, exactly like `crypto_cluster.rs`'s own `_dir: tempfile::TempDir`.
fn scratch_dir(tag: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("uc2-adv-{tag}-"))
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    assert!(!dir.path().starts_with("/tmp"), "test scratch must not live on tmpfs");
    dir
}

fn write_key_file(path: &Path, private: [u8; 32]) {
    std::fs::write(path, private).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

/// Standard-alphabet base64 with padding, matching `uc2_crypto::identity`'s
/// allowlist parser. Hand-rolled rather than adding a `base64` dev-dependency
/// (same rationale as `crypto_cluster.rs`'s copy of this helper).
fn b64_32(bytes: &[u8; 32]) -> String {
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

fn identity_public(dir: &Path, tag: &str, private: [u8; 32]) -> [u8; 32] {
    let key_path = dir.join(format!("{tag}.key"));
    write_key_file(&key_path, private);
    Identity::load(&key_path).unwrap().public_bytes()
}

fn write_allowlist(path: &Path, entries: &[(NodeId, [u8; 32])]) {
    let mut text = String::new();
    for (id, public) in entries {
        text.push_str(&format!("{id} {}\n", b64_32(public)));
    }
    std::fs::write(path, text).unwrap();
}

/// A minimal `NodeConfig` for one victim node, `members` supplied by the
/// caller (real node addresses and/or `FakePeer`-bound addresses alike — the
/// wire does not know the difference). Short election timeouts so a
/// single-voter-reachable majority elects quickly.
fn victim_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    dir: &Path,
    key_path: PathBuf,
    allowlist_path: PathBuf,
    faults: FaultConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: "127.0.0.1:0".parse().unwrap(),
        instance_dir: dir.join(format!("n{id}")),
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 60_000_000,
        election_timeout_max_ns: 120_000_000,
        seed: 0xC0FF_EE00 ^ id as u64,
        faults,
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Enabled {
            key_path,
            allowlist_path,
            rotation: uc2_crypto::rotation::RotationPolicy::default(),
        },
        services: uc2_node::ServicesConfig::default(),
    }
}

// ---------------------------------------------------------- the fake peer
//
// A test-controlled cluster member: a REAL X25519 identity, a REAL Noise-IK
// `SharedTransport`, and a REAL bound `UdpSocket` — but driven by hand
// rather than by a full `Node`'s polling agents. This is what lets a test
// capture, replay, or forge wire bytes on a specific link without needing a
// man-in-the-middle relay: since the test itself IS one honest (or
// dishonest) endpoint, whatever it sends is already "captured" — no
// interception required.

struct FakePeer {
    victim_id: NodeId,
    victim_addr: SocketAddr,
    sock: UdpSocket,
    transport: SharedTransport,
    recv: ReceiveHalf,
    /// Datagrams read while hunting for a different kind — see
    /// `crypto_cluster.rs`'s `CryptoHarness::stash` for the identical
    /// rationale (the victim can emit more than one kind per duty cycle).
    stash: Vec<Vec<u8>>,
}

impl FakePeer {
    /// `claim_id` is embedded as this peer's own identity in every handshake
    /// it initiates (`SharedTransport::new`'s `self_id`) — independent of
    /// `allow`, which is what THIS peer trusts the victim as. Honest peers
    /// pass `claim_id == private`'s "real" id; the impostor tests pass a
    /// `claim_id` that does NOT match the key, or does not match the address
    /// slot the victim has that id registered at.
    fn new(
        dir: &Path,
        tag: &str,
        claim_id: NodeId,
        private: [u8; 32],
        victim_id: NodeId,
        victim_addr: SocketAddr,
        victim_pub: [u8; 32],
    ) -> Self {
        Self::new_at(dir, tag, claim_id, private, victim_id, victim_addr, victim_pub, "127.0.0.1:0".parse().unwrap())
    }

    /// As [`FakePeer::new`], but binds a SPECIFIC local address rather than
    /// an ephemeral port — for the identity-slot attacks, where what makes
    /// the attack the attack is occupying the exact address a victim's
    /// member table has registered for a DIFFERENT id than the one this
    /// peer claims.
    #[allow(clippy::too_many_arguments)]
    fn new_at(
        dir: &Path,
        tag: &str,
        claim_id: NodeId,
        private: [u8; 32],
        victim_id: NodeId,
        victim_addr: SocketAddr,
        victim_pub: [u8; 32],
        bind_addr: SocketAddr,
    ) -> Self {
        let key_path = dir.join(format!("{tag}.key"));
        write_key_file(&key_path, private);
        let allowlist_path = dir.join(format!("{tag}.allowlist"));
        write_allowlist(&allowlist_path, &[(victim_id, victim_pub)]);
        let cfg = CryptoConfig::Enabled {
            key_path,
            allowlist_path,
            rotation: uc2_crypto::rotation::RotationPolicy::default(),
        };
        let transport = SharedTransport::new(&cfg, claim_id).unwrap().unwrap();
        let recv = transport.receive_half();
        let sock = UdpSocket::bind(bind_addr).expect("bind the exact requested local address");
        sock.set_nonblocking(true).unwrap();
        Self { victim_id, victim_addr, sock, transport, recv, stash: Vec::new() }
    }

    fn addr(&self) -> SocketAddr {
        self.sock.local_addr().unwrap()
    }

    fn send_raw(&self, kind: u8, body: &[u8]) {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + body.len()];
        write_datagram_header(
            &mut d,
            &DatagramHeader { position: 0, leadership_term_id: 0, kind, flags: 0, key_epoch: 0 },
        );
        d[DATAGRAM_HEADER_LEN..].copy_from_slice(body);
        let _ = self.sock.send_to(&d, self.victim_addr);
    }

    fn drain(&self, acts: Vec<HandshakeAction>) {
        for act in acts {
            if let HandshakeAction::Send { kind, body, .. } = act {
                self.send_raw(kind, &body);
            }
        }
    }

    /// Poll for one incoming datagram (handshake bootstrap kinds are fed
    /// straight into the handshake state machine; anything else is
    /// returned raw, or stashed if it is not `want`).
    fn poll_one(&mut self, want: Option<u8>, deadline: Instant) -> Option<Vec<u8>> {
        if let Some(w) = want
            && let Some(i) = self.stash.iter().position(|d| read_datagram_header(d).unwrap().kind == w)
        {
            return Some(self.stash.remove(i));
        }
        let mut buf = [0u8; 2048];
        while Instant::now() < deadline {
            match self.sock.recv_from(&mut buf) {
                Ok((n, _)) => {
                    if n < DATAGRAM_HEADER_LEN {
                        continue;
                    }
                    let h = read_datagram_header(&buf[..n]).unwrap();
                    if matches!(h.kind, DGRAM_KIND_HS_INIT | DGRAM_KIND_HS_RESP) {
                        let now = self.transport.now_ns();
                        let acts = self.transport.on_handshake_message(
                            self.victim_id,
                            h.kind,
                            &buf[DATAGRAM_HEADER_LEN..n],
                            now,
                        );
                        self.drain(acts);
                        continue;
                    }
                    if want == Some(h.kind) || want.is_none() {
                        return Some(buf[..n].to_vec());
                    }
                    self.stash.push(buf[..n].to_vec());
                }
                Err(_) => std::thread::yield_now(),
            }
        }
        None
    }

    /// Drive a real Noise-IK handshake to completion, or give up at
    /// `timeout`. Returns whether `is_established` ended up true — never
    /// panics on refusal, so callers can assert either outcome.
    fn try_establish(&mut self, timeout: Duration) -> bool {
        let now = self.transport.now_ns();
        let acts = self.transport.initiate(self.victim_id, now);
        self.drain(acts);
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.transport.is_established(self.victim_id) {
                return true;
            }
            self.poll_one(None, Instant::now() + Duration::from_millis(50));
        }
        self.transport.is_established(self.victim_id)
    }

    /// Wait for a `REQUEST_VOTE` from the victim, open it under this peer's
    /// established session, and decode its body.
    fn recv_request_vote(&mut self, timeout: Duration) -> Option<RequestVoteBody> {
        let deadline = Instant::now() + timeout;
        let mut d = self.poll_one(Some(DGRAM_KIND_REQUEST_VOTE), deadline)?;
        let n = d.len();
        let len = self.recv.open_slice(self.victim_id, &mut d, n).ok()?;
        Some(read_request_vote_body(&d[DATAGRAM_HEADER_LEN..len]).unwrap())
    }

    /// Seal and send a genuine `VOTE` grant, returning the exact wire bytes
    /// sent — the "capture" the replay attack needs, free: since this peer
    /// IS the sender, no interception is required to obtain them.
    fn grant_vote(&self, term: u32) -> Vec<u8> {
        let mut body = [0u8; uc_protocol::v2::datagram::VOTE_BODY_LEN];
        write_vote_body(&mut body, &VoteBody { term, granted: true });
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + body.len()];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: term,
                kind: DGRAM_KIND_VOTE,
                flags: 0,
                key_epoch: 0,
            },
        );
        d[DATAGRAM_HEADER_LEN..].copy_from_slice(&body);
        self.transport.seal_pairwise_control(DGRAM_KIND_VOTE, self.victim_id, &mut d).expect(
            "sealing a VOTE over an established session must succeed",
        );
        self.sock.send_to(&d, self.victim_addr).unwrap();
        d
    }
}

// -------------------------------------------------------------- attack 1

/// **A replayed `VOTE` cannot be recounted.** Vote-counting is where a
/// replay turns into a split-brain risk: AEAD stops forgery, but not
/// replay, so it is specifically the anti-replay window's job to stop a
/// captured `VOTE` grant from being redelivered.
///
/// This is written to be discriminating even though `ElectionSm`'s own
/// per-term `votes_received` set is ALSO idempotent for an immediate
/// re-grant from the same voter (see `duplicate_voter_grant_counted_once`)
/// — that idempotency is real but is a SEPARATE, consensus-layer defense.
/// The property this test pins is the crypto layer's OWN, independent job:
/// the replayed ciphertext must never even reach `Consensus::feed` a second
/// time. `Node::crypto_stats().dropped_replay` is the direct evidence for
/// that — a weakened replay window would leave the election outcome
/// unchanged (idempotency still catches it) but this counter would stop
/// moving, which is exactly the red this test is built to catch (see the
/// task report's weakened-build proof).
#[test]
fn a_replayed_vote_cannot_be_recounted() {
    let _g = serialize();
    let dir_handle = scratch_dir("replay-vote");
    let dir = dir_handle.path();
    let victim_priv = [0x51u8; 32];
    let voter_priv = [0x52u8; 32];
    let victim_pub = identity_public(dir, "victim-pub-probe", victim_priv);
    let voter_pub = identity_public(dir, "voter-pub-probe", voter_priv);

    // Bind the victim's socket first so `FakePeer`s can address it, and
    // reserve a THIRD member slot that never comes up (majority is 2 of 3,
    // so the victim's self-vote + the one honest peer's grant is enough —
    // matches `crypto_cluster.rs`'s "one member never comes up" precedent).
    let dead_slot = UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let victim_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let victim_addr = victim_sock.local_addr().unwrap();

    let mut peer1 = FakePeer::new(dir, "peer1", 1, voter_priv, 0, victim_addr, victim_pub);
    let members = vec![(0, victim_addr), (1, peer1.addr()), (2, dead_slot)];
    let allowlist = [(1u32, voter_pub)];
    let key_path = dir.join("victim.key");
    write_key_file(&key_path, victim_priv);
    let allowlist_path = dir.join("victim.allowlist");
    write_allowlist(&allowlist_path, &allowlist);
    let cfg = victim_config(0, members, dir, key_path, allowlist_path, FaultConfig::default());
    let victim = Node::start_with_socket(cfg, victim_sock).expect("victim boots");

    assert!(peer1.try_establish(Duration::from_secs(10)), "the honest peer must establish");

    let rv = peer1
        .recv_request_vote(Duration::from_secs(10))
        .expect("the victim must campaign and send REQUEST_VOTE");
    let mut vote_bytes = peer1.grant_vote(rv.new_term);

    // Keep answering every further campaign until one grant lands INSIDE its
    // 60-120ms election window. A grant for a term the victim has already
    // campaigned past is (correctly) dropped by `ElectionSm`'s stale-term
    // guard, and a peer that granted only once would then never answer again
    // — so a single ill-timed >120ms scheduling stall (CI vCPU steal)
    // between the RV and the grant wedged this test permanently (nightly
    // 2026-08-18..20; reproduced deterministically with an injected 150ms
    // sleep before a one-shot grant). `vote_bytes` ends as the grant that
    // actually counted — exactly the ciphertext the replay attack must
    // replay. Any RV we receive was sent by a candidate that had already
    // abandoned every earlier term, so a late grant can depose nobody.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !victim.is_leader() {
        assert!(Instant::now() < deadline, "the victim never won its own election");
        if let Some(rv) = peer1.recv_request_vote(Duration::from_millis(20)) {
            vote_bytes = peer1.grant_vote(rv.new_term);
        }
    }
    let term_before = victim.current_term();
    let replay_before = victim.crypto_stats().dropped_replay.load(Relaxed);

    // Replay the EXACT captured ciphertext, several times. Sender identity
    // on this wire is resolved by UDP SOURCE ADDRESS (`CryptoIntake::
    // peer_ids`, an address->NodeId map — never a field inside the
    // payload), so a faithful on-path replay of a captured packet keeps its
    // original source address, exactly as retransmitting the literal
    // captured bytes off the wire would (an attacker relaying a captured
    // packet is not opening a new socket of its own at a fresh port — it is
    // re-emitting the same packet, source address included). Replaying from
    // an unrelated address instead would just be a DIFFERENT attack
    // (`dropped_unknown_peer`), not this one.
    const REPLAYS: usize = 5;
    for _ in 0..REPLAYS {
        peer1.sock.send_to(&vote_bytes, victim_addr).unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while victim.crypto_stats().dropped_replay.load(Relaxed) < replay_before + REPLAYS as u64 {
        assert!(
            Instant::now() < deadline,
            "replayed VOTE bytes were not all rejected as replays (got {}, want >= {})",
            victim.crypto_stats().dropped_replay.load(Relaxed),
            replay_before + REPLAYS as u64
        );
        std::thread::yield_now();
    }

    assert!(victim.is_leader(), "the replay must not have disrupted leadership");
    assert_eq!(victim.current_term(), term_before, "the replay must not have moved the term");

    victim.stop();
}

// -------------------------------------------------------------- attack 2

/// **A peer removed from the allowlist cannot re-establish.**
///
/// Uses two INDEPENDENT `Peers`/session lifetimes for the same real key
/// (`peer_v1` then `peer_v2`) rather than reusing one already-established
/// `FakePeer`, deliberately: an already-`current` session has no liveness
/// check (a documented, deferred T6 finding — `Peers::tick` only restarts a
/// session when `current.is_none()`), so revocation does not retroactively
/// tear down a session that was ALREADY up. That is a different, harder
/// property this test does not claim. What it claims — and what the brief
/// asks — is narrower and just as real: a FRESH dial attempt, after
/// revocation, using the peer's genuine (now-revoked) key, must fail. Using
/// a second, independent `SharedTransport` for the same key makes that
/// claim unambiguous instead of confounding it with session persistence.
#[test]
fn a_peer_removed_from_the_allowlist_cannot_re_establish() {
    let _g = serialize();
    let dir_handle = scratch_dir("revoke-peer");
    let dir = dir_handle.path();
    let victim_priv = [0x61u8; 32];
    let peer_priv = [0x62u8; 32];
    let victim_pub = identity_public(dir, "victim-pub", victim_priv);
    let peer_pub = identity_public(dir, "peer-pub", peer_priv);

    let dead_slot = UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let victim_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let victim_addr = victim_sock.local_addr().unwrap();

    let mut peer_v1 = FakePeer::new(dir, "peer-v1", 2, peer_priv, 0, victim_addr, victim_pub);
    let members = vec![(0, victim_addr), (1, dead_slot), (2, peer_v1.addr())];
    let key_path = dir.join("victim.key");
    write_key_file(&key_path, victim_priv);
    let allowlist_path = dir.join("victim.allowlist");
    // T14 review, I-2: id 1 (`dead_slot`) MUST be allowlisted with SOME
    // well-formed key even though nothing ever legitimately dials from it.
    // Without this, the victim's own proactive `gossip_targets` dialing
    // (`Peers::initiate`) refuses id 1 on its OWN allowlist-presence check
    // every retry cycle (a few times a second) and bumps
    // `crypto_handshake_failures` purely as background noise, unrelated to
    // any attack — the reviewer reproduced `delta=3` over this test's own
    // window with NO attacker present at all, which would have made the
    // "actively refused" assertion below pass regardless of whether the
    // revocation check does anything. With id 1 allowlisted, `initiate`
    // proceeds normally (an unanswered `HS_INIT` into a dead address is
    // silent retries, never a `Failed` action), so `crypto_handshake_
    // failures` moves ONLY when this test's own revoked-peer dial is
    // refused.
    let dead_slot_dummy_pub = [0x33u8; 32]; // never a real key; nothing ever authenticates as id 1
    write_allowlist(&allowlist_path, &[(1u32, dead_slot_dummy_pub), (2u32, peer_pub)]);
    let cfg =
        victim_config(0, members.clone(), dir, key_path, allowlist_path.clone(), FaultConfig::default());
    let victim = Node::start_with_socket(cfg, victim_sock).expect("victim boots");

    // Proves the key is genuinely legitimate (not vacuously-invalid fixture
    // material) before it is revoked.
    assert!(peer_v1.try_establish(Duration::from_secs(10)), "the real key must establish while allowlisted");
    assert!(victim.has_crypto_session_with(2));

    // Revoke: rewrite the allowlist file WITHOUT node 2's entry. Id 1's
    // dummy entry STAYS — dropping it here would reopen exactly the I-2
    // background-noise hole (the victim's own dial to `dead_slot` would
    // start refusing on ITS OWN allowlist check again, contaminating the
    // very delta this test measures right after this point).
    write_allowlist(&allowlist_path, &[(1u32, dead_slot_dummy_pub)]);
    // `Allowlist::reload_if_stale`'s minimum interval is 1s (identity.rs);
    // the live node's crypto-maintenance pass polls it every 20ms, so
    // sleeping past 1s guarantees at least one reload attempt has run.
    std::thread::sleep(Duration::from_millis(1_200));

    let failures_before = victim.crypto_handshake_failures();
    // A FRESH transport/session lifetime, same real (now-revoked) key —
    // bound to id 2's EXACT registered address (peer_v1's), not merely
    // some other allowlisted-looking address: the victim resolves sender
    // identity by transport source address, so a fresh dial from a
    // DIFFERENT address would be caught by `dropped_unknown_peer` instead,
    // which would prove nothing about the allowlist check this test
    // targets. Free peer_v1's port first so peer_v2 can take the exact
    // same one.
    let peer2_slot = peer_v1.addr();
    drop(peer_v1);
    let mut peer_v2 =
        FakePeer::new_at(dir, "peer-v2", 2, peer_priv, 0, victim_addr, victim_pub, peer2_slot);
    assert_eq!(peer_v2.addr(), peer2_slot, "must redial from id 2's exact registered slot");
    let established = peer_v2.try_establish(Duration::from_secs(5));

    assert!(!established, "a revoked peer's fresh dial must not reach Established");
    assert!(!peer_v2.transport.is_established(0), "the initiator side must also see no session");
    assert!(
        victim.crypto_handshake_failures() > failures_before,
        "the victim must have ACTIVELY refused the handshake, not just stayed silent"
    );

    victim.stop();
}

/// T14 review, I-2 regression pin: the SAME fixture as the test above,
/// minus the attack — proving `crypto_handshake_failures` has zero
/// background rate on its own (id 1's dead slot is now allowlisted with a
/// dummy key precisely so the victim's own proactive dialing never refuses
/// itself). Before this fix the reviewer measured `delta=3` here with NO
/// attacker present at all, which meant the "actively refused" assertion in
/// the test above could never have failed regardless of whether revocation
/// worked. If this test ever goes red, the fix above has regressed and that
/// assertion is worthless again.
#[test]
fn a_revoked_peer_fixture_has_zero_handshake_failure_background_rate() {
    let _g = serialize();
    let dir_handle = scratch_dir("revoke-peer-control");
    let dir = dir_handle.path();
    let victim_priv = [0x61u8; 32];
    let peer_priv = [0x62u8; 32];
    let victim_pub = identity_public(dir, "victim-pub", victim_priv);
    let peer_pub = identity_public(dir, "peer-pub", peer_priv);

    let dead_slot = UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let victim_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let victim_addr = victim_sock.local_addr().unwrap();

    let peer_v1 = FakePeer::new(dir, "peer-v1", 2, peer_priv, 0, victim_addr, victim_pub);
    let members = vec![(0, victim_addr), (1, dead_slot), (2, peer_v1.addr())];
    let key_path = dir.join("victim.key");
    write_key_file(&key_path, victim_priv);
    let allowlist_path = dir.join("victim.allowlist");
    let dead_slot_dummy_pub = [0x33u8; 32];
    write_allowlist(&allowlist_path, &[(1u32, dead_slot_dummy_pub), (2u32, peer_pub)]);
    let cfg =
        victim_config(0, members.clone(), dir, key_path, allowlist_path.clone(), FaultConfig::default());
    let victim = Node::start_with_socket(cfg, victim_sock).expect("victim boots");

    // Let the victim settle (its own handshakes with peer_v1's real slot
    // complete) before sampling, then watch for the same ~5s window the
    // real attack's dial takes.
    std::thread::sleep(Duration::from_millis(1_200));
    let f0 = victim.crypto_handshake_failures();
    std::thread::sleep(Duration::from_secs(5));
    let f1 = victim.crypto_handshake_failures();
    assert_eq!(f1, f0, "background handshake-failure rate must be zero with no attacker present");

    victim.stop();
}

/// **An impostor cannot borrow a peer's identity slot** — the Task 6
/// finding this brief calls out by name: a Noise-IK responder learns the
/// initiator's static key from message 1, so "the key decrypts" is not
/// proof of identity. Two sub-attacks:
///
/// (a) a brand-new, nowhere-allowlisted keypair claims the id that matches
///     its OWN address slot exactly (claimed id and transport source
///     agree) — refused by the key-vs-allowlist check
///     (`get_remote_static()` vs `Allowlist::lookup`): the key itself is
///     wrong for that slot.
/// (b) a real, validly-allowlisted-under-a-different-id key claims ITS OWN
///     true id while physically sending from the address slot the victim
///     has registered for a DIFFERENT id — refused, but **not proven to be
///     isolated to the claimed-id-vs-transport-source binding check** (see
///     the correction at its construction site below; this was originally
///     over-claimed and was corrected during T14 review, I-3).
#[test]
fn an_impostor_cannot_borrow_a_peers_identity_slot() {
    let _g = serialize();
    let dir_handle = scratch_dir("impostor");
    let dir = dir_handle.path();
    let victim_priv = [0x71u8; 32];
    let peer2_priv = [0x73u8; 32]; // genuinely allowlisted under id 2
    let victim_pub = identity_public(dir, "victim-pub", victim_priv);
    let peer2_pub = identity_public(dir, "peer2-pub", peer2_priv);

    let victim_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let victim_addr = victim_sock.local_addr().unwrap();

    // The victim's OWN id is deliberately the HIGHEST in this 2-member
    // config (9 > 2). `handshake.rs`'s simultaneous-open tie-break
    // ("lower id wins, higher id's inbound message 1 is silently dropped
    // THIS round — `Vec::new()`, no `Failed` action") is a real, separate,
    // correct mechanism, but it is orthogonal to what this test targets and
    // would otherwise race with it: the victim's own crypto-maintenance
    // pass proactively dials every gossip target (`gossip_targets`, every
    // 20ms), including id 2's slot, so by the time an attacker's message 1
    // arrives there is very likely an in-flight outbound attempt on the
    // victim's side already — and if the victim's id happened to be the
    // LOWER one, that inbound message would be silently swallowed by the
    // tie-break before ever reaching the claimed-id check, producing a
    // false pass for the wrong reason. Making the victim's id the higher
    // one removes that race: `self_id < from` is never true, so the
    // tie-break never intercepts, and every attack reaches the real check.
    let slot2_addr = UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let members = vec![(9, victim_addr), (2, slot2_addr)];
    let key_path = dir.join("victim.key");
    write_key_file(&key_path, victim_priv);
    let allowlist_path = dir.join("victim.allowlist");
    // Genuinely allowlisted under its TRUE id — the allowlist alone must
    // not be enough to let either impostor in.
    write_allowlist(&allowlist_path, &[(2u32, peer2_pub)]);
    let cfg = victim_config(9, members, dir, key_path, allowlist_path.clone(), FaultConfig::default());
    let victim = Node::start_with_socket(cfg, victim_sock).expect("victim boots");

    // Sub-attack (a): a fresh, unrelated keypair claims id 2 while sitting
    // in id 2's OWN registered slot (claimed id and transport source
    // AGREE) — must still be refused, because the key does not match what
    // the allowlist says id 2's key is.
    let f0 = victim.crypto_handshake_failures();
    let random_priv = [0x99u8; 32];
    let mut impostor_a =
        FakePeer::new_at(dir, "impostor-a", 2, random_priv, 9, victim_addr, victim_pub, slot2_addr);
    assert_eq!(impostor_a.addr(), slot2_addr, "must occupy id 2's exact registered slot");
    let est_a = impostor_a.try_establish(Duration::from_secs(4));
    assert!(!est_a, "a key that does not match id 2's allowlisted key must be refused");
    assert!(!victim.has_crypto_session_with(2));
    let f1 = victim.crypto_handshake_failures();
    assert!(f1 > f0, "sub-attack (a) must have been actively refused, not just silently absent");
    drop(impostor_a); // free slot2_addr's port for sub-attack (b)

    // Sub-attack (b): peer 1's real, genuinely-allowlisted-under-id-1 key,
    // claiming its own true id (1) but sent from the address slot the
    // victim has registered for id 2.
    //
    // CORRECTED CLAIM (T14 review, I-3 — the original comment here claimed
    // this "isolates" the claimed-id-vs-transport-source binding check from
    // the key-vs-allowlist check; that is WRONG and was disproven by the
    // reviewer). `on_init`'s `expected_public` is looked up by `from` — the
    // TRANSPORT-resolved id (2 here), never the claimed id — so adding an
    // allowlist entry for id 1 changes nothing about what this attempt is
    // checked against: `expected_public` is still peer 2's key, and the
    // presented key is peer 1's, so the key-vs-allowlist check ALSO refuses
    // this attempt on its own. The reviewer confirmed by deleting
    // `claimed_id != from` from both `on_init` and `on_resp`: all tests in
    // this file stay green, and the refusal reason silently changes from
    // "claims a different node id" to "static key does not match the
    // allowlist" — proving the claimed-id binding is NOT what this
    // sub-attack's assertions actually depend on.
    //
    // This sub-attack therefore still demonstrates a real, useful property
    // (a real key at the wrong slot is refused, full stop) but does NOT
    // isolate the claimed-id-vs-transport-source check specifically. Doing
    // that would require an attacker who already holds the REAL key for the
    // slot they occupy (id 2's real key here) while claiming a different
    // id — which means holding a genuine cluster member's private key, the
    // "malicious cluster member" case this file's own threat model
    // explicitly places out of scope. Whether that check is independently
    // isolatable AT ALL within the stated threat model (attacker holds no
    // node's private key) is therefore left an open question rather than
    // forced into a test that would not actually mean what its name claims.
    let peer1_priv = [0x74u8; 32];
    let peer1_pub = identity_public(dir, "peer1-pub", peer1_priv);
    write_allowlist(&allowlist_path, &[(1u32, peer1_pub), (2u32, peer2_pub)]);
    // Force the reload the victim's own maintenance pass will otherwise
    // pick up on its own cadence (same 1s minimum interval as the
    // revocation test).
    std::thread::sleep(Duration::from_millis(1_200));

    let mut impostor_b =
        FakePeer::new_at(dir, "impostor-b", 1, peer1_priv, 9, victim_addr, victim_pub, slot2_addr);
    assert_eq!(impostor_b.addr(), slot2_addr, "must occupy id 2's exact registered slot");
    // Not asserted: `impostor_b.transport.is_established(9)`. The victim's
    // OWN proactive dial to id 2's slot (`gossip_targets`, unrelated to
    // this attack) is answered by whoever occupies that slot — impostor_b
    // legitimately recognizes the victim's real key as RESPONDER to THAT
    // exchange and may show established from its own local point of view.
    // That is correct Noise-IK behavior, not a security hole: the property
    // that matters is what the VICTIM concludes, never what the attacker's
    // own bookkeeping shows.
    impostor_b.try_establish(Duration::from_secs(4));
    assert!(!victim.has_crypto_session_with(1), "must never be admitted as id 1");
    assert!(!victim.has_crypto_session_with(2), "must never be admitted as id 2 under this key either");
    let f2 = victim.crypto_handshake_failures();
    assert!(f2 > f1, "sub-attack (b) must have been actively refused, not just silently absent");

    victim.stop();
}

// -------------------------------------------------------------- attack 3

/// **A downgrade to cleartext is refused.** With crypto on, an unsealed
/// datagram must not reach the log buffer (`DGRAM_KIND_DATA`, a
/// self-locating stream write) by any path — neither a short,
/// cleartext-shaped forgery nor a longer one that merely LOOKS like it could
/// be sealed.
#[test]
fn a_downgrade_to_cleartext_is_refused() {
    let _g = serialize();
    let dir_handle = scratch_dir("cleartext-downgrade");
    let dir = dir_handle.path();
    let victim_priv = [0x81u8; 32];
    let attacker_addr_holder = UdpSocket::bind("127.0.0.1:0").unwrap();
    let registered_addr = attacker_addr_holder.local_addr().unwrap();
    drop(attacker_addr_holder); // free the port; we just wanted a real, otherwise-unused addr

    let victim_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let victim_addr = victim_sock.local_addr().unwrap();
    let members = vec![(0, victim_addr), (1, registered_addr)];
    let key_path = dir.join("victim.key");
    write_key_file(&key_path, victim_priv);
    let allowlist_path = dir.join("victim.allowlist");
    // The allowlist entry for id 1 does not even need to be a REAL key here —
    // the attacker below never attempts a handshake at all, it only injects
    // DATA-shaped bytes claiming to originate from that registered address.
    write_allowlist(&allowlist_path, &[(1u32, [0x22; 32])]);
    let cfg = victim_config(0, members, dir, key_path, allowlist_path, FaultConfig::default());
    let victim = Node::start_with_socket(cfg, victim_sock).expect("victim boots");

    let append_before = victim.counters().append.load_acquire();
    let auth_failed_before = victim.crypto_stats().dropped_auth_failed.load(Relaxed);
    let cleartext_before = victim.crypto_stats().peer_appears_cleartext.load(Relaxed);
    let unknown_peer_before = victim.crypto_stats().dropped_unknown_peer.load(Relaxed);

    // Case A: short, cleartext-SHAPED forgery from the registered (but never
    // handshaken) address — trips the `peer_appears_cleartext` diagnostic
    // specifically (key_epoch=0 AND shorter than any real sealed frame).
    let registered = UdpSocket::bind(registered_addr).expect("rebind the reserved address");
    let mut short = vec![0u8; DATAGRAM_HEADER_LEN + 4];
    write_datagram_header(
        &mut short,
        &DatagramHeader {
            position: append_before,
            leadership_term_id: 0,
            kind: DGRAM_KIND_DATA,
            flags: 0,
            key_epoch: 0,
        },
    );
    short[DATAGRAM_HEADER_LEN..].copy_from_slice(b"evil");
    assert!(short.len() < DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD, "must be shorter than any real seal");
    registered.send_to(&short, victim_addr).unwrap();

    // Case B: a LONGER forgery that merely looks like it could be sealed
    // (long enough) but carries no real AEAD tag/counter — must fail
    // authentication, not merely the length heuristic.
    let mut long = vec![0u8; DATAGRAM_HEADER_LEN + 200];
    write_datagram_header(
        &mut long,
        &DatagramHeader {
            position: append_before,
            leadership_term_id: 0,
            kind: DGRAM_KIND_DATA,
            flags: 0,
            key_epoch: 0,
        },
    );
    long[DATAGRAM_HEADER_LEN..].fill(0x5A);
    registered.send_to(&long, victim_addr).unwrap();

    // Case C: same payload from a totally unregistered address.
    let stranger = UdpSocket::bind("127.0.0.1:0").unwrap();
    stranger.send_to(&long, victim_addr).unwrap();

    std::thread::sleep(Duration::from_millis(500));

    assert_eq!(
        victim.counters().append.load_acquire(),
        append_before,
        "an unauthenticated DATA-shaped datagram must never advance the log buffer"
    );
    assert!(
        victim.crypto_stats().peer_appears_cleartext.load(Relaxed) > cleartext_before
            || victim.crypto_stats().dropped_auth_failed.load(Relaxed) > auth_failed_before,
        "case A/B must have been actively rejected by the crypto intake, not silently ignored \
         upstream"
    );
    assert!(
        victim.crypto_stats().dropped_unknown_peer.load(Relaxed) > unknown_peer_before,
        "case C (unregistered sender) must have been rejected too"
    );

    // Case D — T14 review, I-4: the brief's OWN stated property is "must
    // not reach the log buffer OR THE CONSENSUS EVENT ROUTE" (emphasis the
    // brief's), and cases A-C only ever exercise the log buffer. Kinds 5-11
    // (APPEND_POSITION/COMMIT_POSITION/REQUEST_VOTE/VOTE/TERM_MAP/
    // READ_PROBE/READ_PROBE_ACK) are forwarded to the consensus agent RAW,
    // before DATA's incidental (and here irrelevant) term-staleness gate —
    // so a cleartext-downgrade bypass that happened to leave DATA harmless
    // (case A/B's `append` assertion can pass for the WRONG reason: a
    // forged `leadership_term_id: 0` gets absorbed by `dropped_stale_term`
    // regardless of whether crypto ever ran) would still be free to hijack
    // an election via this route. A cleartext `REQUEST_VOTE` claiming a
    // term far beyond anything a short test window could reach by natural
    // election churn (this victim never has a real quorum partner, so it
    // legitimately re-campaigns and its own term DOES grow on its own —
    // this must tolerate that, not assume a frozen term).
    let term_before_d = victim.current_term();
    let injected_term = term_before_d + 1000;
    let mut rv_body = [0u8; uc_protocol::v2::datagram::REQUEST_VOTE_BODY_LEN];
    uc_protocol::v2::datagram::write_request_vote_body(
        &mut rv_body,
        &RequestVoteBody { new_term: injected_term, last_term: 0, last_durable: 0 },
    );
    let mut rv_dgram = vec![0u8; DATAGRAM_HEADER_LEN + rv_body.len()];
    write_datagram_header(
        &mut rv_dgram,
        &DatagramHeader {
            position: 0,
            leadership_term_id: injected_term,
            kind: DGRAM_KIND_REQUEST_VOTE,
            flags: 0,
            key_epoch: 0,
        },
    );
    rv_dgram[DATAGRAM_HEADER_LEN..].copy_from_slice(&rv_body);
    assert!(
        rv_dgram.len() < DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD,
        "must be shorter than any real seal, same shape as case A"
    );
    registered.send_to(&rv_dgram, victim_addr).unwrap();

    std::thread::sleep(Duration::from_millis(500));
    assert!(
        victim.current_term() < term_before_d + 500,
        "a cleartext REQUEST_VOTE must not hijack the term toward the injected value (before {}, \
         injected {}, now {})",
        term_before_d,
        injected_term,
        victim.current_term()
    );

    victim.stop();
}

// -------------------------------------------------------------- attack 4

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 {
        self.total
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

fn spawn_storm_cluster(faults: FaultConfig) -> (tempfile::TempDir, Vec<Node>, Vec<PathBuf>) {
    let n = 3;
    let dir = tempfile::Builder::new()
        .prefix("uc2-adv-storm-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    assert!(!dir.path().starts_with("/tmp"));

    let mut privs = Vec::with_capacity(n);
    let mut publics = Vec::with_capacity(n);
    for i in 0..n {
        let p = [0xB0u8 + i as u8; 32];
        publics.push(identity_public(dir.path(), &format!("storm{i}"), p));
        privs.push(p);
    }
    let socks: Vec<UdpSocket> = (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").unwrap()).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as u32, s.local_addr().unwrap())).collect();
    let allowlist: Vec<(NodeId, [u8; 32])> =
        publics.iter().enumerate().map(|(i, p)| (i as u32, *p)).collect();

    let mut nodes = Vec::with_capacity(n);
    let mut dirs = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let key_path = dir.path().join(format!("storm{i}.key"));
        write_key_file(&key_path, privs[i]);
        let allowlist_path = dir.path().join(format!("storm{i}.allowlist"));
        write_allowlist(&allowlist_path, &allowlist);
        let instance_dir = dir.path().join(format!("n{i}"));
        dirs.push(instance_dir.clone());
        // T14 review, M-7: every node's `FaultSocket` otherwise shared the
        // SAME `faults.seed`, so their corrupt/replay draw sequences were
        // correlated (each XorShift64 instance starts from identical
        // state) rather than independent — derive a distinct per-node seed
        // the same way `crypto_cluster.rs`'s own election-seed derivation
        // does, so the three nodes' hostile-wire noise is uncorrelated.
        let node_faults = FaultConfig {
            seed: faults.seed ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            ..faults
        };
        let cfg = NodeConfig {
            id: i as u32,
            members: members.clone(),
            learners: Vec::new(),
            bind: "127.0.0.1:0".parse().unwrap(),
            instance_dir,
            app_id: APP.into(),
            buffer_bytes: 1 << 22,
            max_payload: 256,
            admission_bytes: 256 * 1024,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 0xA1B2_C3D4 ^ i as u64,
            faults: node_faults,
            purge: uc2_node::PurgePolicy::Disabled,
            journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: CryptoConfig::Enabled {
                key_path,
                allowlist_path,
                rotation: uc2_crypto::rotation::RotationPolicy::default(),
            },
            services: uc2_node::ServicesConfig::default(),
        };
        nodes.push(Node::start_with_socket(cfg, sock).expect("storm node boots"));
    }
    (dir, nodes, dirs)
}

/// **Heavy corruption + replay storm never panics and never diverges.** The
/// binding rule for the whole milestone: a node must not be killable by a
/// datagram. `corrupt_per_million`/`replay_per_million` are applied to every
/// node's OWN outbound sends (self-inflicted hostile-wire noise, the same
/// mechanism the seeded drop/dup/reorder faults already use) — every DATA,
/// HEARTBEAT, VOTE, TERM_MAP, COMMIT_POSITION, handshake and group-key
/// datagram in the whole cluster is subject to it.
///
/// A real client submits load against whichever node is currently serving,
/// tolerating leader churn (heavy corruption of REQUEST_VOTE/VOTE/TERM_MAP
/// is exactly what would produce it). If the process is still standing and
/// every node's committed log content agrees at the end, the property
/// holds — irrespective of how much throughput the storm cost.
#[test]
fn heavy_corruption_and_replay_injection_never_panics_and_never_diverges() {
    let _g = serialize();
    let faults = FaultConfig {
        seed: 5,
        corrupt_per_million: 300_000,
        replay_per_million: 300_000,
        ..Default::default()
    };
    let (_dir, mut nodes, dirs) = spawn_storm_cluster(faults);

    // Best-effort load: submit against whichever node currently claims to
    // serve, tolerating failures (a corrupted/replayed REQUEST_VOTE/VOTE
    // storm is expected to churn leadership and stall some submits).
    // Fewer than the brief's illustrative 10_000 — chosen so the run
    // completes in a bounded wall-clock budget on this box under 30%
    // corrupt + 30% replay on every send; see the task report.
    const TARGET_COMMANDS: usize = 400;
    let run_deadline = Instant::now() + Duration::from_secs(90);
    let mut submitted = 0usize;
    let mut client: Option<Client> = None;
    let mut svc_holder: Option<uc2_service::Service<CountSm>> = None;
    while submitted < TARGET_COMMANDS && Instant::now() < run_deadline {
        let leader = nodes.iter().position(|n| n.can_serve());
        let Some(leader) = leader else {
            std::thread::yield_now();
            continue;
        };
        if client.is_none() {
            let d = dirs[leader].clone();
            match ServiceBuilder::new(ServiceConfig::new(&d, APP), CountSm::default()).start() {
                Ok(svc) => {
                    svc_holder = Some(svc);
                    client = Client::connect(&d, APP).ok();
                }
                Err(_) => {
                    std::thread::yield_now();
                    continue;
                }
            }
        }
        if let Some(c) = &client {
            let r: Result<u64, _> = c.submit(&Cmd::Add(1));
            if r.is_ok() {
                submitted += 1;
            } else {
                // The leader we attached to may have stepped down under the
                // storm; drop the client/service and reattach next loop.
                client = None;
                svc_holder = None;
            }
        }
    }
    drop(client);
    drop(svc_holder);

    // The process must still be standing — a panicked agent thread would
    // have already torn down the process (Rust's default panic behavior in
    // a non-`catch_unwind` background thread aborts the WHOLE test binary
    // under this harness's configuration; reaching this line at all is part
    // of the evidence). `Node::stop()` additionally joins every agent
    // thread, which would propagate a panic here if one occurred but did
    // not already abort the process.
    for n in &nodes {
        let _ = n.truncations();
        let _ = n.wipes();
    }

    // T14 review, C-1 (CRITICAL, fixed): the floor of bytes a node has BOTH
    // quorum-committed AND durably recorded to its OWN log — what `apply`
    // actually reads (`min(commit, durable)`), never raw gossiped `commit`
    // alone. `Event::CommitGossip` sets `commit_seen`/emits `AdvanceCommit`
    // with NO clamp to the receiver's own `durable`/`append` (every real
    // consumer clamps downstream instead), so "commit >= target" does NOT
    // mean a node holds those bytes yet — it can legitimately be up to one
    // fsync/replication cycle behind its own gossiped commit. Reading past
    // `durable` there is reading never-written buffer, not divergence.
    fn readable_floor(n: &Node) -> u64 {
        let c = n.counters();
        c.commit.load_acquire().min(c.durable.load_acquire())
    }

    // Quiesce: give the cluster a window to settle on a single leader and
    // converge readable floors, without the storm having been turned off
    // — the property is "never diverges under load," not "converges once
    // calm."
    let settle_deadline = Instant::now() + Duration::from_secs(60);
    let target = loop {
        if let Some(leader) = nodes.iter().position(|n| n.can_serve()) {
            let target = readable_floor(&nodes[leader]);
            if target > 0 {
                break target;
            }
        }
        assert!(
            Instant::now() < settle_deadline,
            "no serving leader with committed+durable bytes ever emerged"
        );
        std::thread::yield_now();
    };
    loop {
        let laggards: Vec<usize> =
            (0..nodes.len()).filter(|&i| readable_floor(&nodes[i]) < target).collect();
        if laggards.is_empty() {
            break;
        }
        assert!(
            Instant::now() < settle_deadline,
            "nodes {laggards:?} never converged to a readable floor of {target} under the storm"
        );
        std::thread::yield_now();
    }

    // Divergence check: every node's committed frame content up to the
    // converged floor must agree byte-for-byte. Walks REAL frame
    // boundaries via `align_frame_len(header.length)` — T14 review, C-1
    // (CRITICAL, fixed): the original version strode a fixed 256 bytes,
    // which violates `read_frame_validated`'s own documented contract
    // ("pos must be a frame start"; a mid-frame `pos` "misreads a payload
    // byte as the length word ... garbage") and only accidentally landed on
    // real frame starts when every frame happened to be exactly 256/n
    // bytes. Leader churn under the storm appends 32-byte `NEW_TERM`
    // frames; an odd count of those shifts every later frame off any fixed
    // stride, and the resulting garbage read (`len_read=256, type_read=0`
    // in the diagnosed failure — neither a real length nor a valid
    // `FRAME_TYPE`) spanned mid-frame past one node's real `append` into
    // its never-initialized buffer tail, manufacturing a "divergence" that
    // was never real.
    /// Reads a frame at `pos` on `n`, briefly retrying `NotCommitted`/
    /// `Overrun` before treating it as a genuine anomaly. `pos < target`
    /// (every node's own converged `min(commit, durable)`) SHOULD make
    /// this always succeed on the first try — a node reporting otherwise
    /// there is exactly the ambiguity the ORIGINAL, pre-fix `target`
    /// computation allowed and this fix removes — but a tiny window
    /// between a counter's release-store and this thread's next poll is
    /// cheap insurance against turning an unrelated scheduling hiccup into
    /// a false failure, which would itself be a false alarm on the very
    /// property this fix targets.
    fn read_frame_with_retry(n: &Node, pos: u64) -> (uc_protocol::v2::frame::FrameHeader, Vec<u8>) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let mut out = Vec::new();
            match n.read_frame_validated(pos, &mut out) {
                uc2_log::buffer::FrameRead::Frame(h) => return (h, out),
                other => {
                    assert!(
                        Instant::now() < deadline,
                        "node reported {other:?} at position {pos} < converged readable floor, \
                         still not Frame after 2s of retrying"
                    );
                    std::thread::yield_now();
                }
            }
        }
    }

    let mut pos = 0u64;
    let mut frames_compared = 0usize;
    const MAX_FRAMES: usize = 50_000; // generous; a real divergence would show far sooner
    while pos < target && frames_compared < MAX_FRAMES {
        let mut refs: Vec<Vec<u8>> = Vec::with_capacity(nodes.len());
        let mut step: Option<u64> = None;
        for n in &nodes {
            let (h, out) = read_frame_with_retry(n, pos);
            if step.is_none() {
                step = Some(uc_protocol::v2::frame::align_frame_len(h.length as usize) as u64);
            }
            refs.push(out);
        }
        let first = &refs[0];
        for other in &refs[1..] {
            assert_eq!(first, other, "divergent committed content at position {pos}");
        }
        frames_compared += 1;
        let step = step.expect("every branch above either sets `step` or panics");
        assert!(step > 0, "align_frame_len returned 0 at position {pos} — a genuinely corrupt frame");
        pos += step;
    }
    assert!(
        frames_compared > 0,
        "the divergence check never read a single real frame up to the converged floor {target}"
    );

    for n in nodes.drain(..) {
        n.stop();
    }
}
