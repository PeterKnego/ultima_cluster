# UC v2.2 (M8) — wire crypto: authenticated, encrypted node-to-node transport

**Date:** 2026-07-28
**Status:** approved design (brainstorm 2026-07-28); next step = implementation plan
**Baseline:** v2.1.0 (M1–M7 complete; post-M7 follow-up wave and the loose-end
T1–T6 arc discharged). The v2 spec
(`2026-07-09-uc-v2-aeron-shaped-smr-design.md` §"Security posture") records
encryption/auth as an **explicit v2.0 non-goal** — "trusted private network
posture, same as stock Aeron" — with a reserved header slot for a later
per-datagram MAC. This design spends that slot and closes the non-goal.

## 1. Goal and locked decisions

Let a UC cluster replicate across a **network path the operator does not
control** (cross-AZ, VPC peering, cross-region, shared infrastructure), where an
adversary can both read and inject packets. That means confidentiality,
authentication, and replay resistance for all node↔node traffic.

This is also the named prerequisite for two deferred capabilities: the remote
admin surface M7 explicitly refused ("no new remote-admin surface before
wire-crypto") and the async cross-region learner sketched in
`docs/notes/uc2-m7-vs-aeron-cluster-standby-2026-07-24.md`. Neither is in scope
here; M8 only removes the blocker.

Decisions locked during the brainstorm:

| Decision | Choice |
|---|---|
| Threat model | **Untrusted network path.** Adversary reads and injects. Confidentiality + authentication + replay resistance. Not in the model: a compromised host, a malicious cluster member, or side channels. |
| Scope | **Node↔node UDP only.** The local IPC boundary (shmem rings, `cnc.dat`) stays cleartext — same host, already gated by `app_id`/`instance_id` + flock, and sealing it would tax the hot path for nothing. |
| Key model | **ATS-shaped hybrid**: pairwise handshake per peer link for identity and key distribution; a cluster-wide group key for the identical-to-N fan-out. |
| Rollout | **Opt-in, off by default, flag day.** A cluster is all-encrypted or all-cleartext. No mixed mode, no permissive window, no downgrade path to attack. |
| Primitives | **X25519** static identity + ephemeral, HKDF (RFC 5869), **AES-256-GCM** AEAD. RustCrypto family; pure Rust, no C toolchain. |
| Handshake | **Noise `IK` via `snow`** — not hand-rolled. Same primitives underneath. |

> **Correction carried from the brainstorm.** The stack was chosen as
> "Ed25519 identity + X25519 ephemeral" *before* the decision to use Noise. In
> Noise, the static identity key **is** an X25519 key and authentication comes
> from the pattern's DH operations, not from a separate signature — so adopting
> `snow` subsumes the Ed25519 layer rather than composing with it. Keeping a
> separate Ed25519 identity would mean signing the Noise static key with it: a
> second identity to distribute and rotate, buying nothing the pattern does not
> already provide. Ed25519 is therefore dropped, and the allowlist holds
> **X25519 static public keys**. (ATS needs its RSA layer precisely because it
> does *not* use a pattern-authenticated handshake — it signs raw ephemeral EC
> keys to bind them to an identity.)
| Rotation | **On becoming leader + periodic + on a committed `Remove*`.** |
| Header posture | **Cleartext header as AAD** (ATS's shape). Metadata leakage accepted, recorded in §7. |
| Acceptance gate | ≤10% throughput regression vs a back-to-back cleartext control on the M5 path, full local proof stack green with crypto ON. Cross-host fleet gate is a **separate, user-approved** step. |
| Wire protocol | **0.3.0 → 0.4.0.** |

### What Aeron does, and where we differ

Aeron Premium's **Aeron Transport Security (ATS)** is the closest prior art, and
unlike M7 — where the reference implementation had no working design to port —
here there is one worth following.

ATS: each driver holds an RSA identity keypair and an allowlist of peer public
keys (SHA-256 fingerprints); an in-band handshake (`SETUP-ATS` /
`STATUS-SETUP-ATS` / `SETUP-ATS-KEY`) exchanges RSA-signed ephemeral EC keys;
ECDHE + HKDF derive session keys; AES-256-GCM seals per frame, with a cleartext
frame header used as AAD. For multicast/MDC a per-*stream* shared key is
distributed to each receiver individually, encrypted under the pairwise key —
so the data plane seals **once** and fans out. ATS is C-driver-only, requires
OpenSSL 3.0.0, and does not interoperate with non-ATS peers.

Sources: [ATS overview](https://aeron.io/premium-docs/aeron-transport-security/ats-overview.html),
[ATS product page](https://aeron.io/aeron-premium/aeron-transport-security/),
[ATS overview PDF](https://hub.aeron.io/hubfs/Aeron-Assets/Aeron-ATS-Overview-Web-May23.pdf).

We adopt: the hybrid key scoping, the cleartext-header-as-AAD placement, the
public-key allowlist (not a CA/PKI), the all-or-nothing interop stance, and
AES-256-GCM.

We differ in four places:

1. **Modern primitives and a pattern-authenticated handshake.** Noise `IK` over
   X25519 instead of RSA-signed ephemeral EC keys; pure Rust instead of OpenSSL.
   ATS chose OpenSSL because the Aeron C driver was already C; UC has no such
   constraint.
2. **Allowlist keyed by node id**, not by key fingerprint — UC membership is
   already id-based, and M7's fresh-forever ids make an id a durable identity.
3. **We rotate; ATS documents no rotation** (keys re-derive only when an end
   terminates). UC has live membership change, so a removal has to mean
   something on the wire.
4. **Nonce derivation is restart-safe by construction** (§4). ATS's docs do not
   address the hazard; UC's `position` reuse under NAK retransmit and its
   restart-resets-counter path make it unavoidable here.

## 2. Component boundaries

**`uc2_crypto`** — new workspace crate. Pure-sync, no sockets, no async, no I/O
beyond reading key files at construction. Owns:

- identity keypair + peer allowlist (load, reload, lookup);
- the handshake driver wrapping `snow` — a driven transition function (feed a
  message + a monotonic tick, get actions out), the `ElectionSm` shape, so
  `uc2_sim` can adjudicate it deterministically;
- the key schedule: epoch store, per-sender-per-boot derivation, rotation
  policy, overlap retention;
- `seal_in_place` / `open_in_place`;
- the anti-replay window.

**`uc_protocol::v2::crypto`** — wire layouts only: the envelope, handshake
datagram bodies, new `DGRAM_KIND_*` constants. Offsets pinned with
offset-assertion tests, as every other v2 layout is. No crypto code here; the
crate's `core`-friendly posture stands.

**`uc2_net`** — exactly two call seams: seal after `assemble()` before
`send_to`, open after `recv_from` before header dispatch. Nothing else in the
sender/receiver changes shape.

**`uc2_node`** — `NodeConfig.crypto: CryptoConfig`, defaulting to `Disabled`,
mirroring `PurgePolicy::Disabled`'s opt-in precedent; plus the rotation hook
(§5).

## 3. Two key scopes, split by datagram kind

The split is forced by a bootstrap constraint: **elections must work before a
leader exists**, so there is nobody to mint a group key yet.

- **Pairwise keys** (per peer link, from the handshake) seal everything unicast
  or low-rate: `REQUEST_VOTE`, `VOTE`, `NAK`, `STATUS`, `APPEND_POSITION`,
  `READ_PROBE_ACK`, `TERM_MAP`, `SNAP_*`, and the M7 admin kinds 16/17. N seals
  is irrelevant at these rates.
- **The group key** seals exactly the high-rate identical-to-N fan-out kinds:
  `DATA`, `HEARTBEAT`, `COMMIT_POSITION`, `READ_PROBE`. One seal, N sends —
  `fan_out`'s structural batching is preserved, which is the whole point of the
  hybrid.

The rule is **by kind, never by destination**. `serve_nak` and the deep-NAK
journal replay both emit `DATA` to a single peer; they still take the group key.
One branch, no per-call-site reasoning.

## 4. Wire format

The reserved `u16` at `OFF_DGRAM_RESERVED` finally gets its documented use — as
the **key epoch**, not as the MAC the comment anticipated (a 16-bit MAC would be
security theatre; the tag goes at the end where GCM puts it).

```
[ 16B header: position | term_id | kind | flags | key_epoch(u16) ]  <- cleartext, AAD
[  8B nonce counter (u64, per-sender monotonic)                  ]  <- cleartext
[  ciphertext (the former payload)                               ]
[ 16B GCM tag                                                    ]
```

24 bytes of overhead. The payload budget drops from 1392 to 1368 bytes at the
default MTU (~1.7%). `budget = mtu - DATAGRAM_HEADER_LEN` becomes
`mtu - DATAGRAM_HEADER_LEN - CRYPTO_OVERHEAD` in the single place it is
computed, which also resizes snapshot chunks.

### The nonce-reuse hazard

Repeating a nonce under one GCM key is catastrophic — it leaks the
authentication subkey, not merely one plaintext. UC offers two ways to do it:

1. `position` is **not** unique per datagram — a NAK retransmit re-sends the
   same position, and `fan_out` sends byte-identical datagrams to N peers. So no
   nonce may be derived from header fields alone.
2. A process restart would reset a naive counter **while the group key is still
   live cluster-wide**.

Closed by construction: the actual sealing key is derived **per sender, per
boot**:

```
k_send = HKDF(group_key, sender_id ‖ boot_salt)
```

`boot_salt` is fresh-random at every process start and advertised in the
handshake. Every peer can derive the leader's key, so fan-out still seals once;
a restarted node's counters live in a brand-new key space, so reuse is
impossible by construction rather than by discipline. The same derivation
applies to pairwise keys. Counter exhaustion (2^64) is unreachable in practice
and asserted.

The GCM nonce is 96 bits and the counter is 64, so the nonce is
`0u32 ‖ counter` — a fixed zero prefix, not a second varying field. Uniqueness
comes entirely from the key being per-sender-per-boot, which is what makes the
zero prefix safe.

### Anti-replay

An IPsec-style sliding window on the counter, per (sender, epoch). AEAD stops
forgery but not replay, and a replayed `VOTE` or admin datagram is not harmless.
The window keys on **counters, not positions**, so a genuine NAK retransmit
(re-sealed, fresh counter) passes cleanly while a captured-and-resent datagram
does not.

## 5. Handshake, key schedule, rotation

### Identity and allowlist

Each node holds an X25519 static keypair (its Noise identity); the private key
file's mode is checked at boot. Peers are authorized by an allowlist file mapping
**node id → static public key**, one per line, SSH `authorized_keys`-style.

**Learners are peers like any other**: they handshake, receive the group key, and
open fan-out traffic. A `RemoveLearner` tombstones an id and therefore triggers
rotation exactly as `RemoveVoter` does (§5).

M7 adds nodes at runtime, so the allowlist is re-read when an unknown peer id
appears (rate-limited to once per second) and on a slow timer. An operator drops
in a key and `uc2ctl add-learner` works without restarting anything.

### Handshake

**Noise via `snow`**, not a hand-rolled AKE. The sketch this design started from
— signed ephemeral keys, transcript binding, lower-id-wins on simultaneous open
— is Noise `IK`/`XX` with worse review; `snow` is pure Rust over the same
primitives. UC keeps ownership of what Noise does not cover: the group key
schedule, rotation policy, the datagram envelope, and the replay window.

The handshake carries the peer's `boot_salt` and rides the same socket under new
datagram kinds (18/19 handshake, 20 group-key delivery; 16/17 are M7 admin).

**No wall-clock dependency**: freshness comes from random nonces, never
timestamps. Deliberate — the M7 fleet gate's first run failed largely on ssh
clock skew, and a handshake that can fail on skew would manufacture that same
class of ghost failure on a real fleet.

### Group key lifecycle

Minted by the **leader**, delivered per peer over that peer's pairwise channel
(`HS_KEY`), acked per peer. It is **memory-only, never written to disk** — the
only secret at rest is the identity private key.

Activation avoids a liveness trap: the leader seals fan-out under epoch E once
all *reachable* peers have acked E, or after a bounded timeout. A peer that
lacks E fails to open those datagrams and recovers through the **existing NAK
repair path** once `HS_KEY` lands — replication never blocks on a dead peer, and
no new recovery mechanism is introduced. Receivers retain E−1 for a bounded
overlap so in-flight datagrams still open.

### Rotation triggers

1. **On becoming leader.** A new leader always mints a fresh epoch. This one
   rule absorbs a class of edge cases: leader self-removal (the outgoing leader
   steps down at the same commit crossing, so it cannot be the rotator),
   crash-handoff, and any rotation a dead leader missed. Cost: a key change per
   election, which is acceptable because elections are rare.
2. **Periodic** — `rotation_interval` (default 1h) or `rotation_bytes` (default
   1 TiB sealed under one epoch), whichever comes first.

Epoch is a `u16` and **wraps**; comparison is modular over a small window, and
the overlap retention in §5 means only two epochs are ever live at once, so wrap
is a non-event. At one rotation per election plus hourly, wrap is on the order of
years.
3. **On a commit that grows the tombstone set** — a `Remove*` committing. It
   reads the same commit-crossing signal `rank_leader` already computes for
   `StepDownRemoved`, so this is a narrow hook, not new consensus machinery. A
   **demote does not rotate**: the node stays in the cluster and must keep
   replicating. The removed node is excluded from distribution for free, because
   `rebuild_net_for_config` has already dropped it from the peer set.

## 6. Data flow and failure modes

**Send.** `assemble()` builds header+body into `scratch` as today; then
`seal_in_place(&mut scratch, kind)` selects the scope by kind, stamps the epoch
into the header, appends the counter, encrypts the payload in place, appends the
tag. The existing staging copy means sealing adds compute but **no extra copy**.

**Receive.** `recv_from` → read the cleartext header → resolve the key by
(kind → scope, source address → peer id, epoch) → replay-window check → open in
place → hand to the existing dispatch unchanged. Everything downstream of the
open sees exactly what it sees today.

Failure handling:

- **Auth failure, replay rejection, unknown epoch** — drop, bump a counter,
  rate-limited log. **Never panic, never fail-stop**: otherwise anyone who can
  reach the port kills a node by sending garbage. Counters surface in stats so an
  operator can tell an attack from a misconfiguration.
- **Unknown epoch** additionally self-heals via the NAK path once `HS_KEY`
  arrives.
- **Handshake failure** (unknown id, bad signature, allowlist miss) — drop,
  rate-limited warn, retry with backoff. The node stays up; an unauthorized peer
  simply never establishes.
- **Missing or unreadable key files at boot** — refuse to start, following M7's
  self-tombstone boot-refusal precedent. A node that cannot authenticate must
  not quietly run cleartext.
- **Mixed-mode cluster** (the likeliest operator error under flag day) — the
  crypto-enabled side emits a *specific* diagnostic ("peer appears to be running
  with crypto disabled") rather than a generic auth failure. Honest limitation:
  the cleartext side sees only malformed frames and cannot know better. Accepted
  because rollout is flag-day and no deployed cluster exists; recorded in the
  gate doc rather than papered over.

## 7. Accepted residual risks

Stated, not hidden:

1. **Metadata leaks to a wire observer.** Cleartext headers expose log positions
   and rates, leadership term numbers, and message kinds — enough to infer commit
   throughput, election activity, and when membership changes. Payload padding
   and header encryption were both considered and rejected (§1); revisit only if
   a deployment names traffic analysis as a threat.
2. **A removed node retains decryption ability until the next rotation.**
   Bounded by the rotation triggers in §5, not by the removal event itself.
   Removing a node's public key from the allowlist stops future handshakes, not
   passive decryption of captured traffic under a key it already holds.
3. **Any group-key holder can forge fan-out traffic as any other node**, because
   the group key is symmetric and shared. Per-node unforgeability holds only for
   pairwise-sealed kinds. This is inherent to seal-once-send-to-many and is what
   ATS's stream key accepts too; the alternative is N seals per fan-out.
4. **No compromised-host story.** A host with the identity private key is a
   cluster member by definition.

## 8. Testing and gates

- **Unit** (`uc2_crypto`): seal/open round-trip; tamper detection across each
  byte class (header, counter, ciphertext, tag); replay-window edges; epoch
  overlap; KATs for derivation; a property test asserting nonce uniqueness
  across restart and rotation.
- **Sim** (`uc2_sim`): the handshake driver under loss/reorder/partition;
  rotation during a partition; a peer that misses an epoch recovering via NAK.
- **Fault injection**: extend `uc2_net::fault` to corrupt, replay, and reorder
  sealed datagrams — assert no panic, no divergence, counters move.
- **Adversarial**: forged datagram under an unknown key rejected; replayed
  `VOTE` rejected; a peer dropped from the allowlist unable to re-establish;
  downgrade attempt rejected.
- **Existing capstones re-run with crypto ON**: `lin_v2`, `lin_partition_v2`,
  the multi-process crashtest, elle, and the M6/M7 gate smokes.
- **Throughput**: M5 harness A/B — cleartext control and encrypted arm
  back-to-back on the same box. **Bar: ≤10% regression**, decide rule
  pre-committed in the gate doc before the run.
- **Fleet gate**: separate, user-approved, cost-bearing step, on the M7
  protocol.
- Wire protocol bumped to **0.4.0**; `releases.md` entry; runbook section for
  key generation, allowlist management, and rotation policy.

## 9. Out of scope

- **The Lean and Veil models.** Wire crypto is not consensus safety — nothing
  here touches the commit or election plane, and pulling it into `proofs/` would
  dilute a trusted base that is currently about exactly one thing.
- **Remote admin surface** and the **async cross-region learner**. M8 removes
  their blocker; each is its own project.
- **Client/service IPC encryption** (§1, scope).
- **FIPS certification, HSM integration, CA/PKI.** The allowlist is the identity
  model.
