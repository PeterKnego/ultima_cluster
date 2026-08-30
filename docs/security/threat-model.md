# Threat model

What `ultima_cluster` defends, against whom, and — the more useful half —
what it does not defend and why. Written for someone deciding whether to run
it, and for a reviewer deciding where to spend their time.

Two documents sit beside this one: [attack surface](attack-surface.md)
enumerates every place the system parses bytes it did not write, and
[self-assessment](self-assessment.md) records what the maintainers found when
they looked. [`SECURITY.md`](/SECURITY.md) is how to report something.

**The single most important sentence:** wire crypto is **opt-in and off by
default**. With `[crypto].enabled = false` the posture is a trusted private
network — the same posture stock Aeron takes — and the decoders in
`uc_protocol::v2` are the *only* thing standing between an unauthenticated UDP
packet and the node's state. That is why they are fuzzed
([VERIFICATION §7](/docs/VERIFICATION.md#7-fuzzing--decoders-total-on-untrusted-bytes)).

---

## 1. Assets

| Asset | Why it matters | Where it lives |
|---|---|---|
| **The committed log bytes and their order** | Everything else is derived from it. A replica that applies different bytes, or the same bytes in a different order, has diverged — and SMR's whole promise is that it cannot. | The log buffer + `uc_journal` segments under the instance directory; replicated over UDP. |
| **The leader's identity** | Only the leader appends and only the leader answers a write. An adversary who can install themselves as leader, or convince a follower that they are one, owns the log. | `ElectionSm` term/vote state, `vote.state` and the term map (`StableValue`), the cnc page's node flags. |
| **The admin control plane** | Membership changes (add/promote/demote/remove) change *who counts in a quorum*. Two added members are a quorum takeover. | The cnc admin slot (`CNC_OFF_ADMIN_AUTH = 3904`, 64 bytes) and, on the forward path, wire kind 16. |
| **The operator's keys** | X25519 statics are the whole cluster-membership credential under Noise `IK`; the admin HMAC key is the whole admin credential. | Key files named by `[crypto]` / `[admin]` in `node.toml`; mode-checked at load. |
| **Audit log integrity** | It is the record of who changed the cluster. A record that can be silently removed is not a record. | `<instance_dir>/audit.jsonl`, append-only, `fsync` per record, written *before* the answer is published. |
| **Availability** | A panic on the receiver agent or the apply thread is a fail-stop: the process dies. Availability is what the fuzz tier actually defends. | Every polling agent; the apply thread in `uc_service`. |

## 2. Actors

- **A network-path adversary on the node↔node path.** Can read, inject,
  replay, reorder, corrupt and drop UDP datagrams. Does **not** hold a node's
  X25519 private key. This is the M8 threat model, and the only adversary wire
  crypto is designed against.
- **A remote client on the gateway TCP port.** Anyone who can open a socket to
  `uc2-gateway`. Unauthenticated by design in this release: there is no client
  credential, no TLS, and `app_id` is a wrong-cluster guard rather than a
  credential.
- **A local process on a node host, same uid.** Can attach to the shmem IPC
  (that is what a client and a service *are*), and can read and write every
  file in the instance directory — including the cnc page, the admin slot and
  the key files. Same uid is **inside** the trust boundary.
- **A local process on a node host, different uid.** Bounded by filesystem
  permissions: the instance directory and the key files. Key files — X25519
  identities and `[admin]` HMAC keys alike — are refused at startup if **any**
  group or other bit is set (`uc_crypto/src/admin.rs`'s
  `check_key_file_perms`: `mode & 0o077 != 0`, shared by
  `Identity::load`), which `uc_node/tests/daemon_refusals.rs` pins as a named
  refusal. The instance directory's own mode is the operator's.
- **A cluster member.** Holds a valid X25519 static, is in the allowlist, and
  holds the symmetric group key. **Out of model** — see §5.
- **The operator.** Trusted, and holds every credential. Their mistakes are
  addressed by named startup refusals, not by access control.

## 3. Trust boundaries

```text
    ── UNTRUSTED ────────────────────────────────────────────────────────

    remote client ──── plain TCP, no client auth ────┐
    (anyone who can                                  │  boundary A
     reach port 9200)                                │  (frame decoder,
                                                     │   MAX_FRAME_LEN,
                                                     ▼   credits)
    ══════════════════════════════ HOST ═════════════════════════════════
                                              ┌─────────────┐
                                              │ uc2-gateway │
                                              │   (Edge)    │
                                              └──────┬──────┘
                                                     │ shmem: ingress MPSC ring
    ┌──────────────┐   shmem: cnc page +   ┌─────────┴──────────┐
    │ uc2ctl       │──▶ admin slot 3904 ──▶│                    │
    │ (local, HMAC │   boundary C          │      uc2-node      │
    │  signed)     │   (signature, expiry, │  4 polling agents  │
    └──────────────┘    instance binding)  │                    │
                                           └───┬────────────┬───┘
    ┌──────────────┐  shmem: SPSC rings,       │            │
    │ uc_service  │◀─ log buffer, cnc ────────┘            │
    │ (apply)      │  boundary D (app_id /                  │
    └──────────────┘   instance_id / version,               │
                       flock, framing)                      │
    ═══════════════════════════════════════════════════════ │ ═══════════
                                                            │ boundary B
    ── UNTRUSTED ─────────────────────────────────────── UDP ▼ ──────────
                             crypto ON : Noise IK + AES-256-GCM (header AAD)
    peer uc2-node ◀────────▶ crypto OFF: cleartext, no source authentication
                                         — decoder totality is the only guard
```

- **A (client ⟶ gateway).** The only boundary an internet-reachable actor
  crosses. Guarded by frame-shape checks and resource caps, **not** by
  authentication.
- **B (node ⟷ node).** Guarded by wire crypto when it is enabled, and by
  nothing but decoder totality and a term check when it is not.
- **C (`uc2ctl` ⟶ cnc admin slot).** A local, filesystem-mediated boundary
  raised into a cryptographic one by M12b's HMAC signature.
- **D (service/client ⟷ node, shmem).** **Not a security boundary.** Same-uid
  local processes are inside the trust boundary; the checks there catch
  mistakes (wrong cluster, stale attach, torn record), not adversaries.

## 4. What is defended, by what

| Surface | With what | Notes |
|---|---|---|
| **Node↔node UDP, `[crypto].enabled = true`** | Noise `IK` handshake over an **allowlist** of X25519 static public keys; **AES-256-GCM** over the datagram envelope with the 16-byte header authenticated as **AAD**; **RFC-6479 anti-replay** window; a rotating **cluster group key** for the fan-out plane; per-peer pairwise keys for the rest. | Confidentiality + integrity + replay resistance against a network-path adversary. Header fields stay in the clear (§5, residual 1). Handshake kinds 18/19 are `Scope::Unsealed` by construction — they are what *creates* a session — so `Peers::on_message` is a genuinely pre-auth parser. |
| **Node↔node UDP, `[crypto].enabled = false`** | **Nothing authenticates the source.** `Receiver::on_datagram` checks the length, decodes the header, and routes; the consensus plane — kinds 5–11 **plus the two M7 admin-forward kinds 16 (`ConfigProposal`) and 17 (`ConfigReply`)**, per `is_consensus_kind` — is forwarded raw to the consensus agent with **no term filter at all** (a higher-term `RequestVote` *must* reach the state machine), and only the remaining kinds are filtered on `leadership_term_id`. The **only** defence is that every `uc_protocol::v2` decoder is total on `&[u8]` — fuzzed nightly. | The one exception is wire **kind 16** (`ConfigProposal`): `Node::on_config_proposal` drops a datagram whose source address resolves to no current member before any work runs. That is an address filter, not authentication — a spoofed source address passes it. **Kind 17 (`ConfigReply`) has no equivalent guard**: it bypasses the term filter with the rest of the consensus plane and is answered on the requesting node's own admin path. |
| **Admin operations** | **HMAC-SHA256** over a canonical message (`uc_crypto::admin::AdminMessage::canonical_bytes`), an `expiry_ns` bound on how long a signed request stays valid, `seq` monotonicity (`seq > last_admin_seq`), and a binding to `instance_id`/`app_id` taken from the node's **boot-time state**, never re-read from the writable cnc page. Every decision — accepted, refused, retried — is written to `audit.jsonl` and `fsync`ed **before** the answer is published. | `auth = "none"` is a legal, explicit choice and prints a warning on every boot. A follower forwards an authenticated request to the leader over the node-to-node socket, so cluster-wide authentication needs `[crypto].enabled = true` too (§6). |
| **IPC (client/service ⟷ node)** | `app_id` + `instance_id` + `protocol_version` checked at every attach; an exclusive `flock` on the instance directory (one node per directory, service and clients take a shared lock as a liveness probe); per-record atomic-after-write length prefix so a reader never sees a torn record. | Correctness and mistake-catching, **not** access control: a same-uid process is already inside. |
| **Gateway (remote clients)** | `MAX_FRAME_LEN = 1 MiB` refused at `decode_header` before any allocation; a per-connection **credit** window that the edge shrinks before it has to say `Backpressure`, with TCP backpressure behind it; `max_connections` (default 1024); a 1 s socket `WRITE_TIMEOUT` so one stalled client cannot pin the writer, plus `request_timeout` on any outstanding completion; `HELLO` before credits are granted. | Resource bounds against an over-eager or hostile client. There is no client authentication and no TLS — see §5. **These caps bound resources, not content:** a well-formed frame carrying a malformed *query body* reaches a typed state machine's bincode decode pre-commit and fail-stops the service (see the attack surface's typed-tier row). Nothing on this path stops that. |
| **Observability endpoint** | Bind address only. The request buffer is capped at 4096 bytes, only `GET` is answered, the status set is exactly `{200, 404, 503}`, and the parser is fuzzed (`uc_node_http`). | **Unauthenticated by design.** `packaging/node.example.toml` says to bind it to loopback or a private address. |
| **On-disk artifacts** | CRC per journal block; `StableValue`'s rotating two-slot write; decoders fuzzed (`uc_journal_record`, `uc_journal_stable_value`, `uc_protocol_cnc`). | **Integrity against corruption, not authenticity against an adversary.** Someone who can write the instance directory can write anything a CRC will accept. |

## 5. Out of model

Each of these is a deliberate boundary, not an oversight.

- **A compromised host.** A host holding the identity private key *is* a
  cluster member by definition. Under plain `IK` the key file is the whole
  credential.
- **A malicious cluster member.** The group key is symmetric, so **any holder
  can forge fan-out traffic as any node**. Per-node unforgeability holds only
  for pairwise-sealed kinds.
- **Traffic analysis.** Datagram headers are cleartext even with crypto on:
  positions, terms, kinds and rates are visible to an observer.
- **A removed node's captured traffic.** Removal stops future handshakes, not
  passive decryption under a group key the node already holds; that is bounded
  by the next rotation.
- **Client authentication on the remote link.** There is none, and there is no
  TLS: an M12 non-goal, stated in the spec ("no TLS on the remote client link
  in this release"). Anyone who can reach the gateway port can submit commands.
  Put it on a private network, or in front of a proxy that terminates
  authentication.
- **`app_id` is not a credential.** It is a wrong-cluster guard so a
  misdirected request reads as "wrong cluster" instead of a confusing
  mid-protocol error.
- **Denial of service beyond the stated caps.** The caps above (`MAX_FRAME_LEN`,
  credits, `max_connections`, the 4 KiB HTTP request buffer, the admission
  window at the ingress ring) bound memory per connection and per request. They
  do not make the system resistant to a determined flood, and nothing here
  rate-limits by source.
- **A stalled FSM is a cluster-scope liveness lever (2.8.0).** M14's report
  ceiling caps a node's durable report by its own FSMs' progress, so one
  stalled or slow FSM process on a *quorum* of hosts stalls commit
  cluster-wide — by design, so a lagging FSM never falls unrecoverably
  behind. In lockstep mode one stalled FSM also parks every sibling on its
  host. Same-uid processes are inside the trust boundary, so this is not a
  new actor; it is a new blast radius for a same-uid mistake (a wedged
  service, a squatted `service.<id>.lock`). The `Uc2ServiceAbsent` /
  `Uc2ServicePinnedAtLagBound` alerts are the detection; restarting the FSM
  is the remedy.
- **Side channels.** No constant-time claims beyond what the underlying
  primitives make; timing, cache and traffic-shape channels are not analysed.
- **Your state machine.** `apply` is your code. Nondeterminism in it diverges
  replicas, and a panic in it fail-stops every replica identically. Neither is
  something this layer can catch — see
  [VERIFICATION §11](/docs/VERIFICATION.md#11-what-is-not-verified).

## 6. Residuals stated elsewhere, and where

**The four accepted residuals of the wire-crypto design**, verbatim from
[`docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md`](/docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md)
§7 ("Accepted residual risks"), which is their canonical text:

> 1. **Metadata leaks to a wire observer.** Cleartext headers expose log positions
>    and rates, leadership term numbers, and message kinds — enough to infer commit
>    throughput, election activity, and when membership changes. Payload padding
>    and header encryption were both considered and rejected (§1); revisit only if
>    a deployment names traffic analysis as a threat.
> 2. **A removed node retains decryption ability until the next rotation.**
>    Bounded by the rotation triggers in §5, not by the removal event itself.
>    Removing a node's public key from the allowlist stops future handshakes, not
>    passive decryption of captured traffic under a key it already holds.
> 3. **Any group-key holder can forge fan-out traffic as any other node**, because
>    the group key is symmetric and shared. Per-node unforgeability holds only for
>    pairwise-sealed kinds. This is inherent to seal-once-send-to-many and is what
>    ATS's stream key accepts too; the alternative is N seals per fan-out.
> 4. **No compromised-host story.** A host with the identity private key is a
>    cluster member by definition. Under plain `IK` the key file **is** the whole
>    credential — there is no second factor — so file permissions and host
>    security carry that weight. `IKpsk2` (§5) is the designed-in upgrade if that
>    becomes unacceptable.

Also stated, in the documents that own them:

- **The kind-16 peer plane is trusted to `[crypto]`** —
  [Configuration § Admin authentication](/docs/reference/configuration.md#admin-authentication).
  A follower forwards an authenticated admin request to the leader as a
  `ConfigProposal` over the node-to-node UDP socket; the leader records which
  peer vouched (`peer:<id>`) and cannot re-verify the operator's signature.
  **`[admin] auth = "hmac"` only authenticates cluster-wide when paired with
  `[crypto].enabled = true`.**
- **The remote link is plain TCP** —
  [The remote protocol (v1)](/docs/reference/remote-protocol.md).
- **An unauthenticated remote client on a gateway can fail-stop a typed-tier
  service pre-commit with one malformed query frame** (the blanket impl's
  decode `.expect`); the raw tier is the workaround — [attack
  surface](attack-surface.md#1-the-inventory) (typed-tier row) and
  [self-assessment §3](self-assessment.md#3-known-weaknesses-not-fixed).
- **What is not verified at all** —
  [VERIFICATION §11](/docs/VERIFICATION.md#11-what-is-not-verified): the open
  `leader_completeness` theorem, the durable dual-reader model gap, and the IPC
  rings that Miri cannot check.

## 7. Reporting

[`SECURITY.md`](/SECURITY.md). Please do not open a public issue for a
suspected vulnerability.
