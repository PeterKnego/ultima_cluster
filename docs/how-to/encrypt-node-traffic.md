# How to encrypt traffic between nodes

Turn on authenticated, encrypted node-to-node UDP for a cluster that is
currently running in the clear.

**This is a flag day.** A cluster is either all-encrypted or all-cleartext.
There is no mixed mode and no permissive window: a node with crypto on drops
unsealed peer traffic, and a cleartext node cannot parse sealed traffic. Every
node flips together, in a coordinated restart.

Read the tooling gap below before scheduling this for production.

## Before you start: there is no key-generation CLI yet

Generating a private key is one command. Deriving the **public** half for the
allowlist currently requires code — the derivation lives in
`uc2_crypto::identity::Identity::public_bytes()` and is exercised by the gate
harness, but is not exposed as an operator command.

Until a keygen command exists, generate keys and allowlists programmatically,
as the tests and gate harness do. If you are planning a real deployment, treat
that command as a prerequisite rather than working around it by hand.

## Generate key material

One private key per node:

```bash
head -c 32 /dev/urandom > node-N.key   # any 32 bytes is a valid X25519 secret
chmod 600 node-N.key
```

The node refuses to start if any group or world permission bit is set, or if
the file is shorter than 32 bytes.

Then assemble one allowlist naming every node, and distribute it to all of
them. Each node additionally gets its own private key, and only its own.

```
1 <base64 X25519 public key>
2 <base64 X25519 public key>   # comments are allowed
3 <base64 X25519 public key>
```

Standard base64 with padding, one peer per line. Blank lines and `#` comments
are ignored. A node's own id may be present or absent.

For the file formats, see [Configuration](../reference/configuration.md#crypto-material).

## Flip the cluster

1. Distribute the allowlist to every node, and each private key to its own node.
2. Stop the cluster. A rolling flip runs split-brain until the last node flips,
   so a coordinated restart is the simpler and supported path.
3. Restart every node with `crypto: CryptoConfig::Enabled { key_path,
   allowlist_path, rotation: RotationPolicy::default() }`.
4. Confirm the leader minted a group epoch, and that the drop counters are
   quiet.

## Confirm it is healthy

Judge health by the **followers'** counters, not the leader's.

| Counter | Healthy value |
|---|---|
| `auth_failed` | 0 everywhere. Non-zero means tampering or a key mismatch. |
| `replay` | small under loss or reorder; sustained growth is an attack signal |
| `unknown_peer` | 0. Otherwise a missing allowlist entry or a stale peer set. |
| `unknown_epoch` | briefly non-zero after a rotation, then self-heals |
| `cleartext_peer` | 0. Non-zero means a peer is very likely still running cleartext — this is the mixed-cluster diagnostic. |
| `seal_failures` | **0 on followers.** On the leader it climbs continuously and is benign. |
| `hs_failures` | 0. Otherwise an unknown id, bad static key, or revoked peer. |

The leader's `seal_failures` climbing is expected and is not a fault: a node
reports its position to `cfg.leader`, which on the leader is itself, and a node
holds no session with itself. Do not alarm on it.

## Authorize a new node later

Drop the joiner's key line into every existing node's allowlist. No restart is
needed — the file is re-read on an mtime change roughly once a second, and
eagerly on a handshake from an unknown id.

Do this **before** running `add-learner`, or the joiner's traffic is refused
until the allowlist propagates.

## Key rotation

The cluster group key rotates on three triggers, whichever comes first:

- a node becoming leader
- a timer or byte budget, by default 1 hour or 1 TiB
- a committed removal, so a removed node cannot decrypt fresh fan-out traffic

A demotion does not rotate. A demoted node is still a member and must keep
replicating.

## What this changes on the wire

The wire protocol becomes 0.4.0 and each datagram grows by 24 bytes — an
8-byte counter and a 16-byte GCM tag — so the payload budget shrinks
accordingly.

The cnc page version is unchanged, so service and client IPC compatibility is
unaffected.

## Known interaction with reconfiguration

An unreachable member that is in the configuration but has no established
session — a learner added but never started, or one whose key never reached the
allowlist — used to cost every new leader a two-second window in which it could
neither replicate nor heartbeat. Fixed in `badd703`; if you are running an
older build and see leader churn during membership changes with crypto on, that
is the cause. The account is in
[The mute leader](../notes/uc2-the-mute-leader.md).
