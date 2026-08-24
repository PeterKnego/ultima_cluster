# Who may change the cluster

*What a signed admin request actually proves, why there is no replay ring, why
the identity the signature is bound to is read from the node's memory and never
from the file on disk — and the one plane where all of this stops holding.
Reference (keys, config, the exact signed bytes):
[configuration §Admin authentication](../reference/configuration.md#admin-authentication).
Walkthrough: [change cluster membership](../how-to/change-cluster-membership.md).*

## What the boundary was before

Nothing, exactly. `uc2ctl` changed cluster membership by writing a request line
into the node's cnc page — a file in the instance directory. The boundary was
filesystem permissions: whoever could write that directory could add a member,
promote a learner, or remove the leader. That is a defensible posture for a
process cluster on a trusted host, and it is still available (`[admin]
auth = "none"`, which prints a warning on every boot and never silences it).

It is not a defensible posture for the same tool run from an operator's laptop
across a shared filesystem, or for an audit question of the form "who removed
node 3 on Tuesday".

## What the tag covers

Every mutating admin request now carries `HMAC-SHA256(key, canonical_bytes)`
under a **named** key — the node's `[admin] keys` list maps names to 32-byte
key files (mode `0600`, or the daemon refuses to start naming the path). The
signed bytes are the whole request and its context:

```
app_id (length-prefixed) ‖ instance_id ‖ seq ‖ nonce ‖ op ‖ id ‖ ip ‖ port ‖ expiry_ns
```

Read that list as a set of claims. `op/id/ip/port` is *what change*.
`app_id` is *which cluster*. `instance_id` is *which boot of that node*. `seq`
is *where in this node's admin order*. `expiry_ns` is *for how long*. Change
any of them and the tag no longer verifies, which is the whole mechanism.

Refusals are named rather than generic: 20 `auth_missing`, 21 `auth_bad_tag`,
22 `auth_expired`, 23 `auth_unknown_key`, 24 `audit_failed`.

## Why there is no replay ring

The design sketch called for a `(seq, nonce)` ring to refuse replays. It was
not built, and the reason is that it would never have refused anything:

- The tag covers `seq`, and the consensus agent only ever acts on
  `seq > last_admin_seq`. A captured request cannot be re-presented at its
  original `seq` — that line is never read again. Re-presenting it at a higher
  `seq` changes the signed bytes, so the tag fails.
- Across a restart `last_admin_seq` resets to 0 — but a restart also
  re-randomizes `instance_id`, which the tag also covers. So the capture is
  bound to a boot that no longer exists.
- `expiry_ns` is what bounds the remaining case: a *live*, correctly-sequenced
  request that is delayed in flight and only then applied.

A ring would have been a second copy of the first two checks, with its own
persistence question. It is a deviation from the sketch, and it is written down
as one.

## The subtlety that made the second bullet true

That argument has a load-bearing assumption: that `instance_id` really does
change on every restart *from the verifier's point of view*. The first
implementation read it out of the cnc page per request — and the cnc page is a
file in the instance directory whose header is only magic-checked.

So an actor with directory write access and **no admin key at all** could:
capture a signed request, wait for (or cause) a restart, `pwrite` the captured
`instance_id` back into the page, re-present the captured bytes — and have the
membership change applied a second time, on a cluster whose whole premise was
that this actor could not make changes.

The fix is one word long: the identity the tag is verified against comes from
the node's own boot-time state (`Consensus::admin_instance_id` /
`admin_app_id`, set once in `Node::start_with`), never re-read from the page.
It is pinned by a regression test that performs exactly that forgery
(`uc2_node/tests/admin_auth.rs::a_capture_replayed_after_a_restart_is_refused`)
and, for anti-vacuity, was confirmed to *pass* the replay with the binding
reverted. Found in review, fixed before merge, never shipped.

## The audit log

Every admin request produces one JSON line in `<instance_dir>/audit.jsonl`
before its answer is published — accepted or refused, with the signing key's
name (or `actor="unverified"` for an unsigned one). Append-only, one `fsync`
per record. If the write or the `fsync` fails, the request is **refused**
(reason 24) rather than answered unrecorded: an answer this file does not hold
would make the file a liar. Read it offline with
`uc2ctl audit --instance-dir D [--tail N] [--json]`.

The cost is one `fsync` on the consensus thread per admin decision. Admin
operations are operator-rate — tens a year on a busy cluster — so this is paid
a few times a year and never inside `apply`, replication, or archive. One
carve-out keeps that true under abuse: a byte-identical re-send of an
already-answered proposal is served from the leader's dedup cache and counted,
not re-recorded, because it repeats an answer the file already holds.

## Where this stops holding

A follower cannot apply a membership change; it forwards the request to the
leader as a `ConfigProposal` datagram (**wire kind 16**) over the node-to-node
UDP socket, not the admin band. The leader cannot re-verify the operator's
signature there — the canonical message is bound to the *requesting* node's
identity — so what it records is which peer vouched for the change
(`peer:<id>`).

The leader does drop a kind-16 datagram whose source address resolves to no
current member, before any work runs. But an address filter is not
authentication: with `[crypto].enabled = false` a network-path adversary who
can spoof a member's UDP source address can inject a proposal onto that plane.

So the honest statement, which appears in the reference, both relevant how-tos
and the README rather than only here:

> **`[admin] auth = "hmac"` authenticates cluster-wide only when paired with
> `[crypto].enabled = true`.**

On a single-node cluster, or with every `uc2ctl` invocation aimed at the
leader, the kind-16 plane is never used and the residual does not arise. It is
stated as a residual rather than closed because closing it means putting an
operator-signed envelope on a peer-to-peer datagram, which is a wire-protocol
change — and `v2.6.0` deliberately does not make one.
