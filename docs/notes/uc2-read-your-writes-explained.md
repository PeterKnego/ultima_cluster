# Read-your-writes reads, explained

*Unreleased: lands with the next cut. Spec:
[`2026-10-08-uc2-read-your-writes-design.md`](../superpowers/specs/2026-10-08-uc2-read-your-writes-design.md)
(read its Errata and As built blocks first).*

## The problem

UC has always had two ways to read:

- **Linearizable**: goes through the leader's read barrier. You see every
  write that completed before you asked. Costs a quorum round trip and only
  the leader serves it.
- **Snapshot**: answered from whatever the local node has applied so far.
  Cheap, and any node can answer, so read capacity scales with the number of
  nodes (see
  [`docs/benchmarks/uc2-read-spread-2026-10-07.md`](../benchmarks/uc2-read-spread-2026-10-07.md):
  2.74x read-only, 3.65x under write load). But a follower may not have applied
  the write you were just told succeeded, so you can read your own past.

**Read-your-writes** is the third mode. It reads on any node at close to
snapshot cost, and guarantees you see everything you have written or read
before, and never go backwards.

| mode | guarantee | where it runs |
|---|---|---|
| `Linearizable` | sees every write completed before the read began | leader only, quorum barrier |
| `Snapshot` | whatever this node has applied; may be stale, even stale against your own writes | any node, local |
| `ReadYourWrites` | sees your own writes and everything you have already read; monotonic | any node, local, after a short wait if the node is behind |

It is weaker than linearizable on purpose: it does not see other clients'
writes that completed after your last operation.

## How the token works

A **token** is a log position. "Answer only from state that has applied at
least up to this position." Each client keeps one, the maximum of everything
it has seen:

- A write's acknowledgement carries the position the write applied at. The
  client records that position plus one.
- A query's answer carries the position the answering node had applied. The
  client records it as is (this gives monotonic reads).

A read-your-writes query sends the token with the query. The node compares it
with its applied position and:

- already there: answers at once (the fast path, the same cost as a snapshot);
- bytes held but not yet applied: parks the read for up to one second and
  answers when it catches up;
- the token is ahead of anything the node has received: answers RETRY at once.

A token is the same on every row (state machine), because all rows walk one
log. A write to row 1 therefore gives you read-your-writes on row 0.

### Inside one client: automatic

```rust
let c = uc_client::Client::connect(/* ... */)?;
let _: u64 = c.submit(&1u64)?;                 // token advances
let v: u64 = c.query_read_your_writes(&())?;   // sees that write, on any node
```

The client also refuses to accept an answer older than the token it sent
(this guards against an applied position that briefly rewinds), re-asking
instead. `Client::connect` on a follower now works for snapshot and
read-your-writes reads; writes and linearizable reads still need the leader.

### Across processes: carry the token

When a request is handled by one process and the next by another (a web
frontend, say), pass the token along explicitly.

```rust
// process A, after a write
let cookie: String = client_a.read_token().to_string(); // 16 hex digits, e.g. "00000000000a3f10"
// ... send `cookie` to the browser, get it back on the next request ...

// process B
let t: uc_client::ReadToken = cookie.parse()?;
client_b.observe(t);                         // raises B's token, never lowers it
let v: u64 = client_b.query_read_your_writes(&())?;
// or, without touching B's own token:
let v: u64 = client_b.query_at_least_on(0, &(), t)?;
```

`ReadToken` is opaque: `NONE`, `from_u64`/`as_u64`, `Display`/`FromStr` in
the 16-hex-digit form. A token of 0 is a plain snapshot read. For remote
clients (`uc_remote`) the type is a separate `ReadToken` with the same
representation and text form; it crosses processes as text anyway.

## What RETRY means here

For a read-your-writes read, RETRY means **this node is behind your token**.
It does not mean the cluster is unhealthy. Either wait a moment and retry the
same node, or send the read to another node (the leader is always caught up
with your writes). Remote clients get this re-send automatically inside the
request timeout. Behind a multi-row lag barrier a slow sibling row can also
hold a row back past the one-second deadline; that is the barrier working.

## Version requirements

- **cnc 3.5** (`FLAG_V2_MIN_POSITION`). The cnc page version rises from 3.4 to
  3.5; a host's node, services and clients upgrade together. A 3.5 client
  attaching to an older page refuses a read-your-writes read with a non-zero
  token by name (`ReadYourWritesUnsupported`) rather than silently degrading.
  A token of 0 still works everywhere.
- **Remote protocol v2** (`FLAG_MIN_POSITION`). v1 clients and v2 gateways
  refuse each other at `HELLO` (`HELLO_REFUSED_VERSION`). A 3.5 gateway beside
  a pre-3.5 node answers the read with a transient RETRY: upgrade the node.
- The node-to-node wire does not change; there is no cluster flag day.

## Bad tokens

- **Forged or ahead-of-cluster tokens** (including `u64::MAX`, or a token kept
  from a cluster that was rebuilt from genesis so positions restarted):
  RETRY at once, one comparison, never parked. A restore from backup keeps
  positions and is unaffected. A token inside an uncommitted tail that is
  later truncated is never satisfied: RETRY at the deadline.
- **Floods**: parked reads are bounded (4096 per node) and organised per row
  so cost per pass does not grow with how many are parked; beyond the cap the
  node answers RETRY.
- Tokens are not authenticated or bound to a cluster identity. The type is
  opaque so that can be added later without an API change.

## See also

- [`smr-read-options-compared.md`](smr-read-options-compared.md): the
  comparison that motivated this. It lives on branch `bench/read-spread` and
  is not on `main` yet; its "not implemented" entry for this option is to be
  moved to implemented when it merges.
- [`uc2-read-barrier-explained.md`](uc2-read-barrier-explained.md): the
  linearizable mode.
- [`docs/reference/remote-protocol.md`](../reference/remote-protocol.md) and
  [`docs/reference/semver-policy.md`](../reference/semver-policy.md).
