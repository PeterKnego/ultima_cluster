# The remote protocol (v1)

The wire format `uc2_gateway`'s `Edge` speaks with remote clients, and that
`uc2_remote`'s `RemoteClient` implements. This is the page a non-Rust port
implements from — every layout below is byte-for-byte, taken from
`uc2_remote/src/frame.rs`.

Transport is a plain TCP stream, one frame after another, no length-prefixed
stream framing beyond each frame's own `len`. All multi-byte integers are
**little-endian**. A string field is a `u16` length prefix followed by that
many UTF-8 bytes (no NUL terminator).

## Header

Every frame starts with a fixed 24-byte header:

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | u32 | `len` | total frame length in bytes, header included (`HEADER_LEN + payload.len()`) |
| 4 | u8 | `type` | frame type, see below |
| 5 | u8 | `flags` | bitmask, see below |
| 6 | u16 | `version` | protocol version; currently `1` (`PROTOCOL_VERSION`) |
| 8 | u64 | `client_id` | the client's self-asserted, stable identity |
| 16 | u64 | `seq` | per-client monotonic sequence number |

`HEADER_LEN = 24`. `MAX_FRAME_LEN = 1 << 20` (1 MiB) — a frame whose declared
`len` exceeds this is refused at decode (`FrameError::TooLong`); the practical
ceiling on a command's own payload is far smaller (see "Payload ceiling"
below).

`client_id` is a client-chosen random `u64`, stable for the client's process
lifetime (or persisted, if you want the edge's session dedup to survive a
restart). `seq` starts at `1` and is monotonic per client; `0` is a legal
first `seq` (see `Sessioned`'s "gap = fresh" rule in
[the state-machine contract](state-machine-contract.md)).

## Frame types

| Value | Name | Direction | Meaning |
|---:|---|---|---|
| 1 | `HELLO` | client → edge | open the session: `app_id`, handshake |
| 2 | `HELLO_OK` | edge → client | handshake accepted: initial credits, current leader hint |
| 3 | `HELLO_REFUSED` | edge → client | handshake refused: reason + detail |
| 4 | `SUBMIT` | client → edge | a write; opaque command bytes |
| 5 | `QUERY` | client → edge | a read; opaque query bytes, `FLAG_LINEARIZABLE` selects the read mode |
| 6 | `RESPONSE` | edge → client | answers a `SUBMIT` or `QUERY` |
| 7 | `STATUS` | edge → client | standalone credit/liveness frame |
| 8 | `REDIRECT` | edge → client | this edge's node cannot serve writes; go here instead |
| 9 | `RETRY` | edge → client | a state signal, not a load signal (see below) |
| 10 | `UNKNOWN` | edge → client | the edge's engine timed the request's slot out |
| 11 | `LEADER_CHANGED` | edge → client | pushed proactively on a leader-watch transition |
| 12 | `PING` | client → edge | client liveness probe |
| 13 | `PONG` | edge → client | answers `PING` |

An edge that receives a frame type it does not expect on that connection
counts it and logs the first occurrence only (`Conn::logged_unexpected`); it
does not tear the connection down for that alone.

## Flags (`RESPONSE`, `QUERY`)

| Bit | Name | Set on | Meaning |
|---:|---|---|---|
| `0x01` | `FLAG_LINEARIZABLE` | `QUERY` | go through the node's linearizable read barrier; unset = snapshot read served locally |
| `0x02` | `FLAG_IS_QUERY` | `RESPONSE` | this response answers a `QUERY`, not a `SUBMIT` |
| `0x04` | `FLAG_REPLAYED` | `RESPONSE` | the `Sessioned` wrapper answered from its cache (`TAG_REPLAYED`) — the write did not re-apply |
| `0x08` | `FLAG_EXPIRED` | `RESPONSE` | the `Sessioned` wrapper could not classify this `seq` (`TAG_EXPIRED`) — outcome unknowable, no response bytes follow |
| `0x10` | `FLAG_ENVELOPED` | `RESPONSE` | the edge is running with `session_envelope = true` — `FLAG_REPLAYED`/`FLAG_EXPIRED` are meaningful only when this is set |

`FLAG_REPLAYED` and `FLAG_EXPIRED` are both lifted off the 1-byte
`Sessioned` tag (`TAG_FRESH = 0` sets neither flag). They are never set
together. With `session_envelope = false` (Aeron parity, dedup left to the
application), the edge never sets `FLAG_ENVELOPED`, `FLAG_REPLAYED`, or
`FLAG_EXPIRED`, and a re-sent write may apply twice — that is the documented
cost of turning the envelope off.

## Typed payloads

### `HELLO` (type 1)

`u16 len ++ app_id: UTF-8 bytes`.

### `HELLO_OK` (type 2)

| Offset | Size | Field |
|---|---|---|
| 0 | u32 | `credits` — initial credit grant |
| 4 | u32 | `leader` — cluster node id, or `u32::MAX` for "no leader hint" |
| 8 | u16 len + bytes | `leader_addr` — that node's gateway address, or empty |

### `HELLO_REFUSED` (type 3)

| Offset | Size | Field |
|---|---|---|
| 0 | u8 | `reason` |
| 1 | u16 len + bytes | `detail` — human-readable, not machine-parsed |

| Reason | Value | Meaning |
|---|---:|---|
| `HELLO_REFUSED_APP_ID` | 1 | wrong `app_id` — the client's problem; every member answers the same way |
| `HELLO_REFUSED_VERSION` | 2 | protocol version mismatch — the client's problem |
| `HELLO_REFUSED_FAULTED` | 3 | this **edge's** problem: its node's shmem instance restarted underneath it and it will never serve again; try a different member |
| `HELLO_REFUSED_BUSY` | 4 | this **edge's** problem, and a transient one: it is already serving `max_connections` clients. Written by the acceptor *before* the peer's `HELLO` is even read (so a client sees it as the answer to its dial), without spawning a reader thread; try a different member |

`FAULTED` and `BUSY` are the two reasons that cost one *member* rather than
the whole dial: a conforming client counts the member as out, skips it, and
carries on down its list — mid-life on an established connection as well as
at the handshake. `APP_ID`/`VERSION` are about the client, so no member would
answer differently and the connect attempt aborts outright.

### `SUBMIT` (type 4), `QUERY` (type 5)

No fixed payload struct — the payload is the opaque command/query bytes,
verbatim. The gateway never interprets them (this is what makes the raw
state-machine tier literally end-to-end). `QUERY` sets `FLAG_LINEARIZABLE` or
leaves it clear for a snapshot read.

### `RESPONSE` (type 6)

A 20-byte `ResponseMeta` header, then the response bytes (`out.len() -
ResponseMeta::LEN` of them; `FLAG_EXPIRED` responses carry zero):

| Offset | Size | Field |
|---|---|---|
| 0 | u32 | `credits` — current grant, piggybacked on every response |
| 4 | u64 | `acked_seq` — highest `seq` from this client the edge has answered |
| 12 | u64 | `position` — the log position the command applied at (`0` for a query) |
| 20 | — | response bytes |

`ResponseMeta::LEN = 20`.

### `STATUS` (type 7)

| Offset | Size | Field |
|---|---|---|
| 0 | u64 | `acked_seq` |
| 8 | u32 | `credits` |

`Status::LEN = 12`. Sent standalone (not piggybacked on a `RESPONSE`) in two
cases: a connection that has gone `status_interval` since its last write
(the idle-liveness tick), or immediately when credits widen back up after a
squeeze. Never sent before `HELLO_OK` is on the wire.

### `REDIRECT` / `LEADER_CHANGED` (types 8, 11)

Both share the `Leader` payload:

| Offset | Size | Field |
|---|---|---|
| 0 | u32 | `node_id` — cluster node id, or `u32::MAX` for "leader unknown" |
| 4 | u16 len + bytes | `addr` — that node's gateway address, empty if unknown |

`REDIRECT` answers one `SUBMIT` that this node cannot take (reactive).
`LEADER_CHANGED` is pushed unsolicited to every ready connection when the
edge's leader watch observes `(can_serve, leader_hint)` change (proactive).
The `u32::MAX` / empty-address sentinel is used in exactly one place by the
reference edge: `on_instance_restart` (see "Instance restart" below) — the
leader watch itself never announces an unresolvable transition (see
[the flow-control note](../notes/uc2-gateway-shapes-and-flow-control.md)).

### `RETRY` (type 9)

| Offset | Size | Field |
|---|---|---|
| 0 | u8 | `reason` |
| 1 | u32 | `retry_after_us` |

`Retry::LEN = 5`.

| Reason | Value | Meaning | Terminal? |
|---|---:|---|---|
| `RETRY_NOT_SERVING` | 1 | this node cannot serve writes and the edge has no leader hint (or the request landed on a connection already latched not-serving) | no — client reconnects elsewhere |
| `RETRY_INSTANCE_RESTART` | 2 | reserved for an edge implementation that wants to say "this node's identity changed" without dropping the connection outright | no |
| `RETRY_SERVICE_UNAVAILABLE` | 3 | the local `Engine` reported backpressure past the request's own timeout | no — same connection, after `retry_after_us` |
| `RETRY_PAYLOAD_TOO_LARGE` | 4 | the payload exceeds the node's `max_payload` | **yes** — the client must not resend; `retry_after_us` is always `0` |

**`RETRY` is a state signal, never a load signal.** It answers "the world
changed, do something different" (reconnect, wait, give up) — it is not a
generic "try again" a client can treat as a load-shedding hint tied to
volume. The reference edge (`uc2_gateway::Edge`) emits reasons 1, 3, and 4
today; it never emits `RETRY_INSTANCE_RESTART` — its own instance-restart
path (below) uses `LEADER_CHANGED{unknown}` plus a closed connection
instead. `RETRY_INSTANCE_RESTART` exists in the protocol for a different
edge implementation that prefers to signal the same fact without dropping
the connection; `RemoteClient` does not special-case it and treats it like
`RETRY_SERVICE_UNAVAILABLE` — same-connection backoff and resend.

`RETRY_NOT_SERVING` gets special client-side handling precisely because of
the **not-serving latch** (see the how-to and the flow-control note): once a
connection has been told `not_serving` for one `SUBMIT`, that connection is
refused for every later `SUBMIT` too, even if this node wins an election a
microsecond later. So `RemoteClient` does not resend on the same connection
— it reconnects, preferring the `leader` address from the most recent
`HELLO_OK`/`REDIRECT`/`LEADER_CHANGED` if that names somewhere else. (`STATUS`
is not in that list: it carries only `acked_seq` and `credits`, no leader
field.)

### `UNKNOWN` (type 10)

No payload. Sent when the edge's local `Engine` reports that a request's
slot timed out before a completion arrived — "may or may not have
committed." The client's default (`resend_on_unknown = true`) resends; with
the session envelope on, a resend of an already-applied write comes back
`FLAG_REPLAYED`, so the resend is what turns "unknown" into a definite
answer. With `resend_on_unknown = false`, `RemoteClient` surfaces
`RemoteError::Unknown` instead.

### `PING` / `PONG` (types 12, 13)

No payload on either. The client sends `PING` when it has written nothing
for `ping_interval` (default 1 s); the edge answers `PONG`. Liveness only —
neither carries credits or session state.

## Flow control — credits

**Credit rule:** the client may have at most `credits` unanswered `seq`s
beyond `acked_seq` — i.e. it may write `seq` only while `seq <= acked_seq +
credits`. `HELLO_OK` grants the initial value; every `RESPONSE` and `STATUS`
carries the current `(acked_seq, credits)` pair and can move either number.

The edge sizes credits from its local `Engine`'s inflight window
(`per_conn_inflight`, the ceiling every connection relaxes back towards) and
runs a **halve/double** scheme entirely on its own side — `RemoteClient`
never adjusts its own window, it only obeys whatever `credits` the edge last
sent. `Conn::squeeze` (`uc2_gateway/src/conn.rs`) **halves** the connection's
credits (floor `1`) the first time one request's retry loop hits
`SubmitError::Backpressure`; `Conn::relax` **doubles** them back on every
completion that arrives while squeezed, capped at `per_conn_inflight` — this
is multiplicative decrease *and* multiplicative increase, not AIMD (there is
no additive step on either side). A standalone `STATUS` is sent the moment a
`relax` widens the ceiling, so the client doesn't wait for the next
`RESPONSE` to notice. A client that ignores its credits is stopped by the
edge simply ceasing to read its socket — the TCP receive window filling is
the backstop, not the mechanism; no frame is ever accepted and then bounced
for capacity.

## Payload ceiling

A command's serialized bytes must fit in one UDP datagram on the node side.
The node's own startup preflight (`uc2_node::preflight`,
`PreflightError::PayloadExceedsMtu`) refuses a configured `max_payload` that
doesn't fit, with the arithmetic spelled out:

```
need = align_frame_len(max_payload + HEADER_LEN) + DATAGRAM_HEADER_LEN + crypto_overhead
refused if need > MTU_DEFAULT
```

— `HEADER_LEN = 32` and `FRAME_ALIGNMENT = 32` (`uc_protocol::v2::frame`,
`align_frame_len` rounds up to the next 32-byte multiple), `DATAGRAM_HEADER_LEN
= 16` and `MTU_DEFAULT = 1408` (`uc_protocol::v2::datagram`, not
operator-configurable), and `crypto_overhead` is `0` unless wire crypto (M8)
is enabled. Solving that with crypto off gives a hard ceiling of
**`max_payload <= 1344` bytes** (`1344 + 32 = 1376`, already 32-aligned;
`1376 + 16 = 1392 <= 1408`; one byte more pushes the aligned frame to `1408`,
for a `need` of `1424`) — the "roughly 1.3 KB" figure elsewhere on this page.
A `SUBMIT`/`QUERY` whose enveloped body exceeds the node's actual configured
`max_payload` (which may be set lower than this ceiling) is refused with
`RETRY{PAYLOAD_TOO_LARGE, retry_after_us: 0}` before it ever reaches the
ring — there is no chunking.

## The session envelope

With `session_envelope = true` (the gateway default), the edge prepends a
fixed 16-byte little-endian header — `client_id: u64` then `seq: u64` — to a
`SUBMIT`'s command bytes before handing them to `apply` (`SUBMIT` only;
`QUERY` is never enveloped). The inner state machine sees `Sessioned<S>`
wrapping its own; `Sessioned::apply` writes a 1-byte tag ahead of the real
response (`TAG_FRESH = 0` / `TAG_REPLAYED = 1` / `TAG_EXPIRED = 2`, no bytes
follow `TAG_EXPIRED`), and the edge lifts that tag off into
`FLAG_REPLAYED`/`FLAG_EXPIRED` on the `RESPONSE` frame rather than leaving it
in the payload. See [the state-machine contract](state-machine-contract.md)
for the full `Sessioned` semantics (window, eviction, snapshot composition)
— they are identical regardless of which tier (`RawStateMachine` or typed
`StateMachine`) the service implements.

## Failover promises (what a conforming client implements)

Every `SUBMIT`/`QUERY` a `RemoteClient`-shaped client issues ends in exactly
one of: a `RESPONSE`, or an error of `Expired` / `Unknown` /
`PayloadTooLarge` / `TimedOut` / `Closed`. `REDIRECT`, `LEADER_CHANGED`,
`RETRY`, and connection loss are **not** outcomes the caller sees — the
client absorbs them:

- **Probe-before-flush.** A freshly (re)connected client does not flush its
  whole pending window immediately. It writes exactly one request and waits
  for something only a willing-to-serve edge can answer (a `RESPONSE`, or a
  `STATUS` whose `acked_seq` covers that request) before writing the rest.
  This is what stops a client from dumping an entire in-flight window onto
  an edge that turns out to be a stale connection to a node that just lost
  the leadership — the probe costs one RTT, not the whole window.
- **Acts on `HELLO_OK`'s leader before flushing.** If `HELLO_OK` names a
  leader other than the edge that answered it, the client hops there first,
  so a pipelined window is flushed at the real leader instead of redirected
  frame by frame.
- **Follows `REDIRECT` and `LEADER_CHANGED`.** Both trigger a reconnect to
  the named address (or round-robin over `members` if unresolvable),
  re-`HELLO`, and a fresh credit grant.
- **`HELLO_REFUSED_FAULTED` and `HELLO_REFUSED_BUSY` move to the next
  member** — at the dial, and mid-life on an established connection alike;
  `APP_ID`/`VERSION` refusals abort the connect attempt outright — no other
  member would answer differently.
- **Ordered re-send.** Every unanswered `seq` is re-sent, in `seq` order, on
  the new connection. With the envelope on this is safe by construction
  (`fresh`/`replayed`/`expired` are all well-defined outcomes); with it off,
  a re-sent write is reported as possibly duplicated — the documented cost
  of running without the envelope, not a client bug.
- **`resend_on_unknown`** (default `true`) resends on `UNKNOWN` the same way;
  set `false` to get `RemoteError::Unknown` instead.
- **`Expired` is terminal** — the edge's session window no longer has this
  `seq`'s answer and it can never be recovered; the caller must decide what
  "outcome unknowable, too late to retry" means for its own application.
- **Liveness.** A `PING` goes out when nothing has been written for
  `ping_interval`; the client declares the connection dead and reconnects
  after `dead_after` with nothing received at all (a `PONG`, a `STATUS`, or
  any other frame all count as "received"). `dead_after` must exceed
  `ping_interval`, which `RemoteConfig::validate` enforces at
  `RemoteClient::connect` (along with a non-empty `app_id` and `members`, and
  a non-zero `max_inflight`) — a refusal there is `RemoteError::Config`,
  raised before any socket is opened.
- **`request_timeout` is enforced while reconnecting, not only while
  connected.** A pending request is failed with `TimedOut` no later than
  `request_timeout + connect_timeout + SWEEP_INTERVAL` (25 ms), *including*
  when the client is disconnected and scanning `members`. A conforming client
  therefore has to run its timeout sweep **between every dial attempt and
  every redirect hop** — not once per pass: one pass costs up to
  `(members + 1 + max_redirect_hops)` attempts, and two shapes starve a
  coarser sweep completely. Every dial *failing* leaves the reader sleeping
  its reconnect backoff (so that sleep is taken in sweep-sized slices), and
  every dial *succeeding* — an edge pair that redirects each request to the
  other, or one edge redirecting to an address that is down — leaves the
  reader in a reconnect churn that never returns to its idle tick at all.
  The `connect_timeout` term is the one attempt already in flight when the
  budget expires; a sweep cannot interrupt a blocking connect. Do **not**
  shorten a dial attempt's budget to what a pending request has left: under a
  steady stream of submits some request is always near expiry, the budget
  pins itself to its floor, and a healthy-but-slow cluster (a cross-region
  hop, or a gateway briefly saturated after a failover) becomes permanently
  unreachable. Two bounded caveats on the same invariant: a peer stalling
  mid-frame parks the reader for up to `dead_after` (next bullet), and a
  submitting thread's write holds the client's state lock for up to its write
  timeout. `RemoteClient::submit`'s credit wait is a *separate*
  `request_timeout` budget from the one the returned ticket spends, so a
  caller's worst case is about twice the configured value.
- **A peer that stalls mid-frame is a dead peer.** A frame that is still
  incomplete after the reader's stall budget (`dead_after` for
  `RemoteClient`, `request_timeout` for the reference edge) fails the
  connection rather than being waited on: half a header and then silence must
  reach the same verdict as silence between frames, or the reader's own tick
  never runs again.

## What this page does not cover

`uc2_gateway::Edge`'s side of failover (the not-serving latch, the leader
watch, head-of-line behaviour) is [the how-to](../how-to/run-a-gateway.md)
and [the flow-control note](../notes/uc2-gateway-shapes-and-flow-control.md).
The exactly-once `Sessioned<S>` wrapper's full semantics are
[the state-machine contract](state-machine-contract.md).
